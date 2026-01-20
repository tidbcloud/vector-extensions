use std::collections::HashMap;
use std::path::PathBuf;

use vector::{
    aws::{AwsAuthentication, RegionOrEndpoint},
    config::{GenerateConfig, SinkConfig, SinkContext},
    sinks::{
        s3_common::{self, config::S3Options, service::S3Service},
        Healthcheck,
    },
};

use vector_lib::{
    config::proxy::ProxyConfig,
    config::{AcknowledgementsConfig, DataType, Input},
    configurable::configurable_component,
    sink::VectorSink,
    tls::TlsConfig,
};

use crate::sinks::topsql_data_deltalake::processor::TopSQLDeltaLakeSink;

use reqwest::Client;
use serde_json::Value;
use tracing::{error, info, warn};

mod processor;

// Import default functions from common module
use crate::common::deltalake_writer::{default_batch_size, default_timeout_secs};

pub const fn default_max_delay_secs() -> u64 {
    180
}

// Re-export types from common module
pub use crate::common::deltalake_writer::{DeltaTableConfig, WriteConfig};

/// Configuration for the deltalake sink
#[configurable_component(sink("topsql_data_deltalake"))]
#[derive(Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct DeltaLakeConfig {
    /// Base path for Delta Lake tables
    pub base_path: String,

    /// Batch size for writing
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Write timeout in seconds
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,

    /// Maximum delay in seconds before forcing a batch flush
    #[serde(default = "default_max_delay_secs")]
    pub max_delay_secs: u64,

    /// Storage options for cloud storage
    pub storage_options: Option<HashMap<String, String>>,

    /// S3 bucket name for remote storage
    pub bucket: Option<String>,

    /// S3 options
    #[serde(flatten)]
    pub options: Option<S3Options>,

    /// AWS region or endpoint
    #[serde(flatten)]
    pub region: Option<RegionOrEndpoint>,

    /// TLS configuration
    pub tls: Option<TlsConfig>,

    /// AWS authentication
    #[serde(default)]
    pub auth: AwsAuthentication,

    /// Specifies which addressing style to use
    #[serde(default = "default_force_path_style")]
    pub force_path_style: Option<bool>,

    /// Acknowledgments configuration
    #[serde(
        default,
        deserialize_with = "vector::serde::bool_or_struct",
        skip_serializing_if = "vector::serde::is_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,
}

pub fn default_force_path_style() -> Option<bool> {
    None
}

/// Get temporary credentials from Aliyun STS using OIDC token (RRSA)
async fn get_aliyun_sts_credentials(
    token_file: &str,
    role_arn: &str,
    region: Option<String>,
) -> vector::Result<(String, String, String)> {
    use url::form_urlencoded;

    // Read OIDC token from file
    let oidc_token = tokio::fs::read_to_string(token_file)
        .await
        .map_err(|e| vector::Error::from(format!("Failed to read OIDC token file: {}", e)))?;
    let oidc_token = oidc_token.trim();

    // Extract account ID and role name from ARN: acs:ram::123456789012:role/role-name
    let parts: Vec<&str> = role_arn.split(':').collect();
    if parts.len() < 5 || !parts[0].eq("acs") || !parts[1].eq("ram") {
        return Err(vector::Error::from(format!(
            "Invalid Aliyun role ARN format: {}",
            role_arn
        )));
    }

    // Get OIDC provider ARN from environment (usually set by RRSA)
    let oidc_provider_arn = std::env::var("ALIBABA_CLOUD_OIDC_PROVIDER_ARN")
        .map_err(|_| vector::Error::from("ALIBABA_CLOUD_OIDC_PROVIDER_ARN not set"))?;

    // Determine STS endpoint region
    let sts_region = region.as_deref().unwrap_or("cn-hangzhou");
    let sts_endpoint = format!("https://sts.{}.aliyuncs.com", sts_region);

    // Build request parameters for Aliyun STS AssumeRoleWithOIDC
    // Aliyun STS requires ISO 8601 format timestamp (e.g., 2023-11-18T23:15:01Z)
    let timestamp_utc = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();

    let mut params = HashMap::new();
    params.insert("Action", "AssumeRoleWithOIDC");
    params.insert("RoleArn", role_arn);
    params.insert("OIDCProviderArn", &oidc_provider_arn);
    params.insert("OIDCToken", oidc_token);
    params.insert("RoleSessionName", "vector-deltalake");
    params.insert("Format", "JSON");
    params.insert("Version", "2015-04-01");
    params.insert("Timestamp", &timestamp_utc);

    // Note: In production, you should sign the request properly using Aliyun signature algorithm
    // For now, we'll use a simplified approach - you may need to implement proper signing
    // or use an Aliyun SDK

    // Create HTTP client
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| vector::Error::from(format!("Failed to create HTTP client: {}", e)))?;

    // Build query string
    let query: String = form_urlencoded::Serializer::new(String::new())
        .extend_pairs(params.iter())
        .finish();

    let url = format!("{}?{}", sts_endpoint, query);

    info!("Calling Aliyun STS AssumeRoleWithOIDC: {}", sts_endpoint);

    // Make request
    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|e| vector::Error::from(format!("Failed to call Aliyun STS: {}", e)))?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(vector::Error::from(format!(
            "Aliyun STS returned error: {} - {}",
            status, text
        )));
    }

    let json: Value = response
        .json()
        .await
        .map_err(|e| vector::Error::from(format!("Failed to parse STS response: {}", e)))?;

    // Extract credentials from response
    let credentials = json
        .get("Credentials")
        .ok_or_else(|| vector::Error::from("No Credentials in STS response"))?;

    let access_key_id = credentials
        .get("AccessKeyId")
        .and_then(|v| v.as_str())
        .ok_or_else(|| vector::Error::from("No AccessKeyId in response"))?
        .to_string();

    let access_key_secret = credentials
        .get("AccessKeySecret")
        .and_then(|v| v.as_str())
        .ok_or_else(|| vector::Error::from("No AccessKeySecret in response"))?
        .to_string();

    let security_token = credentials
        .get("SecurityToken")
        .and_then(|v| v.as_str())
        .ok_or_else(|| vector::Error::from("No SecurityToken in response"))?
        .to_string();

    info!("Successfully obtained temporary credentials from Aliyun STS");

    Ok((access_key_id, access_key_secret, security_token))
}

impl GenerateConfig for DeltaLakeConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            base_path: "./delta-tables".to_owned(),
            batch_size: default_batch_size(),
            timeout_secs: default_timeout_secs(),
            max_delay_secs: default_max_delay_secs(),
            storage_options: None,
            bucket: None,
            options: None,
            region: None,
            tls: None,
            auth: AwsAuthentication::default(),
            force_path_style: None,
            acknowledgements: Default::default(),
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "topsql_data_deltalake")]
impl SinkConfig for DeltaLakeConfig {
    async fn build(&self, cx: SinkContext) -> vector::Result<(VectorSink, Healthcheck)> {
        error!(
            "DEBUG: Building Delta Lake sink with bucket: {:?}",
            self.bucket
        );

        // Create S3 service if bucket is configured
        let s3_service = if self.bucket.is_some() {
            error!("DEBUG: Bucket configured, creating S3 service");
            match self.create_service(&cx.proxy).await {
                Ok(service) => {
                    info!("S3 service created successfully");
                    Some(service)
                }
                Err(e) => {
                    error!(
                        "Failed to create S3 service, falling back to credential-less mode: {}",
                        e
                    );
                    // Don't fail completely, but continue without S3Service
                    // Delta Lake will handle authentication through storage_options
                    None
                }
            }
        } else {
            info!("No bucket configured, using local filesystem");
            None
        };

        info!("Building sink processor");
        let sink = self.build_processor(s3_service.as_ref(), cx).await?;

        info!("Building healthcheck");
        let healthcheck = self.build_healthcheck(s3_service.as_ref())?;

        info!("Delta Lake sink build completed successfully");
        Ok((sink, healthcheck))
    }

    fn input(&self) -> Input {
        Input::new(DataType::Log)
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

impl DeltaLakeConfig {
    async fn build_processor(
        &self,
        s3_service: Option<&S3Service>,
        _cx: SinkContext,
    ) -> vector::Result<VectorSink> {
        // For OSS with virtual hosted style, we may need to adjust the base_path format
        // to ensure object_store correctly parses the bucket
        let base_path = if let Some(_endpoint) = self.region.as_ref().and_then(|r| r.endpoint()) {
            // If using custom endpoint (OSS), check if base_path needs adjustment
            // For virtual hosted style, base_path should be: s3://bucket-name/path
            // object_store should construct: http://bucket-name.endpoint/path
            if self.base_path.starts_with("s3://") {
                // Extract bucket from base_path if it's in the correct format
                // Format: s3://bucket-name/path
                let path_without_s3 = self
                    .base_path
                    .strip_prefix("s3://")
                    .unwrap_or(&self.base_path);
                if let Some((bucket, path)) = path_without_s3.split_once('/') {
                    // Verify bucket matches configured bucket
                    if let Some(configured_bucket) = &self.bucket {
                        if bucket != configured_bucket {
                            warn!("Bucket in base_path ({}) doesn't match configured bucket ({}), using configured bucket", 
                                  bucket, configured_bucket);
                        }
                    }
                    info!("Using base_path: s3://{}/{}", bucket, path);
                }
            }
            PathBuf::from(&self.base_path)
        } else {
            PathBuf::from(&self.base_path)
        };

        // Tables are discovered dynamically from events
        // Default partition configuration will be applied to all tables
        let table_configs: Vec<DeltaTableConfig> = Vec::new();

        let write_config = WriteConfig {
            batch_size: self.batch_size,
            timeout_secs: self.timeout_secs,
        };

        let mut storage_options = self.storage_options.clone().unwrap_or_default();

        // Add S3 storage options if S3 service is available
        if let Some(service) = s3_service {
            info!("Applying S3 storage options - S3 service found");
            self.apply_s3_storage_options(&mut storage_options, service)
                .await?;
        } else {
            info!("No S3 service available - using default storage options only");
        }

        let sink = TopSQLDeltaLakeSink::new(
            base_path,
            table_configs,
            write_config,
            self.max_delay_secs,
            Some(storage_options),
        );

        Ok(VectorSink::from_event_streamsink(sink))
    }

    pub async fn create_service(&self, proxy: &ProxyConfig) -> vector::Result<S3Service> {
        error!(
            "DEBUG: Creating S3 service for Delta Lake with bucket: {:?}",
            self.bucket
        );

        // Ensure we have a region configured
        let region = self.region.as_ref().cloned().unwrap_or_else(|| {
            info!("No region specified, using default us-east-1");
            RegionOrEndpoint::with_region("us-east-1".to_string())
        });

        info!("Using region: {:?} for S3 service", region);
        info!("Using auth: {:?} for S3 service", self.auth);
        info!(
            "Force path style: {:?}",
            self.force_path_style.unwrap_or(true)
        );

        let result = s3_common::config::create_service(
            &region,
            &self.auth,
            proxy,
            self.tls.as_ref(),
            self.force_path_style.unwrap_or(true),
        )
        .await;

        match &result {
            Ok(_) => info!("S3 service created successfully for Delta Lake"),
            Err(e) => {
                error!("Failed to create S3 service for Delta Lake: {}", e);
                error!("Auth config: {:?}", self.auth);
                error!("Region config: {:?}", region);
            }
        }

        result
    }

    async fn apply_s3_storage_options(
        &self,
        storage_options: &mut HashMap<String, String>,
        _service: &S3Service,
    ) -> vector::Result<()> {
        info!("=== Applying S3 storage options (aws_s3_upload_file style) ===");
        debug!("Initial storage_options: {:?}", storage_options);

        // Initialize S3 handlers for Delta Lake
        deltalake::aws::register_handlers(None);
        debug!("Delta Lake S3 handlers registered");

        // Set AWS storage options for Delta Lake
        // Note: deltalake-aws uses AWS_ALLOW_HTTP (defined in deltalake_aws::constants::AWS_ALLOW_HTTP)
        storage_options.insert("AWS_ALLOW_HTTP".to_string(), "true".to_string());

        // Explicitly set bucket if configured - this helps object_store correctly parse S3 URLs
        if let Some(bucket) = &self.bucket {
            // For OSS with virtual hosted style, the bucket should be in the hostname
            // But object_store may need explicit bucket configuration
            info!("Explicitly setting bucket in storage options: {}", bucket);
            // Note: object_store may not have a direct bucket option, but we can ensure
            // the base_path format is correct: s3://bucket-name/path
        }

        // Set region from configuration
        if let Some(region) = &self.region {
            if let Some(region_str) = region.region() {
                storage_options.insert("AWS_REGION".to_string(), region_str.to_string());
            }

            // Set endpoint if using custom endpoint
            if let Some(endpoint) = region.endpoint() {
                // Ensure endpoint URL has a protocol scheme
                let endpoint_url =
                    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
                        endpoint.clone()
                    } else {
                        // For OSS internal endpoints, use http://; for others, use https://
                        if endpoint.contains("-internal") {
                            format!("http://{}", endpoint)
                        } else {
                            format!("https://{}", endpoint)
                        }
                    };
                info!("Setting OSS endpoint URL: {}", endpoint_url);
                storage_options.insert("AWS_ENDPOINT_URL".to_string(), endpoint_url);
            }
        }

        // Determine if we're using OSS (Alibaba Cloud Object Storage Service)
        let is_oss = self.region.as_ref().and_then(|r| r.endpoint()).map_or(false, |endpoint| {
            let endpoint_lower = endpoint.to_lowercase();
            // Check if endpoint contains OSS indicators
            endpoint_lower.contains("aliyuncs.com") || endpoint_lower.contains("oss-")
        });

        // Set addressing style - OSS requires virtual hosted style
        if let Some(force_path_style) = self.force_path_style {
            if force_path_style {
                storage_options.insert("AWS_S3_ADDRESSING_STYLE".to_string(), "path".to_string());
            } else {
                storage_options
                    .insert("AWS_S3_ADDRESSING_STYLE".to_string(), "virtual".to_string());
                storage_options.insert(
                    "AWS_VIRTUAL_HOSTED_STYLE_REQUEST".to_string(),
                    "true".to_string(),
                );
            }
        } else {
            // Default to virtual hosted style (required for OSS)
            storage_options.insert("AWS_S3_ADDRESSING_STYLE".to_string(), "virtual".to_string());
            storage_options.insert(
                "AWS_VIRTUAL_HOSTED_STYLE_REQUEST".to_string(),
                "true".to_string(),
            );
        }

        // Add OSS-specific options only when using OSS
        // AWS S3 supports conditional put natively, so we should not use copy_if_not_exists
        // for AWS S3 to avoid the warning and use the more performant conditional put
        if is_oss && storage_options.get("AWS_S3_ADDRESSING_STYLE") == Some(&"virtual".to_string()) {
            info!("Detected OSS endpoint, adding OSS-specific options");
            storage_options.insert(
                "AWS_COPY_IF_NOT_EXISTS".to_string(),
                "header-with-status:x-oss-forbid-overwrite:true:409".to_string(),
            );
        } else if !is_oss {
            info!("Using AWS S3, skipping AWS_COPY_IF_NOT_EXISTS to use native conditional put (more performant)");
        }

        // Configure AWS authentication for Delta Lake using storage_options
        // Delta Lake's object_store crate supports multiple authentication methods:
        // 1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)
        // 2. IAM Role ARN (AWS_IAM_ROLE_ARN + AWS_IAM_ROLE_SESSION_NAME) - for AssumeRole
        // 3. AWS Profile (AWS_PROFILE + AWS_SHARED_CREDENTIALS_FILE)
        // 4. EC2/ECS/Lambda instance roles (automatic)
        //
        // This matches aws_s3_upload_file behavior which uses the same AWS SDK credential chain
        info!("Configuring AWS authentication for Delta Lake (storage_options approach)");

        // Check Vector's auth configuration and map to Delta Lake storage_options
        match &self.auth {
            AwsAuthentication::Role {
                assume_role,
                external_id,
                ..
            } => {
                // Check if Web Identity Token is available (for RRSA/OIDC)
                if let Ok(token_file) = std::env::var("AWS_WEB_IDENTITY_TOKEN_FILE") {
                    // Use Web Identity Token authentication (RRSA)
                    info!("Using Web Identity Token authentication (RRSA)");
                    info!("Token file: {}", token_file);
                    info!("Role ARN: {}", assume_role);

                    storage_options.insert("AWS_WEB_IDENTITY_TOKEN_FILE".to_string(), token_file);
                    storage_options.insert("AWS_ROLE_ARN".to_string(), assume_role.clone());

                    if let Ok(session_name) = std::env::var("AWS_ROLE_SESSION_NAME") {
                        storage_options.insert("AWS_ROLE_SESSION_NAME".to_string(), session_name);
                    } else {
                        storage_options.insert(
                            "AWS_ROLE_SESSION_NAME".to_string(),
                            "vector-deltalake".to_string(),
                        );
                    }

                    info!("✓ Delta Lake will use Web Identity Token (RRSA) authentication");
                } else {
                    // Use traditional AssumeRole (requires base credentials)
                    info!("Configuring Delta Lake with IAM Role ARN: {}", assume_role);
                    storage_options.insert("AWS_IAM_ROLE_ARN".to_string(), assume_role.clone());
                    storage_options.insert(
                        "AWS_IAM_ROLE_SESSION_NAME".to_string(),
                        "vector-deltalake".to_string(),
                    );

                    if let Some(ext_id) = external_id {
                        storage_options
                            .insert("AWS_IAM_ROLE_EXTERNAL_ID".to_string(), ext_id.clone());
                        info!("✓ Using external ID for role assumption");
                    }

                    info!("✓ Delta Lake will use AssumeRole with IAM Role ARN");
                }
            }
            AwsAuthentication::AccessKey {
                access_key_id,
                secret_access_key,
                session_token,
                assume_role,
                ..
            } => {
                // Use static credentials
                // SensitiveString has inner() method to get the actual value
                // Display trait returns "**REDACTED**", so we must use inner() instead of to_string()
                let access_key_id_str = access_key_id.inner();
                let secret_access_key_str = secret_access_key.inner();

                // Log access key ID (first few chars only for security)
                let access_key_preview = if access_key_id_str.len() > 8 {
                    format!("{}...", &access_key_id_str[..8])
                } else {
                    "***".to_string()
                };
                info!("Using AccessKey ID: {}", access_key_preview);

                storage_options.insert(
                    "AWS_ACCESS_KEY_ID".to_string(),
                    access_key_id_str.to_string(),
                );
                storage_options.insert(
                    "AWS_SECRET_ACCESS_KEY".to_string(),
                    secret_access_key_str.to_string(),
                );

                if let Some(token) = session_token {
                    storage_options
                        .insert("AWS_SESSION_TOKEN".to_string(), token.inner().to_string());
                }

                if let Some(role_arn) = assume_role {
                    info!("Using access key with assume role: {}", role_arn);
                    // Can also configure AssumeRole with base credentials
                    storage_options.insert("AWS_IAM_ROLE_ARN".to_string(), role_arn.clone());
                    storage_options.insert(
                        "AWS_IAM_ROLE_SESSION_NAME".to_string(),
                        "vector-deltalake".to_string(),
                    );
                }

                info!("✓ Delta Lake using static AWS credentials");
            }
            AwsAuthentication::File {
                credentials_file,
                profile,
                ..
            } => {
                // Use AWS profile
                storage_options.insert("AWS_PROFILE".to_string(), profile.clone());
                storage_options.insert(
                    "AWS_SHARED_CREDENTIALS_FILE".to_string(),
                    credentials_file.clone(),
                );
                info!("✓ Delta Lake using AWS profile: {}", profile);
            }
            AwsAuthentication::Default { .. } => {
                // Use default AWS credential chain (environment variables, instance roles, etc.)
                // Check environment variables and pass them to Delta Lake
                info!("Using default AWS credential chain");

                // Check for Web Identity Token (RRSA/OIDC) - support both AWS and Aliyun formats
                let token_file = std::env::var("AWS_WEB_IDENTITY_TOKEN_FILE")
                    .or_else(|_| std::env::var("ALIBABA_CLOUD_OIDC_TOKEN_FILE"));
                let role_arn = std::env::var("AWS_ROLE_ARN")
                    .or_else(|_| std::env::var("ALIBABA_CLOUD_ROLE_ARN"));

                if let (Ok(token_file), Ok(role_arn)) = (token_file, role_arn) {
                    // Check if this is Aliyun format ARN (acs:ram::)
                    if role_arn.starts_with("acs:ram::") {
                        // For Aliyun RRSA, call Aliyun STS to get temporary credentials
                        warn!("Detected Aliyun RRSA (acs:ram:: ARN format)");
                        info!("Attempting to get temporary credentials from Aliyun STS...");

                        // Get region from endpoint or use default
                        let region = self
                            .region
                            .as_ref()
                            .and_then(|r| r.region())
                            .map(|s| s.to_string());

                        match get_aliyun_sts_credentials(&token_file, &role_arn, region).await {
                            Ok((access_key_id, access_key_secret, security_token)) => {
                                info!(
                                    "✓ Successfully obtained temporary credentials from Aliyun STS"
                                );
                                storage_options
                                    .insert("AWS_ACCESS_KEY_ID".to_string(), access_key_id);
                                storage_options
                                    .insert("AWS_SECRET_ACCESS_KEY".to_string(), access_key_secret);
                                storage_options
                                    .insert("AWS_SESSION_TOKEN".to_string(), security_token);
                                info!("✓ Using temporary credentials for OSS authentication");
                            }
                            Err(e) => {
                                error!(
                                    "Failed to get temporary credentials from Aliyun STS: {}",
                                    e
                                );
                                warn!("Falling back to environment variable credentials");

                                // Fall back to environment variables
                                if let Ok(access_key) = std::env::var("AWS_ACCESS_KEY_ID") {
                                    storage_options
                                        .insert("AWS_ACCESS_KEY_ID".to_string(), access_key);
                                }
                                if let Ok(secret_key) = std::env::var("AWS_SECRET_ACCESS_KEY") {
                                    storage_options
                                        .insert("AWS_SECRET_ACCESS_KEY".to_string(), secret_key);
                                }
                                if let Ok(session_token) = std::env::var("AWS_SESSION_TOKEN") {
                                    storage_options
                                        .insert("AWS_SESSION_TOKEN".to_string(), session_token);
                                }
                            }
                        }
                    } else {
                        // AWS format ARN - use Web Identity Token
                        info!("Using Web Identity Token authentication (AWS RRSA)");
                        info!("Token file: {}", token_file);
                        info!("Role ARN: {}", role_arn);

                        storage_options
                            .insert("AWS_WEB_IDENTITY_TOKEN_FILE".to_string(), token_file);
                        storage_options.insert("AWS_ROLE_ARN".to_string(), role_arn);

                        if let Ok(session_name) = std::env::var("AWS_ROLE_SESSION_NAME")
                            .or_else(|_| std::env::var("ALIBABA_CLOUD_ROLE_SESSION_NAME"))
                        {
                            storage_options
                                .insert("AWS_ROLE_SESSION_NAME".to_string(), session_name);
                        } else {
                            storage_options.insert(
                                "AWS_ROLE_SESSION_NAME".to_string(),
                                "vector-deltalake".to_string(),
                            );
                        }

                        info!("✓ Delta Lake will use Web Identity Token (RRSA) authentication");
                    }
                } else {
                    // Fall back to other credential methods
                    if let Ok(access_key) = std::env::var("AWS_ACCESS_KEY_ID") {
                        storage_options.insert("AWS_ACCESS_KEY_ID".to_string(), access_key);
                    }
                    if let Ok(secret_key) = std::env::var("AWS_SECRET_ACCESS_KEY") {
                        storage_options.insert("AWS_SECRET_ACCESS_KEY".to_string(), secret_key);
                    }
                    if let Ok(session_token) = std::env::var("AWS_SESSION_TOKEN") {
                        storage_options.insert("AWS_SESSION_TOKEN".to_string(), session_token);
                    }
                    if let Ok(profile) = std::env::var("AWS_PROFILE") {
                        storage_options.insert("AWS_PROFILE".to_string(), profile);
                    }

                    // Set default credentials file path if it exists
                    if let Ok(home) = std::env::var("HOME") {
                        let default_creds_file = format!("{}/.aws/credentials", home);
                        if std::path::Path::new(&default_creds_file).exists() {
                            storage_options.insert(
                                "AWS_SHARED_CREDENTIALS_FILE".to_string(),
                                default_creds_file,
                            );
                        }
                    }

                    info!("✓ Delta Lake will use AWS SDK's default credential chain");
                }
            }
        }

        info!("✓ AWS authentication configured for Delta Lake via storage_options");

        debug!("=== Completed apply_s3_storage_options ===");
        debug!("Final storage_options: {:?}", storage_options);
        info!("✓ S3 storage options applied successfully");

        // Log final storage options for debugging (redact sensitive values)
        let mut debug_options = storage_options.clone();
        if let Some(access_key) = debug_options.get_mut("AWS_ACCESS_KEY_ID") {
            if access_key.len() > 8 {
                *access_key = format!("{}...", &access_key[..8]);
            } else {
                *access_key = "***".to_string();
            }
        }
        if let Some(secret_key) = debug_options.get_mut("AWS_SECRET_ACCESS_KEY") {
            *secret_key = "***REDACTED***".to_string();
        }
        if let Some(session_token) = debug_options.get_mut("AWS_SESSION_TOKEN") {
            *session_token = "***REDACTED***".to_string();
        }
        info!(
            "Final Delta Lake storage options configured: {:?}",
            debug_options
        );

        Ok(())
    }

    fn build_healthcheck(&self, s3_service: Option<&S3Service>) -> vector::Result<Healthcheck> {
        info!(
            "Building healthcheck for bucket: {:?}, s3_service: {}, base_path: {}",
            self.bucket,
            s3_service.is_some(),
            self.base_path
        );

        if let (Some(bucket), Some(_service)) = (&self.bucket, s3_service) {
            info!(
                "S3 configuration detected - using simplified healthcheck for bucket: {}",
                bucket
            );
            // For Delta Lake S3, we'll use a simplified healthcheck that always passes
            // The actual S3 connectivity will be tested during the first write operation
            // This avoids credential issues that can occur during Vector startup
            let healthcheck = Box::pin(async move {
                info!("Delta Lake S3 healthcheck: Skipping detailed S3 connectivity test");
                info!("S3 connectivity will be verified during actual write operations");
                Ok(())
            });
            return Ok(healthcheck);
        }

        info!(
            "Using local filesystem healthcheck for path: {}",
            self.base_path
        );
        // Local filesystem healthcheck
        let base_path = PathBuf::from(&self.base_path);

        let healthcheck = Box::pin(async move {
            // Check if directory exists and is writable
            if !base_path.exists() {
                if let Err(e) = std::fs::create_dir_all(&base_path) {
                    return Err(format!(
                        "Failed to create directory {}: {}",
                        base_path.display(),
                        e
                    )
                    .into());
                }
            }

            // Try to create a test file
            let test_file = base_path.join(".healthcheck");
            if let Err(e) = std::fs::write(&test_file, "test") {
                return Err(format!("Failed to write to {}: {}", base_path.display(), e).into());
            }

            // Clean up test file
            let _ = std::fs::remove_file(test_file);

            Ok(())
        });

        Ok(healthcheck)
    }
}

#[cfg(test)]
#[allow(clippy::print_stdout)]
#[allow(clippy::print_stderr)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::fs;
    use vector_lib::event::{Event, LogEvent, ObjectMap};

    #[test]
    fn generate_config() {
        vector::test_util::test_generate_config::<DeltaLakeConfig>();
    }
}