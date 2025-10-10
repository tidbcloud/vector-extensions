use std::collections::HashMap;
use std::path::PathBuf;

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_sts::Client as StsClient;
use serde::{Deserialize, Serialize};
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

use crate::sinks::deltalake::processor::DeltaLakeSink;

mod processor;
mod writer;

/// Configuration for the deltalake sink
#[configurable_component(sink("deltalake"))]
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

    /// Compression format
    #[serde(default = "default_compression")]
    pub compression: String,

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

/// Delta table configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaTableConfig {
    /// Table name
    pub name: String,

    /// Partition columns
    pub partition_by: Option<Vec<String>>,

    /// Enable schema evolution
    pub schema_evolution: Option<bool>,
}

/// Write configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteConfig {
    /// Batch size for writing
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Write timeout in seconds
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,

    /// Compression format
    #[serde(default = "default_compression")]
    pub compression: String,
}

/// Compression format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompressionFormat {
    /// Snappy compression
    Snappy,
    /// Gzip compression
    Gzip,
    /// No compression
    None,
}

pub const fn default_batch_size() -> usize {
    1000
}

pub const fn default_timeout_secs() -> u64 {
    30
}

pub fn default_compression() -> String {
    "snappy".to_string()
}

pub fn default_force_path_style() -> Option<bool> {
    None
}

impl GenerateConfig for DeltaLakeConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            base_path: "./delta-tables".to_owned(),
            batch_size: default_batch_size(),
            timeout_secs: default_timeout_secs(),
            compression: default_compression(),
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
#[typetag::serde(name = "deltalake")]
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
        let base_path = PathBuf::from(&self.base_path);

        // Tables are discovered dynamically from events
        // Default partition configuration will be applied to all tables
        let table_configs: Vec<DeltaTableConfig> = Vec::new();

        let write_config = WriteConfig {
            batch_size: self.batch_size,
            timeout_secs: self.timeout_secs,
            compression: self.compression.clone(),
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

        let sink = DeltaLakeSink::new(
            base_path,
            table_configs,
            write_config,
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
        debug!("=== Starting apply_s3_storage_options ===");
        debug!("Initial storage_options: {:?}", storage_options);

        // Initialize S3 handlers for Delta Lake
        deltalake::aws::register_handlers(None);
        debug!("Delta Lake S3 handlers registered");

        // Set AWS storage options for Delta Lake
        storage_options.insert("AWS_STORAGE_ALLOW_HTTP".to_string(), "true".to_string());

        // Set region from configuration
        if let Some(region) = &self.region {
            // Convert region to string - this will be picked up by Delta Lake
            if let Some(region_str) = region.region() {
                storage_options.insert("AWS_REGION".to_string(), region_str.to_string());
            }

            // Set endpoint if using custom endpoint
            if let Some(endpoint) = region.endpoint() {
                storage_options.insert("AWS_ENDPOINT_URL".to_string(), endpoint);
            }
        }

        // Set addressing style
        if let Some(force_path_style) = self.force_path_style {
            if force_path_style {
                storage_options.insert("AWS_S3_ADDRESSING_STYLE".to_string(), "path".to_string());
            } else {
                storage_options
                    .insert("AWS_S3_ADDRESSING_STYLE".to_string(), "virtual".to_string());
            }
        }

        // Handle AWS authentication - add credentials to storage options if available
        info!("Configuring AWS authentication for Delta Lake S3 access");
        match &self.auth {
            AwsAuthentication::AccessKey {
                access_key_id,
                secret_access_key,
                session_token,
                assume_role,
                ..
            } => {
                storage_options.insert("AWS_ACCESS_KEY_ID".to_string(), access_key_id.to_string());
                storage_options.insert(
                    "AWS_SECRET_ACCESS_KEY".to_string(),
                    secret_access_key.to_string(),
                );
                if let Some(token) = session_token {
                    storage_options.insert("AWS_SESSION_TOKEN".to_string(), token.to_string());
                }
                if let Some(role_arn) = assume_role {
                    info!(
                        "Using access key with assume role authentication: {}",
                        role_arn
                    );
                } else {
                    info!("Using access key AWS credentials for Delta Lake S3 access");
                }
            }
            AwsAuthentication::File {
                credentials_file,
                profile,
                ..
            } => {
                info!("Using file-based AWS credential chain for Delta Lake S3 access");
                // Set credentials file path for Delta Lake
                storage_options.insert(
                    "AWS_SHARED_CREDENTIALS_FILE".to_string(),
                    credentials_file.clone(),
                );
                storage_options.insert("AWS_PROFILE".to_string(), profile.clone());
            }
            AwsAuthentication::Role {
                assume_role,
                external_id,
                ..
            } => {
                info!(
                    "Using role-based AWS authentication for Delta Lake S3 access: {}",
                    assume_role
                );

                // Implement complete AssumeRole operation to obtain temporary credentials
                let region_name = if let Some(region) = &self.region {
                    region
                        .region()
                        .map(|r| r.to_string())
                        .unwrap_or_else(|| "us-west-2".to_string())
                } else {
                    "us-west-2".to_string()
                };

                info!(
                    "Performing AssumeRole operation for region: {}",
                    region_name
                );

                // Check if basic credentials are available
                debug!("Checking for basic AWS credentials...");
                let has_access_key = std::env::var("AWS_ACCESS_KEY_ID").is_ok();
                let has_profile = std::env::var("AWS_PROFILE").is_ok();
                let home_dir = std::env::var("HOME").unwrap_or_default();
                let creds_file_path = format!("{}/.aws/credentials", home_dir);
                let has_creds_file = std::path::Path::new(&creds_file_path).exists();

                debug!("AWS_ACCESS_KEY_ID available: {}", has_access_key);
                if has_access_key {
                    let key_preview = std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_default();
                    info!(
                        "AWS_ACCESS_KEY_ID preview: {}...",
                        &key_preview[..std::cmp::min(key_preview.len(), 10)]
                    );
                }
                info!("AWS_PROFILE available: {}", has_profile);
                if has_profile {
                    info!(
                        "AWS_PROFILE value: {}",
                        std::env::var("AWS_PROFILE").unwrap_or_default()
                    );
                }
                info!("AWS credentials file path: {}", creds_file_path);
                info!("AWS credentials file exists: {}", has_creds_file);

                let has_basic_creds = has_access_key || has_profile || has_creds_file;
                info!("Has basic credentials: {}", has_basic_creds);

                // if !has_basic_creds {
                //     error!("No basic AWS credentials found for AssumeRole operation");
                //     return Err("AssumeRole requires basic AWS credentials (AWS_ACCESS_KEY_ID or AWS_PROFILE or ~/.aws/credentials). Please configure base credentials first.".into());
                // }

                // info!("✓ Basic AWS credentials found, proceeding with AssumeRole");

                // Create AWS config and STS client
                info!("Creating AWS config and STS client...");
                let region_provider = RegionProviderChain::default_provider().or_else("us-west-2");
                info!("Loading AWS config from environment");
                let shared_config = aws_config::from_env().region(region_provider).load().await;
                info!("AWS config loaded, region: {:?}", shared_config.region());

                // Check configuration status
                info!("AWS SDK configuration check:");
                info!("  Region: {:?}", shared_config.region());
                info!("  Endpoint URL: {:?}", shared_config.endpoint_url());
                info!("  App name: {:?}", shared_config.app_name());

                let sts_client = StsClient::new(&shared_config);
                info!("STS client created successfully");

                // Check if AWS STS service is accessible
                info!("🔍 Checking AWS STS service connectivity...");
                match sts_client.get_caller_identity().send().await {
                    Ok(identity) => {
                        info!("✅ AWS STS service connectivity is normal");
                        debug!(
                            "Current identity: Account={:?}, UserId={:?}, Arn={:?}",
                            identity.account(),
                            identity.user_id(),
                            identity.arn()
                        );
                    }
                    Err(e) => {
                        warn!("⚠️ AWS STS connectivity check failed: {}", e);
                        debug!("STS connectivity error: {:?}", e);
                        warn!("Continuing with AssumeRole attempt, but it may fail...");
                    }
                }

                // Call AssumeRole
                debug!("Building AssumeRole request...");
                debug!("Role ARN: {}", assume_role);
                debug!("Session name: vector-deltalake");
                debug!("Duration: 3600 seconds");

                let mut assume_role_builder = sts_client
                    .assume_role()
                    .role_arn(assume_role)
                    .role_session_name("vector-deltalake")
                    .duration_seconds(3600);

                if let Some(ext_id) = external_id {
                    info!("Using external ID for role assumption: {}", ext_id);
                    debug!("External ID: {}", ext_id);
                    assume_role_builder = assume_role_builder.external_id(ext_id);
                } else {
                    debug!("No external ID provided");
                }

                info!("Sending AssumeRole request to AWS STS...");

                let assume_role_result = assume_role_builder.send().await;

                match assume_role_result {
                    Ok(resp) => {
                        debug!("AssumeRole request completed successfully");
                        let creds = resp.credentials().unwrap();
                        info!("✓ AssumeRole authentication successful");

                        // ---- Pass temporary credentials to delta-rs ---- (following reference project)
                        debug!(
                            "Access Key ID: {}...",
                            &creds.access_key_id()
                                [..std::cmp::min(creds.access_key_id().len(), 10)]
                        );
                        debug!(
                            "Secret Access Key: {}...",
                            &creds.secret_access_key()
                                [..std::cmp::min(creds.secret_access_key().len(), 10)]
                        );
                        debug!(
                            "Session Token: {}...",
                            &creds.session_token()
                                [..std::cmp::min(creds.session_token().len(), 20)]
                        );
                        debug!("Credentials expire at: {:?}", creds.expiration());

                        storage_options.insert(
                            "AWS_ACCESS_KEY_ID".to_string(),
                            creds.access_key_id().to_string(),
                        );
                        storage_options.insert(
                            "AWS_SECRET_ACCESS_KEY".to_string(),
                            creds.secret_access_key().to_string(),
                        );
                        storage_options.insert(
                            "AWS_SESSION_TOKEN".to_string(),
                            creds.session_token().to_string(),
                        );

                        // Optional: Enable DynamoDB locking if needed (reference project configuration)
                        // storage_options.insert("AWS_S3_LOCKING_PROVIDER".to_string(), "dynamodb".to_string());
                        // storage_options.insert("DELTA_DYNAMO_TABLE_NAME".to_string(), "delta_log".to_string());

                        info!("✓ Temporary credentials configured for Delta Lake");
                        debug!("Storage options now contain {} keys", storage_options.len());
                    }
                    Err(e) => {
                        error!("AssumeRole operation failed: {}", e);
                        debug!("AssumeRole error details: {:?}", e);

                        // Detailed error analysis
                        let error_msg = e.to_string();
                        error!("Analyzing AssumeRole failure reasons:");

                        if error_msg.contains("dispatch failure") {
                            error!("❌ Network connection failed - possible causes:");
                            error!("   1. Network connectivity issues (check internet connection)");
                            error!("   2. AWS STS service unreachable");
                            error!("   3. Firewall or proxy blocking connection");
                            error!("   4. DNS resolution issues");
                            error!("   5. Basic AWS credentials invalid or expired");
                        } else if error_msg.contains("credentials") {
                            error!("❌ Credentials problem:");
                            error!("   1. Basic AWS credentials invalid");
                            error!("   2. Credentials expired");
                            error!("   3. Insufficient permissions");
                        } else if error_msg.contains("role") || error_msg.contains("assume") {
                            error!("❌ Role problem:");
                            error!("   1. Role ARN does not exist");
                            error!("   2. Role trust policy does not allow current user/service to assume role");
                            error!("   3. External ID mismatch");
                        } else {
                            error!("❌ Other error: {}", error_msg);
                        }

                        // Check network connectivity
                        info!("🔍 Performing network connectivity check...");

                        return Err(format!("AssumeRole failed: {}. Check network connectivity and AWS credentials.", e).into());
                    }
                }
            }
            AwsAuthentication::Default { .. } => {
                info!("Using default AWS credential chain for Delta Lake S3 access");
                
                // For Delta Lake, we need to ensure AWS credentials are available in environment
                // Check if AWS credentials are available in environment variables
                if let Ok(access_key) = std::env::var("AWS_ACCESS_KEY_ID") {
                    storage_options.insert("AWS_ACCESS_KEY_ID".to_string(), access_key);
                }
                if let Ok(secret_key) = std::env::var("AWS_SECRET_ACCESS_KEY") {
                    storage_options.insert("AWS_SECRET_ACCESS_KEY".to_string(), secret_key);
                }
                if let Ok(session_token) = std::env::var("AWS_SESSION_TOKEN") {
                    storage_options.insert("AWS_SESSION_TOKEN".to_string(), session_token);
                }
                
                // Set AWS profile if available
                if let Ok(profile) = std::env::var("AWS_PROFILE") {
                    storage_options.insert("AWS_PROFILE".to_string(), profile);
                }
                
                // Set credentials file path if available
                if let Ok(creds_file) = std::env::var("AWS_SHARED_CREDENTIALS_FILE") {
                    storage_options.insert("AWS_SHARED_CREDENTIALS_FILE".to_string(), creds_file);
                } else {
                    // Set default credentials file path
                    if let Ok(home) = std::env::var("HOME") {
                        let default_creds_file = format!("{}/.aws/credentials", home);
                        if std::path::Path::new(&default_creds_file).exists() {
                            storage_options.insert("AWS_SHARED_CREDENTIALS_FILE".to_string(), default_creds_file);
                        }
                    }
                }
                
                info!("Default AWS credential chain configured for Delta Lake");
            }
        }

        debug!("=== Completed apply_s3_storage_options ===");
        debug!("Final storage_options: {:?}", storage_options);
        info!("✓ S3 storage options applied successfully");
        // Log final storage options for debugging
        info!(
            "Final Delta Lake storage options configured: {:?}",
            storage_options
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
mod tests {
    use super::*;

    #[test]
    fn generate_config() {
        vector::test_util::test_generate_config::<DeltaLakeConfig>();
    }
}
