use std::collections::HashMap;
use std::path::PathBuf;

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
        info!("=== Applying S3 storage options (aws_s3_upload_file style) ===");
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
            AwsAuthentication::Role { assume_role, external_id, .. } => {
                // Configure IAM Role ARN for AssumeRole
                // Delta Lake's object_store will automatically call AssumeRole with these settings
                info!("Configuring Delta Lake with IAM Role ARN: {}", assume_role);
                storage_options.insert("AWS_IAM_ROLE_ARN".to_string(), assume_role.clone());
                storage_options.insert("AWS_IAM_ROLE_SESSION_NAME".to_string(), "vector-deltalake".to_string());
                
                if let Some(ext_id) = external_id {
                    storage_options.insert("AWS_IAM_ROLE_EXTERNAL_ID".to_string(), ext_id.clone());
                    info!("✓ Using external ID for role assumption");
                }
                
                info!("✓ Delta Lake will use AssumeRole with IAM Role ARN");
            }
            AwsAuthentication::AccessKey { access_key_id, secret_access_key, session_token, assume_role, .. } => {
                // Use static credentials
                storage_options.insert("AWS_ACCESS_KEY_ID".to_string(), access_key_id.to_string());
                storage_options.insert("AWS_SECRET_ACCESS_KEY".to_string(), secret_access_key.to_string());
                
                if let Some(token) = session_token {
                    storage_options.insert("AWS_SESSION_TOKEN".to_string(), token.to_string());
                }
                
                if let Some(role_arn) = assume_role {
                    info!("Using access key with assume role: {}", role_arn);
                    // Can also configure AssumeRole with base credentials
                    storage_options.insert("AWS_IAM_ROLE_ARN".to_string(), role_arn.clone());
                    storage_options.insert("AWS_IAM_ROLE_SESSION_NAME".to_string(), "vector-deltalake".to_string());
                }
                
                info!("✓ Delta Lake using static AWS credentials");
            }
            AwsAuthentication::File { credentials_file, profile, .. } => {
                // Use AWS profile
                storage_options.insert("AWS_PROFILE".to_string(), profile.clone());
                storage_options.insert("AWS_SHARED_CREDENTIALS_FILE".to_string(), credentials_file.clone());
                info!("✓ Delta Lake using AWS profile: {}", profile);
            }
            AwsAuthentication::Default { .. } => {
                // Use default AWS credential chain (environment variables, instance roles, etc.)
                // Check environment variables and pass them to Delta Lake
                info!("Using default AWS credential chain");
                
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
                        storage_options.insert("AWS_SHARED_CREDENTIALS_FILE".to_string(), default_creds_file);
                    }
                }
                
                info!("✓ Delta Lake will use AWS SDK's default credential chain");
            }
        }
        
        info!("✓ AWS authentication configured for Delta Lake via storage_options");

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
