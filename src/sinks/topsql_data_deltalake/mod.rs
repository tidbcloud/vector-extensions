use std::collections::HashMap;
use std::path::PathBuf;

use vector::{
    aws::{AwsAuthentication, RegionOrEndpoint},
    config::{GenerateConfig, SinkConfig, SinkContext},
    sinks::{
        s3_common::{config::S3Options, service::S3Service},
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

use tracing::{error, info, warn};

mod processor;

// Import default functions from common module
use crate::common::deltalake_s3;
use crate::common::deltalake_writer::{default_batch_size, default_timeout_secs};
use crate::common::meta_store::MetaStoreResolver;

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

    /// Meta-store address used to resolve keyspace to org/cluster path segments
    pub meta_store_addr: Option<String>,

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

impl GenerateConfig for DeltaLakeConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            base_path: "./delta-tables".to_owned(),
            batch_size: default_batch_size(),
            timeout_secs: default_timeout_secs(),
            max_delay_secs: default_max_delay_secs(),
            meta_store_addr: None,
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
        info!(
            "DEBUG: Building Delta Lake sink with bucket: {:?}",
            self.bucket
        );

        // Create S3 service if bucket is configured
        let s3_service = if self.bucket.is_some() {
            info!("DEBUG: Bucket configured, creating S3 service");
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

        let meta_store_resolver = self
            .meta_store_addr
            .as_deref()
            .map(MetaStoreResolver::new)
            .transpose()
            .map_err(|error| {
                vector::Error::from(format!(
                    "failed to build meta-store resolver from meta_store_addr: {}",
                    error
                ))
            })?;

        let sink = TopSQLDeltaLakeSink::new(
            base_path,
            table_configs,
            write_config,
            self.max_delay_secs,
            Some(storage_options),
            meta_store_resolver,
        );

        Ok(VectorSink::from_event_streamsink(sink))
    }

    pub async fn create_service(&self, proxy: &ProxyConfig) -> vector::Result<S3Service> {
        deltalake_s3::create_service(
            self.bucket.as_deref(),
            self.region.as_ref(),
            &self.auth,
            proxy,
            self.tls.as_ref(),
            self.force_path_style,
        )
        .await
    }

    async fn apply_s3_storage_options(
        &self,
        storage_options: &mut HashMap<String, String>,
        _service: &S3Service,
    ) -> vector::Result<()> {
        deltalake_s3::apply_s3_storage_options(
            storage_options,
            self.bucket.as_deref(),
            self.region.as_ref(),
            &self.auth,
            self.force_path_style,
        )
        .await
    }

    fn build_healthcheck(&self, s3_service: Option<&S3Service>) -> vector::Result<Healthcheck> {
        deltalake_s3::build_healthcheck(self.bucket.as_deref(), &self.base_path, s3_service)
    }
}

#[cfg(test)]
#[allow(clippy::print_stdout)]
#[allow(clippy::print_stderr)]
mod tests {
    use super::*;

    #[test]
    fn generate_config() {
        vector::test_util::test_generate_config::<DeltaLakeConfig>();
    }
}
