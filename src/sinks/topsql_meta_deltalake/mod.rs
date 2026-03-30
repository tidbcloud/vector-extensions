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

use crate::sinks::topsql_meta_deltalake::processor::TopSQLDeltaLakeSink;

use tracing::{error, info, warn};

mod processor;

// Import default functions from common module
use crate::common::deltalake_s3;
use crate::common::deltalake_writer::{default_batch_size, default_timeout_secs};
use crate::common::keyspace_cluster::{validate_keyspace_route_template, PdKeyspaceResolver};

pub const fn default_enable_keyspace_cluster_mapping() -> bool {
    false
}

pub const fn default_max_delay_secs() -> u64 {
    180
}

pub const fn default_meta_cache_capacity() -> usize {
    10000
}

// Re-export types from common module
pub use crate::common::deltalake_writer::{DeltaTableConfig, WriteConfig};

/// Configuration for the deltalake sink
#[configurable_component(sink("topsql_meta_deltalake"))]
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

    /// LRU cache capacity for deduplication (shared by SQL meta and PLAN meta)
    #[serde(default = "default_meta_cache_capacity")]
    pub meta_cache_capacity: usize,

    /// Whether to resolve keyspace to org/cluster path segments through PD.
    #[serde(default = "default_enable_keyspace_cluster_mapping")]
    pub enable_keyspace_cluster_mapping: bool,

    /// PD address used to resolve keyspace to org/cluster path segments.
    pub pd_address: Option<String>,

    /// TLS configuration for PD keyspace lookup.
    pub pd_tls: Option<TlsConfig>,

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
            meta_cache_capacity: default_meta_cache_capacity(),
            enable_keyspace_cluster_mapping: default_enable_keyspace_cluster_mapping(),
            pd_address: None,
            pd_tls: None,
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
#[typetag::serde(name = "topsql_meta_deltalake")]
impl SinkConfig for DeltaLakeConfig {
    async fn build(&self, cx: SinkContext) -> vector::Result<(VectorSink, Healthcheck)> {
        info!(
            "DEBUG: Building Delta Lake sink with bucket: {:?}",
            self.bucket
        );
        let is_cloud_path = self.base_path.starts_with("s3://")
            || self.base_path.starts_with("abfss://")
            || self.base_path.starts_with("gs://");

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
        } else if is_cloud_path {
            info!(
                "Cloud storage path detected ({}), using storage_options for authentication",
                &self.base_path[..self.base_path.find("://").unwrap_or(0) + 3]
            );
            None
        } else {
            info!("No bucket configured, using local filesystem");
            None
        };

        info!("Building sink processor");
        let sink = self.build_processor(s3_service.as_ref(), cx).await?;

        info!("Building healthcheck");
        let healthcheck = self.build_healthcheck(s3_service.as_ref(), is_cloud_path)?;

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
        if self.enable_keyspace_cluster_mapping {
            validate_keyspace_route_template(&self.base_path).map_err(vector::Error::from)?;
        }

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

        let keyspace_route_resolver = if self.enable_keyspace_cluster_mapping {
            let pd_address = self.pd_address.as_deref().ok_or_else(|| {
                vector::Error::from(
                    "pd_address is required when enable_keyspace_cluster_mapping is true",
                )
            })?;
            Some(
                PdKeyspaceResolver::new(pd_address, self.pd_tls.as_ref()).map_err(|error| {
                    vector::Error::from(format!(
                        "failed to build PD keyspace resolver from pd_address: {}",
                        error
                    ))
                })?,
            )
        } else {
            None
        };

        let sink = TopSQLDeltaLakeSink::new(
            base_path,
            table_configs,
            write_config,
            self.max_delay_secs,
            Some(storage_options),
            self.meta_cache_capacity,
            keyspace_route_resolver,
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

    fn build_healthcheck(
        &self,
        s3_service: Option<&S3Service>,
        is_cloud_path: bool,
    ) -> vector::Result<Healthcheck> {
        deltalake_s3::build_healthcheck(
            self.bucket.as_deref(),
            &self.base_path,
            s3_service,
            is_cloud_path,
        )
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
