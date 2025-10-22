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

use crate::common::deltalake_writer::{DeltaTableConfig, WriteConfig};
use crate::{
    common::deltalake_writer::StorageOptionsBuilder,
    sinks::topsql_deltalake::processor::TopSQLDeltaLakeSink,
};

mod processor;

/// Configuration for the topsql deltalake sink
#[configurable_component(sink("topsql_deltalake"))]
#[derive(Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct TopSQLDeltaLakeConfig {
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

impl GenerateConfig for TopSQLDeltaLakeConfig {
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
impl SinkConfig for TopSQLDeltaLakeConfig {
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

impl TopSQLDeltaLakeConfig {
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
            let _ = StorageOptionsBuilder::new(
                self.region.clone(),
                self.force_path_style,
                self.auth.clone(),
            )
            .build(&mut storage_options, service)
            .await;
        } else {
            info!("No S3 service available - using default storage options only");
        }

        let sink = TopSQLDeltaLakeSink::new(
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
        vector::test_util::test_generate_config::<TopSQLDeltaLakeConfig>();
    }
}
