//! S3 sink that writes log content partitioned by `component` and `hour_partition`.
//!
//! Expects events with `message`, `component`, and `hour_partition` (e.g. from file_list source).
//! Buffers by (component, hour_partition), then uploads to
//! `key_prefix/{component}/{hour_partition}/part-NNNNN.log` (optionally .log.gz).

use std::num::NonZeroUsize;

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

use crate::sinks::s3_content_partitioned::processor::S3ContentPartitionedSink;

mod processor;

/// S3 sink that partitions by event fields `component` and `hour_partition`.
#[configurable_component(sink("s3_content_partitioned"))]
#[derive(Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct S3ContentPartitionedConfig {
    /// S3 bucket name.
    pub bucket: String,

    /// Key prefix (e.g. `loki` or `logs/raw`). Objects will be written as
    /// `{key_prefix}/{component}/{hour_partition}/part-NNNNN.log` or `.log.gz`.
    #[configurable(metadata(docs::examples = "loki"))]
    pub key_prefix: String,

    /// S3 options (content type, encoding, etc.).
    #[serde(flatten)]
    pub options: S3Options,

    /// AWS region or custom endpoint.
    #[serde(flatten)]
    pub region: RegionOrEndpoint,

    /// TLS configuration for the connection.
    pub tls: Option<TlsConfig>,

    /// AWS authentication.
    #[serde(default)]
    pub auth: AwsAuthentication,

    /// Acknowledgement behaviour.
    #[serde(
        default,
        deserialize_with = "vector::serde::bool_or_struct",
        skip_serializing_if = "vector::serde::is_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,

    /// Max bytes per object before starting a new part. When a partition buffer exceeds this, it is uploaded.
    #[serde(default = "default_max_file_bytes")]
    pub max_file_bytes: usize,

    /// Whether to gzip the uploaded content.
    /// Whether to gzip the uploaded content.
    #[serde(default = "default_compression_gzip")]
    pub compression_gzip: bool,

    /// Whether to use path-style addressing for the bucket.
    #[serde(default = "default_force_path_style")]
    pub force_path_style: Option<bool>,
}

fn default_max_file_bytes() -> usize {
    64 * 1024 * 1024 // 64 MiB
}

fn default_compression_gzip() -> bool {
    true
}

fn default_force_path_style() -> Option<bool> {
    None
}

impl GenerateConfig for S3ContentPartitionedConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            bucket: "".to_owned(),
            key_prefix: "".to_owned(),
            options: S3Options::default(),
            region: RegionOrEndpoint::default(),
            tls: None,
            auth: AwsAuthentication::default(),
            acknowledgements: Default::default(),
            max_file_bytes: default_max_file_bytes(),
            compression_gzip: default_compression_gzip(),
            force_path_style: None,
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "s3_content_partitioned")]
impl SinkConfig for S3ContentPartitionedConfig {
    async fn build(&self, cx: SinkContext) -> vector::Result<(VectorSink, Healthcheck)> {
        let service = self.create_service(&cx.proxy).await?;
        let healthcheck = s3_common::config::build_healthcheck(self.bucket.clone(), service.client().clone())?;
        let sink = S3ContentPartitionedSink::new(
            service.client().clone(),
            self.bucket.clone(),
            self.key_prefix.clone(),
            NonZeroUsize::new(self.max_file_bytes).unwrap_or(NonZeroUsize::new(64 * 1024 * 1024).unwrap()),
            self.compression_gzip,
        );
        Ok((VectorSink::from_event_streamsink(sink), healthcheck))
    }

    fn input(&self) -> Input {
        Input::new(DataType::Log)
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

impl S3ContentPartitionedConfig {
    pub async fn create_service(&self, proxy: &ProxyConfig) -> vector::Result<S3Service> {
        s3_common::config::create_service(
            &self.region,
            &self.auth,
            proxy,
            self.tls.as_ref(),
            self.force_path_style.unwrap_or(true),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_config() {
        vector::test_util::test_generate_config::<S3ContentPartitionedConfig>();
    }
}
