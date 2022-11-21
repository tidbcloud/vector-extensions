use std::path::PathBuf;
use std::time::Duration;

use aws_sdk_s3::Client as S3Client;
use common::checkpointer::Checkpointer;
use serde::{Deserialize, Serialize};
use vector::aws::{AwsAuthentication, RegionOrEndpoint};
use vector::config::{
    AcknowledgementsConfig, GenerateConfig, SinkConfig, SinkContext, SinkDescription,
};
use vector::sinks::s3_common::config::S3Options;
use vector::sinks::s3_common::service::S3Service;
use vector::sinks::{s3_common, Healthcheck};
use vector::tls::TlsConfig;
use vector_core::config::proxy::ProxyConfig;
use vector_core::config::{DataType, Input};
use vector_core::sink::VectorSink;

use crate::processor::S3UploadFileSink;

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct S3UploadFileConfig {
    pub bucket: String,
    #[serde(flatten)]
    pub options: S3Options,
    #[serde(flatten)]
    pub region: RegionOrEndpoint,
    pub tls: Option<TlsConfig>,
    #[serde(default)]
    pub auth: AwsAuthentication,
    #[serde(
        default,
        deserialize_with = "vector::serde::bool_or_struct",
        skip_serializing_if = "vector::serde::skip_serializing_if_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,

    /// The directory used to persist file checkpoint.
    ///
    /// By default, the global `data_dir` option is used. Please make sure the user Vector is running as has write permissions to this directory.
    pub data_dir: Option<PathBuf>,

    /// Delay between receiving upload event and beginning to upload file.
    #[serde(alias = "delay_upload", default = "default_delay_upload_secs")]
    pub delay_upload_secs: u64,

    /// The expire time of uploaded file records which used to prevent duplicate uploads.
    #[serde(alias = "expire_after", default = "default_expire_after_secs")]
    pub expire_after_secs: u64,
}

pub fn default_delay_upload_secs() -> u64 {
    10
}

pub fn default_expire_after_secs() -> u64 {
    1800
}

impl GenerateConfig for S3UploadFileConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            bucket: "".to_owned(),
            options: S3Options::default(),
            region: RegionOrEndpoint::default(),
            tls: None,
            auth: AwsAuthentication::default(),
            acknowledgements: Default::default(),

            data_dir: None,
            delay_upload_secs: default_delay_upload_secs(),
            expire_after_secs: default_expire_after_secs(),
        })
        .unwrap()
    }
}

inventory::submit! {
    SinkDescription::new::<S3UploadFileConfig>("aws_s3_upload_file")
}

#[async_trait::async_trait]
#[typetag::serde(name = "aws_s3_upload_file")]
impl SinkConfig for S3UploadFileConfig {
    async fn build(&self, cx: SinkContext) -> vector::Result<(VectorSink, Healthcheck)> {
        let service = self.create_service(&cx.proxy).await?;
        let healthcheck = self.build_healthcheck(service.client())?;
        let sink = self.build_processor(service, cx)?;
        Ok((sink, healthcheck))
    }

    fn input(&self) -> Input {
        Input::new(DataType::Log)
    }

    fn sink_type(&self) -> &'static str {
        "aws_s3_upload_file"
    }

    fn acknowledgements(&self) -> Option<&AcknowledgementsConfig> {
        Some(&self.acknowledgements)
    }
}

impl S3UploadFileConfig {
    pub fn build_processor(
        &self,
        service: S3Service,
        cx: SinkContext,
    ) -> vector::Result<VectorSink> {
        let data_dir = cx
            .globals
            .resolve_and_make_data_subdir(self.data_dir.as_ref(), self.sink_type())?;
        let mut checkpointer = Checkpointer::new(data_dir);
        checkpointer.read_checkpoints();

        let sink = S3UploadFileSink::new(
            self.bucket.clone(),
            self.options.clone(),
            Duration::from_secs(self.delay_upload_secs),
            Duration::from_secs(self.expire_after_secs),
            service,
            checkpointer,
        );

        Ok(VectorSink::from_event_streamsink(sink))
    }

    pub fn build_healthcheck(&self, client: S3Client) -> vector::Result<Healthcheck> {
        s3_common::config::build_healthcheck(self.bucket.clone(), client)
    }

    pub async fn create_service(&self, proxy: &ProxyConfig) -> vector::Result<S3Service> {
        s3_common::config::create_service(&self.region, &self.auth, proxy, &self.tls).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_config() {
        vector::test_util::test_generate_config::<S3UploadFileConfig>();
    }
}
