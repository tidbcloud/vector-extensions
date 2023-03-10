use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use common::checkpointer::Checkpointer;
use goauth::scopes::Scope;
use vector::config::{GenerateConfig, SinkConfig, SinkContext};
use vector::gcp::{GcpAuthConfig, GcpAuthenticator};
use vector::http::HttpClient;
use vector::sinks::gcs_common::config::{
    build_healthcheck, GcsPredefinedAcl, GcsStorageClass, BASE_URL,
};
use vector::sinks::Healthcheck;
use vector::tls::{TlsConfig, TlsSettings};
use vector_config::{configurable_component, NamedComponent as _};
use vector_core::config::{AcknowledgementsConfig, DataType, Input};
use vector_core::sink::VectorSink;

use crate::processor::GcsUploadFileSink;
use crate::uploader::RequestSettings;

/// Configuration for the `gcp_cloud_storage_upload_file` sink.
#[configurable_component(sink("gcp_cloud_storage_upload_file"))]
#[derive(Debug)]
#[serde(deny_unknown_fields)]
pub struct GcsUploadFileSinkConfig {
    /// The name of the GCS bucket to upload files to.
    pub bucket: String,
    /// The ACL to apply to uploaded files.
    pub acl: Option<GcsPredefinedAcl>,
    /// The storage class to apply to uploaded files.
    pub storage_class: Option<GcsStorageClass>,
    /// The metadata to apply to uploaded files.
    pub metadata: Option<HashMap<String, String>>,
    /// The authentication method to use.
    #[serde(flatten)]
    pub auth: GcpAuthConfig,
    /// The TLS settings.
    pub tls: Option<TlsConfig>,
    /// The acknowledgements settings.
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

pub const fn default_delay_upload_secs() -> u64 {
    10
}

pub const fn default_expire_after_secs() -> u64 {
    1800
}

impl GenerateConfig for GcsUploadFileSinkConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            bucket: "".to_owned(),
            acl: None,
            storage_class: None,
            metadata: None,
            auth: GcpAuthConfig::default(),
            tls: None,
            acknowledgements: AcknowledgementsConfig::default(),
            data_dir: None,
            delay_upload_secs: default_delay_upload_secs(),
            expire_after_secs: default_expire_after_secs(),
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
impl SinkConfig for GcsUploadFileSinkConfig {
    async fn build(&self, cx: SinkContext) -> vector::Result<(VectorSink, Healthcheck)> {
        let auth = self.auth.build(Scope::DevStorageReadWrite).await?;
        let tls = TlsSettings::from_options(&self.tls)?;
        let client = HttpClient::new(tls, cx.proxy())?;
        let healthcheck = build_healthcheck(
            self.bucket.clone(),
            client.clone(),
            format!("{}{}", BASE_URL, self.bucket),
            auth.clone(),
        )?;
        let sink = self.build_sink(client, self.bucket.clone(), auth, cx)?;

        Ok((sink, healthcheck))
    }

    fn input(&self) -> Input {
        Input::new(DataType::Log)
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

impl GcsUploadFileSinkConfig {
    fn build_sink(
        &self,
        client: HttpClient,
        bucket: String,
        auth: GcpAuthenticator,
        cx: SinkContext,
    ) -> vector::Result<VectorSink> {
        let data_dir = cx
            .globals
            .resolve_and_make_data_subdir(self.data_dir.as_ref(), self.get_component_name())?;
        let mut checkpointer = Checkpointer::new(data_dir);
        checkpointer.read_checkpoints();
        let req_settings = RequestSettings::new(self)?;
        let sink = GcsUploadFileSink::new(
            client,
            bucket,
            auth,
            Duration::from_secs(self.delay_upload_secs),
            Duration::from_secs(self.expire_after_secs),
            checkpointer,
            req_settings,
        );

        Ok(VectorSink::from_event_streamsink(sink))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_config() {
        vector::test_util::test_generate_config::<GcsUploadFileSinkConfig>();
    }
}
