use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use goauth::scopes::Scope;
use vector::{
    config::{GenerateConfig, SinkConfig, SinkContext},
    gcp::{GcpAuthConfig, GcpAuthenticator},
    http::HttpClient,
    sinks::gcs_common::config::{build_healthcheck, GcsPredefinedAcl, GcsStorageClass, BASE_URL},
    sinks::Healthcheck,
};
use vector_config::NamedComponent;
use vector_lib::{
    config::{AcknowledgementsConfig, DataType, Input},
    configurable::configurable_component,
    sink::VectorSink,
    tls::{TlsConfig, TlsSettings},
};

use crate::common::checkpointer::Checkpointer;
use crate::sinks::gcp_cloud_storage_upload_file::processor::GcsUploadFileSink;
use crate::sinks::gcp_cloud_storage_upload_file::uploader::RequestSettings;

mod processor;
mod uploader;

/// PLACEHOLDER
#[configurable_component(sink("gcp_cloud_storage_upload_file"))]
#[derive(Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct GcsUploadFileSinkConfig {
    /// PLACEHOLDER
    pub bucket: String,

    /// PLACEHOLDER
    pub acl: Option<GcsPredefinedAcl>,

    /// PLACEHOLDER
    pub storage_class: Option<GcsStorageClass>,

    /// PLACEHOLDER
    pub metadata: Option<HashMap<String, String>>,

    /// PLACEHOLDER
    #[serde(flatten)]
    pub auth: GcpAuthConfig,

    /// PLACEHOLDER
    pub tls: Option<TlsConfig>,

    /// PLACEHOLDER
    #[serde(
        default,
        deserialize_with = "vector::serde::bool_or_struct",
        skip_serializing_if = "vector::serde::is_default"
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
#[typetag::serde(name = "gcp_cloud_storage_upload_file")]
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
