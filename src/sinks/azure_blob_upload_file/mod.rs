use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use azure_storage_blobs::prelude::*;
use vector::{
    config::{GenerateConfig, SinkConfig, SinkContext},
    sinks::azure_common,
    sinks::Healthcheck,
};
use vector_config::NamedComponent;
use vector_lib::{
    config::{AcknowledgementsConfig, DataType, Input},
    configurable::configurable_component,
    sink::VectorSink,
};

use crate::common::checkpointer::Checkpointer;
use crate::sinks::azure_blob_upload_file::processor::AzureBlobUploadFileSink;

mod processor;
mod uploader;

/// PLACEHOLDER
#[configurable_component(sink("azure_blob_upload_file"))]
#[derive(Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct AzureBlobUploadFileConfig {
    /// The Azure Blob Storage Account connection string.
    ///
    /// Authentication with access key is the only supported authentication method.
    ///
    /// Either `storage_account`, or this field, must be specified.
    pub connection_string: Option<String>,

    /// The Azure Blob Storage Account name.
    ///
    /// Attempts to load credentials for the account in the following ways, in order:
    ///
    /// - read from environment variables ([more information][env_cred_docs])
    /// - looks for a [Managed Identity][managed_ident_docs]
    /// - uses the `az` CLI tool to get an access token ([more information][az_cli_docs])
    ///
    /// Either `connection_string`, or this field, must be specified.
    ///
    /// [env_cred_docs]: https://docs.rs/azure_identity/latest/azure_identity/struct.EnvironmentCredential.html
    /// [managed_ident_docs]: https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/overview
    /// [az_cli_docs]: https://docs.microsoft.com/en-us/cli/azure/account?view=azure-cli-latest#az-account-get-access-token
    pub storage_account: Option<String>,

    /// The Azure Blob Storage Endpoint URL.
    ///
    /// This is used to override the default blob storage endpoint URL in cases where you are using
    /// credentials read from the environment/managed identities or access tokens without using an
    /// explicit connection_string (which already explicitly supports overriding the blob endpoint
    /// URL).
    ///
    /// This may only be used with `storage_account` and is ignored when used with
    /// `connection_string`.
    pub endpoint: Option<String>,

    /// The Azure Blob Storage Account container name.
    pub(super) container_name: String,

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

impl GenerateConfig for AzureBlobUploadFileConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            connection_string: Some(String::from("DefaultEndpointsProtocol=https;AccountName=some-account-name;AccountKey=some-account-key;").into()),
            storage_account: Some(String::from("some-account-name")),
            container_name: String::from("logs"),
            endpoint: None,
            acknowledgements: AcknowledgementsConfig::default(),
            data_dir: None,
            delay_upload_secs: default_delay_upload_secs(),
            expire_after_secs: default_expire_after_secs(),
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "azure_blob_upload_file")]
impl SinkConfig for AzureBlobUploadFileConfig {
    async fn build(&self, cx: SinkContext) -> vector::Result<(VectorSink, Healthcheck)> {
        let client = azure_common::config::build_client(
            self.connection_string.clone(),
            self.storage_account.clone(),
            self.container_name.clone(),
            self.endpoint.clone(),
        )?;
        let sink = self.build_sink(client.clone(), cx)?;
        let healthcheck =
            azure_common::config::build_healthcheck(self.container_name.clone(), client.clone())?;
        Ok((sink, healthcheck))
    }

    fn input(&self) -> Input {
        Input::new(DataType::Log)
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

impl AzureBlobUploadFileConfig {
    fn build_sink(
        &self,
        client: Arc<ContainerClient>,
        cx: SinkContext,
    ) -> vector::Result<VectorSink> {
        let data_dir = cx
            .globals
            .resolve_and_make_data_subdir(self.data_dir.as_ref(), self.get_component_name())?;
        let mut checkpointer = Checkpointer::new(data_dir);
        checkpointer.read_checkpoints();
        let sink = AzureBlobUploadFileSink::new(
            client,
            self.container_name.clone(),
            Duration::from_secs(self.delay_upload_secs),
            Duration::from_secs(self.expire_after_secs),
            checkpointer,
        );
        Ok(VectorSink::from_event_streamsink(sink))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_config() {
        vector::test_util::test_generate_config::<AzureBlobUploadFileConfig>();
    }
}
