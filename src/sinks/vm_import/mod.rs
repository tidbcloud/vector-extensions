use futures_util::{FutureExt, SinkExt};
use vector::{
    config::{GenerateConfig, SinkConfig, SinkContext},
    http::HttpClient,
    sinks::{
        self,
        util::{
            http::PartitionHttpSink, BatchConfig, JsonArrayBuffer, PartitionBuffer,
            SinkBatchSettings, TowerRequestConfig,
        },
    },
};
use vector_lib::{
    config::{AcknowledgementsConfig, Input},
    configurable::configurable_component,
    tls::{TlsConfig, TlsSettings},
};

use crate::sinks::vm_import::sink::VMImportSink;

mod encoder;
mod partition;
mod sink;

/// PLACEHOLDER
#[configurable_component(sink("vm_import"))]
#[derive(Debug, Clone)]
pub struct VMImportConfig {
    /// PLACEHOLDER
    pub endpoint: String,

    /// PLACEHOLDER
    pub healthcheck_endpoint: Option<String>,

    /// PLACEHOLDER
    pub tls: Option<TlsConfig>,

    /// PLACEHOLDER
    #[serde(default)]
    pub request: TowerRequestConfig,

    /// PLACEHOLDER
    #[serde(default)]
    pub batch: BatchConfig<VMImportDefaultBatchSettings>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct VMImportDefaultBatchSettings;

impl SinkBatchSettings for VMImportDefaultBatchSettings {
    const MAX_EVENTS: Option<usize> = Some(1_000);
    const MAX_BYTES: Option<usize> = None;
    const TIMEOUT_SECS: f64 = 1.0;
}

impl GenerateConfig for VMImportConfig {
    fn generate_config() -> toml::Value {
        let sample_url = "http://127.0.0.1:8428/api/v1/import";

        toml::Value::try_from(Self {
            tls: Default::default(),
            batch: Default::default(),
            request: Default::default(),
            healthcheck_endpoint: Default::default(),

            endpoint: sample_url.to_owned(),
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "vm_import")]
impl SinkConfig for VMImportConfig {
    async fn build(
        &self,
        cx: SinkContext,
    ) -> vector::Result<(sinks::VectorSink, sinks::Healthcheck)> {
        let endpoint_tmp = self.endpoint.clone().try_into()?;

        let tls_settings = TlsSettings::from_options(&self.tls)?;
        let batch_settings = self.batch.into_batch_settings()?;
        let request_settings = self.request.into_settings();

        let client = HttpClient::new(tls_settings, cx.proxy())?;
        let sink = VMImportSink::new(endpoint_tmp);
        let buffer = PartitionBuffer::new(JsonArrayBuffer::new(batch_settings.size));

        let sink = PartitionHttpSink::new(
            sink,
            buffer,
            request_settings,
            batch_settings.timeout,
            client.clone(),
        )
        .sink_map_err(|e| error!(message = "VM import sink error.", %e));
        let hc = healthcheck(self.healthcheck_endpoint.clone(), client).boxed();

        #[allow(deprecated)] // TODO: remove
        Ok((sinks::VectorSink::from_event_sink(sink), hc))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &AcknowledgementsConfig::DEFAULT
    }
}

async fn healthcheck(endpoint: Option<String>, client: HttpClient) -> vector::Result<()> {
    let endpoint = match endpoint {
        Some(endpoint) => endpoint,
        None => return Ok(()),
    };
    let request = http::Request::get(endpoint).body(hyper::Body::empty())?;
    let response = client.send(request).await?;
    let status = response.status();
    if status.is_success() {
        Ok(())
    } else {
        Err(sinks::HealthcheckError::UnexpectedStatus { status }.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_config() {
        vector::test_util::test_generate_config::<VMImportConfig>();
    }
}
