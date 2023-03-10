use futures_util::{FutureExt, SinkExt};
use vector::config::{AcknowledgementsConfig, GenerateConfig, Input, SinkConfig};
use vector::http::HttpClient;
use vector::sinks::util::http::PartitionHttpSink;
use vector::sinks::util::{
    BatchConfig, JsonArrayBuffer, PartitionBuffer, SinkBatchSettings, TowerRequestConfig,
};
use vector::tls::{TlsConfig, TlsSettings};
use vector::{config, sinks};
use vector_config::configurable_component;

use crate::sink::VMImportSink;

/// Configuration for the `vm_import` sink.
#[configurable_component(sink("vm_import"))]
#[derive(Debug)]
pub struct VMImportConfig {
    /// The endpoint to send events to.
    pub endpoint: String,
    /// The endpoint to use for healthchecks.
    pub healthcheck_endpoint: Option<String>,
    /// The TLS settings.
    pub tls: Option<TlsConfig>,

    /// The request settings.
    #[serde(default)]
    pub request: TowerRequestConfig,
    /// The batch settings.
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
impl SinkConfig for VMImportConfig {
    async fn build(
        &self,
        cx: config::SinkContext,
    ) -> vector::Result<(sinks::VectorSink, sinks::Healthcheck)> {
        let endpoint_tmp = self.endpoint.clone().try_into()?;

        let tls_settings = TlsSettings::from_options(&self.tls)?;
        let batch_settings = self.batch.into_batch_settings()?;
        let request_settings = self.request.unwrap_with(&Default::default());

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
