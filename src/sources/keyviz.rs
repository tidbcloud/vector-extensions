use std::time::Duration;

use chrono::Utc;
use reqwest::{Certificate, Client, Identity};
use serde::{Deserialize, Serialize};
use vector::{
    config::{GenerateConfig, SourceConfig, SourceContext},
    event::LogEvent,
    internal_events::StreamClosedError,
    tls::TlsSettings,
};
use vector_lib::{
    config::{DataType, LogNamespace, SourceOutput},
    configurable::configurable_component,
    internal_event::InternalEvent,
    source::Source,
    tls::TlsConfig,
};

/// PLACEHOLDER
#[configurable_component(source("keyviz"))]
#[derive(Debug, Clone)]
pub struct KeyvizConfig {
    /// PLACEHOLDER
    pub pd_address: String,

    /// PLACEHOLDER
    pub tls: Option<TlsConfig>,
}

impl GenerateConfig for KeyvizConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            pd_address: "127.0.0.1:2379".to_owned(),
            tls: None,
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "keyviz")]
impl SourceConfig for KeyvizConfig {
    async fn build(&self, mut cx: SourceContext) -> vector::Result<Source> {
        self.validate_tls()?;
        let tls = self.tls.clone();
        let pd_address = if tls.is_some() {
            format!("https://{}", self.pd_address)
        } else {
            format!("http://{}", self.pd_address)
        };

        let mut builder = reqwest::Client::builder();
        if let Some(tls) = tls.clone() {
            let ca_file = tls.ca_file.clone().expect("tls ca file must be provided");
            let ca = match tokio::fs::read(ca_file).await {
                Ok(v) => v,
                Err(err) => {
                    error!(message = "Failed to read tls ca file", error = %err);
                    return Err(Box::new(err));
                }
            };
            let settings = TlsSettings::from_options(&Some(tls)).expect("invalid tls settings");
            let (crt, key) = settings.identity_pem().expect("invalid identity pem");
            builder = builder
                .add_root_certificate(Certificate::from_pem(&ca).expect("invalid ca"))
                .identity(Identity::from_pkcs8_pem(&crt, &key).expect("invalid crt & key"));
        }

        let client = match builder
            .timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(10))
            .build()
        {
            Ok(client) => client,
            Err(err) => {
                error!(message = "Failed to build reqwest client", %err);
                return Err(Box::new(err));
            }
        };

        Ok(Box::pin(async move {
            loop {
                let now = Utc::now();
                let filename = now.format("%Y%m%d%H%M").to_string();
                let mut ts = now.timestamp();
                ts -= ts % 60;
                let next_minute_ts = ts + 60;
                fetch_and_send_regions(
                    client.clone(),
                    &pd_address,
                    &mut cx,
                    format!("{}.json", filename),
                )
                .await;
                let now = Utc::now().timestamp();
                if now < next_minute_ts {
                    tokio::select! {
                        _ = &mut cx.shutdown => break,
                        _ = tokio::time::sleep(Duration::from_secs((next_minute_ts - now + 1) as u64)) => {},
                    }
                }
            }
            Ok(())
        }))
    }

    fn outputs(&self, _: LogNamespace) -> Vec<SourceOutput> {
        vec![SourceOutput {
            port: None,
            ty: DataType::Log,
            schema_definition: None,
        }]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

impl KeyvizConfig {
    fn check_key_file(
        tag: &str,
        path: &Option<std::path::PathBuf>,
    ) -> vector::Result<Option<std::fs::File>> {
        if path.is_none() {
            return Ok(None);
        }
        match std::fs::File::open(path.as_ref().unwrap()) {
            Err(e) => Err(format!("failed to open {:?} to load {}: {:?}", path, tag, e).into()),
            Ok(f) => Ok(Some(f)),
        }
    }

    fn validate_tls(&self) -> vector::Result<()> {
        if self.tls.is_none() {
            return Ok(());
        }
        let tls = self.tls.as_ref().unwrap();
        if (tls.ca_file.is_some() || tls.crt_file.is_some() || tls.key_file.is_some())
            && (tls.ca_file.is_none() || tls.crt_file.is_none() || tls.key_file.is_none())
        {
            return Err("ca, cert and private key should be all configured.".into());
        }
        Self::check_key_file("ca key", &tls.ca_file)?;
        Self::check_key_file("cert key", &tls.crt_file)?;
        Self::check_key_file("private key", &tls.key_file)?;
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct RegionsInfo {
    count: usize,
    regions: Vec<RegionInfo>,
}

#[derive(Debug, Deserialize, Serialize)]
struct RegionInfo {
    start_key: String,
    end_key: String,
    written_bytes: u64,
    read_bytes: u64,
    written_keys: u64,
    read_keys: u64,
}

async fn fetch_and_send_regions(
    client: Client,
    pd_address: &str,
    cx: &mut SourceContext,
    filename: String,
) {
    tokio::select! {
        _ = &mut cx.shutdown => {}
        regions = fetch_regions(client.clone(), pd_address) => {
            match regions {
                Ok(regions) => {
                    let json = match serde_json::to_string(&regions) {
                        Ok(v) => v,
                        Err(err) => {
                            error!(message = "Failed to serialize json", %err);
                            return;
                        }
                    };
                    let mut event = LogEvent::from_str_legacy(json);
                    event.insert("filename", filename);
                    if cx.out.send_event(event).await.is_err() {
                        StreamClosedError { count: 1 }.emit();
                    }
                }
                Err(err) => {
                    error!(message = "Failed to fetch regions", %err);
                    return;
                }
            }
        }
    }
}

async fn fetch_regions(client: Client, pd_address: &str) -> reqwest::Result<RegionsInfo> {
    let mut all = RegionsInfo {
        count: 0,
        regions: vec![],
    };
    let mut start_key = String::new();
    loop {
        let mut regions = fetch_regions_part(
            client.clone(),
            pd_address,
            start_key.clone(),
            String::new(),
            51200,
        )
        .await?;
        for region in &mut regions.regions {
            let start_bytes = match hex::decode(&region.start_key) {
                Ok(v) => v,
                Err(err) => {
                    error!(message = "Failed to decode regions info start key", %err);
                    continue;
                }
            };
            let end_bytes = match hex::decode(&region.end_key) {
                Ok(v) => v,
                Err(err) => {
                    error!(message = "Failed to decode regions info end key", %err);
                    continue;
                }
            };
            region.start_key = unsafe { String::from_utf8_unchecked(start_bytes) };
            region.end_key = unsafe { String::from_utf8_unchecked(end_bytes) };
        }
        let last_key = regions.regions.last().map(|r| r.end_key.clone());
        all.regions.append(&mut regions.regions);
        all.count += regions.count;
        start_key = match last_key {
            None => break,
            Some(last_key) => {
                if last_key == "" {
                    break;
                }
                last_key
            }
        };
    }
    Ok(all)
}

async fn fetch_regions_part(
    client: Client,
    pd_address: &str,
    key: String,
    end_key: String,
    limit: i32,
) -> reqwest::Result<RegionsInfo> {
    client
        .get(format!("{}/pd/api/v1/regions/key", pd_address))
        .query(&[
            ("key", key),
            ("end_key", end_key),
            ("limit", limit.to_string()),
        ])
        .send()
        .await?
        .json()
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_config() {
        vector::test_util::test_generate_config::<KeyvizConfig>();
    }
}
