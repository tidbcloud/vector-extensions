use std::time::Duration;

use serde::{Deserialize, Serialize};
use vector_core::config::LogNamespace;

use vector::{
    config::{self, GenerateConfig, Output, SourceConfig, SourceContext, SourceDescription},
    sources,
    tls::TlsConfig,
};

use crate::controller::Controller;

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct TopSQLConfig {
    pub pd_address: String,
    pub tls: Option<TlsConfig>,

    #[serde(default = "default_init_retry_delay")]
    pub init_retry_delay_seconds: f64,
    #[serde(default = "default_topology_fetch_interval")]
    pub topology_fetch_interval_seconds: f64,
}

pub const fn default_init_retry_delay() -> f64 {
    1.0
}

pub const fn default_topology_fetch_interval() -> f64 {
    30.0
}

inventory::submit! {
    SourceDescription::new::<TopSQLConfig>("topsql")
}

impl GenerateConfig for TopSQLConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            pd_address: "127.0.0.1:2379".to_owned(),
            tls: None,
            init_retry_delay_seconds: default_init_retry_delay(),
            topology_fetch_interval_seconds: default_topology_fetch_interval(),
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "topsql")]
impl SourceConfig for TopSQLConfig {
    async fn build(&self, cx: SourceContext) -> vector::Result<sources::Source> {
        self.validate_tls()?;

        let pd_address = self.pd_address.clone();
        let tls = self.tls.clone();
        let topology_fetch_interval = Duration::from_secs_f64(self.topology_fetch_interval_seconds);
        let init_retry_delay = Duration::from_secs_f64(self.init_retry_delay_seconds);
        Ok(Box::pin(async move {
            let controller = Controller::new(
                pd_address,
                topology_fetch_interval,
                init_retry_delay,
                tls,
                &cx.proxy,
                cx.out,
            )
            .await
            .map_err(|error| error!(message = "Source failed.", %error))?;

            controller.run(cx.shutdown).await;

            Ok(())
        }))
    }

    fn outputs(&self, _global_log_namespace: LogNamespace) -> Vec<Output> {
        vec![Output::default(config::DataType::Log)]
    }

    fn source_type(&self) -> &'static str {
        "topsql"
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

impl TopSQLConfig {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_config() {
        vector::test_util::test_generate_config::<TopSQLConfig>();
    }
}
