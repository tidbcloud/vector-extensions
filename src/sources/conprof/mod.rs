use std::time::Duration;

use vector::config::{GenerateConfig, SourceConfig, SourceContext};
use vector_lib::{
    config::{DataType, LogNamespace, SourceOutput},
    configurable::configurable_component,
    source::Source,
    tls::TlsConfig,
};

use crate::sources::conprof::controller::Controller;

mod controller;
mod shutdown;
mod tools;
mod topology;
mod upstream;

/// PLACEHOLDER
#[configurable_component(source("conprof"))]
#[derive(Debug, Clone)]
pub struct ConprofConfig {
    /// PLACEHOLDER
    pub pd_address: String,

    /// PLACEHOLDER
    pub tls: Option<TlsConfig>,

    /// PLACEHOLDER
    // #[serde(default = "default_init_retry_delay")]
    // pub init_retry_delay_seconds: f64,

    /// PLACEHOLDER
    #[serde(default = "default_topology_fetch_interval")]
    pub topology_fetch_interval_seconds: f64,

    /// PLACEHOLDER
    #[serde(default = "default_enable_tikv_heap_profile")]
    pub enable_tikv_heap_profile: bool,
}

// pub const fn default_init_retry_delay() -> f64 {
//     1.0
// }

pub const fn default_topology_fetch_interval() -> f64 {
    30.0
}

pub const fn default_enable_tikv_heap_profile() -> bool {
    false
}

impl GenerateConfig for ConprofConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            pd_address: "127.0.0.1:2379".to_owned(),
            tls: None,
            // init_retry_delay_seconds: default_init_retry_delay(),
            topology_fetch_interval_seconds: default_topology_fetch_interval(),
            enable_tikv_heap_profile: default_enable_tikv_heap_profile(),
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "conprof")]
impl SourceConfig for ConprofConfig {
    async fn build(&self, cx: SourceContext) -> vector::Result<Source> {
        self.validate_tls()?;

        let pd_address = self.pd_address.clone();
        let tls = self.tls.clone();
        let topology_fetch_interval = Duration::from_secs_f64(self.topology_fetch_interval_seconds);
        let enable_tikv_heap_profile = self.enable_tikv_heap_profile;
        // let init_retry_delay = Duration::from_secs_f64(self.init_retry_delay_seconds);
        Ok(Box::pin(async move {
            Controller::new(
                pd_address,
                topology_fetch_interval,
                enable_tikv_heap_profile,
                // init_retry_delay,
                tls,
                &cx.proxy,
                cx.out,
            )
            .await
            .map_err(|error| error!(message = "Source failed.", %error))?
            .run(cx.shutdown)
            .await;
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

impl ConprofConfig {
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
        vector::test_util::test_generate_config::<ConprofConfig>();
    }
}
