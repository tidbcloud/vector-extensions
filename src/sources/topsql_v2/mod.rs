use std::time::Duration;

use serde::{Deserialize, Serialize};
use vector::config::{GenerateConfig, SourceConfig, SourceContext};
use vector_config::Configurable;
use vector_lib::{
    config::{DataType, LogNamespace, SourceOutput},
    configurable::configurable_component,
    source::Source,
    tls::TlsConfig,
};

use crate::sources::topsql_v2::controller::Controller;

mod controller;
mod schema_cache;
pub mod shutdown;
pub mod upstream;

/// Configuration for TopRU (Resource Unit) collection.
#[derive(Debug, Clone, Serialize, Deserialize, Configurable)]
pub struct TopRUConfig {
    /// Enable TopRU collection. When true, subscribe to TopRU data from TiDB.
    #[serde(default = "default_enable_topru")]
    pub enable: bool,

    /// Report interval in seconds. Allowed values: 15, 30, 60. Server validates and applies default if invalid.
    #[serde(default = "default_topru_report_interval")]
    pub report_interval_seconds: u32,

    /// Item interval in seconds. Allowed values: 15, 30, 60. Server validates and applies default if invalid.
    #[serde(default = "default_topru_item_interval")]
    pub item_interval_seconds: u32,
}

fn default_enable_topru() -> bool {
    true
}

fn default_topru_report_interval() -> u32 {
    60
}

fn default_topru_item_interval() -> u32 {
    60
}

impl Default for TopRUConfig {
    fn default() -> Self {
        Self {
            enable: default_enable_topru(),
            report_interval_seconds: default_topru_report_interval(),
            item_interval_seconds: default_topru_item_interval(),
        }
    }
}

/// PLACEHOLDER
#[configurable_component(source("topsql_v2"))]
#[derive(Debug, Clone)]
pub struct TopSQLConfig {
    /// PLACEHOLDER
    pub tidb_group: Option<String>,

    /// PLACEHOLDER
    pub label_k8s_instance: Option<String>,

    /// PLACEHOLDER
    pub pd_address: Option<String>,

    /// PLACEHOLDER
    pub manager_server_address: Option<String>,

    /// PLACEHOLDER
    pub tidb_namespace: Option<String>,

    /// PLACEHOLDER
    pub tls: Option<TlsConfig>,

    /// PLACEHOLDER
    #[serde(default = "default_init_retry_delay")]
    pub init_retry_delay_seconds: f64,

    /// PLACEHOLDER
    #[serde(default = "default_topology_fetch_interval")]
    pub topology_fetch_interval_seconds: f64,

    /// PLACEHOLDER
    #[serde(default = "default_top_n")]
    pub top_n: usize,

    /// PLACEHOLDER
    #[serde(default = "default_downsampling_interval")]
    pub downsampling_interval: u32,

    /// Whether to collect TiKV TopSQL data (`tikv_topsql` and `tikv_topregion`).
    /// When disabled, only TiDB TopSQL/TopRU data is collected.
    #[serde(default = "default_enable_tikv_topsql")]
    pub enable_tikv_topsql: bool,

    /// TopRU (Resource Unit) collection config. Only applies to TiDB upstream.
    #[serde(default)]
    pub topru: TopRUConfig,
}

pub const fn default_init_retry_delay() -> f64 {
    1.0
}

pub const fn default_topology_fetch_interval() -> f64 {
    30.0
}

pub const fn default_top_n() -> usize {
    100
}

pub const fn default_downsampling_interval() -> u32 {
    60
}

pub const fn default_enable_tikv_topsql() -> bool {
    true
}

impl GenerateConfig for TopSQLConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            tidb_group: None,
            label_k8s_instance: None,
            pd_address: None,
            manager_server_address: None,
            tidb_namespace: None,
            tls: None,
            init_retry_delay_seconds: default_init_retry_delay(),
            topology_fetch_interval_seconds: default_topology_fetch_interval(),
            top_n: default_top_n(),
            downsampling_interval: default_downsampling_interval(),
            enable_tikv_topsql: default_enable_tikv_topsql(),
            topru: TopRUConfig::default(),
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "topsql_v2")]
impl SourceConfig for TopSQLConfig {
    async fn build(&self, cx: SourceContext) -> vector::Result<Source> {
        self.validate_tls()?;

        let tidb_group = self.tidb_group.clone();
        let label_k8s_instance = self.label_k8s_instance.clone();
        let pd_address = self.pd_address.clone();
        let manager_server_address = self.manager_server_address.clone();
        let tidb_namespace = self.tidb_namespace.clone();
        let tls = self.tls.clone();
        let topology_fetch_interval = Duration::from_secs_f64(self.topology_fetch_interval_seconds);
        let init_retry_delay = Duration::from_secs_f64(self.init_retry_delay_seconds);
        let top_n = self.top_n;
        let downsampling_interval = self.downsampling_interval;
        let enable_tikv_topsql = self.enable_tikv_topsql;
        let topru = self.topru.clone();
        let schema_update_interval = Duration::from_secs(60);

        Ok(Box::pin(async move {
            let controller = Controller::new(
                pd_address,
                manager_server_address,
                tidb_namespace,
                topology_fetch_interval,
                init_retry_delay,
                top_n,
                downsampling_interval,
                enable_tikv_topsql,
                schema_update_interval,
                tls,
                &cx.proxy,
                tidb_group,
                label_k8s_instance,
                topru,
                cx.out,
            )
            .await
            .map_err(|error| error!(message = "Source failed.", %error))?;

            controller.run(cx.shutdown).await;

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
