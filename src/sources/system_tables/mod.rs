use std::time::Duration;

use vector::config::{GenerateConfig, SourceConfig, SourceContext};
use vector_lib::{
    config::{DataType, LogNamespace, SourceOutput},
    configurable::configurable_component,
    source::Source,
    tls::TlsConfig,
};
use serde::{Deserialize, Serialize};

use crate::sources::system_tables::controller::Controller;

mod controller;
mod collector;

/// Configuration for the system_tables source
#[configurable_component(source("system_tables"))]
#[derive(Debug, Clone)]
pub struct SystemTablesConfig {
    /// PD address for legacy mode (to discover TiDB instances)
    pub pd_address: String,

    /// TiDB group name for nextgen mode
    pub tidb_group: Option<String>,

    /// Database username
    pub database_username: String,
    /// Database password
    pub database_password: String,
    /// Database host
    pub database_host: String,
    /// Database port
    pub database_port: u16,
    /// Database name
    pub database_name: String,
    /// Database max connections
    pub database_max_connections: Option<u32>,
    /// Database connect timeout
    pub database_connect_timeout: Option<u64>,

    /// Short interval for high-frequency tables (seconds)
    pub short_interval: u64,
    /// Long interval for low-frequency tables (seconds)
    pub long_interval: u64,
    /// Data retention days
    pub retention_days: u32,

    /// Tables to collect data from (array of table configurations)
    pub tables: Vec<TableConfig>,

    /// TLS configuration
    pub tls: Option<TlsConfig>,

    /// TiDB topology fetch interval in seconds
    #[serde(default = "default_topology_fetch_interval")]
    pub topology_fetch_interval_seconds: f64,
}

/// Database connection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub username: String,
    pub password: String,
    pub host: String,
    pub port: u16,
    pub database: String,
    pub max_connections: Option<u32>,
    pub connect_timeout: Option<u64>,
}

/// Collection interval configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionConfig {
    /// Short interval for high-frequency tables (seconds)
    pub short_interval: u64,
    /// Long interval for low-frequency tables (seconds)
    pub long_interval: u64,
    /// Data retention days
    pub retention_days: u32,
}

/// Table configuration for data collection
#[derive(Debug, Clone, Serialize, Deserialize, ::vector_config::Configurable)]
pub struct TableConfig {
    /// Source schema name
    #[configurable(derived)]
    pub source_schema: String,
    /// Source table name
    #[configurable(derived)]
    pub source_table: String,
    /// Destination table name
    #[configurable(derived)]
    pub dest_table: String,
    /// Collection interval (short/long)
    #[configurable(derived)]
    pub collection_interval: String,
    /// Optional WHERE clause
    #[configurable(derived)]
    pub where_clause: Option<String>,
    /// Whether this table is enabled
    #[configurable(derived)]
    pub enabled: bool,
}

/// Collection interval type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CollectionInterval {
    /// Use short interval
    Short,
    /// Use long interval
    Long,
    /// Custom interval in seconds
    Custom(u64),
}

pub const fn default_topology_fetch_interval() -> f64 {
    30.0
}

impl GenerateConfig for SystemTablesConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            pd_address: "127.0.0.1:2379".to_owned(),
            tidb_group: None,
            database_username: "root".to_owned(),
            database_password: "".to_owned(),
            database_host: "127.0.0.1".to_owned(),
            database_port: 4000,
            database_name: "test".to_owned(),
            database_max_connections: Some(10),
            database_connect_timeout: Some(30),
            short_interval: 5,
            long_interval: 1800,
            retention_days: 7,
            tables: vec![
                TableConfig {
                    source_schema: "information_schema".to_owned(),
                    source_table: "PROCESSLIST".to_owned(),
                    dest_table: "hist_processlist".to_owned(),
                    collection_interval: "short".to_owned(),
                    where_clause: Some("command != 'Sleep'".to_owned()),
                    enabled: true,
                }
            ],
            tls: None,
            topology_fetch_interval_seconds: default_topology_fetch_interval(),
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "system_tables")]
impl SourceConfig for SystemTablesConfig {
    async fn build(&self, cx: SourceContext) -> vector::Result<Source> {
        let topology_fetch_interval = Duration::from_secs_f64(self.topology_fetch_interval_seconds);
        let pd_address = Some(self.pd_address.clone());
        let tidb_group = self.tidb_group.clone();
        
        // Create DatabaseConfig from flat fields
        let database_config = DatabaseConfig {
            username: self.database_username.clone(),
            password: self.database_password.clone(),
            host: self.database_host.clone(),
            port: self.database_port,
            database: self.database_name.clone(),
            max_connections: self.database_max_connections,
            connect_timeout: self.database_connect_timeout,
        };
        
        // Create CollectionConfig from flat fields
        let collection_config = CollectionConfig {
            short_interval: self.short_interval,
            long_interval: self.long_interval,
            retention_days: self.retention_days,
        };
        
        // Use tables directly from configuration
        let tables = self.tables.clone();
        
        let tls = self.tls.clone();

        Ok(Box::pin(async move {
            let controller = Controller::new(
                pd_address,
                tidb_group,
                topology_fetch_interval,
                database_config,
                collection_config,
                tables,
                tls,
                &cx.proxy,
                cx.out,
            )
            .await
            .map_err(|error| error!(message = "Source failed to initialize.", %error))?;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_config() {
        vector::test_util::test_generate_config::<SystemTablesConfig>();
    }
}
