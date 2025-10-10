use std::env;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use vector::config::{GenerateConfig, SourceConfig, SourceContext};
use vector_lib::{
    config::{DataType, LogNamespace, SourceOutput},
    configurable::configurable_component,
    source::Source,
    tls::TlsConfig,
};

use crate::sources::system_tables::controller::Controller;

// New abstracted collectors
mod collector_factory;
mod collectors;
mod data_collector;

// Main controller
mod controller;

/// Environment variable names for database configuration
pub struct DatabaseEnvVars;

impl DatabaseEnvVars {
    pub const USERNAME: &'static str = "TIDB_USERNAME";
    pub const PASSWORD: &'static str = "TIDB_PASSWORD";
    pub const HOST: &'static str = "TIDB_HOST";
    pub const PORT: &'static str = "TIDB_PORT";
    pub const DATABASE: &'static str = "TIDB_DATABASE";
    pub const MAX_CONNECTIONS: &'static str = "TIDB_MAX_CONNECTIONS";
    pub const CONNECT_TIMEOUT: &'static str = "TIDB_CONNECT_TIMEOUT";

    // TLS related environment variables
    pub const TLS_CA_FILE: &'static str = "TIDB_TLS_CA_FILE";
    pub const TLS_CERT_FILE: &'static str = "TIDB_TLS_CERT_FILE";
    pub const TLS_KEY_FILE: &'static str = "TIDB_TLS_KEY_FILE";
    pub const TLS_VERIFY_CERTIFICATE: &'static str = "TIDB_TLS_VERIFY_CERTIFICATE";
    pub const TLS_VERIFY_HOSTNAME: &'static str = "TIDB_TLS_VERIFY_HOSTNAME";

    // PD/Topology related environment variables
    pub const PD_ADDRESS: &'static str = "PD_ADDRESS";
    pub const TIDB_GROUP: &'static str = "TIDB_GROUP";
    pub const LABEL_K8S_INSTANCE: &'static str = "LABEL_K8S_INSTANCE";

    // PD TLS environment variables
    pub const PD_TLS_CA_FILE: &'static str = "PD_TLS_CA_FILE";
    pub const PD_TLS_CERT_FILE: &'static str = "PD_TLS_CERT_FILE";
    pub const PD_TLS_KEY_FILE: &'static str = "PD_TLS_KEY_FILE";
    pub const PD_TLS_VERIFY_CERTIFICATE: &'static str = "PD_TLS_VERIFY_CERTIFICATE";
    pub const PD_TLS_VERIFY_HOSTNAME: &'static str = "PD_TLS_VERIFY_HOSTNAME";

    // Collection configuration environment variables
    pub const SHORT_INTERVAL: &'static str = "SYSTEM_TABLES_SHORT_INTERVAL";
    pub const LONG_INTERVAL: &'static str = "SYSTEM_TABLES_LONG_INTERVAL";
    pub const RETENTION_DAYS: &'static str = "SYSTEM_TABLES_RETENTION_DAYS";
    pub const TOPOLOGY_FETCH_INTERVAL: &'static str = "TOPOLOGY_FETCH_INTERVAL_SECONDS";
    pub const COLLECTION_METHOD: &'static str = "SYSTEM_TABLES_COLLECTION_METHOD";
}

/// Configuration for the system_tables source
#[configurable_component(source("system_tables"))]
#[derive(Debug, Clone)]
pub struct SystemTablesConfig {
    /// PD address for legacy mode (to discover TiDB instances)
    pub pd_address: Option<String>,

    /// TiDB group name for nextgen mode
    pub tidb_group: Option<String>,

    /// Kubernetes instance label for nextgen mode
    pub label_k8s_instance: Option<String>,

    /// Database username (required for SQL collection method, optional for coprocessor)
    pub database_username: Option<String>,
    /// Database password (required for SQL collection method, optional for coprocessor)
    pub database_password: Option<String>,
    /// Database host (required for SQL collection method, optional for coprocessor)
    pub database_host: Option<String>,
    /// Database port (required for SQL collection method, optional for coprocessor)
    pub database_port: Option<u16>,
    /// Database name (required for SQL collection method, optional for coprocessor)
    pub database_name: Option<String>,
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

    /// TLS configuration for PD/etcd connections
    pub pd_tls: Option<TlsConfig>,

    /// TLS configuration for database connections
    pub database_tls: Option<TlsConfig>,

    /// TiDB topology fetch interval in seconds
    #[serde(default = "default_topology_fetch_interval")]
    pub topology_fetch_interval_seconds: f64,

    /// Collection method: "coprocessor" for gRPC coprocessor-based collection (default), "sql" for SQL-based collection
    #[serde(default = "default_collection_method")]
    pub collection_method: String,
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
    pub tls: Option<TlsConfig>,
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

pub fn default_collection_method() -> String {
    "coprocessor".to_string()
}

/// Helper functions for reading environment variables
impl SystemTablesConfig {
    /// Validate configuration based on collection method
    pub fn validate(&self) -> vector::Result<()> {
        match self.collection_method.to_lowercase().as_str() {
            "sql" => {
                // For SQL collection method, database fields are required
                if self.database_username.is_none() {
                    return Err("missing field `database_username` in `sources.tidb_system_tables` (required for SQL collection method)".into());
                }
                if self.database_password.is_none() {
                    return Err("missing field `database_password` in `sources.tidb_system_tables` (required for SQL collection method)".into());
                }
                if self.database_host.is_none() {
                    return Err("missing field `database_host` in `sources.tidb_system_tables` (required for SQL collection method)".into());
                }
                if self.database_port.is_none() {
                    return Err("missing field `database_port` in `sources.tidb_system_tables` (required for SQL collection method)".into());
                }
                if self.database_name.is_none() {
                    return Err("missing field `database_name` in `sources.tidb_system_tables` (required for SQL collection method)".into());
                }
            }
            "coprocessor" | "http_api" | "custom_grpc" => {
                // For coprocessor and other methods, database fields are optional
                // These methods use gRPC/HTTP to communicate directly with TiKV/PD
                info!("Using {} collection method - database connection fields are optional", self.collection_method);
            }
            _ => {
                return Err(format!("unsupported collection method: {}. Supported methods: sql, coprocessor, http_api, custom_grpc", self.collection_method).into());
            }
        }
        Ok(())
    }
    /// Helper function to build TLS configuration from environment variables
    fn build_tls_config_from_env(
        ca_file_env: &str,
        cert_file_env: &str,
        key_file_env: &str,
        verify_cert_env: &str,
        verify_hostname_env: &str,
    ) -> Option<TlsConfig> {
        let ca_file = env::var(ca_file_env).ok().map(|p| p.into());
        let crt_file = env::var(cert_file_env).ok().map(|p| p.into());
        let key_file = env::var(key_file_env).ok().map(|p| p.into());
        let verify_certificate = env::var(verify_cert_env).ok().and_then(|s| s.parse().ok());
        let verify_hostname = env::var(verify_hostname_env)
            .ok()
            .and_then(|s| s.parse().ok());

        // Only create TLS config if at least one TLS-related env var is set
        if ca_file.is_some() || crt_file.is_some() || key_file.is_some() {
            Some(TlsConfig {
                ca_file,
                crt_file,
                key_file,
                verify_certificate,
                verify_hostname,
                ..Default::default()
            })
        } else {
            None
        }
    }

    /// Merge configuration with values from environment variables
    /// Environment variables take precedence over configuration file values
    pub fn merge_with_env(&mut self) {
        // Override with environment variables if they exist
        if let Ok(val) = env::var(DatabaseEnvVars::PD_ADDRESS) {
            self.pd_address = Some(val);
        }
        if let Ok(val) = env::var(DatabaseEnvVars::TIDB_GROUP) {
            self.tidb_group = Some(val);
        }
        if let Ok(val) = env::var(DatabaseEnvVars::LABEL_K8S_INSTANCE) {
            self.label_k8s_instance = Some(val);
        }
        if let Ok(val) = env::var(DatabaseEnvVars::USERNAME) {
            self.database_username = Some(val);
        }
        if let Ok(val) = env::var(DatabaseEnvVars::PASSWORD) {
            self.database_password = Some(val);
        }
        if let Ok(val) = env::var(DatabaseEnvVars::HOST) {
            self.database_host = Some(val);
        }
        if let Ok(val) = env::var(DatabaseEnvVars::PORT) {
            if let Ok(port) = val.parse() {
                self.database_port = Some(port);
            }
        }
        if let Ok(val) = env::var(DatabaseEnvVars::DATABASE) {
            self.database_name = Some(val);
        }
        if let Ok(val) = env::var(DatabaseEnvVars::MAX_CONNECTIONS) {
            if let Ok(connections) = val.parse() {
                self.database_max_connections = Some(connections);
            }
        }
        if let Ok(val) = env::var(DatabaseEnvVars::CONNECT_TIMEOUT) {
            if let Ok(timeout) = val.parse() {
                self.database_connect_timeout = Some(timeout);
            }
        }
        if let Ok(val) = env::var(DatabaseEnvVars::SHORT_INTERVAL) {
            if let Ok(interval) = val.parse() {
                self.short_interval = interval;
            }
        }
        if let Ok(val) = env::var(DatabaseEnvVars::LONG_INTERVAL) {
            if let Ok(interval) = val.parse() {
                self.long_interval = interval;
            }
        }
        if let Ok(val) = env::var(DatabaseEnvVars::RETENTION_DAYS) {
            if let Ok(days) = val.parse() {
                self.retention_days = days;
            }
        }
        if let Ok(val) = env::var(DatabaseEnvVars::TOPOLOGY_FETCH_INTERVAL) {
            if let Ok(interval) = val.parse() {
                self.topology_fetch_interval_seconds = interval;
            }
        }
        if let Ok(val) = env::var(DatabaseEnvVars::COLLECTION_METHOD) {
            self.collection_method = val;
        }

        // Merge TLS configurations
        if let Some(env_tls) = Self::build_tls_config_from_env(
            DatabaseEnvVars::TLS_CA_FILE,
            DatabaseEnvVars::TLS_CERT_FILE,
            DatabaseEnvVars::TLS_KEY_FILE,
            DatabaseEnvVars::TLS_VERIFY_CERTIFICATE,
            DatabaseEnvVars::TLS_VERIFY_HOSTNAME,
        ) {
            self.database_tls = Some(env_tls);
        }

        if let Some(env_pd_tls) = Self::build_tls_config_from_env(
            DatabaseEnvVars::PD_TLS_CA_FILE,
            DatabaseEnvVars::PD_TLS_CERT_FILE,
            DatabaseEnvVars::PD_TLS_KEY_FILE,
            DatabaseEnvVars::PD_TLS_VERIFY_CERTIFICATE,
            DatabaseEnvVars::PD_TLS_VERIFY_HOSTNAME,
        ) {
            self.pd_tls = Some(env_pd_tls);
        }
    }
}

impl GenerateConfig for SystemTablesConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            pd_address: Some("127.0.0.1:2379".to_owned()),
            tidb_group: None,
            label_k8s_instance: None,
            database_username: Some("root".to_owned()),
            database_password: Some("".to_owned()),
            database_host: Some("127.0.0.1".to_owned()),
            database_port: Some(4000),
            database_name: Some("test".to_owned()),
            database_max_connections: Some(10),
            database_connect_timeout: Some(30),
            short_interval: 5,
            long_interval: 1800,
            retention_days: 7,
            tables: vec![TableConfig {
                source_schema: "information_schema".to_owned(),
                source_table: "PROCESSLIST".to_owned(),
                dest_table: "hist_processlist".to_owned(),
                collection_interval: "short".to_owned(),
                where_clause: Some("command != 'Sleep'".to_owned()),
                enabled: true,
            }],
            pd_tls: None,
            database_tls: None,
            topology_fetch_interval_seconds: default_topology_fetch_interval(),
            collection_method: default_collection_method(),
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "system_tables")]
impl SourceConfig for SystemTablesConfig {
    async fn build(&self, cx: SourceContext) -> vector::Result<Source> {
        // Clone configuration and merge with environment variables
        // Environment variables take precedence over config file values
        let mut config = self.clone();
        config.merge_with_env();

        // Validate configuration based on collection method
        config.validate()?;

        info!("Building system_tables source with configuration:");
        if let (Some(ref host), Some(port), Some(ref database)) = (&config.database_host, config.database_port, &config.database_name) {
            info!("  Database: {}:{}/{}", host, port, database);
        } else {
            info!("  Database: Not configured (using coprocessor method)");
        }
        if let Some(ref username) = config.database_username {
            info!("  Username: {}", username);
        } else {
            info!("  Username: Not configured (using coprocessor method)");
        }
        info!("  Max connections: {:?}", config.database_max_connections);
        info!("  Connect timeout: {:?}", config.database_connect_timeout);
        info!("  Database TLS enabled: {}", config.database_tls.is_some());
        if let Some(ref pd_addr) = config.pd_address {
            info!("  PD address: {}", pd_addr);
        }
        info!("  PD TLS enabled: {}", config.pd_tls.is_some());
        info!("  Tables configured: {}", config.tables.len());

        let topology_fetch_interval =
            Duration::from_secs_f64(config.topology_fetch_interval_seconds);
        let pd_address = config.pd_address.clone();
        let tidb_group = config.tidb_group.clone();
        let label_k8s_instance = config.label_k8s_instance.clone();

        // Create DatabaseConfig from merged configuration only if using SQL collection method
        let database_config = if config.collection_method.to_lowercase() == "sql" {
            DatabaseConfig {
                username: config.database_username.clone().unwrap_or_default(),
                password: config.database_password.clone().unwrap_or_default(),
                host: config.database_host.clone().unwrap_or_default(),
                port: config.database_port.unwrap_or(4000),
                database: config.database_name.clone().unwrap_or_default(),
                max_connections: config.database_max_connections,
                connect_timeout: config.database_connect_timeout,
                tls: config.database_tls.clone(),
            }
        } else {
            // For non-SQL collection methods (coprocessor, etc.), use dummy database config
            // This config won't be used but is required by the Controller constructor
            DatabaseConfig {
                username: "unused".to_string(),
                password: "unused".to_string(),
                host: "unused".to_string(),
                port: 0,
                database: "unused".to_string(),
                max_connections: None,
                connect_timeout: None,
                tls: None,
            }
        };

        // Create CollectionConfig from merged configuration
        let collection_config = CollectionConfig {
            short_interval: config.short_interval,
            long_interval: config.long_interval,
            retention_days: config.retention_days,
        };

        // Use tables from merged configuration
        let tables = config.tables.clone();

        let pd_tls = config.pd_tls.clone();
        let collection_method = config.collection_method.clone();

        Ok(Box::pin(async move {
            info!("Using system tables controller with abstracted collectors");
            let controller = Controller::new(
                pd_address,
                tidb_group,
                label_k8s_instance,
                topology_fetch_interval,
                database_config,
                collection_config,
                tables,
                pd_tls,
                &cx.proxy,
                cx.out,
                collection_method,
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
