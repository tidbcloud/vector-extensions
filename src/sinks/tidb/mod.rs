use std::time::Duration;

use futures_util::FutureExt;
use vector::{
    config::{GenerateConfig, SinkConfig, SinkContext},
    sinks::{Healthcheck, VectorSink as Sink},
};
use vector_lib::{
    config::{AcknowledgementsConfig, Input},
    configurable::configurable_component,
    sink::VectorSink,
    tls::TlsConfig,
};

use crate::sinks::tidb::sink::TiDBSink;

mod sink;

/// Configuration for the TiDB sink
#[configurable_component(sink("tidb"))]
#[derive(Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct TiDBConfig {
    /// Connection string for TiDB/MySQL database
    /// Format: mysql://user:password@host:port/database
    pub connection_string: String,

    /// Table name to insert data into
    pub table: String,

    /// Maximum number of connections in the connection pool
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,

    /// Connection timeout in seconds
    #[serde(default = "default_connection_timeout")]
    pub connection_timeout: u64,

    /// Batch size for inserting records
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// TLS configuration
    pub tls: Option<TlsConfig>,

    /// Acknowledgments configuration
    #[serde(
        default,
        deserialize_with = "vector::serde::bool_or_struct",
        skip_serializing_if = "vector::serde::is_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,
}

pub const fn default_max_connections() -> u32 {
    10
}

pub const fn default_connection_timeout() -> u64 {
    30
}

pub const fn default_batch_size() -> usize {
    1000
}

impl GenerateConfig for TiDBConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            connection_string: "mysql://user:password@localhost:4000/testdb".to_owned(),
            table: "logs".to_owned(),
            max_connections: default_max_connections(),
            connection_timeout: default_connection_timeout(),
            batch_size: default_batch_size(),
            tls: None,
            acknowledgements: Default::default(),
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "tidb")]
impl SinkConfig for TiDBConfig {
    async fn build(
        &self,
        _cx: SinkContext,
    ) -> vector::Result<(Sink, Healthcheck)> {
        let sink = TiDBSink::new(
            self.connection_string.clone(),
            self.table.clone(),
            self.max_connections,
            Duration::from_secs(self.connection_timeout),
            self.batch_size,
        )
        .await?;

        let healthcheck = healthcheck(
            self.connection_string.clone(),
            Duration::from_secs(self.connection_timeout),
        )
        .boxed();

        Ok((VectorSink::from_event_streamsink(sink), healthcheck))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

async fn healthcheck(
    connection_string: String,
    timeout: Duration,
) -> vector::Result<()> {
    use sqlx::mysql::MySqlPoolOptions;

    let pool = MySqlPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(timeout)
        .connect(&connection_string)
        .await
        .map_err(|e| vector::Error::from(format!("Failed to connect to database: {}", e)))?;

    // Execute a simple query to verify connection
    sqlx::query("SELECT 1")
        .execute(&pool)
        .await
        .map_err(|e| vector::Error::from(format!("Healthcheck failed: {}", e)))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_config() {
        vector::test_util::test_generate_config::<TiDBConfig>();
    }
}
