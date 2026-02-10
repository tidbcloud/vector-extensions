use std::path::PathBuf;
use std::time::Duration;

use vector::config::{GenerateConfig, SourceConfig, SourceContext};
use vector_lib::{
    config::{DataType, LogNamespace, SourceOutput},
    configurable::configurable_component,
    source::Source,
};

use crate::sources::delta_lake_watermark::controller::Controller;

mod checkpoint;
mod controller;
mod duckdb_query;

// Ensure the source is registered with typetag
// This is a no-op but ensures the module is loaded
#[allow(dead_code)]
fn _ensure_registered() {
    // The #[typetag::serde] attribute on the impl will register this source
}

/// Configuration for the delta_lake_watermark source
#[configurable_component(source("delta_lake_watermark"))]
#[derive(Debug, Clone)]
pub struct DeltaLakeWatermarkConfig {
    /// Delta Lake table endpoint (e.g., s3://bucket/path/to/delta_table)
    pub endpoint: String,

    /// Cloud provider: aws, gcp, azure, aliyun
    #[serde(default = "default_cloud_provider")]
    pub cloud_provider: String,

    /// Data directory for storing checkpoints
    pub data_dir: PathBuf,

    /// WHERE condition (SQL WHERE clause without WHERE keyword)
    /// Use this for all filtering including time ranges.
    /// Examples:
    ///   - Time range: "time >= 1717632000 AND time <= 1718044799"
    ///   - Business filter: "type = 'error' AND severity > 3"
    ///   - Combined: "time >= 1717632000 AND time <= 1718044799 AND type = 'error'"
    pub condition: Option<String>,

    /// Column name for ordering (typically a timestamp column)
    #[serde(default = "default_order_by_column")]
    pub order_by_column: String,

    /// Batch size for each query
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Poll interval in seconds (for streaming mode)
    #[serde(default = "default_poll_interval_secs")]
    pub poll_interval_secs: u64,

    /// Enable acknowledgements
    #[serde(default = "default_acknowledgements")]
    pub acknowledgements: bool,

    /// Unique ID column for handling same timestamp records.
    /// This column can be of any type (ID, UUID, string, integer, etc.) and is used for
    /// secondary sorting when multiple records share the same timestamp value.
    /// When provided, enables precise incremental sync with no duplicates and no missed data.
    /// Examples: "id", "uuid", "request_id", "record_id", etc.
    pub unique_id_column: Option<String>,

    /// DuckDB memory limit (e.g., "2GB")
    pub duckdb_memory_limit: Option<String>,
}

fn default_cloud_provider() -> String {
    "aws".to_string()
}

fn default_order_by_column() -> String {
    "time".to_string()
}

fn default_batch_size() -> usize {
    10000
}

fn default_poll_interval_secs() -> u64 {
    30
}

fn default_acknowledgements() -> bool {
    true
}

impl GenerateConfig for DeltaLakeWatermarkConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            endpoint: "s3://my-bucket/path/to/delta_table".to_string(),
            cloud_provider: default_cloud_provider(),
            data_dir: PathBuf::from("/var/lib/vector/checkpoints/"),
            condition: Some("time >= '2026-01-01T00:00:00Z' AND time <= '2026-02-01T00:00:00Z' AND type = 'error' AND severity > 3".to_string()),
            order_by_column: default_order_by_column(),
            batch_size: default_batch_size(),
            poll_interval_secs: default_poll_interval_secs(),
            acknowledgements: default_acknowledgements(),
            unique_id_column: Some("unique_id".to_string()),
            duckdb_memory_limit: Some("2GB".to_string()),
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "delta_lake_watermark")]
impl SourceConfig for DeltaLakeWatermarkConfig {
    async fn build(&self, cx: SourceContext) -> vector::Result<Source> {
        // Validate configuration
        self.validate()?;

        let endpoint = self.endpoint.clone();
        let cloud_provider = self.cloud_provider.clone();
        let data_dir = self.data_dir.clone();
        let condition = self.condition.clone();
        let order_by_column = self.order_by_column.clone();
        let batch_size = self.batch_size;
        let poll_interval = Duration::from_secs(self.poll_interval_secs);
        let acknowledgements = self.acknowledgements;
        let unique_id_column = self.unique_id_column.clone();
        let duckdb_memory_limit = self.duckdb_memory_limit.clone();

        // Clone values for the async block
        let endpoint_clone = endpoint.clone();
        let cloud_provider_clone = cloud_provider.clone();
        let data_dir_clone = data_dir.clone();
        let condition_clone = condition.clone();
        let order_by_column_clone = order_by_column.clone();
        let batch_size_clone = batch_size;
        let poll_interval_clone = poll_interval;
        let acknowledgements_clone = acknowledgements;
        let unique_id_column_clone = unique_id_column.clone();
        let duckdb_memory_limit_clone = duckdb_memory_limit.clone();
        let out_clone = cx.out;

        Ok(Box::pin(async move {
            let controller = Controller::new(
                endpoint_clone,
                cloud_provider_clone,
                data_dir_clone,
                condition_clone,
                order_by_column_clone,
                batch_size_clone,
                poll_interval_clone,
                acknowledgements_clone,
                unique_id_column_clone,
                duckdb_memory_limit_clone,
                out_clone,
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
        self.acknowledgements
    }
}

impl DeltaLakeWatermarkConfig {
    fn validate(&self) -> vector::Result<()> {
        // Validate cloud provider
        let valid_providers = ["aws", "gcp", "azure", "aliyun"];
        if !valid_providers.contains(&self.cloud_provider.as_str()) {
            return Err(format!(
                "Invalid cloud_provider: {}. Must be one of: {:?}",
                self.cloud_provider, valid_providers
            )
            .into());
        }

        // Validate endpoint format
        if !self.endpoint.starts_with("s3://")
            && !self.endpoint.starts_with("gs://")
            && !self.endpoint.starts_with("az://")
            && !self.endpoint.starts_with("oss://")
            && !self.endpoint.starts_with("file://")
            && !PathBuf::from(&self.endpoint).is_absolute()
        {
            return Err(format!(
                "Invalid endpoint format: {}. Must start with s3://, gs://, az://, oss://, file://, or be an absolute path",
                self.endpoint
            )
            .into());
        }

        // Validate batch size
        if self.batch_size == 0 {
            return Err("batch_size must be greater than 0".into());
        }

        // Warn if unique_id_column is not provided
        // This is not an error, but users should be aware of the implications
        if self.unique_id_column.is_none() {
            tracing::warn!(
                "unique_id_column is not provided. The source will use >= for checkpoint recovery, \
                which may cause duplicate processing of same-timestamp records after restart. \
                Consider providing unique_id_column (can be any type: ID, UUID, string, integer, etc.) \
                for precise incremental sync."
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn generate_config() {
        vector::test_util::test_generate_config::<DeltaLakeWatermarkConfig>();
    }

    // TC-001: Test configuration validation
    #[test]
    fn test_config_validation_valid() {
        let config = DeltaLakeWatermarkConfig {
            endpoint: "s3://bucket/path/to/table".to_string(),
            cloud_provider: "aws".to_string(),
            data_dir: PathBuf::from("/tmp"),
            condition: Some("time >= '2026-01-01T00:00:00Z' AND time <= '2026-02-01T00:00:00Z'".to_string()),
            order_by_column: "time".to_string(),
            batch_size: 1000,
            poll_interval_secs: 30,
            acknowledgements: true,
            unique_id_column: None,
            duckdb_memory_limit: None,
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation_invalid_cloud_provider() {
        let config = DeltaLakeWatermarkConfig {
            endpoint: "s3://bucket/path/to/table".to_string(),
            cloud_provider: "invalid".to_string(),
            data_dir: PathBuf::from("/tmp"),
            condition: None,
            order_by_column: "time".to_string(),
            batch_size: 1000,
            poll_interval_secs: 30,
            acknowledgements: true,
            unique_id_column: None,
            duckdb_memory_limit: None,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_invalid_endpoint() {
        let config = DeltaLakeWatermarkConfig {
            endpoint: "invalid-endpoint".to_string(),
            cloud_provider: "aws".to_string(),
            data_dir: PathBuf::from("/tmp"),
            condition: None,
            order_by_column: "time".to_string(),
            batch_size: 1000,
            poll_interval_secs: 30,
            acknowledgements: true,
            unique_id_column: None,
            duckdb_memory_limit: None,
        };
        assert!(config.validate().is_err());
    }

    // Note: TC-001 already covers validation tests
    // These tests are kept for backward compatibility but condition validation
    // is now handled by DuckDB query execution, not in config validation

    #[test]
    fn test_config_validation_zero_batch_size() {
        let config = DeltaLakeWatermarkConfig {
            endpoint: "s3://bucket/path/to/table".to_string(),
            cloud_provider: "aws".to_string(),
            data_dir: PathBuf::from("/tmp"),
            condition: None,
            order_by_column: "time".to_string(),
            batch_size: 0,
            poll_interval_secs: 30,
            acknowledgements: true,
            unique_id_column: None,
            duckdb_memory_limit: None,
        };
        assert!(config.validate().is_err());
    }

    // TC-002: Test default values
    #[test]
    fn test_default_values() {
        let config = DeltaLakeWatermarkConfig {
            endpoint: "s3://bucket/path".to_string(),
            cloud_provider: default_cloud_provider(),
            data_dir: PathBuf::from("/tmp"),
            condition: None,
            order_by_column: default_order_by_column(),
            batch_size: default_batch_size(),
            poll_interval_secs: default_poll_interval_secs(),
            acknowledgements: default_acknowledgements(),
            unique_id_column: None,
            duckdb_memory_limit: None,
        };
        assert_eq!(config.cloud_provider, "aws");
        assert_eq!(config.order_by_column, "time");
        assert_eq!(config.batch_size, 10000);
        assert_eq!(config.poll_interval_secs, 30);
        assert_eq!(config.acknowledgements, true);
    }

    // TC-003: Test GenerateConfig
    #[test]
    fn test_generate_config_produces_valid_toml() {
        let config_value = DeltaLakeWatermarkConfig::generate_config();
        assert!(config_value.is_table());
        
        let table = config_value.as_table().unwrap();
        assert!(table.contains_key("endpoint"));
        assert!(table.contains_key("cloud_provider"));
        assert!(table.contains_key("data_dir"));
    }

    #[test]
    fn test_valid_endpoint_formats() {
        let valid_endpoints = vec![
            "s3://bucket/path",
            "gs://bucket/path",
            "az://account/container/path",
            "oss://bucket/path",
            "file:///path/to/table",
            "/absolute/path/to/table",
        ];

        for endpoint in valid_endpoints {
            let config = DeltaLakeWatermarkConfig {
                endpoint: endpoint.to_string(),
                cloud_provider: "aws".to_string(),
                data_dir: PathBuf::from("/tmp"),
                condition: None,
                order_by_column: "time".to_string(),
                batch_size: 1000,
                poll_interval_secs: 30,
                acknowledgements: true,
                unique_id_column: None,
                duckdb_memory_limit: None,
            };
            assert!(config.validate().is_ok(), "Endpoint {} should be valid", endpoint);
        }
    }
}
