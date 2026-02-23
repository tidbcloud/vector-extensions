use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use serde_json::Value;

use crate::sources::system_tables::{CollectionConfig, DatabaseConfig, TableConfig};

/// Global counter for generating unique incremental IDs
static GLOBAL_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Error types for data collection
#[derive(Debug)]
pub enum CollectionError {
    ConnectionError(String),
    QueryError(String),
    ParseError(String),
    ConfigurationError(String),
    NetworkError(String),
}

impl fmt::Display for CollectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CollectionError::ConnectionError(msg) => write!(f, "Connection error: {}", msg),
            CollectionError::QueryError(msg) => write!(f, "Query error: {}", msg),
            CollectionError::ParseError(msg) => write!(f, "Parse error: {}", msg),
            CollectionError::ConfigurationError(msg) => write!(f, "Configuration error: {}", msg),
            CollectionError::NetworkError(msg) => write!(f, "Network error: {}", msg),
        }
    }
}

impl std::error::Error for CollectionError {}

/// Collection method type
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CollectionMethod {
    /// Traditional SQL-based collection via MySQL protocol
    Sql,
    /// gRPC coprocessor-based collection
    Coprocessor,
    /// HTTP API-based collection
    HttpApi,
    /// Custom gRPC service collection
    CustomGrpc,
    /// gRPC push-based collection (e.g., STATEMENTS_SUMMARY)
    GrpcPush,
    /// gRPC pull-based collection (SystemTablePullService::QueryTable)
    GrpcPull,
}

impl fmt::Display for CollectionMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CollectionMethod::Sql => write!(f, "sql"),
            CollectionMethod::Coprocessor => write!(f, "coprocessor"),
            CollectionMethod::HttpApi => write!(f, "http_api"),
            CollectionMethod::CustomGrpc => write!(f, "custom_grpc"),
            CollectionMethod::GrpcPush => write!(f, "grpc_push"),
            CollectionMethod::GrpcPull => write!(f, "grpc_pull"),
        }
    }
}

impl CollectionMethod {
    pub fn from_string(s: &str) -> Result<Self, CollectionError> {
        match s.to_lowercase().as_str() {
            "sql" => Ok(CollectionMethod::Sql),
            "coprocessor" => Ok(CollectionMethod::Coprocessor),
            "http_api" | "http" => Ok(CollectionMethod::HttpApi),
            "custom_grpc" | "grpc" => Ok(CollectionMethod::CustomGrpc),
            "grpc_push" | "push" | "statement_v3" | "v3" | "v3_push" => Ok(CollectionMethod::GrpcPush),
            "grpc_pull" | "pull" => Ok(CollectionMethod::GrpcPull),
            _ => Err(CollectionError::ConfigurationError(format!(
                "Unknown collection method: {}. Supported: sql, coprocessor, http_api, custom_grpc, grpc_push, grpc_pull",
                s
            ))),
        }
    }
}

/// Metadata about the collection process
#[derive(Debug, Clone)]
pub struct CollectionMetadata {
    /// Instance identifier
    pub instance: String,
    /// Table configuration
    pub table_config: TableConfig,
    /// Collection method used
    pub collection_method: CollectionMethod,
    /// Collection timestamp
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Number of rows collected
    pub row_count: usize,
    /// Collection duration in milliseconds
    pub duration_ms: u64,
    /// Additional metadata
    pub extra: HashMap<String, Value>,
}

/// Result of a data collection operation
#[derive(Debug)]
pub struct CollectionResult {
    /// Collected data rows
    pub data: Vec<HashMap<String, Value>>,
    /// Collection metadata
    pub metadata: CollectionMetadata,
}

/// Configuration for collection process
#[derive(Debug, Clone)]
pub struct CollectorConfig {
    /// Instance identifier
    pub instance: String,
    /// Collector-specific configuration
    pub config_type: CollectorConfigType,
}

/// Collector-specific configuration variants
#[derive(Debug, Clone)]
pub enum CollectorConfigType {
    /// SQL collector configuration
    Sql { database_config: DatabaseConfig },
    /// Coprocessor collector configuration
    Coprocessor {
        host: String,
        port: u16,
        #[allow(dead_code)]
        grpc_timeout_secs: u64,
        #[allow(dead_code)]
        max_retries: u32,
        /// TLS configuration for HTTP schema fetching
        tls: Option<crate::sources::system_tables::TlsConfig>,
    },
    /// HTTP API collector configuration
    HttpApi {
        #[allow(dead_code)]
        host: String,
        #[allow(dead_code)]
        port: u16,
        #[allow(dead_code)]
        timeout_secs: u64,
        #[allow(dead_code)]
        max_retries: u32,
    },
    /// gRPC push-based collector configuration
    GrpcPush {
        /// TiDB status host (for RegisterPushTarget)
        host: String,
        /// TiDB status port (gRPC)
        status_port: u16,
        /// Vector's gRPC listen address for receiving push data
        vector_grpc_address: String,
        /// Vector's gRPC listen port
        vector_grpc_port: u16,
        /// gRPC timeout
        grpc_timeout_secs: u64,
        /// Max retries
        max_retries: u32,
    },
    /// gRPC pull-based collector configuration
    GrpcPull {
        /// TiDB status host
        host: String,
        /// TiDB status port (gRPC)
        status_port: u16,
        /// gRPC timeout
        grpc_timeout_secs: u64,
        /// Max retries
        max_retries: u32,
    },
}

impl CollectorConfig {
    /// Create configuration for SQL collector
    pub fn for_sql(instance: String, database_config: DatabaseConfig) -> Self {
        Self {
            instance,
            config_type: CollectorConfigType::Sql { database_config },
        }
    }

    /// Create configuration for Coprocessor collector
    pub fn for_coprocessor(
        instance: String,
        host: String,
        port: u16,
        grpc_timeout_secs: Option<u64>,
        max_retries: Option<u32>,
        tls: Option<crate::sources::system_tables::TlsConfig>,
    ) -> Self {
        Self {
            instance,
            config_type: CollectorConfigType::Coprocessor {
                host,
                port,
                grpc_timeout_secs: grpc_timeout_secs.unwrap_or(30),
                max_retries: max_retries.unwrap_or(3),
                tls,
            },
        }
    }

    /// Create configuration for HTTP API collector
    pub fn for_http_api(
        instance: String,
        host: String,
        port: u16,
        timeout_secs: Option<u64>,
        max_retries: Option<u32>,
    ) -> Self {
        Self {
            instance,
            config_type: CollectorConfigType::HttpApi {
                host,
                port,
                timeout_secs: timeout_secs.unwrap_or(30),
                max_retries: max_retries.unwrap_or(3),
            },
        }
    }

    /// Create configuration for gRPC push collector
    pub fn for_grpc_push(
        instance: String,
        host: String,
        status_port: u16,
        vector_grpc_address: String,
        vector_grpc_port: u16,
        grpc_timeout_secs: Option<u64>,
        max_retries: Option<u32>,
    ) -> Self {
        Self {
            instance,
            config_type: CollectorConfigType::GrpcPush {
                host,
                status_port,
                vector_grpc_address,
                vector_grpc_port,
                grpc_timeout_secs: grpc_timeout_secs.unwrap_or(30),
                max_retries: max_retries.unwrap_or(3),
            },
        }
    }

    /// Create configuration for gRPC pull collector
    pub fn for_grpc_pull(
        instance: String,
        host: String,
        status_port: u16,
        grpc_timeout_secs: Option<u64>,
        max_retries: Option<u32>,
    ) -> Self {
        Self {
            instance,
            config_type: CollectorConfigType::GrpcPull {
                host,
                status_port,
                grpc_timeout_secs: grpc_timeout_secs.unwrap_or(30),
                max_retries: max_retries.unwrap_or(3),
            },
        }
    }
}

/// Abstract trait for data collectors
#[async_trait]
pub trait DataCollector: Send + Sync + 'static {
    /// Get the collection method this collector supports
    fn collection_method(&self) -> CollectionMethod;

    /// Check if this collector can handle the given table
    fn can_collect_table(&self, table: &TableConfig) -> bool;

    /// Initialize the collector (e.g., establish connections, verify config)
    async fn initialize(&mut self) -> Result<(), CollectionError>;

    /// Collect data from a single table
    async fn collect_table_data(
        &self,
        table: &TableConfig,
    ) -> Result<CollectionResult, CollectionError>;

    /// Get collector health status
    async fn health_check(&self) -> Result<(), CollectionError>;

    /// Set output sender for push-based collectors (optional)
    /// If implemented, the collector will send events directly instead of returning them
    fn set_output_sender(&mut self, _sender: vector::SourceSender) {}
}

/// Utility functions for collection
pub mod utils {
    use super::*;
    use vector_lib::event::{Event, LogEvent};

    /// Create a Vector event from collection result
    pub fn create_event_from_result(
        result: &CollectionResult,
        row_data: HashMap<String, Value>,
    ) -> Event {
        let mut event = Event::Log(LogEvent::default());
        let log = event.as_mut_log();

        // Generate unique incremental ID
        let unique_id = GLOBAL_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        log.insert(
            "_vector_id",
            Value::Number(serde_json::Number::from(unique_id)),
        );

        // Add standard metadata
        log.insert(
            "_vector_table",
            result.metadata.table_config.dest_table.clone(),
        );
        log.insert(
            "_vector_source_table",
            result.metadata.table_config.source_table.clone(),
        );
        log.insert(
            "_vector_source_schema",
            result.metadata.table_config.source_schema.clone(),
        );
        log.insert("_vector_instance", result.metadata.instance.clone());
        log.insert("_vector_timestamp", result.metadata.timestamp.to_rfc3339());
        log.insert(
            "_vector_collection_method",
            result.metadata.collection_method.to_string(),
        );

        // Add performance metadata
        log.insert(
            "_vector_collection_duration_ms",
            result.metadata.duration_ms as i64,
        );
        log.insert("_vector_row_count", result.metadata.row_count as i64);

        // Add extra metadata
        for (key, value) in &result.metadata.extra {
            if key == "schema_metadata" {
                // Add schema metadata directly as _schema_metadata for DeltaLake writer
                log.insert("_schema_metadata", value.clone());
            }
            // Intentionally skip writing generic _vector_meta_* fields
        }

        // Add the actual row data
        for (key, value) in row_data {
            log.insert(key.as_str(), value);
        }

        // For non-cluster tables, add instance column to the actual data
        if !result
            .metadata
            .table_config
            .source_table
            .starts_with("CLUSTER_")
        {
            log.insert("instance", result.metadata.instance.clone());
        }

        event
    }

    /// Parse collection interval
    pub fn parse_collection_interval(
        interval_str: &str,
        collection_config: &CollectionConfig,
    ) -> u64 {
        match interval_str {
            "short" => collection_config.short_interval,
            "long" => collection_config.long_interval,
            custom if custom.starts_with("custom=") => {
                if let Some(seconds) = custom.strip_prefix("custom=") {
                    seconds
                        .parse::<u64>()
                        .unwrap_or(collection_config.short_interval)
                } else {
                    collection_config.short_interval
                }
            }
            _ => collection_config.short_interval,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::system_tables::{CollectionConfig, TableConfig};
    use vector_lib::event::Event;

    #[test]
    fn test_collection_method_from_string() {
        assert!(matches!(
            CollectionMethod::from_string("sql").unwrap(),
            CollectionMethod::Sql
        ));
        assert!(matches!(
            CollectionMethod::from_string("coprocessor").unwrap(),
            CollectionMethod::Coprocessor
        ));
        assert!(matches!(
            CollectionMethod::from_string("http_api").unwrap(),
            CollectionMethod::HttpApi
        ));
        assert!(matches!(
            CollectionMethod::from_string("custom_grpc").unwrap(),
            CollectionMethod::CustomGrpc
        ));
        assert!(CollectionMethod::from_string("invalid").is_err());
    }

    #[test]
    fn test_parse_collection_interval() {
        let config = CollectionConfig {
            short_interval: 5,
            long_interval: 1800,
            retention_days: 7,
        };

        assert_eq!(utils::parse_collection_interval("short", &config), 5);
        assert_eq!(utils::parse_collection_interval("long", &config), 1800);
        assert_eq!(utils::parse_collection_interval("custom=600", &config), 600);
        assert_eq!(
            utils::parse_collection_interval("custom=invalid", &config),
            5
        );
        assert_eq!(utils::parse_collection_interval("unknown", &config), 5);
    }

    #[test]
    fn test_event_creation_metadata() {
        let mut row_data = HashMap::new();
        row_data.insert(
            "DIGEST".to_string(),
            serde_json::Value::String("test_digest".to_string()),
        );
        row_data.insert(
            "EXEC_COUNT".to_string(),
            serde_json::Value::Number(serde_json::Number::from(100)),
        );

        let metadata = CollectionMetadata {
            instance: "test_instance".to_string(),
            table_config: TableConfig {
                source_schema: "metrics_schema".to_string(),
                source_table: "CLUSTER_STATEMENTS_SUMMARY".to_string(),
                dest_table: "hist_cluster_statements_summary".to_string(),
                collection_interval: "short".to_string(),
                where_clause: None,
                enabled: true,
                collection_method: None,
            },
            collection_method: CollectionMethod::Coprocessor,
            timestamp: chrono::Utc::now(),
            row_count: 1,
            duration_ms: 150,
            extra: HashMap::new(),
        };

        let collection_result = CollectionResult {
            data: vec![row_data.clone()],
            metadata,
        };

        let event = utils::create_event_from_result(&collection_result, row_data);
        let log_event = match event {
            Event::Log(log_event) => log_event,
            _ => panic!("Expected Log event"),
        };

        assert!(log_event.get("_vector_id").is_some());
        assert!(log_event.get("_vector_table").is_some());
        assert!(log_event.get("_vector_instance").is_some());
        assert!(log_event.get("DIGEST").is_some());
    }
}
