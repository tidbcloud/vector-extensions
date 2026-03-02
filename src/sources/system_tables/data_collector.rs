use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use serde_json::Value;

use crate::sources::system_tables::{
    CollectionConfig as VectorCollectionConfig, DatabaseConfig, TableConfig,
};

/// Re-export proto CollectionConfig for use in CollectionPolicyConfig
pub use crate::sources::system_tables::collectors::grpc_push_collector::proto::CollectionConfig as ProtoCollectionConfig;

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

/// Configuration for collection policy (14 parameters)
#[derive(Debug, Clone)]
pub struct CollectionPolicyConfig {
    /// Aggregation window duration in seconds
    pub aggregation_window_secs: i32,
    /// Push batch size
    pub push_batch_size: i32,
    /// Push interval in seconds
    pub push_interval_secs: i32,
    /// Push timeout in seconds
    pub push_timeout_secs: i32,
    /// Max digests per aggregation window
    pub max_digests_per_window: i32,
    /// Max memory bytes for statement summary
    pub max_memory_bytes: i64,
    /// Whether to collect internal queries
    pub enable_internal_query: bool,
    /// Eviction strategy: "drop_new", "evict_lru", "aggregate_to_other"
    pub eviction_strategy: String,
    /// Early flush threshold (0.0-1.0)
    pub early_flush_threshold: f64,
    /// Retry max attempts
    pub retry_max_attempts: i32,
    /// Retry initial delay in ms
    pub retry_initial_delay_ms: i32,
    /// Retry max delay in ms
    pub retry_max_delay_ms: i32,
    /// Backpressure throttle threshold (0.0-1.0), default 0.8
    pub backpressure_throttle_threshold: f64,
    /// Backpressure reject threshold (0.0-1.0), default 0.95
    pub backpressure_reject_threshold: f64,
}

impl Default for CollectionPolicyConfig {
    fn default() -> Self {
        Self {
            aggregation_window_secs: 10,
            push_batch_size: 100,
            push_interval_secs: 10,
            push_timeout_secs: 30,
            max_digests_per_window: 1000,
            max_memory_bytes: 64 * 1024 * 1024,
            enable_internal_query: false,
            eviction_strategy: "aggregate_to_other".to_string(),
            early_flush_threshold: 0.8,
            retry_max_attempts: 3,
            retry_initial_delay_ms: 100,
            retry_max_delay_ms: 10000,
            backpressure_throttle_threshold: 0.8,
            backpressure_reject_threshold: 0.95,
        }
    }
}

impl CollectionPolicyConfig {
    /// Convert to proto CollectionConfig
    pub fn to_proto(&self) -> ProtoCollectionConfig {
        ProtoCollectionConfig {
            aggregation_window_secs: self.aggregation_window_secs,
            push_batch_size: self.push_batch_size,
            push_interval_secs: self.push_interval_secs,
            push_timeout_secs: self.push_timeout_secs,
            max_digests_per_window: self.max_digests_per_window,
            max_memory_bytes: self.max_memory_bytes,
            enable_internal_query: self.enable_internal_query,
            eviction_strategy: self.eviction_strategy.clone(),
            early_flush_threshold: self.early_flush_threshold,
            retry_max_attempts: self.retry_max_attempts,
            retry_initial_delay_ms: self.retry_initial_delay_ms,
            retry_max_delay_ms: self.retry_max_delay_ms,
            extended_metrics: vec![],
            config_version: 1,
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
        /// Rate limit (requests per second), 0 = unlimited
        rate_limit: u32,
        /// Backpressure threshold (0.0-1.0), triggers throttle at this load
        backpressure_threshold: f64,
        /// Backpressure reject threshold (0.0-1.0), rejects at this load
        backpressure_reject_threshold: f64,
        /// Collection policy configuration
        collection_policy: CollectionPolicyConfig,
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
        rate_limit: Option<u32>,
        backpressure_threshold: Option<f64>,
        backpressure_reject_threshold: Option<f64>,
        collection_policy: Option<CollectionPolicyConfig>,
    ) -> Self {
        let policy = collection_policy.unwrap_or_default();
        Self {
            instance,
            config_type: CollectorConfigType::GrpcPush {
                host,
                status_port,
                vector_grpc_address,
                vector_grpc_port,
                grpc_timeout_secs: grpc_timeout_secs.unwrap_or(30),
                max_retries: max_retries.unwrap_or(3),
                rate_limit: rate_limit.unwrap_or(0),
                backpressure_threshold: backpressure_threshold
                    .unwrap_or(policy.backpressure_throttle_threshold),
                backpressure_reject_threshold: backpressure_reject_threshold
                    .unwrap_or(policy.backpressure_reject_threshold),
                collection_policy: policy,
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
        collection_config: &VectorCollectionConfig,
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

    /// Build gRPC push collection policy from table interval settings.
    ///
    /// Both aggregation window and push interval are aligned to the table interval
    /// so TiDB flush cadence matches vector table config (e.g. long=60s).
    pub fn build_grpc_push_collection_policy(
        interval_str: &str,
        collection_config: &VectorCollectionConfig,
    ) -> CollectionPolicyConfig {
        let interval_secs = parse_collection_interval(interval_str, collection_config);
        let interval_i32 = if interval_secs > i32::MAX as u64 {
            i32::MAX
        } else {
            interval_secs as i32
        };

        let mut policy = CollectionPolicyConfig::default();
        policy.max_digests_per_window = collection_config.stmt_summary_max_digests_per_window;
        policy.max_memory_bytes = collection_config.stmt_summary_max_memory_bytes;
        policy.aggregation_window_secs = interval_i32;
        policy.push_interval_secs = interval_i32;
        policy
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::system_tables::{CollectionConfig as VectorCollectionConfig, TableConfig};
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
        let config = VectorCollectionConfig {
            short_interval: 5,
            long_interval: 1800,
            retention_days: 7,
            stmt_summary_max_digests_per_window: 200_000,
            stmt_summary_max_memory_bytes: 512 * 1024 * 1024,
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
    fn test_build_grpc_push_collection_policy_uses_table_interval() {
        let config = VectorCollectionConfig {
            short_interval: 10,
            long_interval: 60,
            retention_days: 7,
            stmt_summary_max_digests_per_window: 321_000,
            stmt_summary_max_memory_bytes: 768 * 1024 * 1024,
        };

        let policy = utils::build_grpc_push_collection_policy("long", &config);
        assert_eq!(policy.aggregation_window_secs, 60);
        assert_eq!(policy.push_interval_secs, 60);
        assert_eq!(policy.max_digests_per_window, 321_000);
        assert_eq!(policy.max_memory_bytes, 768 * 1024 * 1024);

        let custom_policy = utils::build_grpc_push_collection_policy("custom=120", &config);
        assert_eq!(custom_policy.aggregation_window_secs, 120);
        assert_eq!(custom_policy.push_interval_secs, 120);
        assert_eq!(custom_policy.max_digests_per_window, 321_000);
        assert_eq!(custom_policy.max_memory_bytes, 768 * 1024 * 1024);
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
