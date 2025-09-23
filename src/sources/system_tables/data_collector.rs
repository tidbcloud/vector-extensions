use std::collections::HashMap;
use std::fmt;

use async_trait::async_trait;
use serde_json::Value;
use vector::SourceSender;

use crate::sources::system_tables::{CollectionConfig, DatabaseConfig, TableConfig};

/// Error types for data collection
#[derive(Debug)]
pub enum CollectionError {
    ConnectionError(String),
    QueryError(String),
    ParseError(String),
    ConfigurationError(String),
    TimeoutError(String),
    AuthenticationError(String),
    NetworkError(String),
}

impl fmt::Display for CollectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CollectionError::ConnectionError(msg) => write!(f, "Connection error: {}", msg),
            CollectionError::QueryError(msg) => write!(f, "Query error: {}", msg),
            CollectionError::ParseError(msg) => write!(f, "Parse error: {}", msg),
            CollectionError::ConfigurationError(msg) => write!(f, "Configuration error: {}", msg),
            CollectionError::TimeoutError(msg) => write!(f, "Timeout error: {}", msg),
            CollectionError::AuthenticationError(msg) => write!(f, "Authentication error: {}", msg),
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
}

impl fmt::Display for CollectionMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CollectionMethod::Sql => write!(f, "sql"),
            CollectionMethod::Coprocessor => write!(f, "coprocessor"),
            CollectionMethod::HttpApi => write!(f, "http_api"),
            CollectionMethod::CustomGrpc => write!(f, "custom_grpc"),
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
            _ => Err(CollectionError::ConfigurationError(format!(
                "Unknown collection method: {}. Supported: sql, coprocessor, http_api, custom_grpc",
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
    /// Database configuration
    pub database_config: DatabaseConfig,
    /// Collection configuration
    pub collection_config: CollectionConfig,
    /// Tables to collect
    pub tables: Vec<TableConfig>,
    /// Output sender
    pub out: SourceSender,
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

    /// Cleanup resources
    async fn cleanup(&mut self) -> Result<(), CollectionError>;
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

        // Add standard metadata
        log.insert("_vector_table", result.metadata.table_config.dest_table.clone());
        log.insert("_vector_source_table", result.metadata.table_config.source_table.clone());
        log.insert("_vector_source_schema", result.metadata.table_config.source_schema.clone());
        log.insert("_vector_instance", result.metadata.instance.clone());
        log.insert("_vector_timestamp", result.metadata.timestamp.to_rfc3339());
        log.insert("_vector_collection_method", result.metadata.collection_method.to_string());

        // Add performance metadata
        log.insert("_vector_collection_duration_ms", result.metadata.duration_ms as i64);
        log.insert("_vector_row_count", result.metadata.row_count as i64);

        // Add extra metadata
        for (key, value) in &result.metadata.extra {
            log.insert(format!("_vector_meta_{}", key).as_str(), value.clone());
        }

        // Add the actual row data
        for (key, value) in row_data {
            log.insert(key.as_str(), value);
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
                    seconds.parse::<u64>().unwrap_or(collection_config.short_interval)
                } else {
                    collection_config.short_interval
                }
            }
            _ => collection_config.short_interval,
        }
    }

}

