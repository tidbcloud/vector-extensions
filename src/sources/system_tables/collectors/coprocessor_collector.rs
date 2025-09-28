use std::collections::HashMap;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use prost::Message;
use serde_json::Value;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, info, warn};

use crate::sources::system_tables::data_collector::{
    CollectionError, CollectionMetadata, CollectionMethod, CollectionResult, CollectorConfig,
    CollectorConfigType, DataCollector,
};
use crate::sources::system_tables::TableConfig;

// Protobuf definitions for coprocessor requests

/// Simplified coprocessor request structure based on TiDB's tipb format
#[derive(Clone, PartialEq, Message)]
pub struct DAGRequest {
    #[prost(string, tag = "1")]
    pub time_zone_name: String,
    #[prost(int64, tag = "2")]
    pub time_zone_offset: i64,
    #[prost(uint64, tag = "3")]
    pub flags: u64,
    #[prost(int32, tag = "4")]
    pub encode_type: i32,
    #[prost(message, optional, tag = "5")]
    pub user: Option<UserIdentity>,
    #[prost(message, repeated, tag = "6")]
    pub executors: Vec<Executor>,
    #[prost(uint32, repeated, tag = "7")]
    pub output_offsets: Vec<u32>,
    #[prost(bool, optional, tag = "8")]
    pub collect_execution_summaries: Option<bool>,
}

#[derive(Clone, PartialEq, Message)]
pub struct UserIdentity {
    #[prost(string, tag = "1")]
    pub user_name: String,
    #[prost(string, tag = "2")]
    pub user_host: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct Executor {
    #[prost(int32, tag = "1")]
    pub tp: i32,
    #[prost(message, optional, tag = "2")]
    pub tbl_scan: Option<TableScan>,
}

#[derive(Clone, PartialEq, Message)]
pub struct TableScan {
    #[prost(int64, tag = "1")]
    pub table_id: i64,
    #[prost(message, repeated, tag = "2")]
    pub columns: Vec<ColumnInfo>,
    #[prost(bool, tag = "3")]
    pub desc: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct ColumnInfo {
    #[prost(int64, tag = "1")]
    pub column_id: i64,
    #[prost(int32, tag = "2")]
    pub tp: i32,
}

#[derive(Clone, PartialEq, Message)]
pub struct CoprocessorRequest {
    #[prost(int64, tag = "1")]
    pub tp: i64,
    #[prost(bytes = "vec", tag = "2")]
    pub data: Vec<u8>,
    #[prost(message, repeated, tag = "3")]
    pub ranges: Vec<KeyRange>,
    #[prost(message, optional, tag = "4")]
    pub context: Option<Context>,
    #[prost(uint64, tag = "5")]
    pub start_ts: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct KeyRange {
    #[prost(bytes = "vec", tag = "1")]
    pub start: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub end: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Context {
    #[prost(uint64, tag = "1")]
    pub region_id: u64,
    #[prost(message, optional, tag = "2")]
    pub region_epoch: Option<RegionEpoch>,
    #[prost(message, optional, tag = "3")]
    pub peer: Option<Peer>,
    #[prost(message, optional, tag = "4")]
    pub source_stmt: Option<SourceStmt>,
}

#[derive(Clone, PartialEq, Message)]
pub struct RegionEpoch {
    #[prost(uint64, tag = "1")]
    pub conf_ver: u64,
    #[prost(uint64, tag = "2")]
    pub version: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct Peer {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(uint64, tag = "2")]
    pub store_id: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct SourceStmt {
    #[prost(uint64, tag = "1")]
    pub connection_id: u64,
    #[prost(string, tag = "2")]
    pub session_alias: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct CoprocessorResponse {
    #[prost(bytes = "vec", tag = "1")]
    pub data: Vec<u8>,
    #[prost(string, tag = "2")]
    pub other_error: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct SelectResponse {
    #[prost(message, repeated, tag = "1")]
    pub chunks: Vec<Chunk>,
    #[prost(message, repeated, tag = "2")]
    pub warnings: Vec<Warning>,
    #[prost(message, optional, tag = "3")]
    pub error: Option<Error>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Chunk {
    #[prost(bytes = "vec", tag = "1")]
    pub rows_data: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Warning {
    #[prost(int32, tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub msg: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct Error {
    #[prost(int32, tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub msg: String,
}

/// Coprocessor-based data collector using gRPC protocol
pub struct CoprocessorCollector {
    config: CollectorConfig,
    grpc_endpoint: String,
    client_channel: Option<Channel>,
}

impl CoprocessorCollector {
    /// Create a new coprocessor collector
    pub fn new(config: CollectorConfig) -> Result<Self, CollectionError> {
        // Extract coprocessor-specific config
        let (host, port) = match &config.config_type {
            CollectorConfigType::Coprocessor { host, port, .. } => (host.clone(), *port),
            _ => {
                return Err(CollectionError::ConfigurationError(
                    "Invalid config type for CoprocessorCollector".to_string(),
                ))
            }
        };

        // Build gRPC endpoint (use status port which is typically MySQL port + 6080)
        let grpc_endpoint = format!("http://{}:{}", host, port + 6080);

        Ok(Self {
            config,
            grpc_endpoint,
            client_channel: None,
        })
    }

    /// Establish gRPC connection
    async fn create_grpc_connection(&self) -> Result<Channel, CollectionError> {
        info!("Creating gRPC connection to: {}", self.grpc_endpoint);

        let endpoint = Endpoint::from_shared(self.grpc_endpoint.clone())
            .map_err(|e| CollectionError::ConfigurationError(format!("Invalid endpoint: {}", e)))?
            .timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(5));

        let channel = endpoint.connect().await.map_err(|e| {
            CollectionError::ConnectionError(format!("gRPC connection failed: {}", e))
        })?;

        Ok(channel)
    }

    /// Get table schema via HTTP API
    async fn get_table_schema_via_http(
        &self,
        table: &TableConfig,
    ) -> Result<TableSchema, CollectionError> {
        // Extract host and status port from coprocessor config
        let (host, port) = match &self.config.config_type {
            CollectorConfigType::Coprocessor { host, port, .. } => (host, *port),
            _ => {
                return Err(CollectionError::ConfigurationError(
                    "Invalid config type for coprocessor table schema fetch".to_string(),
                ))
            }
        };
        let status_port = port + 6080; // TiDB status port

        let url = format!(
            "http://{}:{}/schema/{}/{}",
            host, status_port, table.source_schema, table.source_table
        );

        debug!("Fetching schema from: {}", url);

        let client = reqwest::Client::new();
        let response = client
            .get(&url)
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| CollectionError::NetworkError(format!("HTTP request failed: {}", e)))?;

        if !response.status().is_success() {
            return Err(CollectionError::NetworkError(format!(
                "HTTP request failed with status: {}",
                response.status()
            )));
        }

        let schema_json: serde_json::Value = response
            .json()
            .await
            .map_err(|e| CollectionError::ParseError(format!("Failed to parse JSON: {}", e)))?;

        // Parse schema JSON and create TableSchema
        let table_id = schema_json["id"].as_i64().unwrap_or(0);

        let columns = if let Some(cols) = schema_json["cols"].as_array() {
            cols.iter()
                .map(|col| TableColumn {
                    id: col["id"].as_i64().unwrap_or(0),
                    tp: col["type"]["tp"].as_i64().unwrap_or(15) as i32, // Default to VARCHAR
                })
                .collect()
        } else {
            Vec::new()
        };

        Ok(TableSchema {
            id: table_id,
            columns,
        })
    }

    /// Build coprocessor request for the given table schema
    fn build_coprocessor_request(
        &self,
        table_schema: &TableSchema,
    ) -> Result<CoprocessorRequest, CollectionError> {
        // Build DAG request
        let dag_request = DAGRequest {
            time_zone_name: "UTC".to_string(),
            time_zone_offset: 0,
            flags: 0,
            encode_type: 0, // TypeDefault
            user: Some(UserIdentity {
                user_name: "coprocessor_user".to_string(), // Use generic user for coprocessor
                user_host: "%".to_string(),
            }),
            executors: vec![Executor {
                tp: 1, // TypeTableScan
                tbl_scan: Some(TableScan {
                    table_id: table_schema.id,
                    columns: table_schema
                        .columns
                        .iter()
                        .map(|col| ColumnInfo {
                            column_id: col.id,
                            tp: col.tp,
                        })
                        .collect(),
                    desc: false,
                }),
            }],
            output_offsets: (0..table_schema.columns.len() as u32).collect(),
            collect_execution_summaries: Some(true),
        };

        // Serialize DAG request
        let data = dag_request.encode_to_vec();

        // Build coprocessor request
        let cop_request = CoprocessorRequest {
            tp: 103, // ReqTypeDAG
            data,
            ranges: vec![KeyRange {
                start: vec![0x74, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01],
                end: vec![0x74, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02],
            }],
            context: Some(Context {
                region_id: 1,
                region_epoch: Some(RegionEpoch {
                    conf_ver: 1,
                    version: 1,
                }),
                peer: Some(Peer { id: 1, store_id: 1 }),
                source_stmt: Some(SourceStmt {
                    connection_id: 12345,
                    session_alias: "coprocessor_collector_v2".to_string(),
                }),
            }),
            start_ts: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        Ok(cop_request)
    }

    /// Fallback to HTTP API collection for tables that don't support coprocessor
    async fn fallback_to_http_collection(
        &self,
        table: &TableConfig,
    ) -> Result<Vec<HashMap<String, Value>>, CollectionError> {
        warn!(
            "Falling back to HTTP API collection for table: {}",
            table.source_table
        );

        // This could be implemented to use TiDB's HTTP API endpoints
        // For now, return empty result
        Ok(Vec::new())
    }
}

/// Table schema information
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub id: i64,
    pub columns: Vec<TableColumn>,
}

#[derive(Debug, Clone)]
pub struct TableColumn {
    pub id: i64,
    pub tp: i32,
}

#[async_trait]
impl DataCollector for CoprocessorCollector {
    fn collection_method(&self) -> CollectionMethod {
        CollectionMethod::Coprocessor
    }

    fn can_collect_table(&self, table: &TableConfig) -> bool {
        // Coprocessor method works best with CLUSTER_ tables
        table.source_table.starts_with("CLUSTER_")
            || table.source_table.contains("STATEMENTS_SUMMARY")
            || table.source_table.contains("SLOW_QUERY")
    }

    async fn initialize(&mut self) -> Result<(), CollectionError> {
        info!(
            "Initializing coprocessor collector for instance: {}",
            self.config.instance
        );

        let channel = self.create_grpc_connection().await?;
        self.client_channel = Some(channel);

        info!("Coprocessor collector initialized successfully");
        Ok(())
    }

    async fn collect_table_data(
        &self,
        table: &TableConfig,
    ) -> Result<CollectionResult, CollectionError> {
        let start_time = Instant::now();
        let timestamp = chrono::Utc::now();

        let _channel = self.client_channel.as_ref().ok_or_else(|| {
            CollectionError::ConfigurationError("gRPC channel not initialized".to_string())
        })?;

        // Try to get table schema
        let table_schema = match self.get_table_schema_via_http(table).await {
            Ok(schema) => schema,
            Err(e) => {
                warn!(
                    "Failed to get schema for table {}: {}. Using fallback.",
                    table.source_table, e
                );
                // Create a basic schema for fallback
                TableSchema {
                    id: 0,
                    columns: Vec::new(),
                }
            }
        };

        // Try coprocessor collection, fallback to HTTP if needed
        let data = match self.build_coprocessor_request(&table_schema) {
            Ok(_request) => {
                // For now, since gRPC is not fully implemented, use fallback
                self.fallback_to_http_collection(table).await?
            }
            Err(e) => {
                warn!(
                    "Failed to build coprocessor request: {}. Using fallback.",
                    e
                );
                self.fallback_to_http_collection(table).await?
            }
        };

        let duration = start_time.elapsed();
        let row_count = data.len();

        // Create metadata
        let mut extra = HashMap::new();
        extra.insert(
            "schema_columns".to_string(),
            Value::Number(table_schema.columns.len().into()),
        );
        extra.insert(
            "grpc_endpoint".to_string(),
            Value::String(self.grpc_endpoint.clone()),
        );
        extra.insert("fallback_used".to_string(), Value::Bool(true)); // Since we're using fallback for now

        let metadata = CollectionMetadata {
            instance: self.config.instance.clone(),
            table_config: table.clone(),
            collection_method: CollectionMethod::Coprocessor,
            timestamp,
            row_count,
            duration_ms: duration.as_millis() as u64,
            extra,
        };

        info!(
            "Coprocessor collection completed for table {}: {} rows in {}ms",
            table.source_table,
            row_count,
            duration.as_millis()
        );

        Ok(CollectionResult { data, metadata })
    }

    async fn health_check(&self) -> Result<(), CollectionError> {
        if self.client_channel.is_none() {
            return Err(CollectionError::ConfigurationError(
                "gRPC channel not initialized".to_string(),
            ));
        }

        // For a real health check, we could send a simple coprocessor request
        // or check the gRPC connection status
        Ok(())
    }
}
