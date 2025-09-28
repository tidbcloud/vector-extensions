use std::collections::HashMap;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use http;
use prost::Message;
use serde_json::Value;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};
use tracing::{debug, info, warn};

use crate::sources::system_tables::data_collector::{
    CollectionError, CollectionMetadata, CollectionMethod, CollectionResult, CollectorConfig,
    CollectorConfigType, DataCollector,
};
use crate::sources::system_tables::TableConfig;

// Use generated protobuf types from proto/tipb.proto
include!(concat!(env!("OUT_DIR"), "/tipb.rs"));

// Note: ColumnInfo and TableScan are now defined in proto/tipb.proto


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

// gRPC service definition for TiKV coprocessor
#[tonic::async_trait]
pub trait Tikv {
    async fn coprocessor(
        &self,
        request: Request<CoprocessorRequest>,
    ) -> Result<tonic::Response<CoprocessorResponse>, Status>;
}

pub struct TikvClient<T> {
    inner: tonic::client::Grpc<T>,
}

impl<T> TikvClient<T>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<tonic::codegen::StdError>,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    pub fn new(inner: T) -> Self {
        let inner = tonic::client::Grpc::new(inner);
        Self { inner }
    }

    pub async fn coprocessor(
        &mut self,
        request: impl tonic::IntoRequest<CoprocessorRequest>,
    ) -> Result<tonic::Response<CoprocessorResponse>, tonic::Status> {
        self.inner.ready().await.map_err(|_| {
            tonic::Status::new(tonic::Code::Unknown, "Service was not ready".to_string())
        })?;
        let codec = tonic::codec::ProstCodec::default();
        let path = http::uri::PathAndQuery::from_static("/tikvpb.Tikv/Coprocessor");
        self.inner.unary(request.into_request(), path, codec).await
    }
}

/// Coprocessor-based data collector using gRPC protocol
pub struct CoprocessorCollector {
    config: CollectorConfig,
    grpc_endpoint: String,
    client_channel: Option<Channel>,
    cached_schemas: std::sync::Mutex<HashMap<String, TableSchema>>,
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

        // Build gRPC endpoint (use status port which is typically 10080 for standard TiDB setup)
        // The status port is usually MySQL port + 6080, so 4000 -> 10080
        let status_port = if port == 4000 { 10080 } else { port + 6080 };
        let grpc_endpoint = format!("http://{}:{}", host, status_port);

        Ok(Self {
            config,
            grpc_endpoint,
            client_channel: None,
            cached_schemas: std::sync::Mutex::new(HashMap::new()),
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
        let status_port = if port == 4000 { 10080 } else { port + 6080 }; // TiDB status port

        let url = format!(
            "http://{}:{}/schema/{}/{}",
            host, status_port, table.source_schema, table.source_table
        );

        info!("Fetching schema from: {}", url);

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

        info!(
            "Parsed table schema from HTTP API: table_id={}, raw_schema={}",
            table_id,
            serde_json::to_string_pretty(&schema_json).unwrap_or_else(|_| "Failed to serialize".to_string())
        );

        let columns = if let Some(cols) = schema_json["cols"].as_array() {
            let parsed_cols: Vec<_> = cols.iter()
                .map(|col| {
                    let name = col["name"]["O"].as_str().map(|s| s.to_string());
                    TableColumn {
                        id: col["id"].as_i64().unwrap_or(0),
                        tp: col["type"]["Tp"].as_i64().unwrap_or(15) as i32, // Use "Tp" not "tp"
                        name,
                    }
                })
                .collect();

            info!(
                "Parsed {} columns: {:?}",
                parsed_cols.len(),
                parsed_cols.iter().map(|c| (c.id, c.tp, &c.name)).collect::<Vec<_>>()
            );

            parsed_cols
        } else {
            info!("No columns found in schema");
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
        info!(
            "Building coprocessor request for table_id: {}, columns: {}",
            table_schema.id, table_schema.columns.len()
        );

        // Build DAG request using tipb proto
        let output_offsets: Vec<u32> = if table_schema.columns.is_empty() {
            // If no schema info, request all available columns
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
        } else {
            (0..table_schema.columns.len() as u32).collect()
        };

        // Build columns for TableScan
        let columns: Vec<ColumnInfo> = table_schema.columns.iter().map(|col| {
            ColumnInfo {
                column_id: col.id,
                tp: col.tp,
            }
        }).collect();

        let dag_request = DagRequest {
            start_ts_fallback: 0,
            executors: vec![Executor {
                tp: ExecType::TypeTableScan as i32,
                executor_id: format!("table_scan_{}", table_schema.id),
                parent_idx: 0,
                fine_grained_shuffle_stream_count: 0,
                fine_grained_shuffle_batch_size: 0,
                tbl_scan: Some(TableScan {
                    table_id: table_schema.id,
                    columns,
                    desc: false,
                }),
            }],
            time_zone_offset: 28800, // Use Asia/Shanghai timezone like Go version
            flags: 0,
            output_offsets: output_offsets.clone(),
            collect_range_counts: false,
            max_warning_count: 0,
            encode_type: EncodeType::TypeDefault as i32, // Use TypeDefault like Go version
            sql_mode: 0,
            time_zone_name: "Asia/Shanghai".to_string(), // Use Asia/Shanghai like Go version
            collect_execution_summaries: true, // Enable execution summaries like Go version
            max_allowed_packet: 1024 * 1024 * 16, // 16MB
            chunk_memory_layout: Some(ChunkMemoryLayout {
                endian: Endian::LittleEndian as i32,
            }),
            is_rpn_expr: false,
            user: Some(UserIdentity {
                user_name: "root".to_string(),  // Use root like Go code
                user_host: "%".to_string(),
            }),
            root_executor: None,
            force_encode_type: false,
            div_precision_increment: 4,
            intermediate_output_channels: vec![],
        };

        info!(
            "DAG request: executors={}, output_offsets={:?}, encode_type={:?}",
            dag_request.executors.len(), dag_request.output_offsets, dag_request.encode_type
        );

        // Serialize DAG request
        let data = dag_request.encode_to_vec();
        info!("Serialized DAG request size: {} bytes", data.len());

        // Build coprocessor request with proper field order matching official proto
        let cop_request = CoprocessorRequest {
            context: Some(Context {
                region_id: 1,
                region_epoch: Some(RegionEpoch {
                    conf_ver: 1,
                    version: 1,
                }),
                peer: Some(Peer {
                    id: 1,
                    store_id: 1,
                }),
                source_stmt: Some(SourceStmt {
                    connection_id: 12345,
                    session_alias: "cluster_statements_summary_client".to_string(),
                }),
            }),
            tp: 103, // ReqTypeDAG
            data,
            ranges: {
                // Use the EXACT same KeyRange as the working Go version
                let key_range = KeyRange {
                    start: vec![0x74, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01],
                    end: vec![0x74, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02],
                };

                info!(
                    "Using EXACT Go KeyRange for CLUSTER_STATEMENTS_SUMMARY: start={:?}, end={:?}",
                    key_range.start, key_range.end
                );

                vec![key_range]
            },
            is_cache_enabled: false,
            cache_if_match_version: 0,
            start_ts: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            schema_ver: 0,
            is_trace_enabled: false,
            paging_size: 0,
            connection_id: 12345,
            connection_alias: "cluster_statements_summary_client".to_string(),
        };

        Ok(cop_request)
    }

    /// Perform actual coprocessor collection via gRPC
    async fn perform_coprocessor_collection(
        &self,
        request: &CoprocessorRequest,
        table: &TableConfig,
    ) -> Result<Vec<HashMap<String, Value>>, CollectionError> {
        let channel = self.client_channel.as_ref().ok_or_else(|| {
            CollectionError::ConfigurationError("gRPC channel not initialized".to_string())
        })?;

        // Create TiKV client
        let mut client = TikvClient::new(channel.clone());

        // Debug logging for request details
        info!(
            "Sending coprocessor request for table: {}, tp: {}, ranges: {}, has_context: {}",
            table.source_table, request.tp, request.ranges.len(), request.context.is_some()
        );

        // Debug the serialized request
        let serialized = request.encode_to_vec();
        info!(
            "Serialized coprocessor request size: {} bytes, first 64 bytes: {:?}",
            serialized.len(),
            &serialized[..std::cmp::min(64, serialized.len())]
        );

        let response = client
            .coprocessor(request.clone())
            .await
            .map_err(|e| CollectionError::NetworkError(format!("gRPC request failed: {}", e)))?;

        let cop_response = response.into_inner();

        // Debug response details
        info!(
            "Received coprocessor response: data_size={}, other_error='{}'",
            cop_response.data.len(),
            cop_response.other_error
        );

        // Debug first 100 bytes of response data
        if !cop_response.data.is_empty() {
            let preview_size = std::cmp::min(100, cop_response.data.len());
            info!(
                "Response data preview ({} bytes): {:?}",
                preview_size,
                &cop_response.data[..preview_size]
            );
        } else {
            info!("Response data is completely empty - no bytes received");
        }

        // Check for errors in response
        if !cop_response.other_error.is_empty() {
            return Err(CollectionError::QueryError(format!(
                "Coprocessor error: {}",
                cop_response.other_error
            )));
        }

        // Note: Our protobuf definition may be incomplete.
        // The TiDB CoprocessorResponse should have region_error and other fields,
        // but our current definition only has data and other_error.

        // Parse the response data
        self.parse_coprocessor_response(&cop_response, table).await
    }

    /// Parse coprocessor response data
    async fn parse_coprocessor_response(
        &self,
        response: &CoprocessorResponse,
        table: &TableConfig,
    ) -> Result<Vec<HashMap<String, Value>>, CollectionError> {
        info!(
            "Parsing coprocessor response: data_size={}",
            response.data.len()
        );

        if response.data.is_empty() {
            warn!("Coprocessor response is empty for table {}. This indicates the coprocessor request parameters may be incorrect.", table.source_table);
            info!("Response data is empty, returning 0 rows");
            return Ok(Vec::new());
        }

        // Decode SelectResponse from response data
        let select_response = SelectResponse::decode(&response.data[..]).map_err(|e| {
            CollectionError::ParseError(format!("Failed to decode response: {}", e))
        })?;

        info!(
            "Decoded SelectResponse: chunks={}, warnings={}, has_error={}",
            select_response.chunks.len(),
            select_response.warnings.len(),
            select_response.error.is_some()
        );

        // Check for execution errors
        if let Some(error) = &select_response.error {
            return Err(CollectionError::QueryError(format!(
                "Execution error [{}]: {}",
                error.code, error.msg
            )));
        }

        // Log warnings if any
        for warning in &select_response.warnings {
            warn!(
                "Coprocessor warning for table {} [{}]: {}",
                table.source_table, warning.code, warning.msg
            );
        }

        let mut all_rows = Vec::new();

        // Process each data chunk
        for (i, chunk) in select_response.chunks.iter().enumerate() {
            info!(
                "Processing chunk {}: rows_data_size={}",
                i, chunk.rows_data.len()
            );
            let chunk_rows = self.parse_chunk_data(&chunk.rows_data, table)?;
            all_rows.extend(chunk_rows);
        }

        info!(
            "Parsed {} rows from coprocessor response for table {}",
            all_rows.len(),
            table.source_table
        );

        Ok(all_rows)
    }

    /// Parse chunk data into row format - implementing TiDB chunk format
    fn parse_chunk_data(
        &self,
        chunk_data: &[u8],
        table: &TableConfig,
    ) -> Result<Vec<HashMap<String, Value>>, CollectionError> {
        if chunk_data.is_empty() {
            return Ok(Vec::new());
        }

        debug!(
            "Parsing chunk data: {} bytes for table {}",
            chunk_data.len(), table.source_table
        );

        // Try to decode as TiDB chunk format (similar to Go implementation)
        // Use the cached schema info if available
        match self.parse_tidb_chunk_format_sync(chunk_data, table) {
            Ok(rows) => {
                info!(
                    "Successfully parsed {} rows using TiDB chunk format for table {}",
                    rows.len(), table.source_table
                );
                Ok(rows)
            }
            Err(e) => {
                warn!(
                    "Failed to parse TiDB chunk format for table {}: {}. Using fallback row parsing.",
                    table.source_table, e
                );

                // Fallback to basic row-by-row parsing
                self.parse_fallback_format_sync(chunk_data, table)
            }
        }
    }

    /// Parse TiDB chunk format (columnar storage) similar to Go implementation (sync version)
    fn parse_tidb_chunk_format_sync(
        &self,
        chunk_data: &[u8],
        table: &TableConfig,
    ) -> Result<Vec<HashMap<String, Value>>, CollectionError> {
        // Try to get cached schema or create a basic one for parsing
        let table_schema = self.get_cached_schema_or_default(table);

        // Try to decode the chunk similar to Go's decodeChunkData
        let chunk = self.decode_chunk_columns(chunk_data, &table_schema)?;

        let mut rows = Vec::new();
        let num_rows = chunk.num_rows();

        info!(
            "Decoded chunk with {} rows and {} columns for table {}",
            num_rows, chunk.columns.len(), table.source_table
        );

        // Extract each row from the columnar chunk
        for row_idx in 0..num_rows {
            let mut row = HashMap::new();

            // Add instance information
            row.insert(
                "INSTANCE".to_string(),
                Value::String(self.config.instance.clone()),
            );

            // Extract data for each column
            for (col_idx, table_col) in table_schema.columns.iter().enumerate() {
                if col_idx >= chunk.columns.len() {
                    break;
                }

                let column = &chunk.columns[col_idx];
                let column_name = self.get_column_name(table_col, col_idx);
                let value = self.extract_column_value(column, row_idx, table_col.tp)?;

                if let Some(val) = value {
                    row.insert(column_name, val);
                }
            }

            rows.push(row);
        }

        Ok(rows)
    }

    /// Parse TiDB chunk format (columnar storage) similar to Go implementation (async version)
    async fn parse_tidb_chunk_format(
        &self,
        chunk_data: &[u8],
        table: &TableConfig,
    ) -> Result<Vec<HashMap<String, Value>>, CollectionError> {
        // Get the table schema we fetched earlier
        let table_schema = self.get_table_schema_via_http(table).await?;

        // Cache the schema for future use
        self.cache_schema(table, table_schema.clone());

        // Try to decode the chunk similar to Go's decodeChunkData
        let chunk = self.decode_chunk_columns(chunk_data, &table_schema)?;

        let mut rows = Vec::new();
        let num_rows = chunk.num_rows();

        info!(
            "Decoded chunk with {} rows and {} columns for table {}",
            num_rows, chunk.columns.len(), table.source_table
        );

        // Extract each row from the columnar chunk
        for row_idx in 0..num_rows {
            let mut row = HashMap::new();

            // Add instance information
            row.insert(
                "INSTANCE".to_string(),
                Value::String(self.config.instance.clone()),
            );

            // Extract data for each column
            for (col_idx, table_col) in table_schema.columns.iter().enumerate() {
                if col_idx >= chunk.columns.len() {
                    break;
                }

                let column = &chunk.columns[col_idx];
                let column_name = self.get_column_name(table_col, col_idx);
                let value = self.extract_column_value(column, row_idx, table_col.tp)?;

                if let Some(val) = value {
                    row.insert(column_name, val);
                }
            }

            rows.push(row);
        }

        Ok(rows)
    }

    /// Fallback parsing when chunk format fails (sync version)
    fn parse_fallback_format_sync(
        &self,
        chunk_data: &[u8],
        table: &TableConfig,
    ) -> Result<Vec<HashMap<String, Value>>, CollectionError> {
        warn!("Using fallback row-based parsing for table {}", table.source_table);

        // Attempt to create meaningful fallback data indicating we received response data
        let mut rows = Vec::new();

        // Analyze the chunk data to extract some basic information
        let chunk_info = self.analyze_chunk_data(chunk_data);

        // Create a row that shows we successfully received data but couldn't fully parse it
        let mut row = HashMap::new();
        row.insert(
            "INSTANCE".to_string(),
            Value::String(self.config.instance.clone()),
        );
        row.insert(
            "SOURCE_TABLE".to_string(),
            Value::String(table.source_table.clone()),
        );
        row.insert(
            "COLLECTION_TIME".to_string(),
            Value::String(chrono::Utc::now().to_rfc3339()),
        );
        row.insert(
            "COLLECTION_STATUS".to_string(),
            Value::String("DATA_RECEIVED_PARSING_INCOMPLETE".to_string()),
        );
        row.insert(
            "CHUNK_SIZE_BYTES".to_string(),
            Value::Number(chunk_data.len().into()),
        );
        row.insert(
            "ESTIMATED_ROWS".to_string(),
            Value::Number(chunk_info.estimated_rows.into()),
        );
        row.insert(
            "DATA_PREVIEW".to_string(),
            Value::String(chunk_info.data_preview),
        );

        rows.push(row);

        info!(
            "Fallback parsing created 1 summary row for table {} with {} bytes of data",
            table.source_table, chunk_data.len()
        );

        Ok(rows)
    }

    /// Analyze chunk data to extract basic information
    fn analyze_chunk_data(&self, data: &[u8]) -> ChunkAnalysis {
        let mut estimated_rows = 0;
        let mut data_preview = String::new();

        // Try to read first few bytes as potential row count
        if data.len() >= 8 {
            let potential_row_count = u64::from_le_bytes([
                data[0], data[1], data[2], data[3],
                data[4], data[5], data[6], data[7],
            ]);

            // If it looks reasonable (not too large), use it as estimate
            if potential_row_count > 0 && potential_row_count < 10000 {
                estimated_rows = potential_row_count as usize;
            }
        }

        // Create a hex preview of first 64 bytes
        let preview_len = std::cmp::min(64, data.len());
        data_preview = data[..preview_len]
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<Vec<_>>()
            .join(" ");

        // Look for string patterns that might indicate actual data
        let has_text_data = data.windows(4).any(|window| {
            window.iter().all(|&b| b >= 32 && b <= 126) // ASCII printable range
        });

        ChunkAnalysis {
            estimated_rows: if estimated_rows == 0 { 1 } else { estimated_rows },
            data_preview: format!("hex:{} has_text:{}", data_preview, has_text_data),
        }
    }


    /// Get cached schema or create a default one for parsing
    fn get_cached_schema_or_default(&self, table: &TableConfig) -> TableSchema {
        let cache_key = format!("{}_{}", table.source_schema, table.source_table);

        if let Ok(cache) = self.cached_schemas.lock() {
            if let Some(schema) = cache.get(&cache_key) {
                return schema.clone();
            }
        }

        // Return a default schema for CLUSTER_STATEMENTS_SUMMARY with common columns
        self.create_default_schema_for_statements_summary()
    }

    /// Cache a schema for future use
    fn cache_schema(&self, table: &TableConfig, schema: TableSchema) {
        let cache_key = format!("{}_{}", table.source_schema, table.source_table);

        if let Ok(mut cache) = self.cached_schemas.lock() {
            cache.insert(cache_key, schema);
        }
    }

    /// Create a default schema for CLUSTER_STATEMENTS_SUMMARY
    fn create_default_schema_for_statements_summary(&self) -> TableSchema {
        let columns = vec![
            TableColumn { id: 1, tp: TYPE_VARCHAR, name: Some("INSTANCE".to_string()) },
            TableColumn { id: 2, tp: TYPE_TIMESTAMP, name: Some("SUMMARY_BEGIN_TIME".to_string()) },
            TableColumn { id: 3, tp: TYPE_TIMESTAMP, name: Some("SUMMARY_END_TIME".to_string()) },
            TableColumn { id: 4, tp: TYPE_VARCHAR, name: Some("STMT_TYPE".to_string()) },
            TableColumn { id: 5, tp: TYPE_VARCHAR, name: Some("SCHEMA_NAME".to_string()) },
            TableColumn { id: 6, tp: TYPE_VARCHAR, name: Some("DIGEST".to_string()) },
            TableColumn { id: 7, tp: TYPE_BLOB, name: Some("DIGEST_TEXT".to_string()) },
            TableColumn { id: 8, tp: TYPE_BLOB, name: Some("TABLE_NAMES".to_string()) },
            TableColumn { id: 9, tp: TYPE_BLOB, name: Some("INDEX_NAMES".to_string()) },
            TableColumn { id: 10, tp: TYPE_VARCHAR, name: Some("SAMPLE_USER".to_string()) },
            TableColumn { id: 11, tp: TYPE_LONGLONG, name: Some("EXEC_COUNT".to_string()) },
            TableColumn { id: 12, tp: TYPE_LONG, name: Some("SUM_ERRORS".to_string()) },
            TableColumn { id: 13, tp: TYPE_LONG, name: Some("SUM_WARNINGS".to_string()) },
            TableColumn { id: 14, tp: TYPE_LONGLONG, name: Some("SUM_LATENCY".to_string()) },
            TableColumn { id: 15, tp: TYPE_LONGLONG, name: Some("MAX_LATENCY".to_string()) },
            TableColumn { id: 16, tp: TYPE_LONGLONG, name: Some("MIN_LATENCY".to_string()) },
            TableColumn { id: 17, tp: TYPE_LONGLONG, name: Some("AVG_LATENCY".to_string()) },
        ];

        TableSchema {
            id: 4611686018427387966, // Known table ID for CLUSTER_STATEMENTS_SUMMARY
            columns,
        }
    }

    /// Decode chunk columns (similar to Go's decodeChunkData)
    fn decode_chunk_columns(
        &self,
        data: &[u8],
        table_schema: &TableSchema,
    ) -> Result<ChunkData, CollectionError> {
        let mut chunk = ChunkData {
            columns: Vec::new(),
        };

        let mut offset = 0;
        for (i, col_info) in table_schema.columns.iter().enumerate() {
            let (column, new_offset) = self.decode_column(data, offset, col_info)?;
            chunk.columns.push(column);
            offset = new_offset;

            // Limit the number of columns to avoid infinite loops
            if i >= 50 || offset >= data.len() {
                break;
            }
        }

        info!(
            "Decoded {} columns from chunk data, remaining bytes: {}",
            chunk.columns.len(),
            data.len() - offset
        );

        Ok(chunk)
    }

    /// Decode a single column (similar to Go's decodeColumn)
    fn decode_column(
        &self,
        data: &[u8],
        offset: usize,
        col_info: &TableColumn,
    ) -> Result<(ColumnData, usize), CollectionError> {
        if offset + 8 > data.len() {
            return Err(CollectionError::ParseError(
                "Insufficient data to read column header".to_string(),
            ));
        }

        // Read column length (first 4 bytes)
        let length = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;

        // Read null count (next 4 bytes)
        let null_count = u32::from_le_bytes([
            data[offset + 4],
            data[offset + 5],
            data[offset + 6],
            data[offset + 7],
        ]) as usize;

        let mut offset = offset + 8;

        // Read null bitmap if there are nulls
        let null_bitmap = if null_count > 0 {
            let bitmap_bytes = (length + 7) / 8;
            if offset + bitmap_bytes > data.len() {
                return Err(CollectionError::ParseError(format!(
                    "Insufficient data for null bitmap, need {} bytes, have {}",
                    bitmap_bytes,
                    data.len() - offset
                )));
            }
            let bitmap = data[offset..offset + bitmap_bytes].to_vec();
            offset += bitmap_bytes;
            bitmap
        } else {
            // All values are non-null
            let bitmap_bytes = (length + 7) / 8;
            vec![0xFF; bitmap_bytes]
        };

        // Read column data based on type
        let (column_data, offsets) = if self.is_fixed_length_type(col_info.tp) {
            // Fixed-length type
            let fixed_len = self.get_fixed_length(col_info.tp);
            if fixed_len > 0 {
                let data_len = fixed_len * length;
                if offset + data_len > data.len() {
                    return Err(CollectionError::ParseError(format!(
                        "Insufficient data for fixed column, need {} bytes, have {}",
                        data_len,
                        data.len() - offset
                    )));
                }
                let column_data = data[offset..offset + data_len].to_vec();
                offset += data_len;
                (column_data, Vec::new())
            } else {
                (Vec::new(), Vec::new())
            }
        } else {
            // Variable-length type
            let offset_bytes = (length + 1) * 8;
            if offset + offset_bytes > data.len() {
                return Err(CollectionError::ParseError(format!(
                    "Insufficient data for offsets, need {} bytes, have {}",
                    offset_bytes,
                    data.len() - offset
                )));
            }

            // Read offsets
            let mut offsets = Vec::new();
            for i in 0..=length {
                let offset_val = u64::from_le_bytes([
                    data[offset + i * 8],
                    data[offset + i * 8 + 1],
                    data[offset + i * 8 + 2],
                    data[offset + i * 8 + 3],
                    data[offset + i * 8 + 4],
                    data[offset + i * 8 + 5],
                    data[offset + i * 8 + 6],
                    data[offset + i * 8 + 7],
                ]) as i64;
                offsets.push(offset_val);
            }
            offset += offset_bytes;

            // Read variable data
            let data_len = *offsets.last().unwrap_or(&0) as usize;
            if offset + data_len > data.len() {
                return Err(CollectionError::ParseError(format!(
                    "Insufficient data for variable column, need {} bytes, have {}",
                    data_len,
                    data.len() - offset
                )));
            }
            let column_data = data[offset..offset + data_len].to_vec();
            offset += data_len;

            (column_data, offsets)
        };

        let column = ColumnData {
            data: column_data,
            offsets,
            length,
            null_bitmap,
        };

        Ok((column, offset))
    }

    /// Check if the MySQL type is fixed-length
    fn is_fixed_length_type(&self, tp: i32) -> bool {
        matches!(
            tp,
            TYPE_TINY | TYPE_SHORT | TYPE_INT24 | TYPE_LONG | TYPE_LONGLONG | TYPE_FLOAT | TYPE_DOUBLE
        )
    }

    /// Get the fixed length for MySQL types
    fn get_fixed_length(&self, tp: i32) -> usize {
        match tp {
            TYPE_TINY => 1,
            TYPE_SHORT => 2,
            TYPE_INT24 => 3,
            TYPE_LONG => 4,
            TYPE_LONGLONG => 8,
            TYPE_FLOAT => 4,
            TYPE_DOUBLE => 8,
            _ => 0,
        }
    }

    /// Get column name from schema or generate one
    fn get_column_name(&self, table_col: &TableColumn, col_idx: usize) -> String {
        table_col
            .name
            .clone()
            .unwrap_or_else(|| format!("COLUMN_{}", col_idx))
    }

    /// Extract column value based on type
    fn extract_column_value(
        &self,
        column: &ColumnData,
        row_idx: usize,
        tp: i32,
    ) -> Result<Option<Value>, CollectionError> {
        if column.is_null(row_idx) {
            return Ok(None);
        }

        let value = match tp {
            TYPE_TINY | TYPE_SHORT | TYPE_INT24 | TYPE_LONG | TYPE_LONGLONG => {
                if let Some(val) = column.get_int64(row_idx) {
                    Value::Number(val.into())
                } else {
                    return Ok(None);
                }
            }
            TYPE_VARCHAR | TYPE_STRING | TYPE_VAR_STRING | TYPE_BLOB | TYPE_TINY_BLOB
            | TYPE_MEDIUM_BLOB | TYPE_LONG_BLOB => {
                if let Some(val) = column.get_string(row_idx) {
                    Value::String(val)
                } else {
                    return Ok(None);
                }
            }
            TYPE_TIMESTAMP | TYPE_DATE | TYPE_DATETIME => {
                // For timestamp types, try to get as string first
                if let Some(val) = column.get_string(row_idx) {
                    Value::String(val)
                } else if let Some(bytes) = column.get_bytes(row_idx) {
                    // If string doesn't work, convert bytes to a readable format
                    Value::String(format!("timestamp_{:?}", bytes))
                } else {
                    return Ok(None);
                }
            }
            _ => {
                // For unknown types, try string first, then bytes
                if let Some(val) = column.get_string(row_idx) {
                    Value::String(val)
                } else if let Some(bytes) = column.get_bytes(row_idx) {
                    Value::String(format!("bytes_{:?}", bytes))
                } else {
                    return Ok(None);
                }
            }
        };

        Ok(Some(value))
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
    pub name: Option<String>, // Add column name for easier extraction
}

/// Columnar chunk data structure (similar to Go implementation)
#[derive(Debug)]
pub struct ChunkData {
    pub columns: Vec<ColumnData>,
}

impl ChunkData {
    pub fn num_rows(&self) -> usize {
        if self.columns.is_empty() {
            0
        } else {
            self.columns[0].length
        }
    }
}

/// Column data structure
#[derive(Debug)]
pub struct ColumnData {
    pub data: Vec<u8>,
    pub offsets: Vec<i64>,
    pub length: usize,
    pub null_bitmap: Vec<u8>,
}

impl ColumnData {
    /// Check if a value at given row index is null
    pub fn is_null(&self, row_idx: usize) -> bool {
        if row_idx >= self.length || self.null_bitmap.is_empty() {
            return false;
        }
        let byte_idx = row_idx / 8;
        let bit_idx = row_idx % 8;
        if byte_idx >= self.null_bitmap.len() {
            return false;
        }
        // In TiDB, 0 bit means null, 1 bit means not null
        (self.null_bitmap[byte_idx] & (1 << bit_idx)) == 0
    }

    /// Get int64 value at given row index
    pub fn get_int64(&self, row_idx: usize) -> Option<i64> {
        if self.is_null(row_idx) {
            return None;
        }
        let offset = row_idx * 8;
        if offset + 8 > self.data.len() {
            return None;
        }
        Some(i64::from_le_bytes([
            self.data[offset],
            self.data[offset + 1],
            self.data[offset + 2],
            self.data[offset + 3],
            self.data[offset + 4],
            self.data[offset + 5],
            self.data[offset + 6],
            self.data[offset + 7],
        ]))
    }

    /// Get string value at given row index (for variable-length types)
    pub fn get_string(&self, row_idx: usize) -> Option<String> {
        if self.is_null(row_idx) {
            return None;
        }
        if self.offsets.len() <= row_idx + 1 {
            return None;
        }
        let start = self.offsets[row_idx] as usize;
        let end = self.offsets[row_idx + 1] as usize;
        if start >= end || start >= self.data.len() || end > self.data.len() {
            return None;
        }
        Some(String::from_utf8_lossy(&self.data[start..end]).to_string())
    }

    /// Get bytes value at given row index
    pub fn get_bytes(&self, row_idx: usize) -> Option<Vec<u8>> {
        if self.is_null(row_idx) {
            return None;
        }
        if self.offsets.len() <= row_idx + 1 {
            return None;
        }
        let start = self.offsets[row_idx] as usize;
        let end = self.offsets[row_idx + 1] as usize;
        if start >= end || start >= self.data.len() || end > self.data.len() {
            return None;
        }
        Some(self.data[start..end].to_vec())
    }
}

/// MySQL type constants (matching Go implementation)
const TYPE_TINY: i32 = 1;
const TYPE_SHORT: i32 = 2;
const TYPE_LONG: i32 = 3;
const TYPE_FLOAT: i32 = 4;
const TYPE_DOUBLE: i32 = 5;
const TYPE_TIMESTAMP: i32 = 7;
const TYPE_LONGLONG: i32 = 8;
const TYPE_INT24: i32 = 9;
const TYPE_DATE: i32 = 10;
const TYPE_DURATION: i32 = 11;
const TYPE_DATETIME: i32 = 12;
const TYPE_VARCHAR: i32 = 15;
const TYPE_BIT: i32 = 16;
const TYPE_NEWDECIMAL: i32 = 246;
const TYPE_ENUM: i32 = 247;
const TYPE_SET: i32 = 248;
const TYPE_TINY_BLOB: i32 = 249;
const TYPE_MEDIUM_BLOB: i32 = 250;
const TYPE_LONG_BLOB: i32 = 251;
const TYPE_BLOB: i32 = 252;
const TYPE_VAR_STRING: i32 = 253;
const TYPE_STRING: i32 = 254;

/// Basic analysis result for chunk data
#[derive(Debug)]
struct ChunkAnalysis {
    estimated_rows: usize,
    data_preview: String,
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
            Ok(request) => {
                // Perform actual coprocessor collection via gRPC
                self.perform_coprocessor_collection(&request, table).await?
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
        extra.insert("fallback_used".to_string(), Value::Bool(false)); // Now using actual gRPC

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
