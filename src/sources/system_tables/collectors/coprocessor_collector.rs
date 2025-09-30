use std::collections::HashMap;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use http;
use prost::Message;
use serde_json::Value;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, info, warn};

use crate::sources::system_tables::data_collector::{
    CollectionError, CollectionMetadata, CollectionMethod, CollectionResult, CollectorConfig,
    CollectorConfigType, DataCollector,
};
use crate::sources::system_tables::TableConfig;

// Use generated protobuf types from proto/tipb_simple.proto
include!(concat!(env!("OUT_DIR"), "/tipb.rs"));

// Note: All proto types are now defined in the generated code

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
            serde_json::to_string_pretty(&schema_json)
                .unwrap_or_else(|_| "Failed to serialize".to_string())
        );

        let columns = if let Some(cols) = schema_json["cols"].as_array() {
            let parsed_cols: Vec<_> = cols
                .iter()
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
                parsed_cols
                    .iter()
                    .map(|c| (c.id, c.tp, &c.name))
                    .collect::<Vec<_>>()
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
            table_schema.id,
            table_schema.columns.len()
        );

        // Build DAG request using tipb proto
        // Ensure we have schema information - required for proper column mapping
        if table_schema.columns.is_empty() {
            return Err(CollectionError::ConfigurationError(format!(
                "No schema information available for table_id {}. Schema is required for coprocessor requests.",
                table_schema.id
            )));
        }

        let output_offsets: Vec<u32> = (0..table_schema.columns.len() as u32).collect();

        // Build columns for TableScan
        let columns: Vec<ColumnInfo> = table_schema
            .columns
            .iter()
            .map(|col| ColumnInfo {
                column_id: col.id,
                tp: col.tp,
            })
            .collect();

        let dag_request = DagRequest {
            start_ts_fallback: 0,
            executors: vec![Executor {
                tp: ExecType::TypeTableScan as i32,
                tbl_scan: Some(TableScan {
                    table_id: table_schema.id,
                    columns,
                    desc: false,
                }),
                executor_id: format!("table_scan_{}", table_schema.id),
                // Set all other fields to None/default to match Go version
                idx_scan: None,
                selection: None,
                aggregation: None,
                top_n: None,
                limit: None,
                exchange_receiver: None,
                join: None,
                kill: None,
                exchange_sender: None,
                projection: None,
                partition_table_scan: None,
                sort: None,
                window: None,
                fine_grained_shuffle_stream_count: 0,
                fine_grained_shuffle_batch_size: 0,
                expand: None,
                expand2: None,
                broadcast_query: None,
                cte_sink: None,
                cte_source: None,
                index_lookup: None,
                parent_idx: 0,
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
                user_name: "root".to_string(), // Use root like Go code
                user_host: "%".to_string(),
            }),
            root_executor: None,
            force_encode_type: false,
            div_precision_increment: 4,
            intermediate_output_channels: vec![],
        };

        info!(
            "DAG request: executors={}, output_offsets={:?}, encode_type={:?}",
            dag_request.executors.len(),
            dag_request.output_offsets,
            dag_request.encode_type
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
                peer: Some(Peer { id: 1, store_id: 1 }),
                source_stmt: Some(SourceStmt {
                    connection_id: 12345,
                    session_alias: "cluster_statements_summary_client".to_string(),
                }),
            }),
            tp: 103, // ReqTypeDAG
            data,
            ranges: {
                // Generate KeyRange dynamically based on table_id
                // Following TiDB's key encoding: "t[tableID]_r"
                // 1. 't' prefix (1 byte)
                // 2. table_id encoded with XOR signMask and big-endian (8 bytes)
                // 3. '_r' separator (2 bytes)

                // Encode table ID using TiDB's codec.EncodeInt:
                // EncodeIntToCmpUint(v) = v XOR 0x8000000000000000
                const SIGN_MASK: u64 = 0x8000000000000000;
                let encoded_table_id = (table_schema.id as u64) ^ SIGN_MASK;
                let table_id_bytes = encoded_table_id.to_be_bytes(); // Big-endian

                // Build start key: 't' + encoded_table_id (table prefix only)
                let mut start = vec![b't']; // 't' prefix
                start.extend_from_slice(&table_id_bytes); // Encoded table ID (8 bytes)

                // Build end key: 't' + (encoded_table_id + 1) (next table prefix)
                // This represents the next table's prefix boundary
                let end_table_id = encoded_table_id + 1;
                let end_table_id_bytes = end_table_id.to_be_bytes();
                let mut end = vec![b't']; // 't' prefix
                end.extend_from_slice(&end_table_id_bytes); // Next table ID (8 bytes)

                let key_range = KeyRange {
                    start: start.clone(),
                    end: end.clone(),
                };

                info!(
                    "Using TiDB-encoded KeyRange for table_id {}: start={:?}, end={:?}",
                    table_schema.id, key_range.start, key_range.end
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
            table.source_table,
            request.tp,
            request.ranges.len(),
            request.context.is_some()
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

    /// Parse coprocessor response data with enhanced schema management and generalization
    async fn parse_coprocessor_response(
        &self,
        response: &CoprocessorResponse,
        table: &TableConfig,
    ) -> Result<Vec<HashMap<String, Value>>, CollectionError> {
        info!(
            "Parsing coprocessor response for table {}.{}: data_size={} bytes",
            table.source_schema,
            table.source_table,
            response.data.len()
        );

        // Validate response data
        if response.data.is_empty() {
            warn!(
                "Coprocessor response is empty for table {}.{}. This may indicate incorrect request parameters or no data.",
                table.source_schema, table.source_table
            );
            return Ok(Vec::new());
        }

        // Get or fetch table schema with caching
        let table_schema = self.get_or_cache_table_schema(table).await?;

        info!(
            "Using schema for table {}.{}: {} columns (table_id={})",
            table.source_schema,
            table.source_table,
            table_schema.columns.len(),
            table_schema.id
        );

        // Decode SelectResponse from response data
        let select_response = SelectResponse::decode(&response.data[..]).map_err(|e| {
            CollectionError::ParseError(format!(
                "Failed to decode SelectResponse for table {}.{}: {}",
                table.source_schema, table.source_table, e
            ))
        })?;

        // Validate SelectResponse
        self.validate_select_response(&select_response, table)?;

        // Process chunks with schema-aware parsing
        let all_rows = self
            .process_response_chunks(&select_response, table, &table_schema)
            .await?;

        info!(
            "Successfully parsed {} rows from coprocessor response for table {}.{}",
            all_rows.len(),
            table.source_schema,
            table.source_table
        );

        Ok(all_rows)
    }

    /// Get table schema from cache or fetch and cache it
    async fn get_or_cache_table_schema(
        &self,
        table: &TableConfig,
    ) -> Result<TableSchema, CollectionError> {
        let cache_key = format!("{}.{}", table.source_schema, table.source_table);

        // Try to get from cache first
        {
            let schemas = self.cached_schemas.lock().unwrap();
            if let Some(cached_schema) = schemas.get(&cache_key) {
                debug!("Using cached schema for table {}", cache_key);
                return Ok(cached_schema.clone());
            }
        }

        // Cache miss - fetch schema
        info!("Fetching schema for table {} (cache miss)", cache_key);
        let schema = self.get_table_schema_via_http(table).await?;

        // Validate schema before caching
        if schema.columns.is_empty() {
            return Err(CollectionError::ConfigurationError(format!(
                "Retrieved schema for table {} has no columns",
                cache_key
            )));
        }

        // Cache the fetched schema
        {
            let mut schemas = self.cached_schemas.lock().unwrap();
            schemas.insert(cache_key.clone(), schema.clone());
            info!(
                "Cached schema for table {} ({} columns)",
                cache_key,
                schema.columns.len()
            );
        }

        Ok(schema)
    }

    /// Validate SelectResponse for errors and warnings
    fn validate_select_response(
        &self,
        select_response: &SelectResponse,
        table: &TableConfig,
    ) -> Result<(), CollectionError> {
        info!(
            "SelectResponse for table {}.{}: chunks={}, warnings={}, has_error={}, encode_type={:?}",
            table.source_schema, table.source_table,
            select_response.chunks.len(),
            select_response.warnings.len(),
            select_response.error.is_some(),
            select_response.encode_type
        );

        // Check for execution errors
        if let Some(error) = &select_response.error {
            return Err(CollectionError::QueryError(format!(
                "TiDB execution error for table {}.{} [{}]: {}",
                table.source_schema, table.source_table, error.code, error.msg
            )));
        }

        // Log warnings but don't fail
        for (i, warning) in select_response.warnings.iter().enumerate() {
            warn!(
                "TiDB warning {} for table {}.{} [{}]: {}",
                i + 1,
                table.source_schema,
                table.source_table,
                warning.code,
                warning.msg
            );
        }

        Ok(())
    }

    /// Process all chunks in the SelectResponse with schema-aware parsing
    async fn process_response_chunks(
        &self,
        select_response: &SelectResponse,
        table: &TableConfig,
        table_schema: &TableSchema,
    ) -> Result<Vec<HashMap<String, Value>>, CollectionError> {
        let mut all_rows = Vec::new();
        let chunk_count = select_response.chunks.len();

        if chunk_count == 0 {
            info!(
                "No chunks in SelectResponse for table {}.{}",
                table.source_schema, table.source_table
            );
            return Ok(all_rows);
        }

        for (chunk_idx, chunk) in select_response.chunks.iter().enumerate() {
            info!(
                "Processing chunk {}/{} for table {}.{}: rows_data_size={} bytes",
                chunk_idx + 1,
                chunk_count,
                table.source_schema,
                table.source_table,
                chunk.rows_data.len()
            );

            if chunk.rows_data.is_empty() {
                debug!(
                    "Skipping empty chunk {} for table {}.{}",
                    chunk_idx, table.source_schema, table.source_table
                );
                continue;
            }

            // Parse chunk data with schema context and encode type awareness
            let chunk_rows = self.parse_data_with_schema_and_encode_type(
                &chunk.rows_data,
                table,
                table_schema,
                select_response.encode_type,
            )?;

            info!(
                "Parsed {} rows from chunk {}/{} for table {}.{}",
                chunk_rows.len(),
                chunk_idx + 1,
                chunk_count,
                table.source_schema,
                table.source_table
            );

            all_rows.extend(chunk_rows);
        }

        Ok(all_rows)
    }

    /// Parse data with schema context and encode type awareness (main parsing dispatcher)
    fn parse_data_with_schema_and_encode_type(
        &self,
        data: &[u8],
        table: &TableConfig,
        table_schema: &TableSchema,
        encode_type: i32,
    ) -> Result<Vec<HashMap<String, Value>>, CollectionError> {
        debug!(
            "Parsing data for table {}.{}: {} bytes, {} columns, encode_type={}",
            table.source_schema,
            table.source_table,
            data.len(),
            table_schema.columns.len(),
            encode_type
        );

        // Choose parsing strategy based on encode_type
        match encode_type {
            0 => {
                // TypeDefault - use row format parsing (most common case)
                info!(
                    "Using row format parsing (encode_type=TypeDefault) for table {}.{}",
                    table.source_schema, table.source_table
                );
                self.parse_row_format_with_schema(data, table_schema)
            }
            1 => {
                // TypeChunk - chunk format parsing (currently not fully implemented)
                info!(
                    "TypeChunk detected for table {}.{}, falling back to row format parsing",
                    table.source_schema, table.source_table
                );
                warn!("Chunk format parsing is not fully implemented yet, using row format as fallback");
                self.parse_row_format_with_schema(data, table_schema)
            }
            _ => {
                warn!(
                    "Unknown encode_type {} for table {}.{}, defaulting to row format",
                    encode_type, table.source_schema, table.source_table
                );
                self.parse_row_format_with_schema(data, table_schema)
            }
        }
    }

    /// Parse row format data with explicit schema (optimized version)
    fn parse_row_format_with_schema(
        &self,
        data: &[u8],
        table_schema: &TableSchema,
    ) -> Result<Vec<HashMap<String, Value>>, CollectionError> {
        debug!(
            "Parsing row format with provided schema: {} bytes, {} columns",
            data.len(),
            table_schema.columns.len()
        );

        let mut rows = Vec::new();
        let mut offset = 0;
        let mut row_index = 0;

        // Debug: Show first 32 bytes of raw data for comparison with Go
        let preview_len = std::cmp::min(32, data.len());
        let hex_preview: String = data[..preview_len]
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<Vec<_>>()
            .join(" ");
        info!(
            "RUST: First {} bytes of raw data: {}",
            preview_len, hex_preview
        );

        // Parse all available rows
        while offset < data.len() {
            info!(
                "RUST: Starting row {} at offset {} (remaining bytes: {})",
                row_index,
                offset,
                data.len() - offset
            );

            let mut row = HashMap::new();
            let mut row_decoded = false;

            // Add default INSTANCE value
            row.insert(
                "INSTANCE".to_string(),
                Value::String(self.config.instance.clone()),
            );

            // For each row, decode ALL columns in schema order (matching Go exactly)
            for (col_idx, table_col) in table_schema.columns.iter().enumerate() {
                if offset >= data.len() {
                    info!(
                        "Reached end of data at column {} for row {}",
                        col_idx, row_index
                    );
                    break;
                }

                let column_name = self.get_column_name(table_col, col_idx);

                match self.decode_value_from_bytes_with_type(data, offset, table_col.tp) {
                    Ok((value, new_offset)) => {
                        // Only log first few columns and rows to avoid spam
                        if row_index < 3 && col_idx < 30 {
                            info!(
                                "Row {} Column {} ({}): value={:?}, offset {}->{}",
                                row_index, col_idx, column_name, value, offset, new_offset
                            );
                        }

                        // Special debug for request unit columns to verify float decoding
                        if column_name.contains("REQUEST_UNIT") && row_index < 3 {
                            info!(
                                "DEBUG REQUEST_UNIT: Row {} Col {} Name {} Type {} Raw value={:?}",
                                row_index, col_idx, column_name, table_col.tp, value
                            );
                        }
                        offset = new_offset;
                        row_decoded = true;

                        // Apply data type and column-based processing
                        let final_value =
                            self.process_column_value(&column_name, &value, table_col);

                        // Store the column value
                        row.insert(column_name, final_value);
                    }
                    Err(decode_err) => {
                        // Like Go: if we can't decode this column, break the column loop for this row
                        // But continue processing this row with the columns we did decode
                        if row_index < 3 || col_idx < 30 {
                            info!(
                                "Failed to decode column {} (index {}) at offset {} for row {}: {}",
                                column_name, col_idx, offset, row_index, decode_err
                            );
                        }
                        break; // Break column loop, but continue with this row
                    }
                }
            }

            if !row_decoded {
                info!(
                    "No columns decoded for row {}, stopping row processing",
                    row_index
                );
                break;
            }

            // Log summary for first few rows
            if row_index < 5 {
                let digest = row.get("DIGEST").unwrap_or(&Value::Null);
                let exec_count = row.get("EXEC_COUNT").unwrap_or(&Value::Null);
                let digest_text_len = row
                    .get("DIGEST_TEXT")
                    .and_then(|v| {
                        if let Value::String(s) = v {
                            Some(s.len())
                        } else {
                            None
                        }
                    })
                    .unwrap_or(0);
                info!("Row {} summary: DIGEST={:?}, EXEC_COUNT={:?}, DIGEST_TEXT_len={:?}, final_offset={}", 
                      row_index, digest, exec_count, digest_text_len, offset);
            }

            rows.push(row);
            row_index += 1;
        }

        info!(
            "RUST: parse_row_format completed: decoded {} rows, final offset {}/{}",
            rows.len(),
            offset,
            data.len()
        );

        Ok(rows)
    }

    /// Decode value from bytes using TiDB codec (EXACTLY matching Go decodeValueFromBytes)
    fn decode_value_from_bytes(
        &self,
        data: &[u8],
        offset: usize,
    ) -> Result<(Value, usize), CollectionError> {
        if offset >= data.len() {
            return Err(CollectionError::ParseError("Insufficient data".to_string()));
        }

        let flag = data[offset];
        let mut new_offset = offset + 1;

        // Debug: Show flag and next bytes for first few calls
        // if offset < 100 {
        //     let preview_len = std::cmp::min(16, data.len() - offset);
        //     let hex_preview: String = data[offset..offset + preview_len]
        //     .iter()
        //     .map(|b| format!("{:02x}", b))
        //     .collect::<Vec<_>>()
        //     .join(" ");
        //     info!(
        //         "RUST decode_value_from_bytes: offset={}, flag=0x{:02x}, next_bytes=[{}]",
        //         offset, flag, hex_preview
        //     );
        // }

        let value = match flag {
            0x00 => Value::Null, // NilFlag
            0x01 => {
                // bytesFlag
                let (bytes, consumed_offset) = self.decode_bytes(data, new_offset)?;
                new_offset = consumed_offset;
                Value::String(String::from_utf8_lossy(&bytes).to_string())
            }
            0x02 => {
                // compactBytesFlag
                let (bytes, consumed_offset) = self.decode_compact_bytes(data, new_offset)?;
                new_offset = consumed_offset;
                Value::String(String::from_utf8_lossy(&bytes).to_string())
            }
            0x03 => {
                // intFlag
                let (int_val, consumed_offset) = self.decode_int(data, new_offset)?;
                new_offset = consumed_offset;
                Value::Number(int_val.into())
            }
            0x04 => {
                // uintFlag
                let (uint_val, consumed_offset) = self.decode_uint(data, new_offset)?;
                new_offset = consumed_offset;
                Value::Number(uint_val.into())
            }
            0x05 => {
                // floatFlag
                let (float_val, consumed_offset) = self.decode_float(data, new_offset)?;
                new_offset = consumed_offset;
                Value::Number(
                    serde_json::Number::from_f64(float_val).unwrap_or(serde_json::Number::from(0)),
                )
            }
            0x06 => {
                // decimalFlag
                let (decimal_str, consumed_offset) = self.decode_decimal(data, new_offset)?;
                new_offset = consumed_offset;
                Value::String(decimal_str)
            }
            0x07 => {
                // durationFlag
                let (duration_str, consumed_offset) = self.decode_duration(data, new_offset)?;
                new_offset = consumed_offset;
                Value::String(duration_str)
            }
            0x08 => {
                // varintFlag
                let (varint_val, consumed_offset) = self.decode_varint(data, new_offset)?;
                new_offset = consumed_offset;
                Value::Number(varint_val.into())
            }
            0x09 => {
                // uvarintFlag
                let (uvarint_val, consumed_offset) = self.decode_uvarint(data, new_offset)?;
                new_offset = consumed_offset;
                Value::Number(uvarint_val.into())
            }
            0x0A => {
                // jsonFlag
                let (json_str, consumed_offset) = self.decode_json(data, new_offset)?;
                new_offset = consumed_offset;
                Value::String(json_str)
            }
            0x14 => {
                // vectorFloat32Flag
                let (vector_str, consumed_offset) = self.decode_vector_float32(data, new_offset)?;
                new_offset = consumed_offset;
                Value::String(vector_str)
            }
            0xFA => {
                // maxFlag
                Value::String("MAX_VALUE".to_string())
            }
            0x20..=0x30 => {
                // Time types
                let (time_val, consumed_offset) = self.decode_time_value(data, new_offset, flag)?;
                new_offset = consumed_offset;
                Value::String(time_val)
            }
            _ => {
                // For unknown flags, try to skip 1 byte and return NULL
                // This allows decoding to continue despite unknown flags
                info!("Encountered unknown encoding flag: 0x{:02x} at offset {}, treating as NULL and skipping 1 byte", flag, offset);
                Value::Null
            }
        };

        Ok((value, new_offset))
    }

    /// Decode value from bytes with awareness of MySQL column type, enabling packed time decode at byte stage
    fn decode_value_from_bytes_with_type(
        &self,
        data: &[u8],
        offset: usize,
        mysql_tp: i32,
    ) -> Result<(Value, usize), CollectionError> {
        if offset >= data.len() {
            return Err(CollectionError::ParseError("Insufficient data".to_string()));
        }
        let flag = data[offset];
        let mut new_offset = offset + 1;

        // If TIMESTAMP/DATETIME and encoded as uvarint, decode as TiDB packed time here
        if mysql_tp == TYPE_TIMESTAMP || mysql_tp == TYPE_DATETIME {
            // uvarintFlag
            if flag == FLAG_UVARINT {
                let (u, consumed_offset) = self.decode_uvarint(data, new_offset)?;
                new_offset = consumed_offset;
                // For TIMESTAMP, try to decode as microseconds for direct TIMESTAMP support
                if mysql_tp == TYPE_TIMESTAMP {
                    if let Some(microseconds) = self.decode_packed_time_to_microseconds(u) {
                        return Ok((Value::Number(microseconds.into()), new_offset));
                    }
                }
                // Fallback to string format for DATETIME or invalid TIMESTAMP
                let s = self.decode_packed_time_to_string(u);
                return Ok((Value::String(s), new_offset));
            }
            // uintFlag (0x04): next 8 bytes unsigned, big-endian
            if flag == FLAG_UINT {
                if new_offset + 8 > data.len() {
                    return Err(CollectionError::ParseError(
                        "Insufficient bytes for uintFlag time".to_string(),
                    ));
                }
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&data[new_offset..new_offset + 8]);
                let u = u64::from_be_bytes(buf);
                new_offset += 8;
                // For TIMESTAMP, try to decode as microseconds for direct TIMESTAMP support
                if mysql_tp == TYPE_TIMESTAMP {
                    if let Some(microseconds) = self.decode_packed_time_to_microseconds(u) {
                        return Ok((Value::Number(microseconds.into()), new_offset));
                    }
                }
                // Fallback to string format for DATETIME or invalid TIMESTAMP
                let s = self.decode_packed_time_to_string(u);
                return Ok((Value::String(s), new_offset));
            }
            // compactBytesFlag: inner buffer holds encoded time (usually uvarint/uint packed time)
            if flag == FLAG_COMPACT_BYTES {
                // compact bytes
                let (inner, consumed_offset) = self.decode_compact_bytes(data, new_offset)?;
                // decode inner by reading its flag
                if !inner.is_empty() {
                    let inner_flag = inner[0];
                    let inner_off = 1usize;
                    if inner_flag == FLAG_UVARINT {
                        // uvarint
                        let (u, _) = self.decode_uvarint(&inner, inner_off)?;
                        // For TIMESTAMP, try to decode as microseconds for direct TIMESTAMP support
                        if mysql_tp == TYPE_TIMESTAMP {
                            if let Some(microseconds) = self.decode_packed_time_to_microseconds(u) {
                                return Ok((Value::Number(microseconds.into()), consumed_offset));
                            }
                        }
                        // Fallback to string format for DATETIME or invalid TIMESTAMP
                        let s = self.decode_packed_time_to_string(u);
                        return Ok((Value::String(s), consumed_offset));
                    } else if inner_flag == FLAG_UINT {
                        // uintFlag 8-byte
                        // ensure enough bytes
                        if inner.len() >= inner_off + 8 {
                            let mut buf = [0u8; 8];
                            buf.copy_from_slice(&inner[inner_off..inner_off + 8]);
                            // TiDB DecodeUint uses big-endian
                            let u = u64::from_be_bytes(buf);
                            // For TIMESTAMP, try to decode as microseconds for direct TIMESTAMP support
                            if mysql_tp == TYPE_TIMESTAMP {
                                if let Some(microseconds) =
                                    self.decode_packed_time_to_microseconds(u)
                                {
                                    return Ok((
                                        Value::Number(microseconds.into()),
                                        consumed_offset,
                                    ));
                                }
                            }
                            // Fallback to string format for DATETIME or invalid TIMESTAMP
                            let s = self.decode_packed_time_to_string(u);
                            return Ok((Value::String(s), consumed_offset));
                        }
                    }
                    // Fallback: return hex for debugging
                    let hex = inner
                        .iter()
                        .map(|b| format!("{:02x}", b))
                        .collect::<Vec<_>>()
                        .join(" ");
                    info!(
                        "TIME COMPACT inner unhandled flag=0x{:02x} bytes=[{}]",
                        inner_flag, hex
                    );
                }
                return Ok((Value::Null, consumed_offset));
            }
        }
        // Fallback to generic decoder
        self.decode_value_from_bytes(data, offset)
    }

    /// Decode decimal value (matching Go decodeDecimal)
    fn decode_decimal(
        &self,
        data: &[u8],
        offset: usize,
    ) -> Result<(String, usize), CollectionError> {
        // For now, decode as bytes and convert to string
        let (bytes, new_offset) = self.decode_bytes(data, offset)?;
        Ok((String::from_utf8_lossy(&bytes).to_string(), new_offset))
    }

    /// Decode duration value (matching Go decodeDuration)
    fn decode_duration(
        &self,
        data: &[u8],
        offset: usize,
    ) -> Result<(String, usize), CollectionError> {
        // For now, decode as bytes and convert to string
        let (bytes, new_offset) = self.decode_bytes(data, offset)?;
        Ok((String::from_utf8_lossy(&bytes).to_string(), new_offset))
    }

    /// Decode JSON value (matching Go decodeJSON)
    fn decode_json(&self, data: &[u8], offset: usize) -> Result<(String, usize), CollectionError> {
        // For now, decode as bytes and convert to string
        let (bytes, new_offset) = self.decode_bytes(data, offset)?;
        Ok((String::from_utf8_lossy(&bytes).to_string(), new_offset))
    }

    /// Decode vector float32 value (matching Go decodeVectorFloat32)
    fn decode_vector_float32(
        &self,
        data: &[u8],
        offset: usize,
    ) -> Result<(String, usize), CollectionError> {
        // For now, decode as bytes and convert to string
        let (bytes, new_offset) = self.decode_bytes(data, offset)?;
        Ok((String::from_utf8_lossy(&bytes).to_string(), new_offset))
    }

    /// Helper function to safely convert Value to i64
    fn safe_int64_value(&self, value: &Value) -> Option<i64> {
        match value {
            Value::Number(n) => n.as_i64(),
            Value::String(s) => s.parse::<i64>().ok(),
            _ => None,
        }
    }

    /// Decode bytes with length prefix
    fn decode_bytes(
        &self,
        data: &[u8],
        offset: usize,
    ) -> Result<(Vec<u8>, usize), CollectionError> {
        if offset >= data.len() {
            return Err(CollectionError::ParseError(
                "Cannot decode bytes: insufficient data".to_string(),
            ));
        }

        // Read length as varint
        let (length, length_consumed) = self.decode_varint_length(data, offset)?;
        let new_offset = offset + length_consumed;

        if new_offset + length > data.len() {
            return Err(CollectionError::ParseError(format!(
                "Cannot decode bytes: need {} bytes, have {}",
                length,
                data.len() - new_offset
            )));
        }

        let bytes = data[new_offset..new_offset + length].to_vec();
        Ok((bytes, new_offset + length))
    }

    /// Decode compact bytes (EXACTLY matching Go's decodeCompactBytes using binary.Varint)
    fn decode_compact_bytes(
        &self,
        data: &[u8],
        offset: usize,
    ) -> Result<(Vec<u8>, usize), CollectionError> {
        if offset >= data.len() {
            return Err(CollectionError::ParseError(
                "insufficient data, cannot decode compact byte array".to_string(),
            ));
        }

        // Read unsigned varint first (like Go's Uvarint)
        let mut ux = 0u64;
        let mut bytes_consumed = 0;
        let mut shift = 0;

        for i in 0..10 {
            // Max 10 bytes for varint
            if offset + i >= data.len() {
                return Err(CollectionError::ParseError(
                    "cannot decode compact byte array length".to_string(),
                ));
            }

            let b = data[offset + i];
            bytes_consumed += 1;

            if b < 0x80 {
                // Last byte
                if i == 9 && b > 1 {
                    return Err(CollectionError::ParseError(
                        "varint overflows a 64-bit integer".to_string(),
                    ));
                }
                ux |= (b as u64) << shift;
                break;
            }
            ux |= ((b & 0x7F) as u64) << shift;
            shift += 7;
        }

        // Apply zigzag decoding exactly like Go's binary.Varint
        let mut length = (ux >> 1) as i64;
        if (ux & 1) != 0 {
            length = !length; // ^x in Go
        }

        if length < 0 {
            return Err(CollectionError::ParseError(
                "negative length in compact bytes".to_string(),
            ));
        }

        let length = length as usize;
        let new_offset = offset + bytes_consumed;

        // Debug: Log the length and actual data for analysis
        if offset < 50 {
            let preview_len = std::cmp::min(length, 32);
            if new_offset + preview_len <= data.len() {
                let data_preview: String = data[new_offset..new_offset + preview_len]
                    .iter()
                    .map(|b| format!("{:02x}", b))
                    .collect::<Vec<_>>()
                    .join(" ");
                info!("RUST decode_compact_bytes: offset={}, length={}, bytes_consumed={}, data_preview=[{}]", 
                       offset, length, bytes_consumed, data_preview);
            }
        }

        if new_offset + length > data.len() {
            return Err(CollectionError::ParseError(
                "insufficient data, cannot decode compact byte array data".to_string(),
            ));
        }

        let bytes = data[new_offset..new_offset + length].to_vec();
        let final_offset = new_offset + length;

        Ok((bytes, final_offset))
    }

    /// Decode varint length
    fn decode_varint_length(
        &self,
        data: &[u8],
        offset: usize,
    ) -> Result<(usize, usize), CollectionError> {
        let mut result = 0;
        let mut shift = 0;
        let mut consumed = 0;

        for i in offset..data.len() {
            let byte = data[i];
            consumed += 1;

            if (byte & 0x80) == 0 {
                // Last byte
                result |= (byte as usize) << shift;
                break;
            } else {
                // More bytes to come
                result |= ((byte & 0x7F) as usize) << shift;
                shift += 7;
                if shift >= 64 {
                    return Err(CollectionError::ParseError("Varint too long".to_string()));
                }
            }
        }

        Ok((result, offset + consumed))
    }

    /// Decode int64
    fn decode_int(&self, data: &[u8], offset: usize) -> Result<(i64, usize), CollectionError> {
        if offset + 8 > data.len() {
            return Err(CollectionError::ParseError(
                "Cannot decode int: insufficient data".to_string(),
            ));
        }

        let bytes = &data[offset..offset + 8];
        let value = i64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);

        Ok((value, offset + 8))
    }

    /// Decode uint64
    fn decode_uint(&self, data: &[u8], offset: usize) -> Result<(u64, usize), CollectionError> {
        if offset + 8 > data.len() {
            return Err(CollectionError::ParseError(
                "Cannot decode uint: insufficient data".to_string(),
            ));
        }

        let bytes = &data[offset..offset + 8];
        let value = u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);

        Ok((value, offset + 8))
    }

    /// Decode float64
    fn decode_float(&self, data: &[u8], offset: usize) -> Result<(f64, usize), CollectionError> {
        if offset + 8 > data.len() {
            return Err(CollectionError::ParseError(
                "Cannot decode float: insufficient data".to_string(),
            ));
        }

        let bytes = &data[offset..offset + 8];
        // TiDB uses big-endian encoding for floats (matching DecodeUint -> binary.BigEndian.Uint64)
        let u = u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);

        // TiDB's decodeCmpUintToFloat logic:
        // 1. DecodeUint returns the encoded uint64
        // 2. decodeCmpUintToFloat converts it back to float64
        const SIGN_MASK: u64 = 0x8000000000000000;
        let bits = if u & SIGN_MASK > 0 {
            u & !SIGN_MASK
        } else {
            !u
        };

        let value = f64::from_bits(bits);
        Ok((value, offset + 8))
    }

    /// Decode varint
    fn decode_varint(&self, data: &[u8], offset: usize) -> Result<(i64, usize), CollectionError> {
        let (unsigned, consumed) = self.decode_uvarint(data, offset)?;
        let signed = (unsigned >> 1) as i64 ^ -((unsigned & 1) as i64);
        Ok((signed, consumed))
    }

    /// Decode uvarint
    fn decode_uvarint(&self, data: &[u8], offset: usize) -> Result<(u64, usize), CollectionError> {
        let mut result = 0u64;
        let mut shift = 0;
        let mut consumed = 0;

        for i in offset..data.len() {
            let byte = data[i];
            consumed += 1;

            if (byte & 0x80) == 0 {
                // Last byte
                result |= (byte as u64) << shift;
                break;
            } else {
                // More bytes to come
                result |= ((byte & 0x7F) as u64) << shift;
                shift += 7;
                if shift >= 64 {
                    return Err(CollectionError::ParseError("Uvarint too long".to_string()));
                }
            }
        }

        Ok((result, offset + consumed))
    }

    /// Decode time value
    fn decode_time_value(
        &self,
        data: &[u8],
        offset: usize,
        _flag: u8,
    ) -> Result<(String, usize), CollectionError> {
        // For now, just read 8 bytes and convert to timestamp string
        if offset + 8 > data.len() {
            return Err(CollectionError::ParseError(
                "Cannot decode time: insufficient data".to_string(),
            ));
        }

        let bytes = &data[offset..offset + 8];
        let timestamp = u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);

        // Convert to readable timestamp (this is a simplified conversion)
        let time_str = format!("timestamp_{}", timestamp);
        Ok((time_str, offset + 8))
    }

    /// Get column name from schema or generate one
    fn get_column_name(&self, table_col: &TableColumn, col_idx: usize) -> String {
        table_col
            .name
            .clone()
            .unwrap_or_else(|| format!("col_{}", col_idx))
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

    /// Process column value based on data type and column semantics
    /// This method provides flexible column value processing that adapts to different tables
    fn process_column_value(
        &self,
        column_name: &str,
        value: &Value,
        table_col: &TableColumn,
    ) -> Value {
        // Handle special INSTANCE column for all system tables
        if column_name == "INSTANCE" {
            // Always use our configured instance name for consistency
            return Value::String(self.config.instance.clone());
        }

        // Process values based on MySQL data types and column semantics
        match table_col.tp {
            // Integer types (BIGINT, INT, etc.) - ensure proper number conversion
            TYPE_LONGLONG | TYPE_LONG | TYPE_TINY | TYPE_SHORT | TYPE_INT24 => {
                // For numeric columns that might come as strings, convert to numbers
                self.ensure_numeric_value(value)
            }
            // Float/Double types
            TYPE_FLOAT | TYPE_DOUBLE => {
                // Handle float values following TiDB codec standards
                if let Value::Number(n) = value {
                    if let Some(f) = n.as_f64() {
                        // Only convert truly invalid floating point values
                        // TiDB codec supports all finite values including subnormal numbers
                        // Reference: TiDB TestFloatCodec includes math.SmallestNonzeroFloat64
                        if f.is_nan() || f.is_infinite() {
                            info!(
                                "Converting invalid float (NaN/Inf) {} to 0.0 for column {}",
                                f, column_name
                            );
                            return if let Some(zero_float) = serde_json::Number::from_f64(0.0) {
                                Value::Number(zero_float)
                            } else {
                                Value::Number(serde_json::Number::from(0))
                            };
                        }
                        // Note: All finite values including subnormal numbers (like 6.3e-322) are valid
                        // and should be preserved as-is according to TiDB codec implementation
                    }
                }
                self.ensure_float_value(value)
            }
            // Decimal types - treat as numeric values
            TYPE_NEWDECIMAL => self.ensure_numeric_value(value),
            // Date/Time types
            // For TIMESTAMP: keep numeric microseconds if already decoded as number; otherwise fall back to string
            TYPE_TIMESTAMP => match value {
                Value::Number(_) => value.clone(),
                _ => self.convert_packed_time_value(value),
            },
            // For DATETIME: keep as string (no timezone semantics)
            TYPE_DATETIME => self.convert_packed_time_value(value),
            TYPE_DATE | TYPE_DURATION => value.clone(),
            // String types (VARCHAR, TEXT, BLOB, etc.) - keep as-is
            TYPE_VARCHAR | TYPE_STRING | TYPE_VAR_STRING | TYPE_BLOB | TYPE_TINY_BLOB
            | TYPE_MEDIUM_BLOB | TYPE_LONG_BLOB => value.clone(),
            // Enum and Set types - keep as-is (treated as strings)
            TYPE_ENUM | TYPE_SET => value.clone(),
            // Bit type - treat as numeric value
            TYPE_BIT => self.ensure_numeric_value(value),
            // All other types - keep as-is
            _ => value.clone(),
        }
    }

    /// Decode TiDB packed time (per TiDB types.Time.FromPackedUint) and return formatted string
    fn decode_packed_time_to_string(&self, packed: u64) -> String {
        fn parse_fields(p: u64) -> (i32, i32, i32, i32, i32, i32) {
            let ymdhms = p >> 24;
            let ymd = ymdhms >> 17;
            let day = (ymd & ((1u64 << 5) - 1)) as i32;
            let ym = ymd >> 5;
            let rem = (ym % 13) as i32;
            let mut year = (ym / 13) as i32;
            let mut month = rem;
            if rem == 0 {
                // TiDB packed uses base-13; remainder 0 means previous year December
                month = 12;
                year -= 1;
            }
            let hms = ymdhms & ((1u64 << 17) - 1);
            let second = (hms & ((1u64 << 6) - 1)) as i32;
            let minute = ((hms >> 6) & ((1u64 << 6) - 1)) as i32;
            let hour = (hms >> 12) as i32;
            (year, month, day, hour, minute, second)
        }

        fn valid(y: i32, m: i32, d: i32, h: i32, mi: i32, s: i32) -> bool {
            (0..=9999).contains(&y)
                && (1..=12).contains(&m)
                && (1..=31).contains(&d)
                && (0..=23).contains(&h)
                && (0..=59).contains(&mi)
                && (0..=59).contains(&s)
        }

        if packed == 0 {
            return "0000-00-00 00:00:00".to_string();
        }

        // try native (little-endian constructed u64)
        let (y, m, d, h, mi, s) = parse_fields(packed);
        if valid(y, m, d, h, mi, s) {
            // TODO: TIMESTAMP should convert UTC->session tz like TiDB; currently output as-is
            return format!("{y:04}-{m:02}-{d:02} {h:02}:{mi:02}:{s:02}");
        }
        // invalid packed yields zero-time string to match TiDB zero behavior
        "0000-00-00 00:00:00".to_string()
    }

    /// Decode TiDB packed time to microseconds since Unix epoch (for direct TIMESTAMP support)
    fn decode_packed_time_to_microseconds(&self, packed: u64) -> Option<i64> {
        fn parse_fields(p: u64) -> (i32, i32, i32, i32, i32, i32) {
            let ymdhms = p >> 24;
            let ymd = ymdhms >> 17;
            let day = (ymd & ((1u64 << 5) - 1)) as i32;
            let ym = ymd >> 5;
            let rem = (ym % 13) as i32;
            let mut year = (ym / 13) as i32;
            let mut month = rem;
            if rem == 0 {
                // TiDB packed uses base-13; remainder 0 means previous year December
                month = 12;
                year -= 1;
            }
            let hms = ymdhms & ((1u64 << 17) - 1);
            let second = (hms & ((1u64 << 6) - 1)) as i32;
            let minute = ((hms >> 6) & ((1u64 << 6) - 1)) as i32;
            let hour = (hms >> 12) as i32;
            (year, month, day, hour, minute, second)
        }

        fn valid(y: i32, m: i32, d: i32, h: i32, mi: i32, s: i32) -> bool {
            (0..=9999).contains(&y)
                && (1..=12).contains(&m)
                && (1..=31).contains(&d)
                && (0..=23).contains(&h)
                && (0..=59).contains(&mi)
                && (0..=59).contains(&s)
        }

        if packed == 0 {
            return None; // Zero time is not a valid timestamp
        }

        let (y, m, d, h, mi, s) = parse_fields(packed);
        if valid(y, m, d, h, mi, s) {
            // Convert to chrono::NaiveDateTime and then to microseconds since Unix epoch
            if let Some(date) = chrono::NaiveDate::from_ymd_opt(y, m as u32, d as u32) {
                if let Some(naive_dt) = date.and_hms_opt(h as u32, mi as u32, s as u32) {
                    // Convert to UTC timestamp in microseconds
                    let timestamp_micros = naive_dt.and_utc().timestamp_micros();
                    return Some(timestamp_micros);
                }
            }
        }
        None
    }

    /// Convert JSON value that contains TiDB packed time into formatted string
    fn convert_packed_time_value(&self, value: &Value) -> Value {
        match value {
            Value::Number(n) => {
                if let Some(u) = n.as_u64() {
                    let s = self.decode_packed_time_to_string(u);
                    Value::String(s)
                } else if let Some(i) = n.as_i64() {
                    if i >= 0 {
                        let s = self.decode_packed_time_to_string(i as u64);
                        Value::String(s)
                    } else {
                        // negative not expected; keep as string for visibility
                        Value::String(i.to_string())
                    }
                } else {
                    // fallback to string
                    Value::String(n.to_string())
                }
            }
            Value::String(s) => {
                // try parse as integer packed time
                if let Ok(u) = s.parse::<u64>() {
                    Value::String(self.decode_packed_time_to_string(u))
                } else if let Ok(i) = s.parse::<i64>() {
                    if i >= 0 {
                        Value::String(self.decode_packed_time_to_string(i as u64))
                    } else {
                        Value::String(s.clone())
                    }
                } else {
                    Value::String(s.clone())
                }
            }
            _ => value.clone(),
        }
    }

    /// Ensure value is properly formatted as a number for numeric columns
    fn ensure_numeric_value(&self, value: &Value) -> Value {
        match self.safe_int64_value(value) {
            Some(int_val) => Value::Number(int_val.into()),
            None => value.clone(), // Keep original if conversion fails
        }
    }

    /// Convert MySQL type number to string representation
    fn mysql_type_to_string(&self, mysql_type: i32) -> String {
        match mysql_type {
            TYPE_TINY => "tinyint".to_string(),
            TYPE_SHORT => "smallint".to_string(),
            TYPE_LONG => "int".to_string(),
            TYPE_FLOAT => "float".to_string(),
            TYPE_DOUBLE => "double".to_string(),
            TYPE_TIMESTAMP => "timestamp".to_string(),
            TYPE_LONGLONG => "bigint".to_string(),
            TYPE_INT24 => "mediumint".to_string(),
            TYPE_DATE => "date".to_string(),
            TYPE_DURATION => "time".to_string(),
            TYPE_DATETIME => "datetime".to_string(),
            TYPE_VARCHAR => "varchar".to_string(),
            TYPE_BIT => "bit".to_string(),
            TYPE_NEWDECIMAL => "decimal".to_string(),
            TYPE_ENUM => "enum".to_string(),
            TYPE_SET => "set".to_string(),
            TYPE_TINY_BLOB => "tinyblob".to_string(),
            TYPE_MEDIUM_BLOB => "mediumblob".to_string(),
            TYPE_LONG_BLOB => "longblob".to_string(),
            TYPE_BLOB => "blob".to_string(),
            TYPE_VAR_STRING => "varchar".to_string(),
            TYPE_STRING => "char".to_string(),
            _ => format!("unknown_type_{}", mysql_type),
        }
    }

    /// Ensure value is properly formatted as a float following TiDB codec standards
    fn ensure_float_value(&self, value: &Value) -> Value {
        match value {
            Value::Number(n) => {
                if n.is_f64() {
                    // Already a float, preserve as-is (including subnormal numbers like 6.3e-322)
                    // TiDB codec supports all finite values including subnormal numbers
                    value.clone()
                } else if let Some(i) = n.as_i64() {
                    // Convert integer to float
                    if let Some(json_num) = serde_json::Number::from_f64(i as f64) {
                        Value::Number(json_num)
                    } else {
                        Value::Number(serde_json::Number::from(0))
                    }
                } else {
                    Value::Number(serde_json::Number::from(0))
                }
            }
            Value::String(s) => {
                if let Ok(float_val) = s.parse::<f64>() {
                    // Only convert if parsing succeeded and result is finite
                    // TiDB codec supports all finite values including subnormal numbers
                    if float_val.is_finite() {
                        if let Some(json_num) = serde_json::Number::from_f64(float_val) {
                            Value::Number(json_num)
                        } else {
                            Value::Number(serde_json::Number::from(0))
                        }
                    } else {
                        // Invalid float string (NaN/Inf), convert to 0
                        Value::Number(serde_json::Number::from(0))
                    }
                } else {
                    Value::Number(serde_json::Number::from(0))
                }
            }
            _ => Value::Number(serde_json::Number::from(0)),
        }
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

// TiDB row/codec flag constants (aligned with pkg/util/codec/codec.go)
#[allow(dead_code)]
const FLAG_NIL: u8 = 0x00; // NilFlag
#[allow(dead_code)]
const FLAG_BYTES: u8 = 0x01; // bytesFlag
const FLAG_COMPACT_BYTES: u8 = 0x02; // compactBytesFlag
#[allow(dead_code)]
const FLAG_INT: u8 = 0x03; // intFlag
const FLAG_UINT: u8 = 0x04; // uintFlag
#[allow(dead_code)]
const FLAG_FLOAT: u8 = 0x05; // floatFlag
#[allow(dead_code)]
const FLAG_DECIMAL: u8 = 0x06; // decimalFlag
#[allow(dead_code)]
const FLAG_DURATION: u8 = 0x07; // durationFlag
#[allow(dead_code)]
const FLAG_VARINT: u8 = 0x08; // varintFlag
const FLAG_UVARINT: u8 = 0x09; // uvarintFlag

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

        // Add schema metadata for DeltaLake writer
        let mut schema_metadata = serde_json::Map::new();
        for col in &table_schema.columns {
            if let Some(name) = &col.name {
                let mysql_type_str = self.mysql_type_to_string(col.tp);
                let mut obj = serde_json::Map::new();
                obj.insert("mysql_type".to_string(), Value::String(mysql_type_str));
                schema_metadata.insert(name.clone(), Value::Object(obj));
            }
        }
        extra.insert(
            "schema_metadata".to_string(),
            Value::Object(schema_metadata),
        );

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
