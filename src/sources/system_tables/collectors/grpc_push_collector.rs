use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tracing::{debug, error, info};

use vector::SourceSender;

use crate::sources::system_tables::data_collector::{
    CollectionError, CollectionMetadata, CollectionMethod, CollectionResult, CollectorConfig,
    CollectorConfigType, DataCollector,
};
use crate::sources::system_tables::TableConfig;

pub mod proto {
    tonic::include_proto!("systemtable.v1");
}

use proto::statement_push_control_client::StatementPushControlClient;
use proto::system_table_push_service_server::{
    SystemTablePushService, SystemTablePushServiceServer,
};
use proto::{
    CollectionConfig, PingRequest, PingResponse, PushResponse, RegisterPushTargetRequest,
    StatementBatch, TableRowBatch,
};

/// Buffer for received statement batches
type StatementBuffer = Arc<Mutex<Vec<ReceivedBatch>>>;

/// A received batch from TiDB
#[derive(Debug, Clone)]
struct ReceivedBatch {
    cluster_id: String,
    instance_id: String,
    statements: Vec<HashMap<String, Value>>,
    received_at: chrono::DateTime<chrono::Utc>,
}

/// Push table type for gRPC push collectors
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PushTableType {
    /// STATEMENTS_SUMMARY table
    StatementsSummary,
    /// Other tables (future extension)
    Other(String),
}

impl PushTableType {
    /// Check if this table type is supported
    pub fn is_supported(&self) -> bool {
        match self {
            PushTableType::StatementsSummary => true,
            PushTableType::Other(_) => false,
        }
    }

    /// Get the table name
    pub fn table_name(&self) -> &str {
        match self {
            PushTableType::StatementsSummary => "STATEMENTS_SUMMARY",
            PushTableType::Other(name) => name.as_str(),
        }
    }
}

/// gRPC push collector — registers with TiDB, receives push data via gRPC.
/// This is the abstract base for push-based collectors.
pub struct GrpcPushCollector {
    instance: String,
    host: String,
    status_port: u16,
    vector_grpc_address: String,
    vector_grpc_port: u16,
    buffer: StatementBuffer,
    grpc_server_handle: Option<tokio::task::JoinHandle<()>>,
    registered: bool,
    output_sender: Option<SourceSender>,
    table_config: Option<TableConfig>,
    /// The type of table this collector handles
    table_type: PushTableType,
}

impl GrpcPushCollector {
    pub fn new(config: CollectorConfig, table_config: TableConfig) -> Result<Self, CollectionError> {
        match config.config_type {
            CollectorConfigType::GrpcPush {
                host,
                status_port,
                vector_grpc_address,
                vector_grpc_port,
                ..
            } => {
                // Determine table type based on table_config
                let table_type = if table_config.source_table.contains("STATEMENTS_SUMMARY") {
                    PushTableType::StatementsSummary
                } else {
                    PushTableType::Other(table_config.source_table.clone())
                };

                Ok(Self {
                    instance: config.instance,
                    host,
                    status_port,
                    vector_grpc_address,
                    vector_grpc_port,
                    buffer: Arc::new(Mutex::new(Vec::new())),
                    grpc_server_handle: None,
                    registered: false,
                    output_sender: None,
                    table_config: Some(table_config),
                    table_type,
                })
            }
            _ => Err(CollectionError::ConfigurationError(
                "Expected GrpcPush config".to_string(),
            )),
        }
    }

    /// Get the table type this collector handles
    pub fn table_type(&self) -> &PushTableType {
        &self.table_type
    }

    /// Start background task to flush buffer to output immediately
    fn start_buffer_flusher(&self) {
        let buffer = self.buffer.clone();
        let sender = self.output_sender.clone().expect("output_sender not set");
        let table_config = self.table_config.clone().expect("table_config not set");
        let instance = self.instance.clone();

        tokio::spawn(async move {
            // Clone sender once, then reuse the reference
            let mut sender = sender;
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

                let batches: Vec<ReceivedBatch> = {
                    let mut buf = buffer.lock().await;
                    if buf.is_empty() {
                        continue;
                    }
                    buf.drain(..).collect()
                };

                if batches.is_empty() {
                    continue;
                }

                let mut all_rows = Vec::new();
                for batch in &batches {
                    all_rows.extend(batch.statements.clone());
                }

                let row_count = all_rows.len();
                info!(
                    "gRPC push flush: {} rows to output (from {} batches)",
                    row_count,
                    batches.len()
                );

                // Create metadata
                let metadata = CollectionMetadata {
                    instance: instance.clone(),
                    table_config: table_config.clone(),
                    collection_method: CollectionMethod::GrpcPush,
                    timestamp: chrono::Utc::now(),
                    row_count,
                    duration_ms: 0,
                    extra: HashMap::new(),
                };

                // Send each row as an event
                for row_data in &all_rows {
                    use crate::sources::system_tables::data_collector::utils::create_event_from_result;

                    let result = CollectionResult {
                        data: vec![row_data.clone()],
                        metadata: metadata.clone(),
                    };
                    let event = create_event_from_result(&result, row_data.clone());

                    if let Err(e) = sender.send_event(event).await {
                        error!("Failed to send gRPC push event: {}", e);
                    }
                }
            }
        });
    }

    /// Register this Vector instance with TiDB as a push target
    async fn register_push_target(&self) -> Result<(), CollectionError> {
        let endpoint = format!("http://{}:{}", self.host, self.status_port);
        info!(
            "Registering push target with TiDB at {} (vector endpoint: {}:{})",
            endpoint, self.vector_grpc_address, self.vector_grpc_port
        );

        let channel = Channel::from_shared(endpoint.clone())
            .map_err(|e| CollectionError::ConfigurationError(format!("Invalid endpoint: {}", e)))?
            .connect()
            .await
            .map_err(|e| {
                CollectionError::ConnectionError(format!(
                    "Failed to connect to TiDB at {}: {}",
                    endpoint, e
                ))
            })?;

        let mut client = StatementPushControlClient::new(channel);

        // Use the TiDB host as the reachable address (Vector binds 0.0.0.0 but TiDB needs a real IP)
        let reachable_address = if self.vector_grpc_address == "0.0.0.0" {
            self.host.clone()
        } else {
            self.vector_grpc_address.clone()
        };
        let vector_endpoint = format!("{}:{}", reachable_address, self.vector_grpc_port);
        let request = tonic::Request::new(RegisterPushTargetRequest {
            vector_endpoint,
            vector_instance_id: format!("vector-{}", std::process::id()),
            vector_version: env!("CARGO_PKG_VERSION").to_string(),
            tls_config: None,
            collection_config: Some(CollectionConfig {
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
                extended_metrics: vec![],
                config_version: 1,
            }),
        });

        let response = client.register_push_target(request).await.map_err(|e| {
            CollectionError::NetworkError(format!("RegisterPushTarget failed: {}", e))
        })?;

        let resp = response.into_inner();
        if resp.success {
            info!("Successfully registered push target with TiDB at {}", endpoint);
            Ok(())
        } else {
            Err(CollectionError::NetworkError(format!(
                "RegisterPushTarget rejected: {}",
                resp.message
            )))
        }
    }

    /// Start the gRPC server to receive push data from TiDB
    fn start_grpc_server(&mut self) {
        let addr = format!("{}:{}", self.vector_grpc_address, self.vector_grpc_port);
        let buffer = self.buffer.clone();

        let handle = tokio::spawn(async move {
            let addr = addr.parse().expect("Invalid gRPC bind address");
            info!("Starting gRPC push receiver on {}", addr);

            let service = GrpcPushService {
                buffer,
            };

            if let Err(e) = tonic::transport::Server::builder()
                .add_service(SystemTablePushServiceServer::new(service))
                .serve(addr)
                .await
            {
                error!("gRPC push server error: {}", e);
            }
        });

        self.grpc_server_handle = Some(handle);
    }
}

/// gRPC service implementation — receives push data from TiDB
struct GrpcPushService {
    buffer: StatementBuffer,
}

#[tonic::async_trait]
impl SystemTablePushService for GrpcPushService {
    async fn push_statements(
        &self,
        request: tonic::Request<StatementBatch>,
    ) -> Result<tonic::Response<PushResponse>, tonic::Status> {
        let batch = request.into_inner();
        let metadata = batch.metadata.as_ref();
        let cluster_id = metadata.map(|m| m.cluster_id.clone()).unwrap_or_default();
        let instance_id = metadata.map(|m| m.instance_id.clone()).unwrap_or_default();
        let stmt_count = batch.statements.len();

        info!(
            "Received push from TiDB {}/{}: {} statements",
            cluster_id, instance_id, stmt_count
        );

        // Convert proto statements to HashMap<String, Value>
        let mut rows = Vec::with_capacity(stmt_count);
        for stmt in &batch.statements {
            let mut row: HashMap<String, Value> = HashMap::new();
            row.insert("DIGEST".to_string(), Value::String(stmt.digest.clone()));
            row.insert("DIGEST_TEXT".to_string(), Value::String(stmt.normalized_sql.clone()));
            row.insert("STMT_TYPE".to_string(), Value::String(stmt.stmt_type.clone()));
            row.insert("SCHEMA_NAME".to_string(), Value::String(stmt.schema_name.clone()));
            row.insert("TABLE_NAMES".to_string(), Value::String(stmt.table_names.clone()));
            row.insert("EXEC_COUNT".to_string(), Value::Number(stmt.exec_count.into()));
            row.insert("SUM_LATENCY".to_string(), Value::Number(stmt.sum_latency_us.into()));
            row.insert("AVG_LATENCY".to_string(), Value::Number(stmt.avg_latency_us.into()));
            row.insert("MAX_LATENCY".to_string(), Value::Number(stmt.max_latency_us.into()));
            row.insert("MIN_LATENCY".to_string(), Value::Number(stmt.min_latency_us.into()));
            row.insert("SUM_AFFECTED_ROWS".to_string(), Value::Number(stmt.sum_affected_rows.into()));
            row.insert("SUM_RESULT_ROWS".to_string(), Value::Number(stmt.sum_result_rows.into()));
            row.insert("CLUSTER_ID".to_string(), Value::String(cluster_id.clone()));
            row.insert("INSTANCE_ID".to_string(), Value::String(instance_id.clone()));
            if let Some(ref m) = batch.metadata {
                row.insert("WINDOW_START_MS".to_string(), Value::Number(m.window_start_ms.into()));
                row.insert("WINDOW_END_MS".to_string(), Value::Number(m.window_end_ms.into()));
            }
            rows.push(row);
        }

        let received_batch = ReceivedBatch {
            cluster_id,
            instance_id,
            statements: rows,
            received_at: chrono::Utc::now(),
        };

        self.buffer.lock().await.push(received_batch);

        Ok(tonic::Response::new(PushResponse {
            success: true,
            message: format!("Accepted {} statements", stmt_count),
            received_timestamp_ms: chrono::Utc::now().timestamp_millis(),
            accepted_count: stmt_count as i32,
            rejected_count: 0,
            errors: vec![],
        }))
    }

    async fn push_table_rows(
        &self,
        _request: tonic::Request<TableRowBatch>,
    ) -> Result<tonic::Response<PushResponse>, tonic::Status> {
        // Not used for gRPC push — only PushStatements is used
        Ok(tonic::Response::new(PushResponse {
            success: false,
            message: "push_table_rows not supported, use push_statements".to_string(),
            received_timestamp_ms: chrono::Utc::now().timestamp_millis(),
            accepted_count: 0,
            rejected_count: 0,
            errors: vec![],
        }))
    }

    async fn ping(
        &self,
        request: tonic::Request<PingRequest>,
    ) -> Result<tonic::Response<PingResponse>, tonic::Status> {
        let req = request.into_inner();
        debug!("Ping from {}:{}", req.cluster_id, req.instance_id);

        Ok(tonic::Response::new(PingResponse {
            ok: true,
            version: env!("CARGO_PKG_VERSION").to_string(),
            server_timestamp_ms: chrono::Utc::now().timestamp_millis(),
            supported_tables: vec!["STATEMENTS_SUMMARY".to_string()],
            protocol_version: "1.0".to_string(),
            collection_config: Some(CollectionConfig {
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
                extended_metrics: vec![],
                config_version: 1,
            }),
        }))
    }
}

#[async_trait]
impl DataCollector for GrpcPushCollector {
    fn collection_method(&self) -> CollectionMethod {
        CollectionMethod::GrpcPush
    }

    fn can_collect_table(&self, table: &TableConfig) -> bool {
        // Only STATEMENTS_SUMMARY tables are supported for now
        table.source_table.contains("STATEMENTS_SUMMARY")
    }

    async fn initialize(&mut self) -> Result<(), CollectionError> {
        // Start gRPC server first
        self.start_grpc_server();
        // Give server a moment to bind
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Register with TiDB
        self.register_push_target().await?;
        self.registered = true;

        // If output sender is set, start background flusher for real-time processing
        if self.output_sender.is_some() {
            self.start_buffer_flusher();
            info!(
                "gRPC push buffer flusher started for instance {}",
                self.instance
            );
        }

        info!(
            "gRPC push collector initialized for instance {} (table: {:?})",
            self.instance, self.table_type
        );
        Ok(())
    }

    fn set_output_sender(&mut self, sender: vector::SourceSender) {
        self.output_sender = Some(sender);
    }

    async fn collect_table_data(
        &self,
        table: &TableConfig,
    ) -> Result<CollectionResult, CollectionError> {
        // Drain buffer
        let batches: Vec<ReceivedBatch> = {
            let mut buf = self.buffer.lock().await;
            buf.drain(..).collect()
        };

        let mut all_rows = Vec::new();
        for batch in &batches {
            all_rows.extend(batch.statements.clone());
        }

        let row_count = all_rows.len();
        if row_count > 0 {
            info!(
                "gRPC push flushing {} rows from buffer for {}",
                row_count, table.source_table
            );
        }

        Ok(CollectionResult {
            data: all_rows,
            metadata: CollectionMetadata {
                instance: self.instance.clone(),
                table_config: table.clone(),
                collection_method: CollectionMethod::GrpcPush,
                timestamp: chrono::Utc::now(),
                row_count,
                duration_ms: 0,
                extra: HashMap::new(),
            },
        })
    }

    async fn health_check(&self) -> Result<(), CollectionError> {
        if self.registered {
            Ok(())
        } else {
            Err(CollectionError::ConnectionError(
                "Not registered with TiDB".to_string(),
            ))
        }
    }
}
