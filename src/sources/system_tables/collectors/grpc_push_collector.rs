use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

use vector::SourceSender;

use crate::sources::system_tables::data_collector::{
    CollectionError, CollectionMetadata, CollectionMethod, CollectionResult, CollectorConfig,
    CollectorConfigType, CollectionPolicyConfig, DataCollector,
};
use crate::sources::system_tables::TableConfig;
use base64::Engine;

/// Rate limiter for incoming requests (token bucket)
#[derive(Clone)]
struct RateLimiter {
    tokens: Arc<std::sync::atomic::AtomicU64>,
    max_tokens: u64,
    refill_rate: u64,
    last_refill: Arc<tokio::sync::RwLock<std::time::Instant>>,
}

impl RateLimiter {
    fn new(max_tokens: u64, refill_rate: u64) -> Self {
        Self {
            tokens: Arc::new(std::sync::atomic::AtomicU64::new(max_tokens)),
            max_tokens,
            refill_rate,
            last_refill: Arc::new(tokio::sync::RwLock::new(std::time::Instant::now())),
        }
    }

    /// Attempts to consume a token without blocking
    async fn try_acquire(&self) -> bool {
        self.refill().await;
        let current = self.tokens.load(std::sync::atomic::Ordering::Relaxed);
        if current == 0 {
            return false;
        }
        match self.tokens.compare_exchange(
            current,
            current - 1,
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
        ) {
            Ok(_) => true,
            Err(_) => false,
        }
    }

    /// Refills tokens based on elapsed time
    async fn refill(&self) {
        let mut last = self.last_refill.write().await;
        let elapsed = last.elapsed();
        if elapsed < std::time::Duration::from_secs(1) {
            return;
        }
        let tokens_to_add = (elapsed.as_secs() as u64) * self.refill_rate;
        let current = self.tokens.load(std::sync::atomic::Ordering::Relaxed);
        let new_count = (current + tokens_to_add).min(self.max_tokens);
        self.tokens.store(new_count, std::sync::atomic::Ordering::Relaxed);
        *last = std::time::Instant::now();
    }

    /// Returns the current token count
    fn available_tokens(&self) -> u64 {
        self.tokens.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Backpressure state for handling high load
#[derive(Clone)]
struct BackpressureState {
    enabled: bool,
    threshold: f64,
    reject_threshold: f64,
    current_load: Arc<std::sync::atomic::AtomicU64>,
}

impl BackpressureState {
    fn new(threshold: f64, reject_threshold: f64) -> Self {
        Self {
            enabled: false,
            threshold,
            reject_threshold,
            current_load: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Updates the current load and returns action to take
    fn update_load(&self, load: f64) -> BackpressureAction {
        self.current_load.store((load * 100.0) as u64, std::sync::atomic::Ordering::Relaxed);

        if load > self.reject_threshold {
            BackpressureAction::Reject
        } else if load > self.threshold {
            BackpressureAction::Throttle
        } else {
            BackpressureAction::Accept
        }
    }

    /// Gets the current load
    fn current_load(&self) -> f64 {
        self.current_load.load(std::sync::atomic::Ordering::Relaxed) as f64 / 100.0
    }
}

/// Action to take based on backpressure state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackpressureAction {
    Accept,
    Throttle,
    Reject,
}

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
    /// Rate limiter (None means unlimited)
    rate_limiter: Option<RateLimiter>,
    /// Backpressure state
    backpressure: BackpressureState,
    /// Collection policy configuration
    collection_policy: CollectionPolicyConfig,
    /// Buffer capacity for backpressure calculation
    buffer_capacity: usize,
}

impl GrpcPushCollector {
    pub fn new(config: CollectorConfig, table_config: TableConfig) -> Result<Self, CollectionError> {
        match config.config_type {
            CollectorConfigType::GrpcPush {
                host,
                status_port,
                vector_grpc_address,
                vector_grpc_port,
                rate_limit,
                backpressure_threshold,
                backpressure_reject_threshold,
                collection_policy,
                ..
            } => {
                // Determine table type based on table_config
                let table_type = if table_config.source_table.contains("STATEMENTS_SUMMARY") {
                    PushTableType::StatementsSummary
                } else {
                    PushTableType::Other(table_config.source_table.clone())
                };

                // Initialize rate limiter if rate_limit > 0
                let rate_limiter = if rate_limit > 0 {
                    // Use rate_limit as both max_tokens and refill_rate for simplicity
                    // This gives us rate_limit tokens per second
                    Some(RateLimiter::new(rate_limit as u64, rate_limit as u64))
                } else {
                    None
                };

                // Initialize backpressure state
                let backpressure = BackpressureState::new(
                    backpressure_threshold,
                    backpressure_reject_threshold,
                );

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
                    rate_limiter,
                    backpressure,
                    collection_policy,
                    buffer_capacity: 10000, // Default buffer capacity
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
            collection_config: Some(self.collection_policy.to_proto()),
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
        let rate_limiter = self.rate_limiter.clone();
        let backpressure = self.backpressure.clone();
        let buffer_capacity = self.buffer_capacity;
        let collection_policy = self.collection_policy.clone();

        let handle = tokio::spawn(async move {
            let addr = addr.parse().expect("Invalid gRPC bind address");
            info!("Starting gRPC push receiver on {}", addr);

            let service = GrpcPushService {
                buffer,
                rate_limiter,
                backpressure,
                buffer_capacity,
                collection_policy,
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
    rate_limiter: Option<RateLimiter>,
    backpressure: BackpressureState,
    buffer_capacity: usize,
    collection_policy: CollectionPolicyConfig,
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

        // Check rate limiter first
        if let Some(ref limiter) = self.rate_limiter {
            if !limiter.try_acquire().await {
                warn!(
                    "Rate limit exceeded, rejecting {} statements from {}/{}",
                    stmt_count, cluster_id, instance_id
                );
                return Ok(tonic::Response::new(PushResponse {
                    success: false,
                    message: "Rate limit exceeded".to_string(),
                    received_timestamp_ms: chrono::Utc::now().timestamp_millis(),
                    accepted_count: 0,
                    rejected_count: stmt_count as i32,
                    errors: vec!["Rate limit exceeded".to_string()],
                }));
            }
        }

        // Check backpressure (based on buffer size)
        let buffer_size = self.buffer.lock().await.len();
        let buffer_load = buffer_size as f64 / self.buffer_capacity as f64;
        let bp_action = self.backpressure.update_load(buffer_load);

        match bp_action {
            BackpressureAction::Reject => {
                warn!(
                    "Backpressure REJECT: buffer {}/{} ({:.1}%), rejecting {} statements from {}/{}",
                    buffer_size, self.buffer_capacity, buffer_load * 100.0, stmt_count, cluster_id, instance_id
                );
                return Ok(tonic::Response::new(PushResponse {
                    success: false,
                    message: format!("Backpressure: buffer {:.0}% full, rejecting", buffer_load * 100.0),
                    received_timestamp_ms: chrono::Utc::now().timestamp_millis(),
                    accepted_count: 0,
                    rejected_count: stmt_count as i32,
                    errors: vec![format!("Backpressure: buffer {:.0}% full", buffer_load * 100.0)],
                }));
            }
            BackpressureAction::Throttle => {
                warn!(
                    "Backpressure THROTTLE: buffer {}/{} ({:.1}%), throttling {} statements from {}/{}",
                    buffer_size, self.buffer_capacity, buffer_load * 100.0, stmt_count, cluster_id, instance_id
                );
            }
            BackpressureAction::Accept => {
                debug!(
                    "Processing {} statements from {}/{} (buffer: {}/{} = {:.1}%)",
                    stmt_count, cluster_id, instance_id, buffer_size, self.buffer_capacity, buffer_load * 100.0
                );
            }
        }

        info!(
            "Received push from TiDB {}/{}: {} statements",
            cluster_id, instance_id, stmt_count
        );

        // Convert proto statements to HashMap<String, Value> - full 80+ fields
        let mut rows = Vec::with_capacity(stmt_count);
        for stmt in &batch.statements {
            let mut row: HashMap<String, Value> = HashMap::new();

            // ====================================================================
            // IDENTITY FIELDS
            // ====================================================================
            row.insert("digest".to_string(), Value::String(stmt.digest.clone()));
            row.insert("plan_digest".to_string(), Value::String(stmt.plan_digest.clone()));
            row.insert("schema_name".to_string(), Value::String(stmt.schema_name.clone()));
            row.insert("normalized_sql".to_string(), Value::String(stmt.normalized_sql.clone()));
            row.insert("table_names".to_string(), Value::String(stmt.table_names.clone()));
            row.insert("stmt_type".to_string(), Value::String(stmt.stmt_type.clone()));

            // ====================================================================
            // SAMPLE DATA
            // ====================================================================
            row.insert("sample_sql".to_string(), Value::String(stmt.sample_sql.clone()));
            row.insert("sample_plan".to_string(), Value::String(stmt.sample_plan.clone()));
            row.insert("prev_sql".to_string(), Value::String(stmt.prev_sql.clone()));

            // ====================================================================
            // EXECUTION STATISTICS
            // ====================================================================
            row.insert("exec_count".to_string(), Value::Number(stmt.exec_count.into()));
            row.insert("sum_errors".to_string(), Value::Number(stmt.sum_errors.into()));
            row.insert("sum_warnings".to_string(), Value::Number(stmt.sum_warnings.into()));

            // ====================================================================
            // LATENCY METRICS (microseconds)
            // ====================================================================
            row.insert("sum_latency".to_string(), Value::Number(stmt.sum_latency_us.into()));
            row.insert("max_latency".to_string(), Value::Number(stmt.max_latency_us.into()));
            row.insert("min_latency".to_string(), Value::Number(stmt.min_latency_us.into()));
            row.insert("avg_latency".to_string(), Value::Number(stmt.avg_latency_us.into()));
            row.insert("p50_latency".to_string(), Value::Number(stmt.p50_latency_us.into()));
            row.insert("p95_latency".to_string(), Value::Number(stmt.p95_latency_us.into()));
            row.insert("p99_latency".to_string(), Value::Number(stmt.p99_latency_us.into()));

            // ====================================================================
            // PARSE/COMPILE METRICS
            // ====================================================================
            row.insert("sum_parse_latency".to_string(), Value::Number(stmt.sum_parse_latency_us.into()));
            row.insert("max_parse_latency".to_string(), Value::Number(stmt.max_parse_latency_us.into()));
            row.insert("sum_compile_latency".to_string(), Value::Number(stmt.sum_compile_latency_us.into()));
            row.insert("max_compile_latency".to_string(), Value::Number(stmt.max_compile_latency_us.into()));

            // ====================================================================
            // RESOURCE USAGE
            // ====================================================================
            row.insert("sum_mem_bytes".to_string(), Value::Number(stmt.sum_mem_bytes.into()));
            row.insert("max_mem_bytes".to_string(), Value::Number(stmt.max_mem_bytes.into()));
            row.insert("sum_disk_bytes".to_string(), Value::Number(stmt.sum_disk_bytes.into()));
            row.insert("max_disk_bytes".to_string(), Value::Number(stmt.max_disk_bytes.into()));
            row.insert("sum_tidb_cpu".to_string(), Value::Number(stmt.sum_tidb_cpu_us.into()));
            row.insert("sum_tikv_cpu".to_string(), Value::Number(stmt.sum_tikv_cpu_us.into()));

            // ====================================================================
            // TIKV COPROCESSOR METRICS
            // ====================================================================
            row.insert("sum_num_cop_tasks".to_string(), Value::Number(stmt.sum_num_cop_tasks.into()));
            row.insert("sum_process_time".to_string(), Value::Number(stmt.sum_process_time_us.into()));
            row.insert("max_process_time".to_string(), Value::Number(stmt.max_process_time_us.into()));
            row.insert("sum_wait_time".to_string(), Value::Number(stmt.sum_wait_time_us.into()));
            row.insert("max_wait_time".to_string(), Value::Number(stmt.max_wait_time_us.into()));

            // ====================================================================
            // KEY SCAN METRICS
            // ====================================================================
            row.insert("sum_total_keys".to_string(), Value::Number(stmt.sum_total_keys.into()));
            row.insert("max_total_keys".to_string(), Value::Number(stmt.max_total_keys.into()));
            row.insert("sum_processed_keys".to_string(), Value::Number(stmt.sum_processed_keys.into()));
            row.insert("max_processed_keys".to_string(), Value::Number(stmt.max_processed_keys.into()));

            // ====================================================================
            // TRANSACTION METRICS
            // ====================================================================
            row.insert("commit_count".to_string(), Value::Number(stmt.commit_count.into()));
            row.insert("sum_prewrite_time".to_string(), Value::Number(stmt.sum_prewrite_time_us.into()));
            row.insert("max_prewrite_time".to_string(), Value::Number(stmt.max_prewrite_time_us.into()));
            row.insert("sum_commit_time".to_string(), Value::Number(stmt.sum_commit_time_us.into()));
            row.insert("max_commit_time".to_string(), Value::Number(stmt.max_commit_time_us.into()));
            row.insert("sum_write_keys".to_string(), Value::Number(stmt.sum_write_keys.into()));
            row.insert("max_write_keys".to_string(), Value::Number(stmt.max_write_keys.into()));
            row.insert("sum_write_size_bytes".to_string(), Value::Number(stmt.sum_write_size_bytes.into()));
            row.insert("max_write_size_bytes".to_string(), Value::Number(stmt.max_write_size_bytes.into()));

            // ====================================================================
            // ROW STATISTICS
            // ====================================================================
            row.insert("sum_affected_rows".to_string(), Value::Number(stmt.sum_affected_rows.into()));
            row.insert("sum_result_rows".to_string(), Value::Number(stmt.sum_result_rows.into()));
            row.insert("max_result_rows".to_string(), Value::Number(stmt.max_result_rows.into()));
            row.insert("min_result_rows".to_string(), Value::Number(stmt.min_result_rows.into()));

            // ====================================================================
            // PLAN CACHE
            // ====================================================================
            row.insert("plan_in_cache".to_string(), Value::Bool(stmt.plan_in_cache));
            row.insert("plan_cache_hits".to_string(), Value::Number(stmt.plan_cache_hits.into()));

            // ====================================================================
            // TIMESTAMPS
            // ====================================================================
            row.insert("first_seen_ms".to_string(), Value::Number(stmt.first_seen_ms.into()));
            row.insert("last_seen_ms".to_string(), Value::Number(stmt.last_seen_ms.into()));

            // ====================================================================
            // FLAGS
            // ====================================================================
            row.insert("is_internal".to_string(), Value::Bool(stmt.is_internal));
            row.insert("prepared".to_string(), Value::Bool(stmt.prepared));

            // ====================================================================
            // MULTI-TENANCY
            // ====================================================================
            row.insert("keyspace_name".to_string(), Value::String(stmt.keyspace_name.clone()));
            row.insert("keyspace_id".to_string(), Value::Number(stmt.keyspace_id.into()));
            row.insert("resource_group_name".to_string(), Value::String(stmt.resource_group_name.clone()));

            // ====================================================================
            // CLUSTER METADATA
            // ====================================================================
            row.insert("cluster_id".to_string(), Value::String(cluster_id.clone()));
            row.insert("instance_id".to_string(), Value::String(instance_id.clone()));

            // ====================================================================
            // BATCH METADATA
            // ====================================================================
            if let Some(ref m) = batch.metadata {
                row.insert("window_start_ms".to_string(), Value::Number(m.window_start_ms.into()));
                row.insert("window_end_ms".to_string(), Value::Number(m.window_end_ms.into()));
                row.insert("batch_sequence".to_string(), Value::Number(m.batch_sequence.into()));
                row.insert("batch_timestamp_ms".to_string(), Value::Number(m.batch_timestamp_ms.into()));
                row.insert("schema_version".to_string(), Value::String(m.schema_version.clone()));
                row.insert("schema_id".to_string(), Value::Number(m.schema_id.into()));
            }

            // ====================================================================
            // EXTENDED METRICS (dynamic)
            // ====================================================================
            for (key, value) in &stmt.extended_metrics {
                let json_value = match &value.value {
                    Some(proto::metric_value::Value::Int64Val(v)) => Value::Number((*v).into()),
                    Some(proto::metric_value::Value::DoubleVal(v)) => {
                        Value::Number(serde_json::Number::from_f64(*v).unwrap_or(serde_json::Number::from(0)))
                    }
                    Some(proto::metric_value::Value::StringVal(v)) => Value::String(v.clone()),
                    Some(proto::metric_value::Value::BoolVal(v)) => Value::Bool(*v),
                    Some(proto::metric_value::Value::BytesVal(v)) => Value::String(base64::prelude::BASE64_STANDARD.encode(v)),
                    None => Value::Null,
                };
                row.insert(key.clone(), json_value);
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
            collection_config: Some(self.collection_policy.to_proto()),
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
