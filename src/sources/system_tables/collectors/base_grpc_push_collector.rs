//! Base gRPC Push Collector
//!
//! Provides common functionality for gRPC push-based collectors:
//! - Rate limiting (token bucket)
//! - Backpressure (buffer-based load throttling)
//! - Buffer management
//! - gRPC server setup

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

// Re-use the proto from grpc_push_collector to avoid duplicate types
use crate::sources::system_tables::collectors::grpc_push_collector::proto as shared_proto;

use shared_proto::statement_push_control_client::StatementPushControlClient;
use shared_proto::system_table_push_service_server::{
    SystemTablePushService, SystemTablePushServiceServer,
};
use shared_proto::{
    CollectionConfig, PingRequest, PingResponse, PushResponse, RegisterPushTargetRequest,
    StatementBatch, TableRowBatch,
};

/// Buffer for received statement batches
pub type StatementBuffer = Arc<Mutex<Vec<ReceivedBatch>>>;

/// A received batch from TiDB
#[derive(Debug, Clone)]
pub struct ReceivedBatch {
    pub cluster_id: String,
    pub instance_id: String,
    pub statements: Vec<HashMap<String, Value>>,
    pub received_at: chrono::DateTime<chrono::Utc>,
}

/// Rate limiter for incoming requests (token bucket)
#[derive(Clone)]
pub struct RateLimiter {
    tokens: Arc<std::sync::atomic::AtomicU64>,
    max_tokens: u64,
    refill_rate: u64,
    last_refill: Arc<tokio::sync::RwLock<std::time::Instant>>,
}

impl RateLimiter {
    pub fn new(max_tokens: u64, refill_rate: u64) -> Self {
        Self {
            tokens: Arc::new(std::sync::atomic::AtomicU64::new(max_tokens)),
            max_tokens,
            refill_rate,
            last_refill: Arc::new(tokio::sync::RwLock::new(std::time::Instant::now())),
        }
    }

    /// Attempts to consume a token without blocking
    pub async fn try_acquire(&self) -> bool {
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
    pub fn available_tokens(&self) -> u64 {
        self.tokens.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Backpressure state for handling high load
#[derive(Clone)]
pub struct BackpressureState {
    enabled: bool,
    threshold: f64,
    reject_threshold: f64,
    current_load: Arc<std::sync::atomic::AtomicU64>,
}

impl BackpressureState {
    pub fn new(threshold: f64, reject_threshold: f64) -> Self {
        Self {
            enabled: false,
            threshold,
            reject_threshold,
            current_load: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Updates the current load and returns action to take
    pub fn update_load(&self, load: f64) -> BackpressureAction {
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
    pub fn current_load(&self) -> f64 {
        self.current_load.load(std::sync::atomic::Ordering::Relaxed) as f64 / 100.0
    }
}

/// Action to take based on backpressure state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureAction {
    Accept,
    Throttle,
    Reject,
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

/// Base gRPC push collector — provides common functionality for push-based collectors.
/// This handles: rate limiting, backpressure, buffer management, gRPC server, registration.
pub struct BaseGrpcPushCollector {
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

impl BaseGrpcPushCollector {
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
                    buffer_capacity: 10000,
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

    /// Get the buffer for external access
    pub fn buffer(&self) -> StatementBuffer {
        self.buffer.clone()
    }

    /// Get rate limiter for external access
    pub fn rate_limiter(&self) -> Option<RateLimiter> {
        self.rate_limiter.clone()
    }

    /// Get backpressure state for external access
    pub fn backpressure(&self) -> BackpressureState {
        self.backpressure.clone()
    }

    /// Get collection policy for external access
    pub fn collection_policy(&self) -> CollectionPolicyConfig {
        self.collection_policy.clone()
    }

    /// Get buffer capacity
    pub fn buffer_capacity(&self) -> usize {
        self.buffer_capacity
    }

    /// Start background task to flush buffer to output immediately
    pub fn start_buffer_flusher(&self) {
        let buffer = self.buffer.clone();
        let sender = self.output_sender.clone().expect("output_sender not set");
        let table_config = self.table_config.clone().expect("table_config not set");
        let instance = self.instance.clone();

        tokio::spawn(async move {
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
    pub async fn register_push_target(&self) -> Result<(), CollectionError> {
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
    /// Subclasses should override this and call super.start_grpc_server() first
    pub fn start_grpc_server<S>(&mut self, service: S)
    where
        S: SystemTablePushService + Send + Clone + 'static,
    {
        let addr = format!("{}:{}", self.vector_grpc_address, self.vector_grpc_port);

        let handle = tokio::spawn(async move {
            let addr = addr.parse().expect("Invalid gRPC bind address");
            info!("Starting gRPC push receiver on {}", addr);

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

#[async_trait]
impl DataCollector for BaseGrpcPushCollector {
    fn collection_method(&self) -> CollectionMethod {
        CollectionMethod::GrpcPush
    }

    fn can_collect_table(&self, table: &TableConfig) -> bool {
        table.source_table.contains("STATEMENTS_SUMMARY")
    }

    async fn initialize(&mut self) -> Result<(), CollectionError> {
        // Note: gRPC server and registration should be done in subclass
        // after setting up the service handler
        self.registered = true;
        Ok(())
    }

    fn set_output_sender(&mut self, sender: vector::SourceSender) {
        self.output_sender = Some(sender);
    }

    async fn collect_table_data(
        &self,
        table: &TableConfig,
    ) -> Result<CollectionResult, CollectionError> {
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
