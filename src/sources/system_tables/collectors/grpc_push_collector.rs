use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

use vector::SourceSender;

use crate::sources::system_tables::data_collector::{
    CollectionError, CollectionMetadata, CollectionMethod, CollectionPolicyConfig,
    CollectionResult, CollectorConfig, CollectorConfigType, DataCollector,
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
        self.tokens
            .store(new_count, std::sync::atomic::Ordering::Relaxed);
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
        self.current_load
            .store((load * 100.0) as u64, std::sync::atomic::Ordering::Relaxed);

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

const TIDB_STATEMENT_SUMMARY_COLUMNS: &[&str] = &[
    "INSTANCE",
    "SUMMARY_BEGIN_TIME",
    "SUMMARY_END_TIME",
    "STMT_TYPE",
    "SCHEMA_NAME",
    "DIGEST",
    "DIGEST_TEXT",
    "TABLE_NAMES",
    "INDEX_NAMES",
    "SAMPLE_USER",
    "EXEC_COUNT",
    "SUM_ERRORS",
    "SUM_WARNINGS",
    "SUM_LATENCY",
    "MAX_LATENCY",
    "MIN_LATENCY",
    "AVG_LATENCY",
    "AVG_PARSE_LATENCY",
    "MAX_PARSE_LATENCY",
    "AVG_COMPILE_LATENCY",
    "MAX_COMPILE_LATENCY",
    "SUM_COP_TASK_NUM",
    "MAX_COP_PROCESS_TIME",
    "MAX_COP_PROCESS_ADDRESS",
    "MAX_COP_WAIT_TIME",
    "MAX_COP_WAIT_ADDRESS",
    "AVG_PROCESS_TIME",
    "MAX_PROCESS_TIME",
    "AVG_WAIT_TIME",
    "MAX_WAIT_TIME",
    "AVG_BACKOFF_TIME",
    "MAX_BACKOFF_TIME",
    "AVG_TOTAL_KEYS",
    "MAX_TOTAL_KEYS",
    "AVG_PROCESSED_KEYS",
    "MAX_PROCESSED_KEYS",
    "AVG_ROCKSDB_DELETE_SKIPPED_COUNT",
    "MAX_ROCKSDB_DELETE_SKIPPED_COUNT",
    "AVG_ROCKSDB_KEY_SKIPPED_COUNT",
    "MAX_ROCKSDB_KEY_SKIPPED_COUNT",
    "AVG_ROCKSDB_BLOCK_CACHE_HIT_COUNT",
    "MAX_ROCKSDB_BLOCK_CACHE_HIT_COUNT",
    "AVG_ROCKSDB_BLOCK_READ_COUNT",
    "MAX_ROCKSDB_BLOCK_READ_COUNT",
    "AVG_ROCKSDB_BLOCK_READ_BYTE",
    "MAX_ROCKSDB_BLOCK_READ_BYTE",
    "AVG_PREWRITE_TIME",
    "MAX_PREWRITE_TIME",
    "AVG_COMMIT_TIME",
    "MAX_COMMIT_TIME",
    "AVG_GET_COMMIT_TS_TIME",
    "MAX_GET_COMMIT_TS_TIME",
    "AVG_COMMIT_BACKOFF_TIME",
    "MAX_COMMIT_BACKOFF_TIME",
    "AVG_RESOLVE_LOCK_TIME",
    "MAX_RESOLVE_LOCK_TIME",
    "AVG_LOCAL_LATCH_WAIT_TIME",
    "MAX_LOCAL_LATCH_WAIT_TIME",
    "AVG_WRITE_KEYS",
    "MAX_WRITE_KEYS",
    "AVG_WRITE_SIZE",
    "MAX_WRITE_SIZE",
    "AVG_PREWRITE_REGIONS",
    "MAX_PREWRITE_REGIONS",
    "AVG_TXN_RETRY",
    "MAX_TXN_RETRY",
    "SUM_EXEC_RETRY",
    "SUM_EXEC_RETRY_TIME",
    "SUM_BACKOFF_TIMES",
    "BACKOFF_TYPES",
    "AVG_MEM",
    "MAX_MEM",
    "AVG_MEM_ARBITRATION",
    "MAX_MEM_ARBITRATION",
    "AVG_DISK",
    "MAX_DISK",
    "AVG_KV_TIME",
    "AVG_PD_TIME",
    "AVG_BACKOFF_TOTAL_TIME",
    "AVG_WRITE_SQL_RESP_TIME",
    "AVG_TIDB_CPU_TIME",
    "AVG_TIKV_CPU_TIME",
    "MAX_RESULT_ROWS",
    "MIN_RESULT_ROWS",
    "AVG_RESULT_ROWS",
    "PREPARED",
    "AVG_AFFECTED_ROWS",
    "FIRST_SEEN",
    "LAST_SEEN",
    "PLAN_IN_CACHE",
    "PLAN_CACHE_HITS",
    "PLAN_IN_BINDING",
    "QUERY_SAMPLE_TEXT",
    "PREV_SAMPLE_TEXT",
    "PLAN_DIGEST",
    "PLAN",
    "BINARY_PLAN",
    "BINDING_DIGEST",
    "BINDING_DIGEST_TEXT",
    "CHARSET",
    "COLLATION",
    "PLAN_HINT",
    "MAX_REQUEST_UNIT_READ",
    "AVG_REQUEST_UNIT_READ",
    "MAX_REQUEST_UNIT_WRITE",
    "AVG_REQUEST_UNIT_WRITE",
    "MAX_QUEUED_RC_TIME",
    "AVG_QUEUED_RC_TIME",
    "RESOURCE_GROUP",
    "PLAN_CACHE_UNQUALIFIED",
    "PLAN_CACHE_UNQUALIFIED_LAST_REASON",
    "SUM_UNPACKED_BYTES_SENT_TIKV_TOTAL",
    "SUM_UNPACKED_BYTES_RECEIVED_TIKV_TOTAL",
    "SUM_UNPACKED_BYTES_SENT_TIKV_CROSS_ZONE",
    "SUM_UNPACKED_BYTES_RECEIVED_TIKV_CROSS_ZONE",
    "SUM_UNPACKED_BYTES_SENT_TIFLASH_TOTAL",
    "SUM_UNPACKED_BYTES_RECEIVED_TIFLASH_TOTAL",
    "SUM_UNPACKED_BYTES_SENT_TIFLASH_CROSS_ZONE",
    "SUM_UNPACKED_BYTES_RECEIVED_TIFLASH_CROSS_ZONE",
    "STORAGE_KV",
    "STORAGE_MPP",
];

// Default tonic limit is 4 MiB, which is too small for high-QPS statement
// summary windows. Raise both receive/send limits for push RPC payloads.
const GRPC_MAX_MESSAGE_SIZE: usize = 128 * 1024 * 1024;

fn proto_data_type_to_mysql_type(data_type: i32) -> &'static str {
    match data_type {
        // STRING
        1 => "varchar",
        // INT64
        2 => "bigint",
        // UINT64
        3 => "bigint unsigned",
        // FLOAT64
        4 => "double",
        // BOOL
        5 => "tinyint(1)",
        // BYTES
        6 => "blob",
        // TIMESTAMP
        7 => "timestamp",
        // DURATION
        8 => "bigint",
        // JSON
        9 => "json",
        _ => "varchar",
    }
}

fn proto_value_to_mysql_type(value: &proto::Value) -> Option<&'static str> {
    match &value.kind {
        Some(proto::value::Kind::StringVal(_)) => Some("varchar"),
        Some(proto::value::Kind::Int64Val(_)) => Some("bigint"),
        Some(proto::value::Kind::Uint64Val(_)) => Some("bigint unsigned"),
        Some(proto::value::Kind::Float64Val(_)) => Some("double"),
        Some(proto::value::Kind::BoolVal(_)) => Some("tinyint(1)"),
        Some(proto::value::Kind::BytesVal(_)) => Some("blob"),
        Some(proto::value::Kind::TimestampMs(_)) => Some("timestamp"),
        Some(proto::value::Kind::DurationUs(_)) => Some("bigint"),
        Some(proto::value::Kind::JsonVal(_)) => Some("json"),
        Some(proto::value::Kind::NullVal(_)) | None => None,
    }
}

pub(crate) fn build_schema_metadata_from_proto_schema(
    schema: &proto::TableSchema,
    rows: &[proto::TableRow],
) -> serde_json::Map<String, Value> {
    let mut schema_metadata = serde_json::Map::new();
    let mut fallback_cols = Vec::new();
    for (idx, col) in schema.columns.iter().enumerate() {
        let mysql_type = if col.r#type != 0 {
            proto_data_type_to_mysql_type(col.r#type)
        } else {
            fallback_cols.push(col.name.clone());
            rows.iter()
                .filter_map(|r| r.values.get(idx))
                .find_map(proto_value_to_mysql_type)
                .unwrap_or("varchar")
        };

        let mut field_info = serde_json::Map::new();
        field_info.insert(
            "mysql_type".to_string(),
            Value::String(mysql_type.to_string()),
        );
        schema_metadata.insert(col.name.clone(), Value::Object(field_info));
    }
    if !fallback_cols.is_empty() {
        warn!(
            "proto schema contains UNKNOWN column types; fell back to row-value type inference for {:?}",
            fallback_cols
        );
    }
    schema_metadata
}

pub(crate) fn proto_value_to_json(value: &proto::Value) -> Value {
    match &value.kind {
        Some(proto::value::Kind::StringVal(s)) => Value::String(s.clone()),
        Some(proto::value::Kind::Int64Val(i)) => Value::Number((*i).into()),
        Some(proto::value::Kind::Uint64Val(u)) => Value::Number((*u).into()),
        Some(proto::value::Kind::Float64Val(f)) => {
            Value::Number(serde_json::Number::from_f64(*f).unwrap_or(serde_json::Number::from(0)))
        }
        Some(proto::value::Kind::BoolVal(b)) => Value::Bool(*b),
        Some(proto::value::Kind::BytesVal(v)) => {
            Value::String(base64::prelude::BASE64_STANDARD.encode(v))
        }
        // Use microseconds so Delta TIMESTAMP columns are written correctly.
        Some(proto::value::Kind::TimestampMs(ts)) => Value::Number(ts.saturating_mul(1000).into()),
        Some(proto::value::Kind::DurationUs(d)) => Value::Number((*d).into()),
        Some(proto::value::Kind::JsonVal(v)) => serde_json::from_slice(v).unwrap_or(Value::Null),
        Some(proto::value::Kind::NullVal(_)) => Value::Null,
        None => Value::Null,
    }
}

fn value_to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n
            .as_f64()
            .or_else(|| n.as_i64().map(|x| x as f64))
            .or_else(|| n.as_u64().map(|x| x as f64)),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn get_value(row: &HashMap<String, Value>, keys: &[&str]) -> Option<Value> {
    for k in keys {
        if let Some(v) = row.get(*k) {
            return Some(v.clone());
        }
    }
    None
}

fn set_alias(row: &mut HashMap<String, Value>, target: &str, sources: &[&str]) {
    if row.get(target).is_some() {
        return;
    }
    if let Some(v) = get_value(row, sources) {
        row.insert(target.to_string(), v);
    }
}

fn ms_to_utc_string(ms: i64) -> Option<String> {
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
}

pub fn align_statement_summary_row_schema(row: &mut HashMap<String, Value>) {
    set_alias(
        row,
        "DIGEST_TEXT",
        &["DIGEST_TEXT", "digest_text", "normalized_sql"],
    );
    set_alias(
        row,
        "INSTANCE",
        &["INSTANCE", "instance", "instance_id", "INSTANCE_ID"],
    );
    set_alias(
        row,
        "QUERY_SAMPLE_TEXT",
        &[
            "QUERY_SAMPLE_TEXT",
            "query_sample_text",
            "sample_sql",
            "SAMPLE_SQL",
        ],
    );
    set_alias(
        row,
        "PREV_SAMPLE_TEXT",
        &[
            "PREV_SAMPLE_TEXT",
            "prev_sample_text",
            "prev_sql",
            "PREV_SQL",
        ],
    );
    set_alias(row, "PLAN", &["PLAN", "plan", "sample_plan", "SAMPLE_PLAN"]);
    set_alias(
        row,
        "BINARY_PLAN",
        &[
            "BINARY_PLAN",
            "binary_plan",
            "sample_binary_plan",
            "SAMPLE_BINARY_PLAN",
        ],
    );
    set_alias(
        row,
        "BINDING_DIGEST_TEXT",
        &[
            "BINDING_DIGEST_TEXT",
            "binding_digest_text",
            "binding_sql",
            "BINDING_SQL",
        ],
    );
    set_alias(
        row,
        "RESOURCE_GROUP",
        &[
            "RESOURCE_GROUP",
            "resource_group",
            "resource_group_name",
            "RESOURCE_GROUP_NAME",
        ],
    );
    set_alias(
        row,
        "SUM_COP_TASK_NUM",
        &["SUM_COP_TASK_NUM", "sum_cop_task_num", "sum_num_cop_tasks"],
    );
    set_alias(
        row,
        "SUM_EXEC_RETRY",
        &[
            "SUM_EXEC_RETRY",
            "sum_exec_retry",
            "exec_retry_count",
            "EXEC_RETRY_COUNT",
        ],
    );
    set_alias(
        row,
        "SUM_EXEC_RETRY_TIME",
        &[
            "SUM_EXEC_RETRY_TIME",
            "sum_exec_retry_time",
            "exec_retry_time_us",
            "EXEC_RETRY_TIME_US",
        ],
    );
    set_alias(
        row,
        "MAX_BACKOFF_TIME",
        &[
            "MAX_BACKOFF_TIME",
            "max_backoff_time",
            "max_backoff_time_us",
        ],
    );
    set_alias(
        row,
        "MAX_COP_PROCESS_TIME",
        &[
            "MAX_COP_PROCESS_TIME",
            "max_cop_process_time",
            "max_cop_process_time_us",
        ],
    );
    set_alias(
        row,
        "MAX_COP_WAIT_TIME",
        &[
            "MAX_COP_WAIT_TIME",
            "max_cop_wait_time",
            "max_cop_wait_time_us",
        ],
    );
    set_alias(
        row,
        "MAX_GET_COMMIT_TS_TIME",
        &[
            "MAX_GET_COMMIT_TS_TIME",
            "max_get_commit_ts_time",
            "max_get_commit_ts_time_us",
        ],
    );
    set_alias(
        row,
        "MAX_LOCAL_LATCH_WAIT_TIME",
        &[
            "MAX_LOCAL_LATCH_WAIT_TIME",
            "max_local_latch_wait_time",
            "max_local_latch_time_us",
        ],
    );
    set_alias(row, "MAX_DISK", &["MAX_DISK", "max_disk", "max_disk_bytes"]);
    set_alias(row, "MAX_MEM", &["MAX_MEM", "max_mem", "max_mem_bytes"]);
    set_alias(
        row,
        "MAX_PREWRITE_REGIONS",
        &[
            "MAX_PREWRITE_REGIONS",
            "max_prewrite_regions",
            "max_prewrite_region_num",
        ],
    );
    set_alias(
        row,
        "MAX_QUEUED_RC_TIME",
        &[
            "MAX_QUEUED_RC_TIME",
            "max_queued_rc_time",
            "max_ru_wait_duration_us",
        ],
    );
    set_alias(
        row,
        "MAX_WRITE_SIZE",
        &["MAX_WRITE_SIZE", "max_write_size", "max_write_size_bytes"],
    );
    set_alias(
        row,
        "PLAN_CACHE_UNQUALIFIED",
        &[
            "PLAN_CACHE_UNQUALIFIED",
            "plan_cache_unqualified",
            "plan_cache_unqualified_count",
        ],
    );
    set_alias(
        row,
        "MAX_REQUEST_UNIT_READ",
        &["MAX_REQUEST_UNIT_READ", "max_request_unit_read", "max_rru"],
    );
    set_alias(
        row,
        "MAX_REQUEST_UNIT_WRITE",
        &[
            "MAX_REQUEST_UNIT_WRITE",
            "max_request_unit_write",
            "max_wru",
        ],
    );

    if row.get("SUMMARY_BEGIN_TIME").is_none() {
        if let Some(v) = get_value(row, &["window_start_ms", "WINDOW_START_MS"]) {
            if let Some(ms) = value_to_f64(&v).map(|x| x as i64) {
                if let Some(ts) = ms_to_utc_string(ms) {
                    row.insert("SUMMARY_BEGIN_TIME".to_string(), Value::String(ts));
                }
            }
        }
    }
    if row.get("SUMMARY_END_TIME").is_none() {
        if let Some(v) = get_value(row, &["window_end_ms", "WINDOW_END_MS"]) {
            if let Some(ms) = value_to_f64(&v).map(|x| x as i64) {
                if let Some(ts) = ms_to_utc_string(ms) {
                    row.insert("SUMMARY_END_TIME".to_string(), Value::String(ts));
                }
            }
        }
    }
    if row.get("FIRST_SEEN").is_none() {
        if let Some(v) = get_value(row, &["first_seen", "first_seen_ms", "FIRST_SEEN_MS"]) {
            if let Some(ms) = value_to_f64(&v).map(|x| x as i64) {
                if let Some(ts) = ms_to_utc_string(ms) {
                    row.insert("FIRST_SEEN".to_string(), Value::String(ts));
                }
            }
        }
    }
    if row.get("LAST_SEEN").is_none() {
        if let Some(v) = get_value(row, &["last_seen", "last_seen_ms", "LAST_SEEN_MS"]) {
            if let Some(ms) = value_to_f64(&v).map(|x| x as i64) {
                if let Some(ts) = ms_to_utc_string(ms) {
                    row.insert("LAST_SEEN".to_string(), Value::String(ts));
                }
            }
        }
    }

    let exec_count = get_value(row, &["EXEC_COUNT", "exec_count"]).and_then(|v| value_to_f64(&v));
    if let Some(exec) = exec_count.filter(|x| *x > 0.0) {
        if row.get("AVG_GET_COMMIT_TS_TIME").is_none() {
            if let Some(sum_v) = get_value(
                row,
                &["sum_get_commit_ts_time_us", "SUM_GET_COMMIT_TS_TIME_US"],
            ) {
                if let Some(sum) = value_to_f64(&sum_v) {
                    if let Some(n) = serde_json::Number::from_f64(sum / exec) {
                        row.insert("AVG_GET_COMMIT_TS_TIME".to_string(), Value::Number(n));
                    }
                }
            }
        }
        if row.get("AVG_TIDB_CPU_TIME").is_none() {
            if let Some(sum_v) = get_value(row, &["sum_tidb_cpu", "SUM_TIDB_CPU"]) {
                if let Some(sum) = value_to_f64(&sum_v) {
                    if let Some(n) = serde_json::Number::from_f64(sum / exec) {
                        row.insert("AVG_TIDB_CPU_TIME".to_string(), Value::Number(n));
                    }
                }
            }
        }
        if row.get("AVG_TIKV_CPU_TIME").is_none() {
            if let Some(sum_v) = get_value(row, &["sum_tikv_cpu", "SUM_TIKV_CPU"]) {
                if let Some(sum) = value_to_f64(&sum_v) {
                    if let Some(n) = serde_json::Number::from_f64(sum / exec) {
                        row.insert("AVG_TIKV_CPU_TIME".to_string(), Value::Number(n));
                    }
                }
            }
        }
    }

    let mut aligned = HashMap::with_capacity(TIDB_STATEMENT_SUMMARY_COLUMNS.len());
    for col in TIDB_STATEMENT_SUMMARY_COLUMNS {
        let lc = col.to_ascii_lowercase();
        let value = get_value(row, &[*col, &lc]).unwrap_or(Value::Null);
        aligned.insert((*col).to_string(), value);
    }
    *row = aligned;
}

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
    pub fn new(
        config: CollectorConfig,
        table_config: TableConfig,
    ) -> Result<Self, CollectionError> {
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
                let backpressure =
                    BackpressureState::new(backpressure_threshold, backpressure_reject_threshold);

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

                    match sender.send_event(event).await {
                        Ok(_) => {}
                        Err(e) => {
                            error!("Failed to send gRPC push event: {}", e);
                        }
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

        // Decide advertise address for TiDB callback.
        // - Bind address can be 0.0.0.0
        // - Advertise address must be reachable by TiDB in the cluster
        let reachable_address = if self.vector_grpc_address == "0.0.0.0" {
            if let Ok(addr) = std::env::var("VECTOR_GRPC_ADVERTISE_ADDRESS") {
                if !addr.trim().is_empty() {
                    info!(
                        "Using VECTOR_GRPC_ADVERTISE_ADDRESS for push target: {}",
                        addr
                    );
                    addr
                } else {
                    self.host.clone()
                }
            } else if let Ok(svc_host) = std::env::var("VECTOR_GRPC_SERVICE_HOST") {
                if !svc_host.trim().is_empty() {
                    info!(
                        "Using VECTOR_GRPC_SERVICE_HOST for push target: {}",
                        svc_host
                    );
                    svc_host
                } else {
                    self.host.clone()
                }
            } else if let Ok(pod_ip) = std::env::var("POD_IP") {
                if !pod_ip.trim().is_empty() {
                    info!("Using POD_IP for push target: {}", pod_ip);
                    pod_ip
                } else {
                    warn!(
                        "No advertise address env found; fallback to TiDB host {} (may be unreachable)",
                        self.host
                    );
                    self.host.clone()
                }
            } else {
                warn!(
                    "No advertise address env found; fallback to TiDB host {} (may be unreachable)",
                    self.host
                );
                self.host.clone()
            }
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
            info!(
                "Successfully registered push target with TiDB at {}",
                endpoint
            );
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
                .add_service(
                    SystemTablePushServiceServer::new(service)
                        .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE)
                        .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE),
                )
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
                    message: format!(
                        "Backpressure: buffer {:.0}% full, rejecting",
                        buffer_load * 100.0
                    ),
                    received_timestamp_ms: chrono::Utc::now().timestamp_millis(),
                    accepted_count: 0,
                    rejected_count: stmt_count as i32,
                    errors: vec![format!(
                        "Backpressure: buffer {:.0}% full",
                        buffer_load * 100.0
                    )],
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
                    stmt_count,
                    cluster_id,
                    instance_id,
                    buffer_size,
                    self.buffer_capacity,
                    buffer_load * 100.0
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
            row.insert(
                "plan_digest".to_string(),
                Value::String(stmt.plan_digest.clone()),
            );
            row.insert(
                "schema_name".to_string(),
                Value::String(stmt.schema_name.clone()),
            );
            row.insert(
                "normalized_sql".to_string(),
                Value::String(stmt.normalized_sql.clone()),
            );
            // Backward compatibility alias expected by existing diagnosis queries
            row.insert(
                "digest_text".to_string(),
                Value::String(stmt.normalized_sql.clone()),
            );
            row.insert(
                "table_names".to_string(),
                Value::String(stmt.table_names.clone()),
            );
            row.insert(
                "stmt_type".to_string(),
                Value::String(stmt.stmt_type.clone()),
            );

            // ====================================================================
            // SAMPLE DATA
            // ====================================================================
            row.insert(
                "sample_sql".to_string(),
                Value::String(stmt.sample_sql.clone()),
            );
            row.insert(
                "sample_plan".to_string(),
                Value::String(stmt.sample_plan.clone()),
            );
            row.insert("prev_sql".to_string(), Value::String(stmt.prev_sql.clone()));

            // ====================================================================
            // EXECUTION STATISTICS
            // ====================================================================
            row.insert(
                "exec_count".to_string(),
                Value::Number(stmt.exec_count.into()),
            );
            row.insert(
                "sum_errors".to_string(),
                Value::Number(stmt.sum_errors.into()),
            );
            row.insert(
                "sum_warnings".to_string(),
                Value::Number(stmt.sum_warnings.into()),
            );

            // ====================================================================
            // LATENCY METRICS (microseconds)
            // ====================================================================
            row.insert(
                "sum_latency".to_string(),
                Value::Number(stmt.sum_latency_us.into()),
            );
            row.insert(
                "max_latency".to_string(),
                Value::Number(stmt.max_latency_us.into()),
            );
            row.insert(
                "min_latency".to_string(),
                Value::Number(stmt.min_latency_us.into()),
            );
            row.insert(
                "avg_latency".to_string(),
                Value::Number(stmt.avg_latency_us.into()),
            );
            row.insert(
                "p50_latency".to_string(),
                Value::Number(stmt.p50_latency_us.into()),
            );
            row.insert(
                "p95_latency".to_string(),
                Value::Number(stmt.p95_latency_us.into()),
            );
            row.insert(
                "p99_latency".to_string(),
                Value::Number(stmt.p99_latency_us.into()),
            );

            // ====================================================================
            // PARSE/COMPILE METRICS
            // ====================================================================
            row.insert(
                "sum_parse_latency".to_string(),
                Value::Number(stmt.sum_parse_latency_us.into()),
            );
            row.insert(
                "max_parse_latency".to_string(),
                Value::Number(stmt.max_parse_latency_us.into()),
            );
            row.insert(
                "sum_compile_latency".to_string(),
                Value::Number(stmt.sum_compile_latency_us.into()),
            );
            row.insert(
                "max_compile_latency".to_string(),
                Value::Number(stmt.max_compile_latency_us.into()),
            );

            // ====================================================================
            // RESOURCE USAGE
            // ====================================================================
            row.insert(
                "sum_mem_bytes".to_string(),
                Value::Number(stmt.sum_mem_bytes.into()),
            );
            row.insert(
                "max_mem_bytes".to_string(),
                Value::Number(stmt.max_mem_bytes.into()),
            );
            row.insert(
                "sum_disk_bytes".to_string(),
                Value::Number(stmt.sum_disk_bytes.into()),
            );
            row.insert(
                "max_disk_bytes".to_string(),
                Value::Number(stmt.max_disk_bytes.into()),
            );
            row.insert(
                "sum_tidb_cpu".to_string(),
                Value::Number(stmt.sum_tidb_cpu_us.into()),
            );
            row.insert(
                "sum_tikv_cpu".to_string(),
                Value::Number(stmt.sum_tikv_cpu_us.into()),
            );

            // ====================================================================
            // TIKV COPROCESSOR METRICS
            // ====================================================================
            row.insert(
                "sum_num_cop_tasks".to_string(),
                Value::Number(stmt.sum_num_cop_tasks.into()),
            );
            row.insert(
                "sum_process_time".to_string(),
                Value::Number(stmt.sum_process_time_us.into()),
            );
            row.insert(
                "max_process_time".to_string(),
                Value::Number(stmt.max_process_time_us.into()),
            );
            row.insert(
                "sum_wait_time".to_string(),
                Value::Number(stmt.sum_wait_time_us.into()),
            );
            row.insert(
                "max_wait_time".to_string(),
                Value::Number(stmt.max_wait_time_us.into()),
            );

            // ====================================================================
            // KEY SCAN METRICS
            // ====================================================================
            row.insert(
                "sum_total_keys".to_string(),
                Value::Number(stmt.sum_total_keys.into()),
            );
            row.insert(
                "max_total_keys".to_string(),
                Value::Number(stmt.max_total_keys.into()),
            );
            row.insert(
                "sum_processed_keys".to_string(),
                Value::Number(stmt.sum_processed_keys.into()),
            );
            row.insert(
                "max_processed_keys".to_string(),
                Value::Number(stmt.max_processed_keys.into()),
            );

            // ====================================================================
            // TRANSACTION METRICS
            // ====================================================================
            row.insert(
                "commit_count".to_string(),
                Value::Number(stmt.commit_count.into()),
            );
            row.insert(
                "sum_prewrite_time".to_string(),
                Value::Number(stmt.sum_prewrite_time_us.into()),
            );
            row.insert(
                "max_prewrite_time".to_string(),
                Value::Number(stmt.max_prewrite_time_us.into()),
            );
            row.insert(
                "sum_commit_time".to_string(),
                Value::Number(stmt.sum_commit_time_us.into()),
            );
            row.insert(
                "max_commit_time".to_string(),
                Value::Number(stmt.max_commit_time_us.into()),
            );
            row.insert(
                "sum_write_keys".to_string(),
                Value::Number(stmt.sum_write_keys.into()),
            );
            row.insert(
                "max_write_keys".to_string(),
                Value::Number(stmt.max_write_keys.into()),
            );
            row.insert(
                "sum_write_size_bytes".to_string(),
                Value::Number(stmt.sum_write_size_bytes.into()),
            );
            row.insert(
                "max_write_size_bytes".to_string(),
                Value::Number(stmt.max_write_size_bytes.into()),
            );

            // ====================================================================
            // ROW STATISTICS
            // ====================================================================
            row.insert(
                "sum_affected_rows".to_string(),
                Value::Number(stmt.sum_affected_rows.into()),
            );
            row.insert(
                "sum_result_rows".to_string(),
                Value::Number(stmt.sum_result_rows.into()),
            );
            row.insert(
                "max_result_rows".to_string(),
                Value::Number(stmt.max_result_rows.into()),
            );
            row.insert(
                "min_result_rows".to_string(),
                Value::Number(stmt.min_result_rows.into()),
            );

            // ====================================================================
            // PLAN CACHE
            // ====================================================================
            row.insert("plan_in_cache".to_string(), Value::Bool(stmt.plan_in_cache));
            row.insert(
                "plan_cache_hits".to_string(),
                Value::Number(stmt.plan_cache_hits.into()),
            );

            // ====================================================================
            // TIMESTAMPS
            // ====================================================================
            row.insert(
                "first_seen_ms".to_string(),
                Value::Number(stmt.first_seen_ms.into()),
            );
            row.insert(
                "last_seen_ms".to_string(),
                Value::Number(stmt.last_seen_ms.into()),
            );

            // ====================================================================
            // FLAGS
            // ====================================================================
            row.insert("is_internal".to_string(), Value::Bool(stmt.is_internal));
            row.insert("prepared".to_string(), Value::Bool(stmt.prepared));

            // ====================================================================
            // MULTI-TENANCY
            // ====================================================================
            row.insert(
                "keyspace_name".to_string(),
                Value::String(stmt.keyspace_name.clone()),
            );
            row.insert(
                "keyspace_id".to_string(),
                Value::Number(stmt.keyspace_id.into()),
            );
            row.insert(
                "resource_group_name".to_string(),
                Value::String(stmt.resource_group_name.clone()),
            );

            // ====================================================================
            // CLUSTER METADATA
            // ====================================================================
            row.insert("cluster_id".to_string(), Value::String(cluster_id.clone()));
            row.insert(
                "instance_id".to_string(),
                Value::String(instance_id.clone()),
            );

            // ====================================================================
            // BATCH METADATA
            // ====================================================================
            if let Some(ref m) = batch.metadata {
                let summary_begin_time =
                    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(m.window_start_ms)
                        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                        .unwrap_or_default();
                let summary_end_time =
                    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(m.window_end_ms)
                        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                        .unwrap_or_default();

                row.insert(
                    "window_start_ms".to_string(),
                    Value::Number(m.window_start_ms.into()),
                );
                row.insert(
                    "window_end_ms".to_string(),
                    Value::Number(m.window_end_ms.into()),
                );
                // Backward compatibility aliases expected by old SQL predicates
                row.insert(
                    "SUMMARY_BEGIN_TIME".to_string(),
                    Value::String(summary_begin_time.clone()),
                );
                row.insert(
                    "SUMMARY_END_TIME".to_string(),
                    Value::String(summary_end_time.clone()),
                );
                row.insert(
                    "summary_begin_time".to_string(),
                    Value::String(summary_begin_time),
                );
                row.insert(
                    "summary_end_time".to_string(),
                    Value::String(summary_end_time),
                );
                row.insert(
                    "batch_sequence".to_string(),
                    Value::Number(m.batch_sequence.into()),
                );
                row.insert(
                    "batch_timestamp_ms".to_string(),
                    Value::Number(m.batch_timestamp_ms.into()),
                );
                row.insert(
                    "schema_version".to_string(),
                    Value::String(m.schema_version.clone()),
                );
                row.insert("schema_id".to_string(), Value::Number(m.schema_id.into()));
            }

            // ====================================================================
            // EXTENDED METRICS (dynamic)
            // ====================================================================
            for (key, value) in &stmt.extended_metrics {
                let json_value = match &value.value {
                    Some(proto::metric_value::Value::Int64Val(v)) => Value::Number((*v).into()),
                    Some(proto::metric_value::Value::DoubleVal(v)) => Value::Number(
                        serde_json::Number::from_f64(*v).unwrap_or(serde_json::Number::from(0)),
                    ),
                    Some(proto::metric_value::Value::StringVal(v)) => Value::String(v.clone()),
                    Some(proto::metric_value::Value::BoolVal(v)) => Value::Bool(*v),
                    Some(proto::metric_value::Value::BytesVal(v)) => {
                        Value::String(base64::prelude::BASE64_STANDARD.encode(v))
                    }
                    None => Value::Null,
                };
                row.insert(key.clone(), json_value);
            }

            align_statement_summary_row_schema(&mut row);

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
        request: tonic::Request<TableRowBatch>,
    ) -> Result<tonic::Response<PushResponse>, tonic::Status> {
        let batch = request.into_inner();
        let metadata = batch.metadata.as_ref();
        let cluster_id = metadata.map(|m| m.cluster_id.clone()).unwrap_or_default();
        let instance_id = metadata.map(|m| m.instance_id.clone()).unwrap_or_default();
        let row_count = batch.rows.len();

        if let Some(ref limiter) = self.rate_limiter {
            if !limiter.try_acquire().await {
                warn!("Rate limit exceeded, rejecting {} rows", row_count);
                return Ok(tonic::Response::new(PushResponse {
                    success: false,
                    message: "Rate limit exceeded".to_string(),
                    received_timestamp_ms: chrono::Utc::now().timestamp_millis(),
                    accepted_count: 0,
                    rejected_count: row_count as i32,
                    errors: vec!["Rate limit exceeded".to_string()],
                }));
            }
        }

        let buffer_size = self.buffer.lock().await.len();
        let buffer_load = buffer_size as f64 / self.buffer_capacity as f64;
        let bp_action = self.backpressure.update_load(buffer_load);
        match bp_action {
            BackpressureAction::Reject => {
                warn!(
                    "Backpressure REJECT: buffer {:.0}% full",
                    buffer_load * 100.0
                );
                return Ok(tonic::Response::new(PushResponse {
                    success: false,
                    message: format!("Backpressure: buffer {:.0}% full", buffer_load * 100.0),
                    received_timestamp_ms: chrono::Utc::now().timestamp_millis(),
                    accepted_count: 0,
                    rejected_count: row_count as i32,
                    errors: vec![],
                }));
            }
            BackpressureAction::Throttle => {
                warn!(
                    "Backpressure THROTTLE: buffer {:.0}% full",
                    buffer_load * 100.0
                );
            }
            BackpressureAction::Accept => {}
        }

        let schema = match batch.schema.as_ref() {
            Some(s) => s,
            None => {
                return Ok(tonic::Response::new(PushResponse {
                    success: false,
                    message: "missing schema in TableRowBatch".to_string(),
                    received_timestamp_ms: chrono::Utc::now().timestamp_millis(),
                    accepted_count: 0,
                    rejected_count: row_count as i32,
                    errors: vec!["missing schema in TableRowBatch".to_string()],
                }));
            }
        };

        info!("Received push: {} table rows", row_count);

        let schema_metadata = build_schema_metadata_from_proto_schema(schema, &batch.rows);
        let mut rows = Vec::with_capacity(row_count);
        let mut rejected = 0i32;
        for row in &batch.rows {
            if schema.columns.len() != row.values.len() {
                rejected += 1;
                continue;
            }

            let mut out = HashMap::with_capacity(schema.columns.len() + 1);
            for (col, value) in schema.columns.iter().zip(row.values.iter()) {
                let val = proto_value_to_json(value);
                out.insert(col.name.clone(), val);
            }
            out.insert(
                "_schema_metadata".to_string(),
                Value::Object(schema_metadata.clone()),
            );
            rows.push(out);
        }

        let accepted = rows.len() as i32;
        let rejected_count = rejected + (row_count as i32 - accepted - rejected);

        let received_batch = ReceivedBatch {
            cluster_id,
            instance_id,
            statements: rows,
            received_at: chrono::Utc::now(),
        };

        self.buffer.lock().await.push(received_batch);

        Ok(tonic::Response::new(PushResponse {
            success: rejected_count == 0,
            message: format!("Accepted {} rows", accepted),
            received_timestamp_ms: chrono::Utc::now().timestamp_millis(),
            accepted_count: accepted,
            rejected_count,
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
            info!("Starting buffer flusher for GrpcPush collector...");
            self.start_buffer_flusher();
            info!(
                "gRPC push buffer flusher started for instance {}",
                self.instance
            );
        } else {
            warn!("Output sender not set for GrpcPush collector!");
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
