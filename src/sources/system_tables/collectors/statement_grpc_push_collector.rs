// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Statement-specific gRPC Push Collector
//!
//! Provides statement-specific field extraction (80+ fields) and
//! gRPC service implementation for STATEMENTS_SUMMARY tables.

use std::collections::HashMap;

use async_trait::async_trait;
use serde_json::Value;
use tracing::{debug, info, warn};

use crate::sources::system_tables::collectors::base_grpc_push_collector::{
    BackpressureAction, BackpressureState, RateLimiter, ReceivedBatch, StatementBuffer,
};
use crate::sources::system_tables::collectors::grpc_push_collector::{
    align_statement_summary_row_schema, build_schema_metadata_from_proto_schema,
    proto_value_to_json,
};
use crate::sources::system_tables::data_collector::CollectionPolicyConfig;

// Re-use the proto from grpc_push_collector to avoid duplicate types
use crate::sources::system_tables::collectors::grpc_push_collector::proto as shared_proto;

use shared_proto::system_table_push_service_server::{
    SystemTablePushService, SystemTablePushServiceServer,
};
use shared_proto::{PingRequest, PingResponse, PushResponse, StatementBatch, TableRowBatch};

use base64::Engine;

/// Extract statement fields from proto to HashMap (80+ fields)
pub fn extract_statement_fields(
    stmt: &shared_proto::Statement,
    cluster_id: &str,
    instance_id: &str,
    batch: &StatementBatch,
) -> HashMap<String, Value> {
    let mut row: HashMap<String, Value> = HashMap::new();

    // IDENTITY FIELDS
    row.insert("DIGEST".to_string(), Value::String(stmt.digest.clone()));
    row.insert(
        "PLAN_DIGEST".to_string(),
        Value::String(stmt.plan_digest.clone()),
    );
    row.insert(
        "SCHEMA_NAME".to_string(),
        Value::String(stmt.schema_name.clone()),
    );
    row.insert(
        "DIGEST_TEXT".to_string(),
        Value::String(stmt.normalized_sql.clone()),
    );
    row.insert(
        "TABLE_NAMES".to_string(),
        Value::String(stmt.table_names.clone()),
    );
    row.insert(
        "STMT_TYPE".to_string(),
        Value::String(stmt.stmt_type.clone()),
    );

    // SAMPLE DATA
    row.insert(
        "SAMPLE_SQL".to_string(),
        Value::String(stmt.sample_sql.clone()),
    );
    row.insert(
        "SAMPLE_PLAN".to_string(),
        Value::String(stmt.sample_plan.clone()),
    );
    row.insert("PREV_SQL".to_string(), Value::String(stmt.prev_sql.clone()));

    // EXECUTION STATISTICS
    row.insert(
        "EXEC_COUNT".to_string(),
        Value::Number(stmt.exec_count.into()),
    );
    row.insert(
        "SUM_ERRORS".to_string(),
        Value::Number(stmt.sum_errors.into()),
    );
    row.insert(
        "SUM_WARNINGS".to_string(),
        Value::Number(stmt.sum_warnings.into()),
    );

    // LATENCY METRICS
    row.insert(
        "SUM_LATENCY".to_string(),
        Value::Number(stmt.sum_latency_us.into()),
    );
    row.insert(
        "SUM_LATENCY_US".to_string(),
        Value::Number(stmt.sum_latency_us.into()),
    );
    row.insert(
        "MAX_LATENCY_US".to_string(),
        Value::Number(stmt.max_latency_us.into()),
    );
    row.insert(
        "MIN_LATENCY_US".to_string(),
        Value::Number(stmt.min_latency_us.into()),
    );
    row.insert(
        "AVG_LATENCY_US".to_string(),
        Value::Number(stmt.avg_latency_us.into()),
    );
    row.insert(
        "P50_LATENCY_US".to_string(),
        Value::Number(stmt.p50_latency_us.into()),
    );
    row.insert(
        "P95_LATENCY_US".to_string(),
        Value::Number(stmt.p95_latency_us.into()),
    );
    row.insert(
        "P99_LATENCY_US".to_string(),
        Value::Number(stmt.p99_latency_us.into()),
    );

    // PARSE/COMPILE METRICS
    row.insert(
        "SUM_PARSE_LATENCY_US".to_string(),
        Value::Number(stmt.sum_parse_latency_us.into()),
    );
    row.insert(
        "MAX_PARSE_LATENCY_US".to_string(),
        Value::Number(stmt.max_parse_latency_us.into()),
    );
    row.insert(
        "SUM_COMPILE_LATENCY_US".to_string(),
        Value::Number(stmt.sum_compile_latency_us.into()),
    );
    row.insert(
        "MAX_COMPILE_LATENCY_US".to_string(),
        Value::Number(stmt.max_compile_latency_us.into()),
    );

    // RESOURCE USAGE
    row.insert(
        "SUM_MEM_BYTES".to_string(),
        Value::Number(stmt.sum_mem_bytes.into()),
    );
    row.insert(
        "MAX_MEM_BYTES".to_string(),
        Value::Number(stmt.max_mem_bytes.into()),
    );
    row.insert(
        "SUM_DISK_BYTES".to_string(),
        Value::Number(stmt.sum_disk_bytes.into()),
    );
    row.insert(
        "MAX_DISK_BYTES".to_string(),
        Value::Number(stmt.max_disk_bytes.into()),
    );
    row.insert(
        "SUM_TIDB_CPU_US".to_string(),
        Value::Number(stmt.sum_tidb_cpu_us.into()),
    );
    row.insert(
        "SUM_TIKV_CPU_US".to_string(),
        Value::Number(stmt.sum_tikv_cpu_us.into()),
    );

    // TIKV COPROCESSOR METRICS
    row.insert(
        "SUM_NUM_COP_TASKS".to_string(),
        Value::Number(stmt.sum_num_cop_tasks.into()),
    );
    row.insert(
        "SUM_PROCESS_TIME_US".to_string(),
        Value::Number(stmt.sum_process_time_us.into()),
    );
    row.insert(
        "MAX_PROCESS_TIME_US".to_string(),
        Value::Number(stmt.max_process_time_us.into()),
    );
    row.insert(
        "SUM_WAIT_TIME_US".to_string(),
        Value::Number(stmt.sum_wait_time_us.into()),
    );
    row.insert(
        "MAX_WAIT_TIME_US".to_string(),
        Value::Number(stmt.max_wait_time_us.into()),
    );

    // KEY SCAN METRICS
    row.insert(
        "SUM_TOTAL_KEYS".to_string(),
        Value::Number(stmt.sum_total_keys.into()),
    );
    row.insert(
        "MAX_TOTAL_KEYS".to_string(),
        Value::Number(stmt.max_total_keys.into()),
    );
    row.insert(
        "SUM_PROCESSED_KEYS".to_string(),
        Value::Number(stmt.sum_processed_keys.into()),
    );
    row.insert(
        "MAX_PROCESSED_KEYS".to_string(),
        Value::Number(stmt.max_processed_keys.into()),
    );

    // TRANSACTION METRICS
    row.insert(
        "COMMIT_COUNT".to_string(),
        Value::Number(stmt.commit_count.into()),
    );
    row.insert(
        "SUM_PREWRITE_TIME_US".to_string(),
        Value::Number(stmt.sum_prewrite_time_us.into()),
    );
    row.insert(
        "MAX_PREWRITE_TIME_US".to_string(),
        Value::Number(stmt.max_prewrite_time_us.into()),
    );
    row.insert(
        "SUM_COMMIT_TIME_US".to_string(),
        Value::Number(stmt.sum_commit_time_us.into()),
    );
    row.insert(
        "MAX_COMMIT_TIME_US".to_string(),
        Value::Number(stmt.max_commit_time_us.into()),
    );
    row.insert(
        "SUM_WRITE_KEYS".to_string(),
        Value::Number(stmt.sum_write_keys.into()),
    );
    row.insert(
        "MAX_WRITE_KEYS".to_string(),
        Value::Number(stmt.max_write_keys.into()),
    );
    row.insert(
        "SUM_WRITE_SIZE_BYTES".to_string(),
        Value::Number(stmt.sum_write_size_bytes.into()),
    );
    row.insert(
        "MAX_WRITE_SIZE_BYTES".to_string(),
        Value::Number(stmt.max_write_size_bytes.into()),
    );

    // ROW STATISTICS
    row.insert(
        "SUM_AFFECTED_ROWS".to_string(),
        Value::Number(stmt.sum_affected_rows.into()),
    );
    row.insert(
        "SUM_RESULT_ROWS".to_string(),
        Value::Number(stmt.sum_result_rows.into()),
    );
    row.insert(
        "MAX_RESULT_ROWS".to_string(),
        Value::Number(stmt.max_result_rows.into()),
    );
    row.insert(
        "MIN_RESULT_ROWS".to_string(),
        Value::Number(stmt.min_result_rows.into()),
    );

    // PLAN CACHE
    row.insert("PLAN_IN_CACHE".to_string(), Value::Bool(stmt.plan_in_cache));
    row.insert(
        "PLAN_CACHE_HITS".to_string(),
        Value::Number(stmt.plan_cache_hits.into()),
    );

    // TIMESTAMPS
    row.insert(
        "FIRST_SEEN_MS".to_string(),
        Value::Number(stmt.first_seen_ms.into()),
    );
    row.insert(
        "LAST_SEEN_MS".to_string(),
        Value::Number(stmt.last_seen_ms.into()),
    );

    // FLAGS
    row.insert("IS_INTERNAL".to_string(), Value::Bool(stmt.is_internal));
    row.insert("PREPARED".to_string(), Value::Bool(stmt.prepared));

    // MULTI-TENANCY
    row.insert(
        "KEYSPACE_NAME".to_string(),
        Value::String(stmt.keyspace_name.clone()),
    );
    row.insert(
        "KEYSPACE_ID".to_string(),
        Value::Number(stmt.keyspace_id.into()),
    );
    row.insert(
        "RESOURCE_GROUP_NAME".to_string(),
        Value::String(stmt.resource_group_name.clone()),
    );

    // CLUSTER METADATA
    row.insert(
        "CLUSTER_ID".to_string(),
        Value::String(cluster_id.to_string()),
    );
    row.insert(
        "INSTANCE_ID".to_string(),
        Value::String(instance_id.to_string()),
    );

    // BATCH METADATA
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
            "WINDOW_START_MS".to_string(),
            Value::Number(m.window_start_ms.into()),
        );
        row.insert(
            "WINDOW_END_MS".to_string(),
            Value::Number(m.window_end_ms.into()),
        );
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
            "BATCH_SEQUENCE".to_string(),
            Value::Number(m.batch_sequence.into()),
        );
        row.insert(
            "BATCH_TIMESTAMP_MS".to_string(),
            Value::Number(m.batch_timestamp_ms.into()),
        );
        row.insert(
            "SCHEMA_VERSION".to_string(),
            Value::String(m.schema_version.clone()),
        );
        row.insert("SCHEMA_ID".to_string(), Value::Number(m.schema_id.into()));
    }

    // EXTENDED METRICS (dynamic)
    for (key, value) in &stmt.extended_metrics {
        let json_value = match &value.value {
            Some(shared_proto::metric_value::Value::Int64Val(v)) => Value::Number((*v).into()),
            Some(shared_proto::metric_value::Value::DoubleVal(v)) => Value::Number(
                serde_json::Number::from_f64(*v).unwrap_or(serde_json::Number::from(0)),
            ),
            Some(shared_proto::metric_value::Value::StringVal(v)) => Value::String(v.clone()),
            Some(shared_proto::metric_value::Value::BoolVal(v)) => Value::Bool(*v),
            Some(shared_proto::metric_value::Value::BytesVal(v)) => {
                Value::String(base64::prelude::BASE64_STANDARD.encode(v))
            }
            None => Value::Null,
        };
        row.insert(key.clone(), json_value);
    }

    if let Some(v) = row.get("avg_request_unit_read").cloned() {
        row.insert("AVG_REQUEST_UNIT_READ".to_string(), v);
    }
    if let Some(v) = row.get("avg_request_unit_write").cloned() {
        row.insert("AVG_REQUEST_UNIT_WRITE".to_string(), v);
    }

    align_statement_summary_row_schema(&mut row);

    row
}

/// gRPC service implementation for statement push
pub struct StatementGrpcPushService {
    buffer: StatementBuffer,
    rate_limiter: Option<RateLimiter>,
    backpressure: BackpressureState,
    buffer_capacity: usize,
    collection_policy: CollectionPolicyConfig,
}

impl StatementGrpcPushService {
    pub fn new(
        buffer: StatementBuffer,
        rate_limiter: Option<RateLimiter>,
        backpressure: BackpressureState,
        buffer_capacity: usize,
        collection_policy: CollectionPolicyConfig,
    ) -> Self {
        Self {
            buffer,
            rate_limiter,
            backpressure,
            buffer_capacity,
            collection_policy,
        }
    }

    /// Create and start the gRPC server
    pub fn start_server(self, addr: &str) -> tokio::task::JoinHandle<()> {
        let addr_string = addr.to_string();
        tokio::spawn(async move {
            let addr = addr_string.parse().expect("Invalid gRPC bind address");
            info!("Starting gRPC push receiver on {}", addr);

            if let Err(e) = tonic::transport::Server::builder()
                .add_service(SystemTablePushServiceServer::new(self))
                .serve(addr)
                .await
            {
                tracing::error!("gRPC push server error: {}", e);
            }
        })
    }
}

#[tonic::async_trait]
impl SystemTablePushService for StatementGrpcPushService {
    async fn push_statements(
        &self,
        request: tonic::Request<StatementBatch>,
    ) -> Result<tonic::Response<PushResponse>, tonic::Status> {
        let batch = request.into_inner();
        let metadata = batch.metadata.as_ref();
        let cluster_id = metadata.map(|m| m.cluster_id.clone()).unwrap_or_default();
        let instance_id = metadata.map(|m| m.instance_id.clone()).unwrap_or_default();
        let stmt_count = batch.statements.len();

        // Check rate limiter
        if let Some(ref limiter) = self.rate_limiter {
            if !limiter.try_acquire().await {
                warn!("Rate limit exceeded, rejecting {} statements", stmt_count);
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

        // Check backpressure
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
                    rejected_count: stmt_count as i32,
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

        info!("Received push: {} statements", stmt_count);

        // Convert proto statements to HashMap
        let mut rows = Vec::with_capacity(stmt_count);
        for stmt in &batch.statements {
            let row = extract_statement_fields(stmt, &cluster_id, &instance_id, &batch);
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

        let received_batch = ReceivedBatch {
            cluster_id,
            instance_id,
            statements: rows,
            received_at: chrono::Utc::now(),
        };

        self.buffer.lock().await.push(received_batch);

        Ok(tonic::Response::new(PushResponse {
            success: rejected == 0,
            message: format!("Accepted {} rows", accepted),
            received_timestamp_ms: chrono::Utc::now().timestamp_millis(),
            accepted_count: accepted,
            rejected_count: rejected,
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
