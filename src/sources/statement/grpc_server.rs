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

use std::sync::Arc;
use std::time::Instant;

use tokio::sync::mpsc;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};

use super::config::StatementConfig;
use super::contract::ContractValidator;
use super::schema::SchemaRegistry;
use super::storage::StatementStorage;
use super::health::{HealthChecker, BackpressureState, BackpressureAction, RateLimiter};

// Proto generated types - these would be generated from systemtable.proto
// For now, we define placeholder types that match the proto schema

pub mod proto {
    tonic::include_proto!("systemtable.v1");
}

use proto::system_table_push_service_server::{SystemTablePushService, SystemTablePushServiceServer};
use proto::{
    BatchMetadata, PingRequest, PingResponse, PushResponse, Statement, StatementBatch,
};

/// Statement receiver handles incoming gRPC push requests from TiDB.
pub struct StatementReceiver {
    config: Arc<StatementConfig>,
    contract_validator: Arc<ContractValidator>,
    schema_registry: Arc<SchemaRegistry>,
    storage: Arc<StatementStorage>,

    // Health and backpressure
    health_checker: Arc<HealthChecker>,
    backpressure: Arc<std::sync::Mutex<BackpressureState>>,
    rate_limiter: Arc<RateLimiter>,

    // Metrics
    statements_received: std::sync::atomic::AtomicU64,
    statements_stored: std::sync::atomic::AtomicU64,
    push_requests: std::sync::atomic::AtomicU64,
}

impl StatementReceiver {
    /// Creates a new StatementReceiver.
    pub fn new(
        config: StatementConfig,
        contract_validator: ContractValidator,
        schema_registry: SchemaRegistry,
        storage: StatementStorage,
    ) -> Self {
        let health_checker = Arc::new(HealthChecker::new());
        health_checker.mark_ready();

        // Create rate limiter: max 1000 requests per second
        let rate_limiter = Arc::new(RateLimiter::new(1000, 1000));

        // Create backpressure state: throttle at 80% load, reject at 95%
        let backpressure = Arc::new(std::sync::Mutex::new(BackpressureState::new(0.8, 0.95)));

        Self {
            config: Arc::new(config),
            contract_validator: Arc::new(contract_validator),
            schema_registry: Arc::new(schema_registry),
            storage: Arc::new(storage),
            health_checker,
            backpressure,
            rate_limiter,
            statements_received: std::sync::atomic::AtomicU64::new(0),
            statements_stored: std::sync::atomic::AtomicU64::new(0),
            push_requests: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Starts the gRPC server.
    pub async fn start(self) -> Result<(), Box<dyn std::error::Error>> {
        let addr = self.config.grpc_bind_address().parse()?;

        info!("Starting statement receiver gRPC server on {}", addr);

        let receiver = Arc::new(self);

        tonic::transport::Server::builder()
            .add_service(SystemTablePushServiceServer::new(StatementService {
                receiver: receiver.clone(),
            }))
            .serve(addr)
            .await?;

        Ok(())
    }

    /// Processes a statement batch.
    async fn process_batch(&self, batch: StatementBatch) -> Result<PushResponse, Status> {
        let start = Instant::now();

        // Check rate limiter
        if !self.rate_limiter.try_acquire().await {
            self.health_checker.record_grpc_error();
            return Ok(PushResponse {
                success: false,
                message: "rate limit exceeded".to_string(),
                received_timestamp_ms: chrono::Utc::now().timestamp_millis(),
                accepted_count: 0,
                rejected_count: batch.statements.len() as i32,
                errors: vec!["rate limit exceeded".to_string()],
            });
        }

        // Check backpressure
        {
            let mut bp = self.backpressure.lock().unwrap();
            let load = self.calculate_current_load();
            let action = bp.update_load(load);

            match action {
                BackpressureAction::Reject => {
                    self.health_checker.record_grpc_error();
                    return Ok(PushResponse {
                        success: false,
                        message: format!("server under load (load: {:.2}), rejecting requests", load),
                        received_timestamp_ms: chrono::Utc::now().timestamp_millis(),
                        accepted_count: 0,
                        rejected_count: batch.statements.len() as i32,
                        errors: vec!["server under load".to_string()],
                    });
                }
                BackpressureAction::Throttle => {
                    // Add a small delay to throttle the request
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                BackpressureAction::Accept => {}
            }
        }

        self.push_requests.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Validate metadata
        let metadata = batch.metadata.as_ref().ok_or_else(|| {
            Status::invalid_argument("batch metadata is required")
        })?;

        if metadata.instance_id.is_empty() {
            return Ok(PushResponse {
                success: false,
                message: "instance_id is required".to_string(),
                received_timestamp_ms: chrono::Utc::now().timestamp_millis(),
                accepted_count: 0,
                rejected_count: batch.statements.len() as i32,
                errors: vec!["missing instance_id".to_string()],
            });
        }

        let statements_count = batch.statements.len();
        self.statements_received.fetch_add(statements_count as u64, std::sync::atomic::Ordering::Relaxed);

        // Update schema registry with any new fields
        self.update_schema_registry(&metadata, &batch.statements);

        // Validate and store statements
        let mut accepted = 0i32;
        let mut rejected = 0i32;
        let mut errors = Vec::new();

        for stmt in &batch.statements {
            match self.validate_and_store(metadata, stmt).await {
                Ok(_) => {
                    accepted += 1;
                    self.statements_stored.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Err(e) => {
                    rejected += 1;
                    if errors.len() < 10 {
                        errors.push(e.to_string());
                    }
                    warn!("Failed to store statement {}: {}", stmt.digest, e);
                }
            }
        }

        let latency = start.elapsed();

        // Record metrics
        self.health_checker.record_grpc_request(latency.as_millis() as u64);
        if rejected > 0 {
            self.health_checker.record_grpc_error();
        }

        debug!(
            "Processed batch: {} statements, {} accepted, {} rejected, {:?}",
            statements_count, accepted, rejected, latency
        );

        // Update backpressure state with current load
        self.update_storage_metrics();

        Ok(PushResponse {
            success: rejected == 0,
            message: format!("Stored {}/{} statements", accepted, statements_count),
            received_timestamp_ms: chrono::Utc::now().timestamp_millis(),
            accepted_count: accepted,
            rejected_count: rejected,
            errors,
        })
    }

    /// Updates the schema registry with newly discovered fields.
    fn update_schema_registry(&self, metadata: &BatchMetadata, statements: &[Statement]) {
        // Discover extended metric names
        for field_name in &metadata.field_names {
            self.schema_registry.register_extended_field(field_name.clone());
        }

        // Also scan statements for extended_metrics keys
        for stmt in statements {
            for key in stmt.extended_metrics.keys() {
                self.schema_registry.register_extended_field(key.clone());
            }
        }
    }

    /// Validates a statement against the contract and stores it.
    async fn validate_and_store(
        &self,
        metadata: &BatchMetadata,
        stmt: &Statement,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Validate required fields
        if stmt.digest.is_empty() {
            return Err("digest is required".into());
        }
        if stmt.exec_count == 0 {
            return Err("exec_count must be > 0".into());
        }

        // Validate against contract if strict validation is enabled
        if self.config.contract.strict_validation {
            self.contract_validator.validate(stmt)?;
        }

        // Store the statement
        self.storage.store(metadata, stmt).await?;

        Ok(())
    }

    /// Returns current metrics.
    pub fn metrics(&self) -> ReceiverMetrics {
        self.health_check()
    }

    /// Returns health checker reference.
    pub fn health_check(&self) -> Arc<HealthChecker> {
        self.health_checker.clone()
    }

    /// Calculates current system load (0.0 to 1.0).
    fn calculate_current_load(&self) -> f64 {
        // Estimate load based on pending requests and buffer sizes
        let storage_buffer_size = self.storage.buffer_size();
        let max_buffer_size = 10000; // Configurable threshold

        // Calculate load based on buffer utilization
        let buffer_load = (storage_buffer_size as f64 / max_buffer_size as f64).min(1.0);

        // Combine with request rate (simplified)
        let requests_total = self.push_requests.load(std::sync::atomic::Ordering::Relaxed);
        let request_load = if requests_total > 1000 {
            0.5
        } else {
            (requests_total as f64 / 2000.0).min(0.5)
        };

        (buffer_load + request_load).min(1.0)
    }

    /// Updates storage-related metrics.
    fn update_storage_metrics(&self) {
        let buffer_size = self.storage.buffer_size();
        self.health_checker.set_storage_buffer_size(buffer_size);
        self.health_checker.set_storage_healthy(buffer_size < 5000); // Healthy if buffer < 5000
    }
}

/// Metrics for the statement receiver.
#[derive(Debug, Clone)]
pub struct ReceiverMetrics {
    pub statements_received: u64,
    pub statements_stored: u64,
    pub push_requests: u64,
}

/// gRPC service implementation.
struct StatementService {
    receiver: Arc<StatementReceiver>,
}

#[tonic::async_trait]
impl SystemTablePushService for StatementService {
    async fn push_statements(
        &self,
        request: Request<StatementBatch>,
    ) -> Result<Response<PushResponse>, Status> {
        let batch = request.into_inner();
        let response = self.receiver.process_batch(batch).await?;
        Ok(Response::new(response))
    }

    async fn push_table_rows(
        &self,
        _request: Request<proto::TableRowBatch>,
    ) -> Result<Response<PushResponse>, Status> {
        // Not implemented for statement-specific receiver
        Err(Status::unimplemented("push_table_rows not implemented"))
    }

    async fn ping(
        &self,
        request: Request<PingRequest>,
    ) -> Result<Response<PingResponse>, Status> {
        let req = request.into_inner();
        debug!("Ping from cluster={} instance={}", req.cluster_id, req.instance_id);

        Ok(Response::new(PingResponse {
            ok: true,
            version: env!("CARGO_PKG_VERSION").to_string(),
            server_timestamp_ms: chrono::Utc::now().timestamp_millis(),
            supported_tables: vec!["STATEMENTS_SUMMARY".to_string()],
            protocol_version: "1.0".to_string(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ping() {
        // Test ping response
        let config = StatementConfig::default();
        let validator = ContractValidator::new(None);
        let registry = SchemaRegistry::new();
        let storage = StatementStorage::new_local("/tmp/test".to_string());

        let receiver = StatementReceiver::new(config, validator, registry, storage);
        let service = StatementService {
            receiver: Arc::new(receiver),
        };

        let request = Request::new(PingRequest {
            cluster_id: "test-cluster".to_string(),
            instance_id: "test-instance".to_string(),
        });

        let response = service.ping(request).await.unwrap();
        assert!(response.get_ref().ok);
    }
}
