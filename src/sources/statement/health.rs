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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

/// Health status of the component.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    #[serde(rename = "healthy")]
    Healthy,
    #[serde(rename = "degraded")]
    Degraded,
    #[serde(rename = "unhealthy")]
    Unhealthy,
}

/// Health check response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckResponse {
    pub status: HealthStatus,
    pub version: String,
    pub uptime_seconds: f64,
    pub components: ComponentHealth,
}

/// Component health details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealth {
    pub grpc_server: ComponentStatus,
    pub storage: ComponentStatus,
    pub schema_registry: ComponentStatus,
}

/// Status of a single component.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentStatus {
    pub healthy: bool,
    pub last_check: String,
    pub error: Option<String>,
    pub metrics: ComponentMetrics,
}

/// Metrics for a component.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentMetrics {
    pub requests_total: u64,
    pub errors_total: u64,
    pub avg_latency_ms: f64,
    pub buffer_size: Option<usize>,
}

/// Health checker for monitoring component health.
pub struct HealthChecker {
    start_time: Instant,
    ready: AtomicBool,
    shutdown: AtomicBool,

    // Component metrics
    grpc_requests: AtomicU64,
    grpc_errors: AtomicU64,
    grpc_latency_total_ms: AtomicU64,
    grpc_latency_count: AtomicU64,

    storage_errors: AtomicU64,
    storage_buffer_size: AtomicU64,

    // Component status flags
    grpc_healthy: AtomicBool,
    storage_healthy: AtomicBool,
    schema_healthy: AtomicBool,
}

impl HealthChecker {
    /// Creates a new health checker.
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            ready: AtomicBool::new(false),
            shutdown: AtomicBool::new(false),
            grpc_requests: AtomicU64::new(0),
            grpc_errors: AtomicU64::new(0),
            grpc_latency_total_ms: AtomicU64::new(0),
            grpc_latency_count: AtomicU64::new(0),
            storage_errors: AtomicU64::new(0),
            storage_buffer_size: AtomicU64::new(0),
            grpc_healthy: AtomicBool::new(true),
            storage_healthy: AtomicBool::new(true),
            schema_healthy: AtomicBool::new(true),
        }
    }

    /// Marks the service as ready.
    pub fn mark_ready(&self) {
        self.ready.store(true, Ordering::Release);
    }

    /// Checks if the service is ready.
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    /// Initiates graceful shutdown.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    /// Checks if shutdown has been initiated.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    /// Records a gRPC request.
    pub fn record_grpc_request(&self, latency_ms: u64) {
        self.grpc_requests.fetch_add(1, Ordering::Relaxed);
        self.grpc_latency_total_ms.fetch_add(latency_ms, Ordering::Relaxed);
        self.grpc_latency_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a gRPC error.
    pub fn record_grpc_error(&self) {
        self.grpc_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a storage error.
    pub fn record_storage_error(&self) {
        self.storage_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Sets the storage buffer size.
    pub fn set_storage_buffer_size(&self, size: usize) {
        self.storage_buffer_size.store(size as u64, Ordering::Relaxed);
    }

    /// Sets the health status of the gRPC server.
    pub fn set_grpc_healthy(&self, healthy: bool) {
        self.grpc_healthy.store(healthy, Ordering::Relaxed);
    }

    /// Sets the health status of the storage.
    pub fn set_storage_healthy(&self, healthy: bool) {
        self.storage_healthy.store(healthy, Ordering::Relaxed);
    }

    /// Sets the health status of the schema registry.
    pub fn set_schema_healthy(&self, healthy: bool) {
        self.schema_healthy.store(healthy, Ordering::Relaxed);
    }

    /// Returns the current health status.
    pub fn health_status(&self) -> HealthCheckResponse {
        let grpc_requests = self.grpc_requests.load(Ordering::Relaxed);
        let grpc_errors = self.grpc_errors.load(Ordering::Relaxed);
        let storage_errors = self.storage_errors.load(Ordering::Relaxed);
        let buffer_size = self.storage_buffer_size.load(Ordering::Relaxed) as usize;

        let latency_count = self.grpc_latency_count.load(Ordering::Relaxed);
        let avg_latency_ms = if latency_count > 0 {
            self.grpc_latency_total_ms.load(Ordering::Relaxed) as f64 / latency_count as f64
        } else {
            0.0
        };

        let grpc_healthy = self.grpc_healthy.load(Ordering::Relaxed);
        let storage_healthy = self.storage_healthy.load(Ordering::Relaxed);
        let schema_healthy = self.schema_healthy.load(Ordering::Relaxed);

        // Determine overall health
        let status = if grpc_healthy && storage_healthy && schema_healthy {
            HealthStatus::Healthy
        } else if grpc_healthy || storage_healthy || schema_healthy {
            HealthStatus::Degraded
        } else {
            HealthStatus::Unhealthy
        };

        let now = chrono::Utc::now().to_rfc3339();

        HealthCheckResponse {
            status,
            version: env!("CARGO_PKG_VERSION").to_string(),
            uptime_seconds: self.start_time.elapsed().as_secs_f64(),
            components: ComponentHealth {
                grpc_server: ComponentStatus {
                    healthy: grpc_healthy,
                    last_check: now.clone(),
                    error: if grpc_healthy { None } else { Some("gRPC server unhealthy".to_string()) },
                    metrics: ComponentMetrics {
                        requests_total: grpc_requests,
                        errors_total: grpc_errors,
                        avg_latency_ms,
                        buffer_size: None,
                    },
                },
                storage: ComponentStatus {
                    healthy: storage_healthy,
                    last_check: now.clone(),
                    error: if storage_healthy { None } else { Some("storage unhealthy".to_string()) },
                    metrics: ComponentMetrics {
                        requests_total: grpc_requests,
                        errors_total: storage_errors,
                        avg_latency_ms: 0.0,
                        buffer_size: Some(buffer_size),
                    },
                },
                schema_registry: ComponentStatus {
                    healthy: schema_healthy,
                    last_check: now,
                    error: if schema_healthy { None } else { Some("schema registry unhealthy".to_string()) },
                    metrics: ComponentMetrics {
                        requests_total: 0,
                        errors_total: 0,
                        avg_latency_ms: 0.0,
                        buffer_size: None,
                    },
                },
            },
        }
    }

    /// Returns a simple liveness status.
    pub fn liveness(&self) -> bool {
        !self.shutdown.load(Ordering::Acquire)
    }

    /// Returns a simple readiness status.
    pub fn readiness(&self) -> bool {
        self.ready.load(Ordering::Acquire)
            && self.grpc_healthy.load(Ordering::Relaxed)
            && self.storage_healthy.load(Ordering::Relaxed)
    }
}

impl Default for HealthChecker {
    fn default() -> Self {
        Self::new()
    }
}

/// Backpressure state for handling high load.
#[derive(Debug, Clone)]
pub struct BackpressureState {
    pub enabled: bool,
    pub threshold: f64,
    pub current_load: f64,
    pub reject_threshold: f64,
}

impl BackpressureState {
    /// Creates a new backpressure state.
    pub fn new(threshold: f64, reject_threshold: f64) -> Self {
        Self {
            enabled: false,
            threshold,
            current_load: 0.0,
            reject_threshold,
        }
    }

    /// Updates the current load and returns whether to reject requests.
    pub fn update_load(&mut self, load: f64) -> BackpressureAction {
        self.current_load = load;

        if load > self.reject_threshold {
            self.enabled = true;
            return BackpressureAction::Reject;
        } else if load > self.threshold {
            self.enabled = true;
            return BackpressureAction::Throttle;
        } else if self.enabled && load < self.threshold * 0.8 {
            // Hysteresis: disable backpressure only after dropping below 80% of threshold
            self.enabled = false;
            return BackpressureAction::Accept;
        }

        if self.enabled {
            BackpressureAction::Throttle
        } else {
            BackpressureAction::Accept
        }
    }
}

/// Action to take based on backpressure state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureAction {
    /// Accept the request normally.
    Accept,
    /// Throttle the request (add delay).
    Throttle,
    /// Reject the request.
    Reject,
}

/// Rate limiter for incoming requests.
pub struct RateLimiter {
    tokens: Arc<AtomicU64>,
    max_tokens: u64,
    refill_rate: u64,
    last_refill: Arc<RwLock<Instant>>,
}

impl RateLimiter {
    /// Creates a new rate limiter.
    pub fn new(max_tokens: u64, refill_rate: u64) -> Self {
        Self {
            tokens: Arc::new(AtomicU64::new(max_tokens)),
            max_tokens,
            refill_rate,
            last_refill: Arc::new(RwLock::new(Instant::now())),
        }
    }

    /// Attempts to consume a token.
    pub async fn try_acquire(&self) -> bool {
        self.refill().await;

        let mut current = self.tokens.load(Ordering::Relaxed);
        loop {
            if current == 0 {
                return false;
            }
            match self.tokens.compare_exchange_weak(
                current,
                current - 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    /// Consumes a token, returning the wait time if throttled.
    pub async fn acquire(&self) -> Result<(), Duration> {
        if self.try_acquire().await {
            Ok(())
        } else {
            // Calculate wait time until next refill
            let last = *self.last_refill.read().await;
            let elapsed = last.elapsed();
            let tokens_needed = 1;
            let wait_time = Duration::from_millis(
                (tokens_needed * 1000 / self.refill_rate.max(1)) as u64,
            );
            Ok(wait_time)
        }
    }

    /// Refills tokens based on elapsed time.
    async fn refill(&self) {
        let mut last = self.last_refill.write().await;
        let elapsed = last.elapsed();
        if elapsed < Duration::from_secs(1) {
            return;
        }

        let tokens_to_add = (elapsed.as_secs() as u64) * self.refill_rate;
        let mut current = self.tokens.load(Ordering::Relaxed);
        let new_count = (current + tokens_to_add).min(self.max_tokens);
        self.tokens.store(new_count, Ordering::Relaxed);
        *last = Instant::now();
    }

    /// Returns the current token count.
    pub fn available_tokens(&self) -> u64 {
        self.tokens.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_checker_initial_state() {
        let checker = HealthChecker::new();
        assert!(!checker.is_ready());
        assert!(checker.liveness());
    }

    #[test]
    fn test_health_checker_ready() {
        let checker = HealthChecker::new();
        checker.mark_ready();
        assert!(checker.is_ready());
    }

    #[test]
    fn test_backpressure_accept() {
        let mut bp = BackpressureState::new(0.8, 0.95);
        let action = bp.update_load(0.5);
        assert_eq!(action, BackpressureAction::Accept);
        assert!(!bp.enabled);
    }

    #[test]
    fn test_backpressure_throttle() {
        let mut bp = BackpressureState::new(0.8, 0.95);
        let action = bp.update_load(0.85);
        assert_eq!(action, BackpressureAction::Throttle);
        assert!(bp.enabled);
    }

    #[test]
    fn test_backpressure_reject() {
        let mut bp = BackpressureState::new(0.8, 0.95);
        let action = bp.update_load(0.98);
        assert_eq!(action, BackpressureAction::Reject);
        assert!(bp.enabled);
    }

    #[tokio::test]
    async fn test_rate_limiter() {
        let limiter = RateLimiter::new(10, 5); // 10 tokens, refill 5 per second

        // Should be able to acquire all tokens
        for _ in 0..10 {
            assert!(limiter.try_acquire().await);
        }

        // Should be unable to acquire more
        assert!(!limiter.try_acquire().await);

        // Wait and refill
        tokio::time::sleep(Duration::from_secs(1)).await;
        limiter.refill().await;

        // Should have tokens again
        assert!(limiter.try_acquire().await);
    }
}
