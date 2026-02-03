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

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, warn};

use super::config::{S3Config, StorageBackend, StorageConfig};
use super::grpc_server::proto::{BatchMetadata, Statement};

/// Buffered statement record for storage.
#[derive(Debug, Clone)]
pub struct StatementRecord {
    /// Metadata from the batch
    pub cluster_id: String,
    pub instance_id: String,
    pub window_start_ms: i64,
    pub window_end_ms: i64,

    /// Statement data
    pub stmt: Statement,

    /// Receive timestamp
    pub received_at: DateTime<Utc>,
}

/// Storage for statement data, supporting both S3 and local file storage.
pub struct StatementStorage {
    config: StorageConfig,
    backend: StorageBackendImpl,

    /// Buffer for batching writes
    buffer: Mutex<VecDeque<StatementRecord>>,
    buffer_size: std::sync::atomic::AtomicUsize,

    /// Last flush time
    last_flush: RwLock<Instant>,

    /// Metrics
    statements_stored: std::sync::atomic::AtomicU64,
    bytes_written: std::sync::atomic::AtomicU64,
    flush_count: std::sync::atomic::AtomicU64,
}

enum StorageBackendImpl {
    Local(LocalStorage),
    S3(S3Storage),
}

impl StatementStorage {
    /// Creates a new StatementStorage from config.
    pub fn new(config: StorageConfig) -> Self {
        let backend = match config.backend {
            StorageBackend::Local => {
                let path = config.local.as_ref()
                    .map(|l| l.path.clone())
                    .unwrap_or_else(|| "/var/lib/vector/statements".to_string());
                StorageBackendImpl::Local(LocalStorage::new(path))
            }
            StorageBackend::S3 => {
                let s3_config = config.s3.clone().expect("S3 config required for S3 backend");
                StorageBackendImpl::S3(S3Storage::new(s3_config))
            }
        };

        Self {
            config,
            backend,
            buffer: Mutex::new(VecDeque::new()),
            buffer_size: std::sync::atomic::AtomicUsize::new(0),
            last_flush: RwLock::new(Instant::now()),
            statements_stored: std::sync::atomic::AtomicU64::new(0),
            bytes_written: std::sync::atomic::AtomicU64::new(0),
            flush_count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Creates a new local storage.
    pub fn new_local(path: String) -> Self {
        Self::new(StorageConfig {
            backend: StorageBackend::Local,
            local: Some(super::config::LocalConfig { path }),
            ..Default::default()
        })
    }

    /// Stores a statement.
    pub async fn store(
        &self,
        metadata: &BatchMetadata,
        stmt: &Statement,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let record = StatementRecord {
            cluster_id: metadata.cluster_id.clone(),
            instance_id: metadata.instance_id.clone(),
            window_start_ms: metadata.window_start_ms,
            window_end_ms: metadata.window_end_ms,
            stmt: stmt.clone(),
            received_at: Utc::now(),
        };

        // Estimate record size (rough approximation)
        let record_size = estimate_record_size(&record);

        // Add to buffer
        {
            let mut buffer = self.buffer.lock().await;
            buffer.push_back(record);
            self.buffer_size.fetch_add(record_size, std::sync::atomic::Ordering::Relaxed);
        }

        self.statements_stored.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Check if we should flush
        self.maybe_flush().await?;

        Ok(())
    }

    /// Checks if flush is needed and flushes if so.
    async fn maybe_flush(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let buffer_size = self.buffer_size.load(std::sync::atomic::Ordering::Relaxed);
        let last_flush = *self.last_flush.read().await;

        let should_flush = buffer_size >= self.config.max_buffer_size_bytes
            || last_flush.elapsed() >= Duration::from_secs(self.config.flush_interval_secs);

        if should_flush {
            self.flush().await?;
        }

        Ok(())
    }

    /// Flushes the buffer to storage.
    pub async fn flush(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let records: Vec<StatementRecord> = {
            let mut buffer = self.buffer.lock().await;
            let records: Vec<_> = buffer.drain(..).collect();
            self.buffer_size.store(0, std::sync::atomic::Ordering::Relaxed);
            records
        };

        if records.is_empty() {
            return Ok(());
        }

        info!("Flushing {} statement records to storage", records.len());

        // Update last flush time
        {
            let mut last_flush = self.last_flush.write().await;
            *last_flush = Instant::now();
        }

        // Write to storage backend
        let bytes_written = match &self.backend {
            StorageBackendImpl::Local(local) => local.write(&records).await?,
            StorageBackendImpl::S3(s3) => s3.write(&records).await?,
        };

        self.bytes_written.fetch_add(bytes_written, std::sync::atomic::Ordering::Relaxed);
        self.flush_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        Ok(())
    }

    /// Returns storage metrics.
    pub fn metrics(&self) -> StorageMetrics {
        StorageMetrics {
            statements_stored: self.statements_stored.load(std::sync::atomic::Ordering::Relaxed),
            bytes_written: self.bytes_written.load(std::sync::atomic::Ordering::Relaxed),
            flush_count: self.flush_count.load(std::sync::atomic::Ordering::Relaxed),
            buffer_size: self.buffer_size.load(std::sync::atomic::Ordering::Relaxed),
        }
    }

    /// Returns the current buffer size in bytes.
    pub fn buffer_size(&self) -> usize {
        self.buffer_size.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Storage metrics.
#[derive(Debug, Clone)]
pub struct StorageMetrics {
    pub statements_stored: u64,
    pub bytes_written: u64,
    pub flush_count: u64,
    pub buffer_size: usize,
}

/// Local file storage implementation.
struct LocalStorage {
    base_path: PathBuf,
}

impl LocalStorage {
    fn new(path: String) -> Self {
        Self {
            base_path: PathBuf::from(path),
        }
    }

    async fn write(&self, records: &[StatementRecord]) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        if records.is_empty() {
            return Ok(0);
        }

        // Create directory structure: base_path/year=YYYY/month=MM/day=DD/hour=HH/
        let now = Utc::now();
        let partition_path = self.base_path
            .join(format!("year={}", now.format("%Y")))
            .join(format!("month={}", now.format("%m")))
            .join(format!("day={}", now.format("%d")))
            .join(format!("hour={}", now.format("%H")));

        tokio::fs::create_dir_all(&partition_path).await?;

        // Generate unique filename
        let filename = format!(
            "statements_{}_{}.json",
            now.format("%Y%m%d_%H%M%S"),
            uuid::Uuid::new_v4().to_string()[..8].to_string()
        );

        let file_path = partition_path.join(&filename);

        // Serialize records to JSON (for now; Parquet would be used in production)
        let json = serde_json::to_vec_pretty(&records.iter().map(|r| {
            serde_json::json!({
                "cluster_id": r.cluster_id,
                "instance_id": r.instance_id,
                "window_start_ms": r.window_start_ms,
                "window_end_ms": r.window_end_ms,
                "received_at": r.received_at.to_rfc3339(),
                "digest": r.stmt.digest,
                "plan_digest": r.stmt.plan_digest,
                "schema_name": r.stmt.schema_name,
                "exec_count": r.stmt.exec_count,
                "sum_latency_us": r.stmt.sum_latency_us,
                "max_latency_us": r.stmt.max_latency_us,
                "min_latency_us": r.stmt.min_latency_us,
                "avg_latency_us": r.stmt.avg_latency_us,
                "p50_latency_us": r.stmt.p50_latency_us,
                "p95_latency_us": r.stmt.p95_latency_us,
                "p99_latency_us": r.stmt.p99_latency_us,
            })
        }).collect::<Vec<_>>())?;

        let bytes = json.len() as u64;
        tokio::fs::write(&file_path, json).await?;

        debug!("Wrote {} bytes to {}", bytes, file_path.display());

        Ok(bytes)
    }
}

/// S3 storage implementation.
struct S3Storage {
    config: S3Config,
}

impl S3Storage {
    fn new(config: S3Config) -> Self {
        Self { config }
    }

    async fn write(&self, records: &[StatementRecord]) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        if records.is_empty() {
            return Ok(0);
        }

        // Generate S3 key with partitioning
        let now = Utc::now();
        let key = format!(
            "{prefix}year={year}/month={month}/day={day}/hour={hour}/statements_{ts}_{uuid}.parquet",
            prefix = self.config.prefix,
            year = now.format("%Y"),
            month = now.format("%m"),
            day = now.format("%d"),
            hour = now.format("%H"),
            ts = now.format("%Y%m%d_%H%M%S"),
            uuid = &uuid::Uuid::new_v4().to_string()[..8]
        );

        // TODO: Implement actual S3 upload with Parquet format
        // For now, just log what we would do
        info!(
            "Would write {} records to s3://{}/{} (Parquet format, {} compression)",
            records.len(),
            self.config.bucket,
            key,
            "snappy"
        );

        // Return estimated size
        let estimated_bytes = records.len() as u64 * 500; // ~500 bytes per record
        Ok(estimated_bytes)
    }
}

/// Estimates the memory size of a statement record.
fn estimate_record_size(record: &StatementRecord) -> usize {
    let mut size = 256; // Base struct size

    size += record.cluster_id.len();
    size += record.instance_id.len();
    size += record.stmt.digest.len();
    size += record.stmt.plan_digest.len();
    size += record.stmt.schema_name.len();
    size += record.stmt.normalized_sql.len();
    size += record.stmt.table_names.len();
    size += record.stmt.sample_sql.len();
    size += record.stmt.sample_plan.len();

    // Extended metrics
    for (k, _) in &record.stmt.extended_metrics {
        size += k.len() + 16; // Key + value overhead
    }

    size
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_local_storage() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = StatementStorage::new_local(temp_dir.path().to_string_lossy().to_string());

        let metadata = BatchMetadata {
            cluster_id: "test-cluster".to_string(),
            instance_id: "test-instance".to_string(),
            window_start_ms: 1000,
            window_end_ms: 2000,
            batch_sequence: 1,
            batch_timestamp_ms: 1500,
            schema_version: "1.0.0".to_string(),
            schema_id: 0,
            field_names: vec![],
        };

        let stmt = Statement {
            digest: "test-digest".to_string(),
            exec_count: 100,
            sum_latency_us: 1000000,
            ..Default::default()
        };

        storage.store(&metadata, &stmt).await.unwrap();

        let metrics = storage.metrics();
        assert_eq!(metrics.statements_stored, 1);
    }
}
