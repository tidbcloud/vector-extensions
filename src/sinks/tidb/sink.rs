use std::time::Duration;

use futures::{stream::BoxStream, StreamExt};
use sqlx::MySqlPool;
use vector_lib::{
    event::Event,
    sink::StreamSink,
};

use tracing::{error, info, warn};

/// TiDB sink that writes events to MySQL/TiDB database
pub struct TiDBSink {
    pool: MySqlPool,
    table: String,
    batch_size: usize,
}

impl TiDBSink {
    /// Create a new TiDB sink
    pub async fn new(
        connection_string: String,
        table: String,
        max_connections: u32,
        connection_timeout: Duration,
        batch_size: usize,
    ) -> vector::Result<Self> {
        use sqlx::mysql::MySqlPoolOptions;

        // Create connection pool with options
        let pool = MySqlPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(connection_timeout)
            .connect(&connection_string)
            .await
            .map_err(|e| vector::Error::from(format!("Failed to create connection pool: {}", e)))?;

        info!(
            message = "TiDB sink initialized",
            table = %table,
            max_connections = max_connections,
            batch_size = batch_size
        );

        Ok(Self {
            pool,
            table,
            batch_size,
        })
    }

    /// Insert a batch of events into the database
    async fn insert_batch(&self, events: Vec<Event>) -> vector::Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        // Insert batch using a prepared statement
        // Note: This is a simplified implementation. In production, you might want to:
        // 1. Support custom table schemas
        // 2. Map specific event fields to table columns
        // 3. Handle schema evolution
        let query = format!(
            "INSERT INTO {} (log_line, log_timestamp, task_id) VALUES (?, ?, ?)",
            self.table
        );

        for event in events {
            // Extract LogEvent from Event
            let log_event = match event {
                Event::Log(log) => log,
                Event::Metric(_) => {
                    warn!(message = "Metric events are not supported, skipping");
                    continue;
                }
                Event::Trace(_) => {
                    warn!(message = "Trace events are not supported, skipping");
                    continue;
                }
            };

            // Extract fields from the log event
            let log_line = log_event
                .get("message")
                .or_else(|| log_event.get("log"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| {
                    // Fallback: serialize the entire event as JSON
                    serde_json::to_string(&log_event)
                        .unwrap_or_else(|_| "{}".to_string())
                });

            let timestamp = log_event
                .get("timestamp")
                .and_then(|v| v.as_str())
                .or_else(|| {
                    log_event
                        .get("time")
                        .and_then(|v| v.as_str())
                })
                .map(|s| s.to_string())
                .unwrap_or_else(|| {
                    // Fallback to current time if no timestamp field found
                    chrono::Utc::now().to_rfc3339()
                });

            let task_id = log_event
                .get("task_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| "default".to_string());

            sqlx::query(&query)
                .bind(&log_line)
                .bind(&timestamp)
                .bind(&task_id)
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    error!(message = "Failed to insert event", error = %e);
                    vector::Error::from(format!("Failed to insert event: {}", e))
                })?;
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl StreamSink<Event> for TiDBSink {
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        info!(
            message = "TiDB sink starting",
            table = %self.table,
            batch_size = self.batch_size
        );

        let mut input = input.ready_chunks(self.batch_size);

        while let Some(events) = input.next().await {
            if let Err(e) = self.insert_batch(events).await {
                error!(message = "Failed to insert batch", error = %e);
                // Continue processing other batches
            }
        }

        Ok(())
    }
}
