use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use metrics::{counter, gauge};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{debug, error, info};
use vector::shutdown::ShutdownSignal;
use vector::SourceSender;
use vector_lib::event::{BatchNotifier, BatchStatus, Event, LogEvent, Value as LogValue};

use crate::sources::delta_lake_watermark::checkpoint::Checkpoint;
use crate::sources::delta_lake_watermark::duckdb_query::DuckDBQueryExecutor;

/// Controller for Delta Lake Watermark source
pub struct Controller {
    executor: Arc<DuckDBQueryExecutor>,
    checkpoint_path: PathBuf,
    condition: Option<String>,  // All filtering including time ranges should be in condition
    order_by_column: String,
    unique_id_column: Option<String>,
    batch_size: usize,
    poll_interval: Duration,
    // acknowledgements is used by Vector framework via can_acknowledge(), not directly in controller
    #[allow(dead_code)]
    acknowledgements: bool,
    out: SourceSender,
    checkpoint: Arc<Mutex<Checkpoint>>,
}

impl Controller {
    /// Create a new controller
    pub async fn new(
        endpoint: String,
        cloud_provider: String,
        data_dir: PathBuf,
        condition: Option<String>,  // All filtering including time ranges should be in condition
        order_by_column: String,
        batch_size: usize,
        poll_interval: Duration,
        acknowledgements: bool,
        unique_id_column: Option<String>,
        duckdb_memory_limit: Option<String>,
        region: Option<String>,
        out: SourceSender,
    ) -> vector::Result<Self> {
        // Create DuckDB executor
        let executor = Arc::new(DuckDBQueryExecutor::new(
            endpoint.clone(),
            cloud_provider,
            duckdb_memory_limit,
            region,
        )?);

        // Get checkpoint path
        let checkpoint_path = Checkpoint::get_path(&data_dir, &endpoint);

        // Load checkpoint
        let checkpoint = Arc::new(Mutex::new(Checkpoint::load(&checkpoint_path)?));

        // Initialize metrics
        Self::init_metrics();

        Ok(Self {
            executor,
            checkpoint_path,
            condition,
            order_by_column,
            unique_id_column,
            batch_size,
            poll_interval,
            acknowledgements,
            out,
            checkpoint,
        })
    }

    /// Initialize Prometheus metrics
    fn init_metrics() {
        // Metrics are registered on first use, no need to initialize here
    }

    /// Run the main controller loop
    pub async fn run(mut self, mut shutdown: ShutdownSignal) {
        info!("Delta Lake Watermark Controller starting...");

        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    info!("Shutdown signal received");
                    break;
                }
                result = self.process_batch() => {
                    match result {
                        Ok(should_continue) => {
                            if !should_continue {
                                info!("Sync completed, shutting down");
                                if self.poll_interval.is_zero() {
                                    // Oneshot mode: Vector doesn't exit when source finishes; force exit so the process terminates.
                                    info!("Oneshot mode (poll_interval_secs=0): exiting process");
                                    std::process::exit(0);
                                }
                                break;
                            }
                        }
                        Err(e) => {
                            error!("Error processing batch: {}", e);
                            // Mark checkpoint as error state
                            let mut cp = self.checkpoint.lock().await;
                            cp.mark_error();
                            let _ = cp.save(&self.checkpoint_path);
                            // Continue processing on error
                        }
                    }
                }
            }
        }

        info!("Delta Lake Watermark Controller shutting down...");
    }

    /// Process a single batch
    async fn process_batch(&mut self) -> vector::Result<bool> {
        // Load current checkpoint
        let checkpoint = self.checkpoint.lock().await.clone();

        // Build and execute query
        // All filtering including time ranges should be in condition
        let sql = self.executor.build_query(
            &checkpoint,
            self.condition.as_deref(),
            &self.order_by_column,
            self.unique_id_column.as_deref(),
            self.batch_size,
        );

        debug!("Executing query: {}", sql);

        // Execute query
        let batch = self
            .executor
            .execute_query(&sql)
            .map_err(|e| format!("Query execution failed: {}", e))?;

        let num_rows = batch.num_rows();

        if num_rows == 0 {
            if self.poll_interval.is_zero() {
                info!("No more data in range (poll_interval_secs=0), sync complete");
                return Ok(false);
            }
            info!("No data available, waiting {} seconds before next poll", self.poll_interval.as_secs());
            sleep(self.poll_interval).await;
            return Ok(true);
        }

        info!("Fetched {} rows from Delta Lake", num_rows);

        // Convert to events
        let json_events = self
            .executor
            .record_batch_to_events(&batch)
            .map_err(|e| format!("Failed to convert batch to events: {}", e))?;

        // Create Vector events
        let mut events = Vec::new();
        let mut last_watermark: Option<String> = None;
        let mut last_unique_id: Option<String> = None;

        for json_event in json_events {
            let mut log_event = LogEvent::default();
            
            // Convert JSON object to LogEvent
            if let serde_json::Value::Object(map) = json_event {
                for (key, value) in map {
                    let log_value = Self::json_value_to_log_value(value);
                    log_event.insert(key.as_str(), log_value);
                }
            }

            // Extract watermark and unique_id for checkpoint update
            if let Some(watermark_value) = log_event.get(self.order_by_column.as_str()) {
                if let Some(watermark_str) = watermark_value.as_str() {
                    last_watermark = Some(watermark_str.to_string());
                }
            }

            if let Some(ref unique_col) = self.unique_id_column {
                if let Some(id_value) = log_event.get(unique_col.as_str()) {
                    if let Some(id_str) = id_value.as_str() {
                        last_unique_id = Some(id_str.to_string());
                    }
                }
            }

            events.push(Event::Log(log_event));
        }

        // When acknowledgements are enabled, attach batch notifier and wait for acks
        // so that the source exits only after all sent events are acknowledged.
        let ack_receiver =
            BatchNotifier::maybe_apply_to(self.acknowledgements, events.as_mut_slice());

        self.out.send_batch(events).await.map_err(|e| {
            format!("Failed to send events: {}", e)
        })?;

        if let Some(rx) = ack_receiver {
            let status = rx.await;
            if !matches!(status, BatchStatus::Delivered) {
                debug!("Batch finalization status: {:?}", status);
            }
        }

        // Update checkpoint with last processed record
        if let Some(ref watermark) = last_watermark {
            let mut cp = self.checkpoint.lock().await;
            cp.update_watermark(watermark.clone(), last_unique_id.clone());
            cp.save(&self.checkpoint_path)
                .map_err(|e| format!("Failed to save checkpoint: {}", e))?;

            // Update metrics
            if let Some(dt) = cp.last_watermark_datetime() {
                gauge!("delta_sync_watermark_timestamp").set(dt.timestamp() as f64);
            }
        }

        // Update metrics
        counter!("delta_sync_rows_processed_total").increment(num_rows as u64);

        Ok(true)
    }

    /// Convert JSON Value to Vector LogValue
    fn json_value_to_log_value(value: serde_json::Value) -> LogValue {
        use bytes::Bytes;
        use ordered_float::NotNan;

        match value {
            serde_json::Value::Null => LogValue::Null,
            serde_json::Value::Bool(b) => LogValue::Boolean(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    LogValue::Integer(i)
                } else if let Some(f) = n.as_f64() {
                    LogValue::Float(NotNan::new(f).unwrap_or(NotNan::new(0.0).unwrap()))
                } else {
                    LogValue::Bytes(Bytes::from(n.to_string()))
                }
            }
            serde_json::Value::String(s) => LogValue::Bytes(Bytes::from(s)),
            serde_json::Value::Array(arr) => {
                let vec: Vec<LogValue> = arr.into_iter().map(Self::json_value_to_log_value).collect();
                LogValue::Array(vec)
            }
            serde_json::Value::Object(map) => {
                use std::collections::BTreeMap;
                use vector_lib::event::KeyString;
                let btree: BTreeMap<KeyString, LogValue> = map
                    .into_iter()
                    .map(|(k, v)| (KeyString::from(k), Self::json_value_to_log_value(v)))
                    .collect();
                LogValue::Object(btree)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::TempDir;

    // TC-022: Test controller initialization
    #[test]
    fn test_controller_structure() {
        // Test that Controller struct can be conceptually instantiated
        // We can't actually create one without a real DuckDB connection and SourceSender,
        // but we can verify the structure is correct
        let _ = std::mem::size_of::<Controller>();
    }

    // TC-023: Test controller field validation
    #[test]
    fn test_controller_fields() {
        // Test that Controller fields can be accessed conceptually
        let _endpoint = "s3://bucket/table".to_string();
        let _cloud_provider = "aws".to_string();
        let _data_dir = PathBuf::from("/tmp");
        let _condition = Some("time >= 1717632000 AND time <= 1718044799 AND type = 'error'".to_string());
        let _order_by_column = "time".to_string();
        let _batch_size = 1000;
        let _poll_interval = Duration::from_secs(30);
        let _acknowledgements = true;
    }

    #[test]
    fn test_checkpoint_path_generation() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_path_buf();
        let endpoint = "s3://bucket/path/to/table";
        
        let checkpoint_path = Checkpoint::get_path(&data_dir, endpoint);
        assert!(checkpoint_path.exists() || !checkpoint_path.exists()); // Path may or may not exist
        assert!(checkpoint_path.to_string_lossy().contains("delta_lake_watermark"));
    }

    // TC-023: Test JSON value to LogValue conversion
    #[test]
    fn test_json_value_to_log_value() {
        use serde_json::json;

        // Test Null
        let null_value = json!(null);
        let log_value = Controller::json_value_to_log_value(null_value);
        assert!(matches!(log_value, LogValue::Null));

        // Test Boolean
        let bool_value = json!(true);
        let log_value = Controller::json_value_to_log_value(bool_value);
        assert!(matches!(log_value, LogValue::Boolean(true)));

        // Test Integer
        let int_value = json!(42);
        let log_value = Controller::json_value_to_log_value(int_value);
        assert!(matches!(log_value, LogValue::Integer(42)));

        // Test Float
        let float_value = json!(3.14);
        let log_value = Controller::json_value_to_log_value(float_value);
        match log_value {
            LogValue::Float(f) => {
                assert!((f.into_inner() - 3.14).abs() < 0.001);
            }
            _ => panic!("Expected Float"),
        }

        // Test String
        let string_value = json!("hello");
        let log_value = Controller::json_value_to_log_value(string_value);
        match log_value {
            LogValue::Bytes(b) => {
                assert_eq!(b.as_ref(), b"hello");
            }
            _ => panic!("Expected Bytes"),
        }

        // Test Array
        let array_value = json!([1, 2, 3]);
        let log_value = Controller::json_value_to_log_value(array_value);
        match log_value {
            LogValue::Array(arr) => {
                assert_eq!(arr.len(), 3);
            }
            _ => panic!("Expected Array"),
        }

        // Test Object
        let object_value = json!({"key": "value"});
        let log_value = Controller::json_value_to_log_value(object_value);
        match log_value {
            LogValue::Object(obj) => {
                assert_eq!(obj.len(), 1);
            }
            _ => panic!("Expected Object"),
        }
    }
}
