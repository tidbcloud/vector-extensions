use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use futures::{stream::BoxStream, StreamExt};
use tokio::sync::Mutex;
use vector_lib::event::Event;
use vector_lib::sink::StreamSink;

use crate::common::deltalake_writer::{DeltaLakeWriter, DeltaTableConfig, WriteConfig};

/// Delta Lake sink processor
pub struct DeltaLakeSink {
    base_path: PathBuf,
    tables: Vec<DeltaTableConfig>,
    write_config: WriteConfig,
    storage_options: Option<HashMap<String, String>>,
    writers: Arc<Mutex<HashMap<String, DeltaLakeWriter>>>,
}

impl DeltaLakeSink {
    /// Create a new Delta Lake sink
    pub fn new(
        base_path: PathBuf,
        tables: Vec<DeltaTableConfig>,
        write_config: WriteConfig,
        storage_options: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            base_path,
            tables,
            write_config,
            storage_options,
            writers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Process events and write to Delta Lake
    async fn process_events(
        &self,
        events: Vec<Event>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if events.is_empty() {
            return Ok(());
        }

        // Log batch summary
        info!("Sink processing batch: {} events", events.len());

        // Group events by table (prefer dest_table, fallback to table)
        let mut table_events: HashMap<String, Vec<Event>> = HashMap::new();

        for event in events {
            if let Event::Log(log_event) = event {
                let table_name = log_event
                    .get("_vector_table")
                    .and_then(|v| v.as_str())
                    .or_else(|| log_event.get("dest_table").and_then(|v| v.as_str()))
                    .or_else(|| log_event.get("table").and_then(|v| v.as_str()));
                if let Some(table_name) = table_name {
                    table_events
                        .entry(table_name.to_string())
                        .or_default()
                        .push(Event::Log(log_event));
                }
            }
        }

        // Write each table's events
        for (table_name, table_events) in table_events {
            if let Err(e) = self.write_table_events(&table_name, table_events).await {
                let error_msg = e.to_string();
                if error_msg.contains("log segment")
                    || error_msg.contains("Invalid table version")
                    || error_msg.contains("not found")
                    || error_msg.contains("No such file or directory")
                {
                    panic!(
                        "Delta Lake corruption detected for table {}: {}",
                        table_name, error_msg
                    );
                } else {
                    error!("Failed to write events to table {}: {}", table_name, e);
                }
            }
        }

        Ok(())
    }

    /// Write events to a specific table
    async fn write_table_events(
        &self,
        table_name: &str,
        events: Vec<Event>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Get or create writer for this table
        let mut writers = self.writers.lock().await;
        let writer = writers.entry(table_name.to_string()).or_insert_with(|| {
            let table_path = if self.base_path.to_string_lossy().starts_with("s3://") {
                // For S3 paths, append the table name to the S3 path
                PathBuf::from(format!(
                    "{}/{}",
                    self.base_path.to_string_lossy(),
                    table_name
                ))
            } else {
                // For local paths, use join as before
                self.base_path.join(table_name)
            };

            // Partition columns come from event _schema_metadata._partition_by,
            // set by the system_tables source via TableConfig.partition_by
            let table_config = self
                .tables
                .iter()
                .find(|t| t.name == table_name)
                .cloned()
                .unwrap_or_else(|| {
                    info!(
                        "Creating default table config for {}",
                        table_name
                    );
                    DeltaTableConfig {
                        name: table_name.to_string(),
                        schema_evolution: Some(true),
                    }
                });

            DeltaLakeWriter::new(
                table_path,
                table_config,
                self.write_config.clone(),
                self.storage_options.clone(),
            )
        });

        // Write events
        writer.write_events(events).await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl StreamSink<Event> for DeltaLakeSink {
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        info!(
            "Delta Lake sink starting with batch_size: {}, timeout_secs: {}",
            self.write_config.batch_size, self.write_config.timeout_secs
        );

        let mut input = input.ready_chunks(self.write_config.batch_size);

        while let Some(events) = input.next().await {
            if let Err(e) = self.process_events(events).await {
                error!("Failed to process events: {}", e);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use vector_lib::event::LogEvent;

    fn create_test_event(table_field: &str, table_name: &str) -> Event {
        let mut log = LogEvent::from(BTreeMap::new());
        log.insert(table_field, table_name);
        log.insert("test_field", "test_value");
        Event::Log(log)
    }

    #[test]
    fn test_table_name_extraction_from_vector_table() {
        let event = create_test_event("_vector_table", "test_table");
        if let Event::Log(log) = &event {
            let table_name = log
                .get("_vector_table")
                .and_then(|v| v.as_str())
                .or_else(|| log.get("dest_table").and_then(|v| v.as_str()))
                .or_else(|| log.get("table").and_then(|v| v.as_str()));
            assert_eq!(table_name.as_deref(), Some("test_table"));
        }
    }

    #[test]
    fn test_table_name_extraction_from_dest_table() {
        let event = create_test_event("dest_table", "my_dest_table");
        if let Event::Log(log) = &event {
            let table_name = log
                .get("_vector_table")
                .and_then(|v| v.as_str())
                .or_else(|| log.get("dest_table").and_then(|v| v.as_str()))
                .or_else(|| log.get("table").and_then(|v| v.as_str()));
            assert_eq!(table_name.as_deref(), Some("my_dest_table"));
        }
    }

    #[test]
    fn test_table_name_extraction_from_table() {
        let event = create_test_event("table", "fallback_table");
        if let Event::Log(log) = &event {
            let table_name = log
                .get("_vector_table")
                .and_then(|v| v.as_str())
                .or_else(|| log.get("dest_table").and_then(|v| v.as_str()))
                .or_else(|| log.get("table").and_then(|v| v.as_str()));
            assert_eq!(table_name.as_deref(), Some("fallback_table"));
        }
    }

    #[test]
    fn test_table_name_priority() {
        // _vector_table should have highest priority
        let mut log = LogEvent::from(BTreeMap::new());
        log.insert("_vector_table", "priority_table");
        log.insert("dest_table", "other_table");
        log.insert("table", "another_table");
        let event = Event::Log(log);

        if let Event::Log(log) = &event {
            let table_name = log
                .get("_vector_table")
                .and_then(|v| v.as_str())
                .or_else(|| log.get("dest_table").and_then(|v| v.as_str()))
                .or_else(|| log.get("table").and_then(|v| v.as_str()));
            assert_eq!(table_name.as_deref(), Some("priority_table"));
        }
    }

    #[test]
    fn test_events_grouping_by_table() {
        let events = vec![
            create_test_event("_vector_table", "table_a"),
            create_test_event("_vector_table", "table_b"),
            create_test_event("_vector_table", "table_a"),
        ];

        let mut table_events: HashMap<String, Vec<Event>> = HashMap::new();

        for event in events {
            if let Event::Log(log_event) = event {
                let table_name = log_event
                    .get("_vector_table")
                    .and_then(|v| v.as_str())
                    .or_else(|| log_event.get("dest_table").and_then(|v| v.as_str()))
                    .or_else(|| log_event.get("table").and_then(|v| v.as_str()));
                if let Some(table_name) = table_name {
                    table_events
                        .entry(table_name.to_string())
                        .or_default()
                        .push(Event::Log(log_event));
                }
            }
        }

        assert_eq!(table_events.len(), 2);
        assert_eq!(table_events.get("table_a").unwrap().len(), 2);
        assert_eq!(table_events.get("table_b").unwrap().len(), 1);
    }
}
