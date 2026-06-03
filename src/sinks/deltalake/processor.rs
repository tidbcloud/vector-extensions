use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures::{stream::BoxStream, StreamExt};
use tokio::sync::Mutex;
use vector_lib::event::Event;
use vector_lib::sink::StreamSink;

use crate::common::deltalake_writer::{
    is_stale_delta_log_error, DeltaLakeWriter, DeltaTableConfig, WriteConfig,
};

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
                error!("Failed to write events to table {}: {}", table_name, e);
            }
        }

        Ok(())
    }

    /// Write events to a specific table, evicting and reopening the writer once on stale log errors.
    async fn write_table_events(
        &self,
        table_name: &str,
        events: Vec<Event>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match self
            .write_table_events_once(table_name, &events)
            .await
        {
            Ok(()) => Ok(()),
            Err(e) if is_stale_delta_log_error(&e.to_string()) => {
                warn!(
                    "Stale Delta log for table {}, evicting cached writer and retrying once: {}",
                    table_name, e
                );
                self.writers.lock().await.remove(table_name);
                tokio::time::sleep(Duration::from_millis(200)).await;
                self.write_table_events_once(table_name, &events).await
            }
            Err(e) => Err(e),
        }
    }

    async fn write_table_events_once(
        &self,
        table_name: &str,
        events: &[Event],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Get or create writer for this table
        let mut writers = self.writers.lock().await;
        let writer = writers.entry(table_name.to_string()).or_insert_with(|| {
            let path_str = self.base_path.to_string_lossy();
            let is_cloud_path = path_str.starts_with("s3://")
                || path_str.starts_with("abfss://")
                || path_str.starts_with("gs://");

            let table_path = if is_cloud_path {
                // For cloud storage paths, use string formatting to preserve the URI scheme
                // PathBuf::join would corrupt the scheme prefix
                PathBuf::from(format!("{}/{}", path_str.trim_end_matches('/'), table_name))
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
                    info!("Creating default table config for {}", table_name);
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
        writer.write_events(events.to_vec()).await?;

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
    use std::fs;
    use vector_lib::event::{LogEvent, ObjectMap};

    fn create_test_event(table_name: &str, index: i64) -> Event {
        let mut log = LogEvent::from(BTreeMap::new());
        log.insert("_vector_table", table_name);
        log.insert("_vector_source_table", "TEST_SOURCE");
        log.insert("_vector_source_schema", "test_schema");
        log.insert("_vector_instance", "test-instance");
        log.insert("_vector_timestamp", "2024-06-01T00:00:00Z");
        log.insert("id", index);
        log.insert("value", format!("row-{index}"));

        let mut schema_meta = ObjectMap::new();
        schema_meta.insert("_partition_by".into(), vector_lib::event::Value::from("date"));
        let mut id_meta = ObjectMap::new();
        id_meta.insert("mysql_type".into(), vector_lib::event::Value::from("bigint"));
        schema_meta.insert("id".into(), vector_lib::event::Value::Object(id_meta));
        let mut value_meta = ObjectMap::new();
        value_meta.insert(
            "mysql_type".into(),
            vector_lib::event::Value::from("varchar(64)"),
        );
        schema_meta.insert("value".into(), vector_lib::event::Value::Object(value_meta));
        log.insert("_schema_metadata", vector_lib::event::Value::Object(schema_meta));

        Event::Log(log)
    }

    fn create_test_event_legacy(table_field: &str, table_name: &str) -> Event {
        let mut log = LogEvent::from(BTreeMap::new());
        log.insert(table_field, table_name);
        log.insert("test_field", "test_value");
        Event::Log(log)
    }

    #[test]
    fn test_table_name_extraction_from_vector_table() {
        let event = create_test_event_legacy("_vector_table", "test_table");
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
        let event = create_test_event_legacy("dest_table", "my_dest_table");
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
        let event = create_test_event_legacy("table", "fallback_table");
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
            create_test_event_legacy("_vector_table", "table_a"),
            create_test_event_legacy("_vector_table", "table_b"),
            create_test_event_legacy("_vector_table", "table_a"),
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

    fn delta_log_json_files(delta_log_path: &std::path::Path) -> Vec<PathBuf> {
        let mut files: Vec<PathBuf> = fs::read_dir(delta_log_path)
            .expect("read _delta_log")
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
            .collect();
        files.sort();
        files
    }

    #[tokio::test]
    async fn sink_process_events_recovers_after_simulated_compaction() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let base_path = std::env::temp_dir().join(format!("deltalake_sink_stale_log_{nanos}"));
        let table_name = "recover_table";
        let table_path = base_path.join(table_name);
        fs::create_dir_all(&table_path).expect("create table dir");

        let sink = DeltaLakeSink::new(
            base_path.clone(),
            vec![DeltaTableConfig {
                name: table_name.to_string(),
                schema_evolution: Some(true),
            }],
            WriteConfig {
                batch_size: 1000,
                timeout_secs: 30,
            },
            None,
        );

        for batch in 0..5 {
            let events: Vec<Event> = (0..3)
                .map(|i| create_test_event(table_name, i))
                .collect();
            sink.process_events(events)
                .await
                .expect("seed batch should write");
            let _ = batch;
        }

        let delta_log_path = table_path.join("_delta_log");
        let json_files = delta_log_json_files(&delta_log_path);
        assert!(json_files.len() >= 3);
        let latest = json_files.last().expect("latest delta log json").clone();
        fs::remove_file(latest).expect("simulate compaction");

        // Must not panic; stale-log recovery should allow the batch to be written.
        sink.process_events(vec![create_test_event(table_name, 999)])
            .await
            .expect("sink should recover after simulated compaction");

        assert!(delta_log_json_files(&delta_log_path).len() >= 1);
        let _ = fs::remove_dir_all(&base_path);
    }
}
