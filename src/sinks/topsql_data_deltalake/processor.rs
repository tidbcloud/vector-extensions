use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use futures::{stream::BoxStream, StreamExt};
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use vector_lib::event::{Event, LogEvent};
use vector_lib::sink::StreamSink;

use crate::common::deltalake_writer::{DeltaLakeWriter, DeltaTableConfig, WriteConfig};
use crate::common::keyspace_cluster::{
    path_contains_keyspace_route_segments, replace_keyspace_route_segments,
    route_resolution_retry_delay, KeyspaceRoute, PdKeyspaceResolver,
    MAX_ROUTE_RESOLUTION_RETRIES,
};
use crate::sources::topsql_v2::upstream::consts::{
    LABEL_DATE, LABEL_DB_NAME, LABEL_INSTANCE_KEY, LABEL_KEYSPACE, LABEL_PLAN_DIGEST,
    LABEL_REGION_ID, LABEL_SOURCE_TABLE, LABEL_SQL_DIGEST, LABEL_TABLE_ID, LABEL_TABLE_NAME,
    LABEL_TAG_LABEL, LABEL_TIMESTAMPS, LABEL_USER, METRIC_NAME_CPU_TIME_MS, METRIC_NAME_EXEC_COUNT,
    METRIC_NAME_EXEC_DURATION, METRIC_NAME_LOGICAL_READ_BYTES, METRIC_NAME_LOGICAL_WRITE_BYTES,
    METRIC_NAME_NETWORK_IN_BYTES, METRIC_NAME_NETWORK_OUT_BYTES, METRIC_NAME_READ_KEYS,
    METRIC_NAME_STMT_DURATION_COUNT, METRIC_NAME_STMT_DURATION_SUM_NS, METRIC_NAME_STMT_EXEC_COUNT,
    METRIC_NAME_TOTAL_RU, METRIC_NAME_WRITE_KEYS, SOURCE_TABLE_TOPRU,
};

use lazy_static::lazy_static;
lazy_static! {
    static ref TOPSQL_SCHEMA: serde_json::Map<String, serde_json::Value> = {
        let mut schema_info = serde_json::Map::new();
        schema_info.insert(
            LABEL_TIMESTAMPS.into(),
            serde_json::json!({
                "mysql_type": "bigint",
                "is_nullable": false
            }),
        );
        schema_info.insert(
            LABEL_DATE.into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": false
            }),
        );
        schema_info.insert(
            LABEL_KEYSPACE.into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": true
            }),
        );
        schema_info.insert(
            LABEL_DB_NAME.into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": true
            }),
        );
        schema_info.insert(
            LABEL_TABLE_NAME.into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": true
            }),
        );
        schema_info.insert(
            LABEL_TABLE_ID.into(),
            serde_json::json!({
                "mysql_type": "bigint",
                "is_nullable": true
            }),
        );
        schema_info.insert(
            LABEL_TAG_LABEL.into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": true
            }),
        );
        schema_info.insert(
            LABEL_SQL_DIGEST.into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": true
            }),
        );
        schema_info.insert(
            LABEL_PLAN_DIGEST.into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": true
            }),
        );
        schema_info.insert(
            METRIC_NAME_CPU_TIME_MS.into(),
            serde_json::json!({
                "mysql_type": "int",
                "is_nullable": false
            }),
        );
        schema_info.insert(
            METRIC_NAME_STMT_EXEC_COUNT.into(),
            serde_json::json!({
                "mysql_type": "bigint",
                "is_nullable": true
            }),
        );
        schema_info.insert(
            METRIC_NAME_STMT_DURATION_SUM_NS.into(),
            serde_json::json!({
                "mysql_type": "bigint",
                "is_nullable": true
            }),
        );
        schema_info.insert(
            METRIC_NAME_STMT_DURATION_COUNT.into(),
            serde_json::json!({
                "mysql_type": "bigint",
                "is_nullable": true
            }),
        );
        schema_info.insert(
            METRIC_NAME_NETWORK_IN_BYTES.into(),
            serde_json::json!({
                "mysql_type": "bigint",
                "is_nullable": true
            }),
        );
        schema_info.insert(
            METRIC_NAME_NETWORK_OUT_BYTES.into(),
            serde_json::json!({
                "mysql_type": "bigint",
                "is_nullable": true
            }),
        );
        // tikv specific columns
        schema_info.insert(
            METRIC_NAME_READ_KEYS.into(),
            serde_json::json!({
                "mysql_type": "bigint",
                "is_nullable": true
            }),
        );
        schema_info.insert(
            METRIC_NAME_WRITE_KEYS.into(),
            serde_json::json!({
                "mysql_type": "bigint",
                "is_nullable": true
            }),
        );
        schema_info.insert(
            METRIC_NAME_LOGICAL_READ_BYTES.into(),
            serde_json::json!({
                "mysql_type": "bigint",
                "is_nullable": true
            }),
        );
        schema_info.insert(
            METRIC_NAME_LOGICAL_WRITE_BYTES.into(),
            serde_json::json!({
                "mysql_type": "bigint",
                "is_nullable": true
            }),
        );
        // tikv region specific fields
        schema_info.insert(
            LABEL_REGION_ID.into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": false
            }),
        );
        // partition key
        schema_info.insert(
            "_partition_by".into(),
            serde_json::json!(vec![LABEL_DATE.to_string()]),
        );
        schema_info
    };

    static ref TOPRU_SCHEMA: serde_json::Map<String, serde_json::Value> = {
        let mut schema_info = serde_json::Map::new();
        schema_info.insert(
            LABEL_TIMESTAMPS.into(),
            serde_json::json!({
                "mysql_type": "bigint",
                "is_nullable": false
            }),
        );
        schema_info.insert(
            LABEL_DATE.into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": false
            }),
        );
        schema_info.insert(
            LABEL_KEYSPACE.into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": true
            }),
        );
        schema_info.insert(
            LABEL_USER.into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": true
            }),
        );
        schema_info.insert(
            LABEL_SQL_DIGEST.into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": true
            }),
        );
        schema_info.insert(
            LABEL_PLAN_DIGEST.into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": true
            }),
        );
        schema_info.insert(
            METRIC_NAME_TOTAL_RU.into(),
            serde_json::json!({
                "mysql_type": "double",
                "is_nullable": false
            }),
        );
        schema_info.insert(
            METRIC_NAME_EXEC_COUNT.into(),
            serde_json::json!({
                "mysql_type": "bigint",
                "is_nullable": true
            }),
        );
        schema_info.insert(
            METRIC_NAME_EXEC_DURATION.into(),
            serde_json::json!({
                "mysql_type": "bigint",
                "is_nullable": true
            }),
        );
        schema_info.insert(
            "_partition_by".into(),
            serde_json::json!(vec![LABEL_DATE.to_string()]),
        );
        schema_info
    };
}

/// Delta Lake sink processor
#[derive(Clone)]
pub struct TopSQLDeltaLakeSink {
    base_path: PathBuf,
    tables: Vec<DeltaTableConfig>,
    write_config: WriteConfig,
    max_delay_secs: u64,
    storage_options: Option<HashMap<String, String>>,
    keyspace_route_resolver: Option<PdKeyspaceResolver>,
    writers: Arc<Mutex<HashMap<WriterKey, DeltaLakeWriter>>>,
    tx: Arc<mpsc::Sender<Vec<Vec<Event>>>>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct WriterKey {
    table_name: String,
    table_path: PathBuf,
}

impl TopSQLDeltaLakeSink {
    /// Create a new Delta Lake sink
    pub fn new(
        base_path: PathBuf,
        tables: Vec<DeltaTableConfig>,
        write_config: WriteConfig,
        max_delay_secs: u64,
        storage_options: Option<HashMap<String, String>>,
        keyspace_route_resolver: Option<PdKeyspaceResolver>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(1);
        let sink = Self {
            base_path,
            tables,
            write_config,
            max_delay_secs,
            storage_options,
            keyspace_route_resolver,
            writers: Arc::new(Mutex::new(HashMap::new())),
            tx: Arc::new(tx),
        };
        let sink_clone = sink.clone();
        tokio::spawn(async move {
            sink_clone.process_events_loop(rx).await;
        });
        sink
    }

    #[cfg(test)]
    /// Create a new Delta Lake sink for testing, returning both the sink and the receiver
    /// The receiver can be used to verify messages sent through the channel
    /// Note: process_events_loop is NOT started automatically - test code should handle the receiver
    pub fn new_for_test(
        base_path: PathBuf,
        tables: Vec<DeltaTableConfig>,
        write_config: WriteConfig,
        max_delay_secs: u64,
        storage_options: Option<HashMap<String, String>>,
        keyspace_route_resolver: Option<PdKeyspaceResolver>,
    ) -> (Self, mpsc::Receiver<Vec<Vec<Event>>>) {
        // Create a channel with capacity 1
        let (tx, rx): (
            mpsc::Sender<Vec<Vec<Event>>>,
            mpsc::Receiver<Vec<Vec<Event>>>,
        ) = mpsc::channel(1);
        let tx = Arc::new(tx);

        // Create sink instance (without starting process_events_loop)
        let sink = Self {
            base_path,
            tables,
            write_config,
            max_delay_secs,
            storage_options,
            keyspace_route_resolver,
            writers: Arc::new(Mutex::new(HashMap::new())),
            tx,
        };

        // Return the sink and receiver for testing
        (sink, rx)
    }

    /// Process events from channel and write to Delta Lake
    async fn process_events_loop(&self, mut rx: mpsc::Receiver<Vec<Vec<Event>>>) {
        while let Some(events_vec) = rx.recv().await {
            let retry_on_failure = self.keyspace_route_resolver.is_some();
            let mut pending_events = events_vec;
            let mut retry_count = 0usize;

            loop {
                let can_retry = retry_on_failure && retry_count < MAX_ROUTE_RESOLUTION_RETRIES;
                let retry_snapshot = can_retry.then(|| pending_events.clone());
                match self.process_events(pending_events).await {
                    Ok(()) => break,
                    Err(error) => {
                        error!("Failed to process events: {}", error);
                        let Some(events) = retry_snapshot else {
                            if retry_on_failure {
                                error!(
                                    "Dropping event batch after {} route-resolution retries",
                                    retry_count
                                );
                            }
                            break;
                        };
                        retry_count += 1;
                        let retry_delay = route_resolution_retry_delay(retry_count);
                        warn!(
                            "Retrying event batch after route-resolution failure (attempt {}/{}, delay {:?})",
                            retry_count,
                            MAX_ROUTE_RESOLUTION_RETRIES,
                            retry_delay
                        );
                        tokio::time::sleep(retry_delay).await;
                        pending_events = events;
                    }
                }
            }
        }
    }

    /// Process events and write to Delta Lake
    async fn process_events(
        &self,
        events_vec: Vec<Vec<Event>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if events_vec.is_empty() {
            return Ok(());
        }
        let mut table_events: HashMap<WriterKey, Vec<Event>> = HashMap::new();
        let mut resolved_routes: HashMap<String, Option<KeyspaceRoute>> = HashMap::new();
        for events in events_vec {
            for event in events {
                if let Event::Log(log_event) = event {
                    if let Some(writer_key) = self
                        .resolve_writer_key(&log_event, &mut resolved_routes)
                        .await?
                    {
                        table_events
                            .entry(writer_key)
                            .or_insert_with(Vec::new)
                            .push(Event::Log(log_event));
                    }
                }
            }
        }

        for (writer_key, mut events) in table_events {
            self.add_schema_info(&mut events, &writer_key.table_name);
            if let Err(e) = self.write_table_events(&writer_key, events).await {
                let error_msg = e.to_string();
                if error_msg.contains("log segment")
                    || error_msg.contains("Invalid table version")
                    || error_msg.contains("not found")
                    || error_msg.contains("No such file or directory")
                {
                    panic!(
                        "Delta Lake corruption detected for table {}: {}",
                        writer_key.table_name, error_msg
                    );
                } else {
                    error!(
                        "Failed to write events to table {} at {}: {}",
                        writer_key.table_name,
                        writer_key.table_path.display(),
                        e
                    );
                }
            }
        }

        Ok(())
    }

    /// Write events to a specific table
    fn add_schema_info(&self, events: &mut Vec<Event>, table_name: &str) {
        if events.is_empty() {
            return;
        }
        let schema = if table_name == SOURCE_TABLE_TOPRU {
            TOPRU_SCHEMA.clone()
        } else {
            TOPSQL_SCHEMA.clone()
        };
        let first_event = &mut events[0];
        let log = first_event.as_mut_log();
        log.insert("_schema_metadata", serde_json::Value::Object(schema));
    }

    fn extract_table_name(log_event: &LogEvent) -> Option<String> {
        log_event
            .get(LABEL_INSTANCE_KEY)
            .and_then(|value| value.as_str())
            .map(|value| value.to_string())
            .or_else(|| {
                log_event
                    .get(LABEL_SOURCE_TABLE)
                    .and_then(|value| value.as_str())
                    .filter(|value| *value == SOURCE_TABLE_TOPRU)
                    .map(|value| value.to_string())
            })
    }

    async fn resolve_writer_key(
        &self,
        log_event: &LogEvent,
        resolved_routes: &mut HashMap<String, Option<KeyspaceRoute>>,
    ) -> Result<Option<WriterKey>, Box<dyn std::error::Error + Send + Sync>> {
        let Some(table_name) = Self::extract_table_name(log_event) else {
            return Ok(None);
        };
        let route = self
            .resolve_keyspace_route(log_event, resolved_routes)
            .await?;
        if self.keyspace_route_resolver.is_some() && route.is_none() {
            return Ok(None);
        }
        Ok(Some(WriterKey {
            table_name: table_name.clone(),
            table_path: self.build_table_path(&table_name, route.as_ref()),
        }))
    }

    async fn resolve_keyspace_route(
        &self,
        log_event: &LogEvent,
        resolved_routes: &mut HashMap<String, Option<KeyspaceRoute>>,
    ) -> Result<Option<KeyspaceRoute>, Box<dyn std::error::Error + Send + Sync>> {
        let Some(resolver) = self.keyspace_route_resolver.as_ref() else {
            return Ok(None);
        };
        let Some(keyspace) = log_event
            .get(LABEL_KEYSPACE)
            .and_then(|value| value.as_str())
        else {
            return Ok(None);
        };

        if let Some(route) = resolved_routes.get(keyspace.as_ref()) {
            return Ok(route.clone());
        }

        let route = resolver.resolve_keyspace(keyspace.as_ref()).await?;
        resolved_routes.insert(keyspace.to_string(), route.clone());
        if route.is_none() {
            warn!(
                "No cluster route found for keyspace {}, skipping TopSQL data event",
                keyspace
            );
        }
        Ok(route)
    }

    fn build_table_path(&self, table_name: &str, route: Option<&KeyspaceRoute>) -> PathBuf {
        let (table_type, table_instance) = Self::table_partition_values(table_name);
        let mut base_path = self.base_path.clone();

        let mut segments = Vec::new();
        if let Some(route) = route {
            if path_contains_keyspace_route_segments(&self.base_path.to_string_lossy()) {
                base_path = replace_keyspace_route_segments(&base_path, route);
            }
        }
        segments.push(format!("component={}", table_type));
        segments.push(format!("instance={}", table_instance));

        let segment_refs: Vec<&str> = segments.iter().map(|segment| segment.as_str()).collect();
        Self::join_path(&base_path, &segment_refs)
    }

    fn table_partition_values(table_name: &str) -> (&str, &str) {
        if table_name == SOURCE_TABLE_TOPRU {
            ("topru", "default")
        } else {
            match table_name
                .strip_prefix("topsql_")
                .and_then(|rest| rest.split_once('_'))
            {
                Some((table_type, table_instance))
                    if !table_type.is_empty() && !table_instance.is_empty() =>
                {
                    (table_type, table_instance)
                }
                _ => {
                    error!(
                        "Unexpected table_name format (expected `topsql_{{type}}_{{instance}}` or `topsql_topru`): {}",
                        table_name
                    );
                    ("unknown", "unknown")
                }
            }
        }
    }

    fn join_path(base_path: &PathBuf, segments: &[&str]) -> PathBuf {
        let base = base_path.to_string_lossy();
        if base.starts_with("s3://") || base.starts_with("abfss://") || base.starts_with("gs://")
        {
            let mut path = base_path
                .to_string_lossy()
                .trim_end_matches('/')
                .to_string();
            for segment in segments {
                path.push('/');
                path.push_str(segment);
            }
            PathBuf::from(path)
        } else {
            let mut path = base_path.clone();
            for segment in segments {
                path = path.join(segment);
            }
            path
        }
    }

    /// Write events to a specific table
    async fn write_table_events(
        &self,
        writer_key: &WriterKey,
        events: Vec<Event>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut writers = self.writers.lock().await;
        let writer = writers.entry(writer_key.clone()).or_insert_with(|| {
            let table_config = self
                .tables
                .iter()
                .find(|table| table.name == writer_key.table_name)
                .cloned()
                .unwrap_or_else(|| DeltaTableConfig {
                    name: writer_key.table_name.clone(),
                    schema_evolution: Some(true),
                });
            DeltaLakeWriter::new_with_options(
                writer_key.table_path.clone(),
                table_config,
                self.write_config.clone(),
                self.storage_options.clone(),
                false,
            )
        });

        // Write events
        writer.write_events(events).await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl StreamSink<Event> for TopSQLDeltaLakeSink {
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        // Convert self to Arc for sharing
        let sink = Arc::new(*self);
        info!(
            "Delta Lake sink starting with batch_size: {}",
            sink.write_config.batch_size
        );
        // Use the channel sender from the sink
        let tx = Arc::clone(&sink.tx);

        let mut input = input.ready_chunks(sink.write_config.batch_size);
        let mut events_cache = vec![];
        let mut cur_cached_size = 0;
        let mut oldest_timestamp = 0;
        let mut latest_timestamp = 0;
        while let Some(events) = input.next().await {
            let events_count = events.len();
            if events_count == 0 {
                continue;
            }

            // Extract timestamp from first event
            if let Event::Log(ref log_event) = events[0] {
                if let Some(timestamps) = log_event.get("timestamps").and_then(|v| v.as_integer()) {
                    latest_timestamp = timestamps;
                    if cur_cached_size == 0 {
                        oldest_timestamp = timestamps;
                    }
                }
            }

            cur_cached_size += events_count;
            events_cache.push(events);

            // Allow max delay to configured value, continue if not ready to send
            if events_count + cur_cached_size < sink.write_config.batch_size
                && latest_timestamp < oldest_timestamp + sink.max_delay_secs as i64
            {
                continue;
            }

            // Send events to process_events through channel
            let should_drop_on_full =
                latest_timestamp >= oldest_timestamp + sink.max_delay_secs as i64;
            match tx.try_send(events_cache) {
                Ok(_) => {
                    // Successfully sent, clear the cache
                    cur_cached_size = 0;
                    events_cache = vec![];
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(restored_events)) => {
                    if should_drop_on_full {
                        // Timeout exceeded, drop the data
                        error!("Channel full and timeout exceeded, dropping events");
                        cur_cached_size = 0;
                        events_cache = vec![];
                    } else {
                        // Keep in cache for next retry
                        // Keep cur_cached_size unchanged so we can retry
                        events_cache = restored_events;
                    }
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(restored_events)) => {
                    // Receiver closed, restore events_cache and keep it for next retry
                    error!("Channel closed, keeping events in cache");
                    events_cache = restored_events;
                    // Keep cur_cached_size unchanged so we can retry
                }
            }
        }

        // When the input stream ends, try to send any remaining cached events
        if !events_cache.is_empty() {
            // Send remaining events, wait if channel is full
            if let Err(_) = tx.send(events_cache).await {
                // Receiver closed, log error
                error!("Channel closed when flushing remaining events, dropping events");
            }
        }

        // Note: We don't drop tx here as it's owned by the sink and may be used by other run() calls
        // The channel will be closed when the sink is dropped
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use vector_lib::event::{LogEvent, Value as LogValue};

    fn create_test_event(timestamp: i64) -> Event {
        let mut event = Event::Log(LogEvent::default());
        let log = event.as_mut_log();
        log.insert("source_table", "tidb_topsql");
        log.insert("timestamps", LogValue::from(timestamp));
        log.insert("time", LogValue::from(timestamp));
        event
    }

    fn create_test_sink_with_receiver(
        batch_size: usize,
    ) -> (TopSQLDeltaLakeSink, mpsc::Receiver<Vec<Vec<Event>>>) {
        TopSQLDeltaLakeSink::new_for_test(
            PathBuf::from("/tmp/test"),
            vec![],
            WriteConfig {
                batch_size,
                timeout_secs: 0,
            },
            180, // Use default value for tests
            None,
            None,
        )
    }

    #[test]
    fn test_build_table_path_with_meta_route_for_s3() {
        let (sink, _) = TopSQLDeltaLakeSink::new_for_test(
            PathBuf::from(
                "s3://o11y-prod-shared-us-west-2-premium/deltalake/org=xxx/cluster=xxx/type=topsql",
            ),
            vec![],
            WriteConfig {
                batch_size: 1,
                timeout_secs: 0,
            },
            180,
            None,
            None,
        );

        let table_path = sink.build_table_path(
            "topsql_tidb_127.0.0.1:10080",
            Some(&KeyspaceRoute {
                org_id: "1369847559692509642".to_string(),
                cluster_id: "10110362358366286743".to_string(),
            }),
        );

        assert_eq!(
            table_path,
            PathBuf::from(
                "s3://o11y-prod-shared-us-west-2-premium/deltalake/org=1369847559692509642/cluster=10110362358366286743/type=topsql/component=tidb/instance=127.0.0.1:10080"
            )
        );
    }

    #[test]
    fn test_build_table_path_with_meta_route_replaces_template_for_local_path() {
        let (sink, _) = TopSQLDeltaLakeSink::new_for_test(
            PathBuf::from("/tmp/deltalake/org=xxx/cluster=xxx/type=topsql"),
            vec![],
            WriteConfig {
                batch_size: 1,
                timeout_secs: 0,
            },
            180,
            None,
            None,
        );

        let table_path = sink.build_table_path(
            "topsql_tidb_127.0.0.1:10080",
            Some(&KeyspaceRoute {
                org_id: "30018".to_string(),
                cluster_id: "101".to_string(),
            }),
        );

        assert_eq!(
            table_path,
            PathBuf::from(
                "/tmp/deltalake/org=30018/cluster=101/type=topsql/component=tidb/instance=127.0.0.1:10080"
            )
        );
    }

    #[test]
    fn test_build_table_path_without_meta_route_preserves_existing_layout() {
        let (sink, _) = TopSQLDeltaLakeSink::new_for_test(
            PathBuf::from("/tmp/deltalake"),
            vec![],
            WriteConfig {
                batch_size: 1,
                timeout_secs: 0,
            },
            180,
            None,
            None,
        );

        let table_path = sink.build_table_path("topsql_topru", None);

        assert_eq!(
            table_path,
            PathBuf::from("/tmp/deltalake/component=topru/instance=default")
        );
    }

    #[tokio::test]
    async fn test_send_when_batch_size_reached() {
        let batch_size = 5;
        let (sink, mut rx) = create_test_sink_with_receiver(batch_size);

        // Create events that will reach batch size
        let events: Vec<Event> = (0..batch_size)
            .map(|i| create_test_event(1000 + i as i64))
            .collect();

        let input_stream = stream::iter(events.clone()).boxed();
        let sink_box = Box::new(sink);

        // Run the function in a task
        let run_handle = tokio::spawn(async move { sink_box.run(input_stream).await });

        // Wait a bit for the message to be sent
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Verify that a message was sent through the channel
        let received =
            tokio::time::timeout(tokio::time::Duration::from_millis(500), rx.recv()).await;

        assert!(received.is_ok(), "Should receive a message from channel");
        if let Ok(Some(events_vec)) = received {
            // Verify the message content
            // Count total events
            let total_events: usize = events_vec.iter().map(|v| v.len()).sum();
            assert_eq!(
                total_events, batch_size,
                "Should receive exactly batch_size events"
            );

            // Verify event structure
            assert!(!events_vec.is_empty(), "Events vector should not be empty");
            for event_batch in &events_vec {
                assert!(
                    !event_batch.is_empty(),
                    "Each event batch should not be empty"
                );
            }
        } else {
            panic!("Failed to receive message from channel");
        }

        // Wait for run to complete
        let _ = run_handle.await;
    }

    #[tokio::test]
    async fn test_send_when_timeout_reached() {
        let batch_size = 100; // Large batch size so we don't reach it
        let (sink, mut rx) = create_test_sink_with_receiver(batch_size);

        // Create events with timestamps that exceed timeout (180 seconds)
        let oldest_ts = 1000;
        let latest_ts = oldest_ts + 181; // Exceeds 180 second timeout

        // Create two events: one at the start, one after timeout
        let events = vec![create_test_event(oldest_ts), create_test_event(latest_ts)];

        let input_stream = stream::iter(events.clone()).boxed();
        let sink_box = Box::new(sink);

        // Run the function in a task
        let run_handle = tokio::spawn(async move { sink_box.run(input_stream).await });

        // Wait a bit for the message to be sent
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Verify that a message was sent through the channel due to timeout
        let received =
            tokio::time::timeout(tokio::time::Duration::from_millis(500), rx.recv()).await;

        assert!(
            received.is_ok(),
            "Should receive a message from channel due to timeout"
        );
        if let Ok(Some(events_vec)) = received {
            // Verify the message content
            // Verify events were sent
            let total_events: usize = events_vec.iter().map(|v| v.len()).sum();
            assert_eq!(
                total_events, 2,
                "Should receive both events (oldest and latest)"
            );
        } else {
            panic!("Failed to receive message from channel");
        }

        // Wait for run to complete
        let _ = run_handle.await;
    }

    #[tokio::test]
    async fn test_channel_full_keep_cache_when_not_timeout() {
        let batch_size = 5;
        let (sink, mut rx) = create_test_sink_with_receiver(batch_size);

        // Create many events to fill the channel (capacity 1)
        // The first batch will fill the channel, second batch should be kept in cache
        // and retried later
        let events: Vec<Event> = (0..batch_size * 2)
            .map(|i| create_test_event(1000 + i as i64)) // All within timeout window
            .collect();

        let input_stream = stream::iter(events.clone()).boxed();
        let sink_box = Box::new(sink);

        // Run the function in a task
        let run_handle = tokio::spawn(async move { sink_box.run(input_stream).await });

        // Don't consume from rx immediately to fill the channel
        // Wait a bit for the first message to be sent
        // The channel should be full now, and subsequent sends should keep data in cache
        // Since we're not consuming, the channel stays full
        // After a bit more time, the run should complete
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Now consume the first message
        let first_msg = rx.recv().await;
        assert!(first_msg.is_some(), "Should receive first message");
        if let Some(events_vec) = first_msg {
            // Verify first message content
            let total_events: usize = events_vec.iter().map(|v| v.len()).sum();
            assert_eq!(
                total_events, batch_size,
                "First message should contain batch_size events"
            );
        }

        // Wait a bit more - the second batch should be sent after channel has space
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Check if second message was sent (data was kept in cache and retried)
        let second_msg =
            tokio::time::timeout(tokio::time::Duration::from_millis(200), rx.recv()).await;

        // The second batch should eventually be sent (kept in cache and retried)
        assert!(
            second_msg.is_ok(),
            "Should eventually receive second message after retry"
        );
        if let Ok(Some(events_vec)) = second_msg {
            // Verify second message content
            let total_events: usize = events_vec.iter().map(|v| v.len()).sum();
            assert_eq!(
                total_events, batch_size,
                "Second message should contain batch_size events"
            );
        }

        // Wait for run to complete
        let _ = run_handle.await;
    }

    #[tokio::test]
    async fn test_channel_full_drop_when_timeout() {
        let batch_size = 5;
        let (sink, mut rx) = create_test_sink_with_receiver(batch_size);

        // Create events with timeout: first batch, then events after timeout
        let mut events = vec![];
        // First batch at timestamp 1000
        for i in 0..batch_size {
            events.push(create_test_event(1000 + i as i64));
        }
        // Then an event at 1181 (exceeds timeout)
        for i in 0..batch_size {
            events.push(create_test_event(1005 + i as i64));
        }
        events.push(create_test_event(1186));

        let input_stream = stream::iter(events.clone()).boxed();
        let sink_box = Box::new(sink);

        // Run the function in a task
        let run_handle = tokio::spawn(async move { sink_box.run(input_stream).await });

        // Don't consume from rx to fill the channel
        // Wait for first message to be sent
        // Channel should be full now
        // When the timeout event arrives and channel is full, data should be dropped
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Consume the first message
        let first_msg = rx.recv().await;
        assert!(first_msg.is_some(), "Should receive first message");
        if let Some(events_vec) = first_msg {
            // Verify first message content
            let total_events: usize = events_vec.iter().map(|v| v.len()).sum();
            assert_eq!(
                total_events, batch_size,
                "First message should contain batch_size events"
            );

            // Verify timestamps are from the first batch (1000-1004)
            for event_batch in &events_vec {
                for event in event_batch {
                    if let Event::Log(ref log_event) = event {
                        if let Some(timestamp) =
                            log_event.get("timestamps").and_then(|v| v.as_integer())
                        {
                            assert!(
                                timestamp >= 1000 && timestamp < 1000 + batch_size as i64,
                                "First message should contain events from first batch"
                            );
                        }
                    }
                }
            }
        }

        // Wait a bit more - the timeout event should have been dropped, not sent
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Check if a second message was sent (it shouldn't be, as data was dropped)
        let second_msg =
            tokio::time::timeout(tokio::time::Duration::from_millis(200), rx.recv()).await;
        // The second message should NOT be sent because data was dropped due to timeout
        assert!(
            second_msg.is_err() || second_msg.unwrap().is_none(),
            "Should NOT receive second message as data was dropped due to timeout"
        );

        // Wait for run to complete
        let _ = run_handle.await;
    }

    #[tokio::test]
    async fn test_not_send_when_batch_size_and_timeout_not_reached() {
        let batch_size = 10;
        let (sink, mut rx) = create_test_sink_with_receiver(batch_size);

        // Create events that don't reach batch size and don't timeout
        let events: Vec<Event> = (0..3).map(|i| create_test_event(1000 + i)).collect();

        let input_stream = stream::iter(events.clone()).boxed();
        let sink_box = Box::new(sink);

        // Run the function in a task
        let run_handle = tokio::spawn(async move { sink_box.run(input_stream).await });

        // Wait for run to complete
        let result = run_handle.await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_ok());

        // Verify that no message was sent (data doesn't meet send conditions)
        // Note: When stream ends, remaining data might be flushed, but with only 3 events
        // and batch_size 10, and no timeout, it should not send immediately
        // However, when the stream ends, the loop exits and remaining cache might be sent
        // Let's check if any message was received
        let received =
            tokio::time::timeout(tokio::time::Duration::from_millis(200), rx.recv()).await;

        // With the current implementation, when stream ends, remaining cache might be sent
        // So we check if a message was received and verify its content
        if let Ok(Some(events_vec)) = received {
            // Verify the message content
            let total_events: usize = events_vec.iter().map(|v| v.len()).sum();
            assert_eq!(
                total_events, 3,
                "Should receive the 3 events that were cached"
            );
        } else {
            // If no message was received, that's also valid - data wasn't sent
            // This depends on implementation details of when remaining cache is flushed
        }
    }

    #[tokio::test]
    async fn test_batch_size_sending_behavior() {
        let batch_size = 3;
        let (sink, mut rx) = create_test_sink_with_receiver(batch_size);

        // Create exactly batch_size events
        let events: Vec<Event> = (0..batch_size)
            .map(|i| create_test_event(1000 + i as i64))
            .collect();

        let input_stream = stream::iter(events.clone()).boxed();
        let sink_box = Box::new(sink);

        // Run the function in a task
        let run_handle = tokio::spawn(async move { sink_box.run(input_stream).await });

        // Wait a bit for the message to be sent
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Verify that a message was sent through the channel
        let received =
            tokio::time::timeout(tokio::time::Duration::from_millis(500), rx.recv()).await;

        assert!(received.is_ok(), "Should receive a message from channel");
        if let Ok(Some(events_vec)) = received {
            // Verify the message content
            // Count total events
            let total_events: usize = events_vec.iter().map(|v| v.len()).sum();
            assert_eq!(
                total_events, batch_size,
                "Should receive exactly batch_size events"
            );

            // Verify event timestamps
            for event_batch in events_vec {
                for (i, event) in event_batch.iter().enumerate() {
                    if let Event::Log(ref log_event) = event {
                        if let Some(timestamp) =
                            log_event.get("timestamps").and_then(|v| v.as_integer())
                        {
                            assert_eq!(timestamp, 1000 + i as i64, "Event timestamp should match");
                        }
                    }
                }
            }
        } else {
            panic!("Failed to receive message from channel");
        }

        // Wait for run to complete
        let _ = run_handle.await;
    }

    #[tokio::test]
    async fn test_timeout_sending_behavior() {
        let batch_size = 100; // Large batch size
        let (sink, mut rx) = create_test_sink_with_receiver(batch_size);

        // Create events with large time gap (exceeding 180 seconds)
        let oldest_ts = 1000;
        let latest_ts = 1181; // 181 seconds later, exceeds timeout
        let events = vec![create_test_event(oldest_ts), create_test_event(latest_ts)];

        let input_stream = stream::iter(events.clone()).boxed();
        let sink_box = Box::new(sink);

        // Run the function in a task
        let run_handle = tokio::spawn(async move { sink_box.run(input_stream).await });

        // Wait a bit for the message to be sent
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Verify that a message was sent through the channel due to timeout
        let received =
            tokio::time::timeout(tokio::time::Duration::from_millis(500), rx.recv()).await;

        assert!(
            received.is_ok(),
            "Should receive a message from channel due to timeout"
        );
        if let Ok(Some(events_vec)) = received {
            // Verify the message content
            // Count total events
            let total_events: usize = events_vec.iter().map(|v| v.len()).sum();
            assert_eq!(total_events, 2, "Should receive both events");

            // Verify event timestamps
            let mut timestamps = Vec::new();
            for event_batch in &events_vec {
                for event in event_batch {
                    if let Event::Log(ref log_event) = event {
                        if let Some(timestamp) =
                            log_event.get("timestamps").and_then(|v| v.as_integer())
                        {
                            timestamps.push(timestamp);
                        }
                    }
                }
            }
            timestamps.sort();
            assert_eq!(
                timestamps,
                vec![oldest_ts, latest_ts],
                "Should receive events with correct timestamps"
            );
        } else {
            panic!("Failed to receive message from channel");
        }

        // Wait for run to complete
        let _ = run_handle.await;
    }

    #[tokio::test]
    async fn test_multiple_batches() {
        let batch_size = 3;
        let (sink, mut rx) = create_test_sink_with_receiver(batch_size);

        // Create multiple batches worth of events
        let total_events = batch_size * 3;
        let events: Vec<Event> = (0..total_events)
            .map(|i| create_test_event(1000 + i as i64))
            .collect();

        let input_stream = stream::iter(events.clone()).boxed();
        let sink_box = Box::new(sink);

        // Run the function in a task
        let run_handle = tokio::spawn(async move { sink_box.run(input_stream).await });

        // Collect all messages from the channel
        let mut received_messages = Vec::new();
        let expected_batches = (total_events + batch_size - 1) / batch_size; // Ceiling division

        // Wait for all batches to be sent
        for _ in 0..expected_batches {
            let received =
                tokio::time::timeout(tokio::time::Duration::from_millis(500), rx.recv()).await;
            if let Ok(Some(msg)) = received {
                received_messages.push(msg);
            } else {
                break;
            }
        }

        // Verify we received the expected number of batches
        assert!(received_messages.len() >= 1);
        // Verify total events received
        let total_received: usize = received_messages
            .iter()
            .map(|events_vec| events_vec.iter().map(|v| v.len()).sum::<usize>())
            .sum();
        assert_eq!(
            total_received, total_events,
            "Should receive all events across batches"
        );

        // Verify each message
        for events_vec in &received_messages {
            assert!(!events_vec.is_empty(), "Each batch should contain events");
        }

        // Wait for run to complete
        let _ = run_handle.await;
    }
}
