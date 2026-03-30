use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{stream::BoxStream, StreamExt};
use lru::LruCache;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use vector_lib::event::{Event, LogEvent};
use vector_lib::sink::StreamSink;

use crate::common::deltalake_writer::{DeltaLakeWriter, DeltaTableConfig, WriteConfig};
use crate::common::keyspace_cluster::{
    path_contains_keyspace_route_segments, replace_keyspace_route_segments,
};
use crate::common::keyspace_cluster::{KeyspaceRoute, PdKeyspaceResolver};
use crate::sources::topsql_v2::upstream::consts::{
    LABEL_DATE, LABEL_ENCODED_NORMALIZED_PLAN, LABEL_KEYSPACE, LABEL_NORMALIZED_PLAN,
    LABEL_NORMALIZED_SQL, LABEL_PLAN_DIGEST, LABEL_SOURCE_TABLE, LABEL_SQL_DIGEST,
    SOURCE_TABLE_TOPSQL_PLAN_META, SOURCE_TABLE_TOPSQL_SQL_META,
};

use lazy_static::lazy_static;
lazy_static! {
    static ref SQL_META_SCHEMA: serde_json::Map<String, serde_json::Value> = {
        let mut schema_info = serde_json::Map::new();
        schema_info.insert(
            LABEL_DATE.into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": false
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
            LABEL_KEYSPACE.into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": true
            }),
        );
        schema_info.insert(
            LABEL_NORMALIZED_SQL.into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": true
            }),
        );
        // partition key
        schema_info.insert(
            "_partition_by".into(),
            serde_json::json!(vec![LABEL_DATE.to_string()]),
        );
        schema_info
    };
    static ref PLAN_META_SCHEMA: serde_json::Map<String, serde_json::Value> = {
        let mut schema_info = serde_json::Map::new();
        schema_info.insert(
            LABEL_DATE.into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": false
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
            LABEL_KEYSPACE.into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": true
            }),
        );
        schema_info.insert(
            LABEL_NORMALIZED_PLAN.into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": true
            }),
        );
        schema_info.insert(
            LABEL_ENCODED_NORMALIZED_PLAN.into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": true
            }),
        );
        // partition key
        schema_info.insert(
            "_partition_by".into(),
            serde_json::json!(vec![LABEL_DATE.to_string()]),
        );
        schema_info
    };
}

/// When buffer size exceeds this value, events will be flushed
const EVENT_BUFFER_MAX_SIZE: usize = 1000;
const ROUTE_RESOLUTION_RETRY_DELAY: Duration = Duration::from_secs(5);
const MAX_ROUTE_RESOLUTION_RETRIES: usize = 5;
const MAX_ROUTE_RESOLUTION_RETRY_DELAY: Duration = Duration::from_secs(60);

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
    // LRU cache for SQL meta deduplication: key -> ()
    seen_keys_sql_meta: Arc<Mutex<LruCache<String, ()>>>,
    // LRU cache for PLAN meta deduplication: key -> ()
    seen_keys_plan_meta: Arc<Mutex<LruCache<String, ()>>>,
    // Buffer for events to be flushed
    new_event_buffer: Arc<Mutex<Vec<Event>>>,
    // Dedup keys pending commit — written to LRU only after a successful flush.
    // Each entry is (source_table, dedup_key) parallel to new_event_buffer.
    pending_dedup_keys: Arc<Mutex<Vec<(String, String)>>>,
    // Last flush time
    last_flush_time: Arc<Mutex<Instant>>,
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
        meta_cache_capacity: usize,
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
            seen_keys_sql_meta: Arc::new(Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(meta_cache_capacity).unwrap(),
            ))), // LRU cache with configurable capacity
            seen_keys_plan_meta: Arc::new(Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(meta_cache_capacity).unwrap(),
            ))), // LRU cache with configurable capacity
            new_event_buffer: Arc::new(Mutex::new(Vec::new())),
            pending_dedup_keys: Arc::new(Mutex::new(Vec::new())),
            last_flush_time: Arc::new(Mutex::new(Instant::now())),
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
        meta_cache_capacity: usize,
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
            seen_keys_sql_meta: Arc::new(Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(meta_cache_capacity).unwrap(),
            ))), // LRU cache with configurable capacity
            seen_keys_plan_meta: Arc::new(Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(meta_cache_capacity).unwrap(),
            ))), // LRU cache with configurable capacity
            new_event_buffer: Arc::new(Mutex::new(Vec::new())),
            pending_dedup_keys: Arc::new(Mutex::new(Vec::new())),
            last_flush_time: Arc::new(Mutex::new(Instant::now())),
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
                        // Clear stale state so the retry snapshot is processed
                        // from scratch — pending keys were never committed to
                        // LRU, and the buffer was already drained by flush.
                        self.pending_dedup_keys.lock().await.clear();
                        self.new_event_buffer.lock().await.clear();
                        let Some(events) = retry_snapshot else {
                            if retry_on_failure {
                                error!(
                                    "Dropping meta event batch after {} route-resolution retries",
                                    retry_count
                                );
                            }
                            break;
                        };
                        retry_count += 1;
                        let retry_delay = route_resolution_retry_delay(retry_count);
                        warn!(
                            "Retrying meta event batch after route-resolution failure (attempt {}/{}, delay {:?})",
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

    /// Extract deduplication key from event
    /// Returns (table_name, key) if key can be extracted, None otherwise
    /// table_name is the value of LABEL_SOURCE_TABLE (e.g., SOURCE_TABLE_TOPSQL_SQL_META or SOURCE_TABLE_TOPSQL_PLAN_META)
    /// key format: digest_date (e.g., sql_digest_2024-01-01)
    fn extract_event_key(
        &self,
        log_event: &vector_lib::event::LogEvent,
    ) -> Option<(String, String)> {
        // Get table_name from source_table
        let table_name = log_event
            .get(LABEL_SOURCE_TABLE)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())?;

        // Get date from log_event
        let date = log_event
            .get(LABEL_DATE)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())?;

        let keyspace = log_event
            .get(LABEL_KEYSPACE)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        // Extract key based on source_table type
        if table_name == SOURCE_TABLE_TOPSQL_SQL_META {
            // For SQL meta: use sql_digest_date format
            if let Some(sql_digest) = log_event
                .get(LABEL_SQL_DIGEST)
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
            {
                let key = match keyspace.as_deref() {
                    Some(keyspace) => format!("{}_{}_{}", keyspace, sql_digest, date),
                    None => format!("{}_{}", sql_digest, date),
                };
                return Some((table_name, key));
            }
        } else if table_name == SOURCE_TABLE_TOPSQL_PLAN_META {
            // For PLAN meta: use plan_digest_date format
            if let Some(plan_digest) = log_event
                .get(LABEL_PLAN_DIGEST)
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
            {
                let key = match keyspace.as_deref() {
                    Some(keyspace) => format!("{}_{}_{}", keyspace, plan_digest, date),
                    None => format!("{}_{}", plan_digest, date),
                };
                return Some((table_name, key));
            }
        }

        // If no key found or source_table doesn't match, return None (event will be skipped)
        None
    }

    /// Flush buffer to Delta Lake
    async fn flush_buffer(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut buffer = self.new_event_buffer.lock().await;
        let mut pending_keys = self.pending_dedup_keys.lock().await;
        if buffer.is_empty() {
            pending_keys.clear();
            return Ok(());
        }

        let drained_events: Vec<Event> = buffer.drain(..).collect();
        let drained_pending_keys: Vec<(String, String)> = pending_keys.drain(..).collect();
        drop(buffer);
        drop(pending_keys);

        if drained_events.len() != drained_pending_keys.len() {
            return Err(format!(
                "mismatched buffered events ({}) and pending dedup keys ({})",
                drained_events.len(),
                drained_pending_keys.len()
            )
            .into());
        }

        // Group events by writer target so each org/cluster route gets its own table.
        let mut table_events: HashMap<WriterKey, Vec<Event>> = HashMap::new();
        let mut table_dedup_keys: HashMap<WriterKey, Vec<(String, String)>> = HashMap::new();
        let mut resolved_routes: HashMap<String, Option<KeyspaceRoute>> = HashMap::new();

        for (event, dedup_key) in drained_events
            .into_iter()
            .zip(drained_pending_keys.into_iter())
        {
            if let Event::Log(log_event) = event {
                if let Some(writer_key) = self
                    .resolve_writer_key(&log_event, &mut resolved_routes)
                    .await?
                {
                    table_events
                        .entry(writer_key.clone())
                        .or_insert_with(Vec::new)
                        .push(Event::Log(log_event));
                    table_dedup_keys
                        .entry(writer_key)
                        .or_insert_with(Vec::new)
                        .push(dedup_key);
                }
            }
        }

        let mut committed_dedup_keys = Vec::new();

        // Write table's events
        for (writer_key, mut events) in table_events {
            self.add_schema_info(&writer_key.table_name, &mut events);
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
            } else if let Some(keys) = table_dedup_keys.remove(&writer_key) {
                committed_dedup_keys.extend(keys);
            }
        }

        // Commit dedup keys only for writes that actually succeeded.
        self.commit_dedup_keys(committed_dedup_keys).await;

        // Update last flush time
        *self.last_flush_time.lock().await = Instant::now();

        Ok(())
    }

    /// Process events and write to Delta Lake
    async fn process_events(
        &self,
        events_vec: Vec<Vec<Event>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if events_vec.is_empty() {
            return Ok(());
        }

        let seen_keys_sql_meta = self.seen_keys_sql_meta.lock().await;
        let seen_keys_plan_meta = self.seen_keys_plan_meta.lock().await;
        let mut buffer = self.new_event_buffer.lock().await;
        let mut pending_keys = self.pending_dedup_keys.lock().await;
        let last_flush = *self.last_flush_time.lock().await;
        let current_time = Instant::now();
        let flush_interval = Duration::from_secs(self.max_delay_secs);

        // Process all events
        for events in events_vec {
            for event in events {
                if let Event::Log(log_event) = event {
                    // Extract key from event
                    if let Some((table_name, key)) = self.extract_event_key(&log_event) {
                        // Select the appropriate LRU cache based on table_name (source_table)
                        let seen_keys = match table_name.as_str() {
                            SOURCE_TABLE_TOPSQL_SQL_META => &*seen_keys_sql_meta,
                            SOURCE_TABLE_TOPSQL_PLAN_META => &*seen_keys_plan_meta,
                            _ => continue, // Skip unknown event types
                        };

                        // Check if key is already committed in LRU cache
                        if seen_keys.peek(&key).is_some() {
                            continue;
                        }

                        // Check if key is already pending in this unflushed batch
                        if pending_keys
                            .iter()
                            .any(|(t, k)| t == &table_name && k == &key)
                        {
                            continue;
                        }

                        // Stage the key — it will be committed to LRU only after
                        // flush_buffer succeeds, so a retry can re-process the
                        // same events without them being silently dropped.
                        pending_keys.push((table_name, key));
                        buffer.push(Event::Log(log_event));
                    }
                    // If key cannot be extracted, skip the event
                }
            }
        }

        // Release locks before checking flush conditions
        drop(seen_keys_sql_meta);
        drop(seen_keys_plan_meta);

        // Check if buffer is full or time interval reached
        let buffer_full = buffer.len() >= EVENT_BUFFER_MAX_SIZE;
        let time_reached = current_time >= last_flush + flush_interval;

        if buffer_full || time_reached {
            // Release buffer lock before flushing
            drop(pending_keys);
            drop(buffer);

            // Flush buffer to deltalake
            self.flush_buffer().await?;
        }

        Ok(())
    }

    async fn resolve_writer_key(
        &self,
        log_event: &LogEvent,
        resolved_routes: &mut HashMap<String, Option<KeyspaceRoute>>,
    ) -> Result<Option<WriterKey>, Box<dyn std::error::Error + Send + Sync>> {
        let Some(table_name) = log_event
            .get(LABEL_SOURCE_TABLE)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
        else {
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
                "No cluster route found for keyspace {}, skipping TopSQL meta event",
                keyspace
            );
        }
        Ok(route)
    }

    fn build_table_path(&self, table_name: &str, route: Option<&KeyspaceRoute>) -> PathBuf {
        let mut base_path = self.base_path.clone();
        let mut segments = Vec::new();
        if let Some(route) = route {
            if path_contains_keyspace_route_segments(&self.base_path.to_string_lossy()) {
                base_path = replace_keyspace_route_segments(&base_path, route);
            }
        }
        segments.push(format!("component={}", table_name));

        let segment_refs: Vec<&str> = segments.iter().map(|segment| segment.as_str()).collect();
        Self::join_path(&base_path, &segment_refs)
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
    fn add_schema_info(&self, table_name: &str, events: &mut Vec<Event>) {
        if events.is_empty() {
            return;
        }
        let first_event = &mut events[0];
        let log = first_event.as_mut_log();

        // Select schema based on table_name (which is actually source_table)
        let schema = match table_name {
            SOURCE_TABLE_TOPSQL_SQL_META => &*SQL_META_SCHEMA,
            SOURCE_TABLE_TOPSQL_PLAN_META => &*PLAN_META_SCHEMA,
            _ => {
                error!("Unknown table_name in add_schema_info: {}", table_name);
                return; // Return early if table_name doesn't match any known type
            }
        };

        log.insert(
            "_schema_metadata",
            serde_json::Value::Object(schema.clone()),
        );
    }

    /// Write events to a specific table
    async fn write_table_events(
        &self,
        writer_key: &WriterKey,
        events: Vec<Event>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Get or create writer for this table
        let mut writers = self.writers.lock().await;
        let writer = writers.entry(writer_key.clone()).or_insert_with(|| {
            let table_config = self
                .tables
                .iter()
                .find(|t| t.name == writer_key.table_name)
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

    async fn commit_dedup_keys(&self, dedup_keys: Vec<(String, String)>) {
        if dedup_keys.is_empty() {
            return;
        }

        let mut seen_sql = self.seen_keys_sql_meta.lock().await;
        let mut seen_plan = self.seen_keys_plan_meta.lock().await;
        for (table_name, key) in dedup_keys {
            match table_name.as_str() {
                SOURCE_TABLE_TOPSQL_SQL_META => {
                    seen_sql.put(key, ());
                }
                SOURCE_TABLE_TOPSQL_PLAN_META => {
                    seen_plan.put(key, ());
                }
                _ => {}
            }
        }
    }
}

fn route_resolution_retry_delay(retry_count: usize) -> Duration {
    let multiplier = 1u64 << retry_count.saturating_sub(1).min(6);
    let delay_secs = ROUTE_RESOLUTION_RETRY_DELAY
        .as_secs()
        .saturating_mul(multiplier)
        .min(MAX_ROUTE_RESOLUTION_RETRY_DELAY.as_secs());
    Duration::from_secs(delay_secs)
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
    use hyper::service::{make_service_fn, service_fn};
    use hyper::{Body, Request, Response, Server};
    use std::convert::Infallible;
    use std::net::TcpListener;
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
            10000, // Use default LRU cache capacity for tests
            None,
        )
    }

    fn create_meta_event(
        source_table: &str,
        digest_field: &str,
        digest: &str,
        keyspace: Option<&str>,
    ) -> LogEvent {
        let mut log = LogEvent::default();
        log.insert(LABEL_SOURCE_TABLE, source_table);
        log.insert(LABEL_DATE, "2026-03-25");
        log.insert(digest_field, digest);
        if let Some(keyspace) = keyspace {
            log.insert(LABEL_KEYSPACE, keyspace);
        }
        log
    }

    #[test]
    fn test_extract_event_key_includes_keyspace() {
        let (sink, _) = create_test_sink_with_receiver(1);
        let sql_meta_event = create_meta_event(
            SOURCE_TABLE_TOPSQL_SQL_META,
            LABEL_SQL_DIGEST,
            "SQL_DIGEST",
            Some("ks-a"),
        );
        let plan_meta_event = create_meta_event(
            SOURCE_TABLE_TOPSQL_PLAN_META,
            LABEL_PLAN_DIGEST,
            "PLAN_DIGEST",
            Some("ks-b"),
        );

        assert_eq!(
            sink.extract_event_key(&sql_meta_event),
            Some((
                SOURCE_TABLE_TOPSQL_SQL_META.to_string(),
                "ks-a_SQL_DIGEST_2026-03-25".to_string(),
            ))
        );
        assert_eq!(
            sink.extract_event_key(&plan_meta_event),
            Some((
                SOURCE_TABLE_TOPSQL_PLAN_META.to_string(),
                "ks-b_PLAN_DIGEST_2026-03-25".to_string(),
            ))
        );
    }

    #[tokio::test]
    async fn test_missing_route_does_not_commit_dedup_key() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server =
            Server::from_tcp(listener)
                .unwrap()
                .serve(make_service_fn(move |_| async move {
                    Ok::<_, Infallible>(service_fn(move |_request: Request<Body>| async move {
                        Ok::<_, Infallible>(Response::new(Body::from(
                            r#"{"config":{"tenant_id":"30018"}}"#,
                        )))
                    }))
                }));
        let server_handle = tokio::spawn(server);

        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let resolver =
            PdKeyspaceResolver::new_with_client(format!("http://{}", address), None, client);

        let (sink, _) = TopSQLDeltaLakeSink::new_for_test(
            PathBuf::from("/tmp/test/org=xxx/cluster=xxx/type=topsql"),
            vec![],
            WriteConfig {
                batch_size: 1,
                timeout_secs: 0,
            },
            0,
            None,
            10000,
            Some(resolver),
        );

        let log_event = create_meta_event(
            SOURCE_TABLE_TOPSQL_SQL_META,
            LABEL_SQL_DIGEST,
            "SQL_DIGEST",
            Some("ks-missing"),
        );
        sink.process_events(vec![vec![Event::Log(log_event)]])
            .await
            .unwrap();

        assert_eq!(sink.seen_keys_sql_meta.lock().await.len(), 0);
        assert_eq!(sink.seen_keys_plan_meta.lock().await.len(), 0);
        assert!(sink.pending_dedup_keys.lock().await.is_empty());
        assert!(sink.new_event_buffer.lock().await.is_empty());

        server_handle.abort();
    }

    #[test]
    fn test_build_table_path_with_keyspace_route_for_s3() {
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
            10000,
            None,
        );

        let table_path = sink.build_table_path(
            SOURCE_TABLE_TOPSQL_SQL_META,
            Some(&KeyspaceRoute {
                org_id: "30018".to_string(),
                cluster_id: "10155668891296301432".to_string(),
            }),
        );

        assert_eq!(
            table_path,
            PathBuf::from(
                "s3://o11y-prod-shared-us-west-2-premium/deltalake/org=30018/cluster=10155668891296301432/type=topsql/component=topsql_sql_meta"
            )
        );
    }

    #[test]
    fn test_build_table_path_with_keyspace_route_replaces_template_for_local_path() {
        let (sink, _) = TopSQLDeltaLakeSink::new_for_test(
            PathBuf::from("/tmp/test/org=xxx/cluster=xxx/type=topsql"),
            vec![],
            WriteConfig {
                batch_size: 1,
                timeout_secs: 0,
            },
            180,
            None,
            10000,
            None,
        );

        let table_path = sink.build_table_path(
            SOURCE_TABLE_TOPSQL_PLAN_META,
            Some(&KeyspaceRoute {
                org_id: "30018".to_string(),
                cluster_id: "101".to_string(),
            }),
        );

        assert_eq!(
            table_path,
            PathBuf::from("/tmp/test/org=30018/cluster=101/type=topsql/component=topsql_plan_meta")
        );
    }

    #[test]
    fn test_build_table_path_without_keyspace_route_preserves_existing_layout() {
        let (sink, _) = create_test_sink_with_receiver(1);

        let table_path = sink.build_table_path(SOURCE_TABLE_TOPSQL_PLAN_META, None);

        assert_eq!(
            table_path,
            PathBuf::from("/tmp/test/component=topsql_plan_meta")
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
