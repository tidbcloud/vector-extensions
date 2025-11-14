use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use tokio::sync::MutexGuard;

use futures::{stream::BoxStream, StreamExt};
use hashlru::Cache;
use tokio::sync::Mutex;
use vector_lib::event::Event;
use vector_lib::event::Value as LogValue;
use vector_lib::sink::StreamSink;

use crate::common::deltalake_writer::{DeltaLakeWriter, DeltaTableConfig, WriteConfig};
use crate::sources::topsql::upstream::consts::LABEL_INSTANCE;
use crate::sources::topsql::upstream::consts::LABEL_INSTANCE_TYPE;
use crate::sources::topsql::upstream::consts::METRIC_NAME_CPU_TIME_MS;
use crate::sources::topsql::upstream::consts::METRIC_NAME_STMT_DURATION_COUNT;
use crate::sources::topsql::upstream::consts::METRIC_NAME_STMT_DURATION_SUM_NS;
use crate::sources::topsql::upstream::consts::{
    LABEL_NORMALIZED_PLAN, LABEL_NORMALIZED_SQL, LABEL_PLAN_DIGEST, LABEL_REGION_ID,
    LABEL_SQL_DIGEST, METRIC_NAME_LOGICAL_READ_BYTES, METRIC_NAME_LOGICAL_WRITE_BYTES,
    METRIC_NAME_NETWORK_IN_BYTES, METRIC_NAME_NETWORK_OUT_BYTES, METRIC_NAME_READ_KEYS,
    METRIC_NAME_STMT_EXEC_COUNT, METRIC_NAME_WRITE_KEYS,
};

use lazy_static::lazy_static;

lazy_static! {
    static ref TOPSQL_SCHEMA: serde_json::Map<String, serde_json::Value> = {
        let mut schema_info = serde_json::Map::new();
        schema_info.insert(
            "timestamps".into(),
            serde_json::json!({
                "mysql_type": "bigint",
                "is_nullable": false
            }),
        );
        schema_info.insert(
            LABEL_INSTANCE_TYPE.into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": false
            }),
        );
        schema_info.insert(
            LABEL_INSTANCE.into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": false
            }),
        );
        schema_info.insert(
            LABEL_SQL_DIGEST.into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": false
            }),
        );
        schema_info.insert(
            LABEL_PLAN_DIGEST.into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": false
            }),
        );
        schema_info.insert(
            LABEL_NORMALIZED_SQL.into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": false
            }),
        );
        schema_info.insert(
            LABEL_NORMALIZED_PLAN.into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": false
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
        schema_info
    };
    static ref INSTANCE_SCHEMA: serde_json::Map<String, serde_json::Value> = {
        let mut schema_info = serde_json::Map::new();
        schema_info.insert(
            "timestamps".into(),
            serde_json::json!({
                "mysql_type": "bigint",
                "is_nullable": false
            }),
        );
        schema_info.insert(
            LABEL_INSTANCE_TYPE.into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": false
            }),
        );
        schema_info.insert(
            LABEL_INSTANCE.into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": false
            }),
        );
        schema_info.insert(
            "tidb_cluster_id".into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": true
            }),
        );
        schema_info.insert(
            "keyspace_name".into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": true
            }),
        );
        schema_info.insert(
            "vm_account_id".into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": true
            }),
        );
        schema_info.insert(
            "vm_project_id".into(),
            serde_json::json!({
                "mysql_type": "text",
                "is_nullable": true
            }),
        );
        schema_info
    };    
}

#[derive(Default, Eq, PartialEq, Clone, Hash)]
struct TiKVExecCountKey {
    sql_digest: String,
    plan_digest: String,
    timestamps: u64,
    instance: String,
}

/// Delta Lake sink processor
pub struct TopSQLDeltaLakeSink {
    base_path: PathBuf,
    tables: Vec<DeltaTableConfig>,
    write_config: WriteConfig,
    storage_options: Option<HashMap<String, String>>,
    writers: Arc<Mutex<HashMap<String, DeltaLakeWriter>>>,
    sql_cache: Arc<Mutex<Cache<String, String>>>,
    plan_cache: Arc<Mutex<Cache<String, String>>>,
    tikv_exec_count_cache: Arc<Mutex<Cache<TiKVExecCountKey, u64>>>,
    tidb_event_cache: Arc<Mutex<Vec<Event>>>,
    parallelism: AtomicUsize,
}

impl TopSQLDeltaLakeSink {
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
            sql_cache: Arc::new(Mutex::new(Cache::new(100000))), // TODO: Cache size can be adjusted
            plan_cache: Arc::new(Mutex::new(Cache::new(100000))),
            tikv_exec_count_cache: Arc::new(Mutex::new(Cache::new(5000))),
            tidb_event_cache: Arc::new(Mutex::new(Vec::new())),
            parallelism: AtomicUsize::new(0),
        }
    }

    fn process_tidb_records_events<'a>(
        &self,
        table_events: &mut HashMap<String, Vec<Event>>,
        sql_cache: &mut MutexGuard<'a, Cache<String, String>>,
        plan_cache: &mut MutexGuard<'a, Cache<String, String>>,
        tikv_exec_count_cache: &mut MutexGuard<'a, Cache<TiKVExecCountKey, u64>>,
        tidb_event_cache: &mut MutexGuard<'a, Vec<Event>>,
    ) {
        let table_name = "tidb_topsql";
        info!("tidb event cache size: {}", tidb_event_cache.len());
        for event in tidb_event_cache.iter_mut() {
            if let Event::Log(ref mut log_event) = event {
                // Enrich SQL and Plan from cache
                let mut tikv_exec_count_key = TiKVExecCountKey::default();
                if let Some(sql_digest) = log_event.get(LABEL_SQL_DIGEST).and_then(|v| v.as_str()) {
                    info!("tidb sql cache: {} ", sql_cache.len());
                    tikv_exec_count_key.sql_digest = sql_digest.to_string();
                    if let Some(sql) = sql_cache.get(&sql_digest.to_string()) {
                        log_event.insert(LABEL_NORMALIZED_SQL, sql.clone());
                    } else {
                        info!("tidb sql_digest: {} not found in sql_cache", sql_digest);
                    }
                }
                if let Some(plan_digest) = log_event.get(LABEL_PLAN_DIGEST).and_then(|v| v.as_str())
                {
                    tikv_exec_count_key.plan_digest = plan_digest.to_string();
                    info!("tidb plan cache: {} ", plan_cache.len());
                    if let Some(plan) = plan_cache.get(&plan_digest.to_string()) {
                        log_event.insert(LABEL_NORMALIZED_PLAN, plan.clone());
                    }
                }
                if let Some(timestamps) = log_event.get("timestamps").and_then(|v| v.as_integer()) {
                    tikv_exec_count_key.timestamps = timestamps as u64;
                }
                {
                    let tikv_exec_map = log_event
                        .get("topsql_tikv_stmt_exec_count")
                        .and_then(|v| v.as_object());
                    if let Some(tikv_exec_map) = tikv_exec_map {
                        for (key, value) in tikv_exec_map {
                            tikv_exec_count_key.instance = key.to_string();
                            let count = value.as_integer().unwrap_or(0) as u64;
                            if count == 0 {
                                continue;
                            }

                            let handle = tikv_exec_count_cache.get_mut(&tikv_exec_count_key);
                            if let Some(handle) = handle {
                                *handle += count;
                            } else {
                                tikv_exec_count_cache.insert(tikv_exec_count_key.clone(), count);
                            }
                        }
                    }
                }
                table_events
                    .entry("topsql_data".into())
                    .or_insert_with(Vec::new)
                    .push(Event::Log(log_event.clone()));
            }
        }
    }

    /// Process events and write to Delta Lake
    async fn process_events(
        &self,
        events_vec: Vec<Vec<Event>>,
        cache_tidb_events: bool,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if events_vec.is_empty() {
            return Ok(());
        }
        // Group events by source_table
        let mut table_events: HashMap<String, Vec<Event>> = HashMap::new();
        let mut sql_cache = self.sql_cache.lock().await;
        let mut plan_cache = self.plan_cache.lock().await;
        let mut tikv_exec_count_cache = self.tikv_exec_count_cache.lock().await;
        let mut tidb_event_cache = self.tidb_event_cache.lock().await;
        let mut tidb_event_cache_cleared = false;

        for events in events_vec {
            for event in events {
                if let Event::Log(mut log_event) = event {
                    let table_name: String;
                    {
                        let table_name_ref = log_event.get("source_table").and_then(|v| v.as_str());
                        if let Some(table_name_v2) = table_name_ref {
                            table_name = table_name_v2.to_string();
                        } else {
                            continue;
                        }
                    }
                    match table_name.as_str() {
                        "tidb_sql_meta" => {
                            log_event
                                .get(LABEL_SQL_DIGEST)
                                .and_then(|v| v.as_str())
                                .map(|sql_digest| {
                                    let handle = sql_cache.get_mut(&sql_digest.to_string());
                                    if let Some(handle) = handle {
                                        if let Some(sql) =
                                            log_event.get(LABEL_NORMALIZED_SQL).and_then(|v| v.as_str())
                                        {
                                            *handle = sql.to_string();
                                        }
                                    } else {
                                        if let Some(sql) =
                                            log_event.get(LABEL_NORMALIZED_SQL).and_then(|v| v.as_str())
                                        {
                                            sql_cache.insert(sql_digest.to_string(), sql.to_string());
                                        }
                                    }
                                });
                        }
                        "tidb_plan_meta" => {
                            log_event
                                .get(LABEL_PLAN_DIGEST)
                                .and_then(|v| v.as_str())
                                .map(|plan_digest| {
                                    let handle = plan_cache.get_mut(&plan_digest.to_string());
                                    if let Some(handle) = handle {
                                        if let Some(plan) = log_event
                                            .get(LABEL_NORMALIZED_PLAN)
                                            .and_then(|v| v.as_str())
                                        {
                                            *handle = plan.to_string();
                                        }
                                    } else {
                                        if let Some(plan) = log_event
                                            .get(LABEL_NORMALIZED_PLAN)
                                            .and_then(|v| v.as_str())
                                        {
                                            plan_cache
                                                .insert(plan_digest.to_string(), plan.to_string());
                                        }
                                    }
                                });
                        }
                        "tidb_topsql" => {
                            if cache_tidb_events {
                                tidb_event_cache.push(Event::Log(log_event.clone()));
                                continue;
                            }
                        }
                        "tikv_topsql" => {
                            // handle tidb events first, since tikv events may depend on tidb's tikv_exec_count info
                            if !tidb_event_cache_cleared {
                                self.process_tidb_records_events(
                                    &mut table_events,
                                    &mut sql_cache,
                                    &mut plan_cache,
                                    &mut tikv_exec_count_cache,
                                    &mut tidb_event_cache,
                                );
                                tidb_event_cache.clear();
                                tidb_event_cache_cleared = true;
                            }

                            let mut tikv_exec_count_key = TiKVExecCountKey::default();
                            // Enrich SQL and Plan from cache
                            if let Some(sql_digest) =
                                log_event.get(LABEL_SQL_DIGEST).and_then(|v| v.as_str())
                            {
                                tikv_exec_count_key.sql_digest = sql_digest.to_string();
                                if let Some(sql) = sql_cache.get(&sql_digest.to_string()) {
                                    log_event.insert(LABEL_NORMALIZED_SQL, sql.clone());
                                } else {
                                    info!("tikv sql_digest: {} not found in sql_cache", sql_digest);
                                }
                            }
                            if let Some(plan_digest) =
                                log_event.get(LABEL_PLAN_DIGEST).and_then(|v| v.as_str())
                            {
                                tikv_exec_count_key.plan_digest = plan_digest.to_string();
                                if let Some(plan) = plan_cache.get(&plan_digest.to_string()) {
                                    log_event.insert(LABEL_NORMALIZED_PLAN, plan.clone());
                                }
                            }
                            if let Some(timestamps) =
                                log_event.get("timestamps").and_then(|v| v.as_integer())
                            {
                                tikv_exec_count_key.timestamps = timestamps as u64;
                            }
                            if let Some(instance) = log_event.get("instance").and_then(|v| v.as_str()) {
                                tikv_exec_count_key.instance = instance.to_string();
                            }
                            {
                                let exec_count = tikv_exec_count_cache
                                    .get(&tikv_exec_count_key)
                                    .unwrap_or(&0);
                                log_event
                                    .insert(METRIC_NAME_STMT_EXEC_COUNT, LogValue::from(*exec_count));
                            }

                            table_events
                                .entry("topsql_data".into())
                                .or_insert_with(Vec::new)
                                .push(Event::Log(log_event));
                        }
                        "tikv_topregion" => {
                            table_events
                                .entry("topsql_data".into())
                                .or_insert_with(Vec::new)
                                .push(Event::Log(log_event));
                        }                        
                        "instance" => {
                            table_events
                                .entry("topsql_instance".into())
                                .or_insert_with(Vec::new)
                                .push(Event::Log(log_event));
                        }
                        _ => {
                            // Ignore other tables
                        }
                    }
                }
            }
        }

        // Writeh table's events
        for (table_name, mut table_events) in table_events {
            self.add_schema_info(&table_name, &mut table_events);
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
    fn add_schema_info(&self, source_table_name: &str, events: &mut Vec<Event>) {
        if events.is_empty() {
            return;
        }
        match source_table_name {
            "topsql_data" => {
                let first_event = &mut events[0];
                let log = first_event.as_mut_log();
                log.insert(
                    "_schema_metadata",
                    serde_json::Value::Object(TOPSQL_SCHEMA.clone()),
                );
            }
            "topsql_instance" => {
                let first_event = &mut events[0];
                let log = first_event.as_mut_log();
                log.insert(
                    "_schema_metadata",
                    serde_json::Value::Object(INSTANCE_SCHEMA.clone()),
                );
            }
            _ => {}
        }
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

            let table_config = self
                .tables
                .iter()
                .find(|t| t.name == table_name)
                .cloned()
                .unwrap_or_else(|| DeltaTableConfig {
                    name: table_name.to_string(),
                    partition_by: Some(vec!["date".to_string()]),
                    schema_evolution: Some(true),
                    standard_columns: None,
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
impl StreamSink<Event> for TopSQLDeltaLakeSink {
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let c = self.parallelism.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if c > 0 {
            error!("Delta Lake sink parallelism exceeded: {}", c + 1);
        }
        info!(
            "Delta Lake sink starting with batch_size: {}, timeout_secs: {}",
            self.write_config.batch_size, self.write_config.timeout_secs
        );

        let mut input = input.ready_chunks(self.write_config.batch_size);
        let mut events_cache = vec![];
        let mut cur_cache_size = 0;
        let mut oldest_timestamp = 0;
        let mut latest_timestamp = 0;
        while let Some(events) = input.next().await {
            let events_count = events.len();
            if events_count > 0 {
                if let Event::Log(ref log_event) = events[0] {
                    if let Some(timestamps) =
                    log_event.get("timestamps").and_then(|v| v.as_integer())
                    {
                        latest_timestamp = timestamps;
                        if cur_cache_size == 0 {
                            oldest_timestamp = timestamps;
                        }
                    }
                }
            } else {
                continue;
            }
            cur_cache_size += events_count;
            events_cache.push(events);
            // Allow max delay to 3 minutes
            if events_count + cur_cache_size < self.write_config.batch_size && latest_timestamp < oldest_timestamp + 180 {
                continue;
            }
            if let Err(e) = self.process_events(events_cache, true).await {
                error!("Failed to process events: {}", e);
            }
            cur_cache_size = 0;
            events_cache = vec![];
        }
        self.parallelism.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}
