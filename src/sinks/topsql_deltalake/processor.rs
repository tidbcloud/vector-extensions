use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use futures::{stream::BoxStream, StreamExt};
use hashlru::Cache;
use tokio::sync::Mutex;
use vector_lib::event::Event;
use vector_lib::sink::StreamSink;

use crate::common::deltalake_writer::{DeltaLakeWriter, DeltaTableConfig, WriteConfig};
use crate::sources::topsql::upstream::consts::{
    LABEL_NORMALIZED_PLAN, LABEL_NORMALIZED_SQL, LABEL_PLAN_DIGEST, LABEL_REGION_ID,
    LABEL_SQL_DIGEST, METRIC_NAME_LOGICAL_READ_BYTES, METRIC_NAME_LOGICAL_WRITE_BYTES,
    METRIC_NAME_NETWORK_IN_BYTES, METRIC_NAME_NETWORK_OUT_BYTES, METRIC_NAME_READ_KEYS,
    METRIC_NAME_WRITE_KEYS,
};

/// Delta Lake sink processor
pub struct TopSQLDeltaLakeSink {
    base_path: PathBuf,
    tables: Vec<DeltaTableConfig>,
    write_config: WriteConfig,
    storage_options: Option<HashMap<String, String>>,
    writers: Arc<Mutex<HashMap<String, DeltaLakeWriter>>>,
    sql_cache: Arc<Mutex<Cache<String, String>>>,
    plan_cache: Arc<Mutex<Cache<String, String>>>,
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
            sql_cache: Arc::new(Mutex::new(Cache::new(10000))),
            plan_cache: Arc::new(Mutex::new(Cache::new(10000))),
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
        let mut sql_cache = self.sql_cache.lock().await;
        let mut plan_cache = self.plan_cache.lock().await;

        for event in events {
            if let Event::Log(mut log_event) = event {
                let table_name: String;
                {
                    let table_name_ref = log_event.get("dest_table").and_then(|v| v.as_str());
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
                                }
                            });
                    }
                    "tidb_topsql" | "tikv_topsql" => {
                        // Enrich SQL and Plan from cache
                        if let Some(sql_digest) =
                            log_event.get(LABEL_SQL_DIGEST).and_then(|v| v.as_str())
                        {
                            if let Some(sql) = sql_cache.get(&sql_digest.to_string()) {
                                log_event.insert(LABEL_NORMALIZED_SQL, sql.clone());
                            }
                        }
                        if let Some(plan_digest) =
                            log_event.get(LABEL_PLAN_DIGEST).and_then(|v| v.as_str())
                        {
                            if let Some(plan) = plan_cache.get(&plan_digest.to_string()) {
                                log_event.insert(LABEL_NORMALIZED_PLAN, plan.clone());
                            }
                        }

                        table_events
                            .entry(table_name.to_string())
                            .or_insert_with(Vec::new)
                            .push(Event::Log(log_event));
                    }
                    "tikv_topregion" => {
                        table_events
                            .entry(table_name.to_string())
                            .or_insert_with(Vec::new)
                            .push(Event::Log(log_event));
                    }
                    _ => {
                        // Ignore other tables
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
    fn add_schema_info(&self, table_name: &str, events: &mut Vec<Event>) {
        if events.is_empty() {
            return;
        }

        match table_name {
            "tidb_topsql" => {
                let first_event = &mut events[0];
                let mut schema_info = serde_json::Map::new();
                schema_info.insert(
                    "timestamps".into(),
                    serde_json::json!({
                        "mysql_type": "text",
                        "is_nullable": false
                    }),
                );
                schema_info.insert(
                    "instance_type".into(),
                    serde_json::json!({
                        "mysql_type": "text",
                        "is_nullable": false
                    }),
                );
                schema_info.insert(
                    "instance".into(),
                    serde_json::json!({
                        "mysql_type": "text",
                        "is_nullable": false
                    }),
                );
                schema_info.insert(
                    "sql_digest".into(),
                    serde_json::json!({
                        "mysql_type": "text",
                        "is_nullable": false
                    }),
                );
                schema_info.insert(
                    "plan_digest".into(),
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
                    "cpu_time_ms".into(),
                    serde_json::json!({
                        "mysql_type": "int",
                        "is_nullable": false
                    }),
                );
                schema_info.insert(
                    "stmt_exec_count".into(),
                    serde_json::json!({
                        "mysql_type": "bigint",
                        "is_nullable": true
                    }),
                );
                schema_info.insert(
                    "stmt_duration_sum_ns".into(),
                    serde_json::json!({
                        "mysql_type": "bigint",
                        "is_nullable": true
                    }),
                );
                schema_info.insert(
                    "stmt_duration_count".into(),
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
                let log = first_event.as_mut_log();
                log.insert(
                    "_schema_metadata",
                    serde_json::Value::Object(schema_info.clone()),
                );
            }
            "tikv_topsql" => {
                let first_event = &mut events[0];
                let mut schema_info = serde_json::Map::new();
                schema_info.insert(
                    "timestamps".into(),
                    serde_json::json!({
                        "mysql_type": "text",
                        "is_nullable": false
                    }),
                );
                schema_info.insert(
                    "instance_type".into(),
                    serde_json::json!({
                        "mysql_type": "text",
                        "is_nullable": false
                    }),
                );
                schema_info.insert(
                    "instance".into(),
                    serde_json::json!({
                        "mysql_type": "text",
                        "is_nullable": false
                    }),
                );
                schema_info.insert(
                    "sql_digest".into(),
                    serde_json::json!({
                        "mysql_type": "text",
                        "is_nullable": false
                    }),
                );
                schema_info.insert(
                    "plan_digest".into(),
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
                    "cpu_time_ms".into(),
                    serde_json::json!({
                        "mysql_type": "int",
                        "is_nullable": false
                    }),
                );
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
                let log = first_event.as_mut_log();
                log.insert(
                    "_schema_metadata",
                    serde_json::Value::Object(schema_info.clone()),
                );
            }
            "tikv_topregion" => {
                let first_event = &mut events[0];
                let mut schema_info = serde_json::Map::new();
                schema_info.insert(
                    "timestamps".into(),
                    serde_json::json!({
                        "mysql_type": "text",
                        "is_nullable": false
                    }),
                );
                schema_info.insert(
                    "instance_type".into(),
                    serde_json::json!({
                        "mysql_type": "text",
                        "is_nullable": false
                    }),
                );
                schema_info.insert(
                    "instance".into(),
                    serde_json::json!({
                        "mysql_type": "text",
                        "is_nullable": false
                    }),
                );
                schema_info.insert(
                    LABEL_REGION_ID.into(),
                    serde_json::json!({
                        "mysql_type": "text",
                        "is_nullable": false
                    }),
                );
                schema_info.insert(
                    "cpu_time_ms".into(),
                    serde_json::json!({
                        "mysql_type": "int",
                        "is_nullable": false
                    }),
                );
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
                let log = first_event.as_mut_log();
                log.insert(
                    "_schema_metadata",
                    serde_json::Value::Object(schema_info.clone()),
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
