use std::collections::HashMap;
use std::time::Duration;

use futures::{stream::BoxStream, StreamExt};
use sqlx::{MySqlPool, Row};
use vector_lib::{
    event::{Event, LogEvent, Value},
    sink::StreamSink,
};

use tracing::{debug, error, info, warn};

/// Column information from database schema
#[derive(Debug, Clone)]
struct ColumnInfo {
    name: String,
    data_type: String,
    is_nullable: bool,
    /// Maximum character length for string types (extracted from VARCHAR(n), CHAR(n), etc.)
    /// None means no limit (TEXT, LONGTEXT, etc.)
    max_length: Option<usize>,
}

/// TiDB sink that writes events to MySQL/TiDB database
pub struct TiDBSink {
    pool: MySqlPool,
    table: String,
    batch_size: usize,
    /// Cached table schema: column name -> ColumnInfo
    schema: HashMap<String, ColumnInfo>,
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

        // Query table schema to get column information
        let schema = Self::get_table_schema(&pool, &table).await?;

        info!(
            message = "TiDB sink initialized",
            table = %table,
            columns = schema.len(),
            max_connections = max_connections,
            batch_size = batch_size
        );

        Ok(Self {
            pool,
            table,
            batch_size,
            schema,
        })
    }

    /// Query table schema to get column information
    async fn get_table_schema(
        pool: &MySqlPool,
        table: &str,
    ) -> vector::Result<HashMap<String, ColumnInfo>> {
        let schema_sql = format!("SHOW COLUMNS FROM {}", table);

        debug!("Querying table schema: {}", schema_sql);

        let rows = sqlx::query(&schema_sql)
            .fetch_all(pool)
            .await
            .map_err(|e| {
                vector::Error::from(format!("Failed to query table schema: {}", e))
            })?;

        let mut schema = HashMap::new();
        for row in rows {
            // Field name
            let field_name: String = row
                .try_get("Field")
                .map_err(|e| vector::Error::from(format!("Failed to get field name: {}", e)))?;
            
            // Field type - MySQL may return as BLOB, so we need to handle it as bytes first
            let field_type: String = row
                .try_get::<Vec<u8>, _>("Type")
                .ok()
                .and_then(|bytes| String::from_utf8(bytes).ok())
                .or_else(|| {
                    // Fallback: try as String directly
                    row.try_get::<String, _>("Type").ok()
                })
                .ok_or_else(|| {
                    vector::Error::from("Failed to get field type: could not decode as bytes or string")
                })?;
            
            // Nullable info
            let is_nullable: String = row
                .try_get("Null")
                .map_err(|e| vector::Error::from(format!("Failed to get nullable info: {}", e)))?;

            // Extract max length from data type (e.g., VARCHAR(255) -> 255)
            let max_length = Self::extract_max_length(&field_type);

            schema.insert(
                field_name.clone(),
                ColumnInfo {
                    name: field_name,
                    data_type: field_type,
                    is_nullable: is_nullable == "YES",
                    max_length,
                },
            );
        }

        debug!("Table schema loaded: {} columns", schema.len());
        Ok(schema)
    }

    /// Extract maximum length from MySQL data type string
    /// Examples: "VARCHAR(255)" -> Some(255), "TEXT" -> None, "CHAR(10)" -> Some(10)
    fn extract_max_length(data_type: &str) -> Option<usize> {
        // Check for VARCHAR(n), CHAR(n), etc.
        if let Some(start) = data_type.find('(') {
            if let Some(end) = data_type.find(')') {
                if let Ok(length) = data_type[start + 1..end].parse::<usize>() {
                    return Some(length);
                }
            }
        }
        // TEXT, LONGTEXT, MEDIUMTEXT, TINYTEXT, BLOB, etc. have no explicit length limit
        None
    }

    /// Extract value from log event for a given column
    /// Tries to match event field names to column names (case-insensitive)
    fn extract_value_for_column(&self, log_event: &LogEvent, column_name: &str) -> Option<String> {
        // Try exact match first
        if let Some(value) = log_event.get(column_name) {
            return Some(self.value_to_string(value));
        }

        // Try case-insensitive match by iterating through event fields
        if let Some(iter) = log_event.all_event_fields() {
            for (key, value) in iter {
                if key.as_ref().eq_ignore_ascii_case(column_name) {
                    return Some(self.value_to_string(value));
                }
            }
        }

        None
    }

    /// Convert Vector Value to String for SQL binding
    fn value_to_string(&self, value: &Value) -> String {
        match value {
            Value::Bytes(bytes) => {
                // Try to parse as string first
                if let Ok(s) = std::str::from_utf8(bytes.as_ref()) {
                    s.to_string()
                } else {
                    format!("{:?}", bytes)
                }
            }
            Value::Integer(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Boolean(b) => b.to_string(),
            Value::Timestamp(ts) => {
                // Convert Vector timestamp to MySQL DATETIME format
                ts.to_rfc3339().split('T').collect::<Vec<&str>>().join(" ")
                    .split('+')
                    .next()
                    .unwrap_or("")
                    .to_string()
            }
            Value::Null => "NULL".to_string(),
            Value::Object(_) | Value::Array(_) => {
                // Serialize complex types as JSON
                serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string())
            }
            Value::Regex(_) => {
                // Convert regex to string representation
                format!("{:?}", value)
            }
        }
    }

    /// Convert timestamp string to MySQL DATETIME format
    fn convert_timestamp_to_mysql_format(&self, ts_str: &str) -> String {
        // Try to parse ISO 8601 format and convert to MySQL DATETIME format
        let ts_str = ts_str.replace('Z', "+00:00");
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&ts_str) {
            dt.format("%Y-%m-%d %H:%M:%S").to_string()
        } else if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(ts_str.as_str(), "%Y-%m-%dT%H:%M:%S") {
            dt.format("%Y-%m-%d %H:%M:%S").to_string()
        } else {
            // If parsing fails, try to use the string as-is (might already be in MySQL format)
            ts_str.to_string()
        }
    }

    /// Insert a batch of events into the database
    async fn insert_batch(&self, events: Vec<Event>) -> vector::Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        // Build INSERT statement dynamically based on table schema
        // Only include columns that exist in the schema and have matching event fields
        let mut columns: Vec<String> = Vec::new();
        for column_info in self.schema.values() {
            // Skip auto-increment or auto-generated columns (like id, created_at)
            // These will be handled by the database
            if column_info.name == "id" || column_info.name == "created_at" {
                continue;
            }
            columns.push(column_info.name.clone());
        }

        if columns.is_empty() {
            return Err(vector::Error::from(
                "No insertable columns found in table schema",
            ));
        }

        let placeholders: Vec<String> = (0..columns.len()).map(|_| "?".to_string()).collect();
        let query = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.table,
            columns.join(", "),
            placeholders.join(", ")
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

            // Build values for each column
            let mut query_builder = sqlx::query(&query);
            for column_name in &columns {
                let value = self.extract_value_for_column(&log_event, column_name);

                // Handle timestamp columns specially - convert to MySQL format
                let column_info = self.schema.get(column_name).unwrap();
                let mut final_value = if column_info.data_type.to_lowercase().contains("datetime")
                    || column_info.data_type.to_lowercase().contains("timestamp")
                {
                    value
                        .as_ref()
                        .map(|v| self.convert_timestamp_to_mysql_format(v))
                } else {
                    value
                };

                // Truncate string values if they exceed column max length
                if let Some(ref mut v) = final_value {
                    if let Some(max_len) = column_info.max_length {
                        if v.len() > max_len {
                            warn!(
                                message = "Truncating value for column",
                                column = %column_name,
                                original_length = v.len(),
                                max_length = max_len
                            );
                            *v = v.chars().take(max_len).collect::<String>();
                        }
                    }
                }

                // Bind value (use NULL for missing values if column is nullable)
                if let Some(v) = final_value {
                    query_builder = query_builder.bind(v);
                } else if column_info.is_nullable {
                    query_builder = query_builder.bind::<Option<String>>(None);
                } else {
                    // For non-nullable columns, use a default value based on type
                    let default = if column_info.data_type.to_lowercase().contains("int") {
                        "0".to_string()
                    } else if column_info.data_type.to_lowercase().contains("float")
                        || column_info.data_type.to_lowercase().contains("double")
                    {
                        "0.0".to_string()
                    } else if column_info.data_type.to_lowercase().contains("datetime")
                        || column_info.data_type.to_lowercase().contains("timestamp")
                    {
                        chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()
                    } else {
                        "".to_string()
                    };
                    query_builder = query_builder.bind(default);
                }
            }

            query_builder
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    error!(
                        message = "Failed to insert event",
                        error = %e,
                        table = %self.table
                    );
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
