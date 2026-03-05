use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::{stream::BoxStream, StreamExt};
use sqlx::{MySqlPool, Row};
use tokio::sync::Mutex;
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
    /// Cached table schema: column name -> ColumnInfo.
    /// None when table doesn't exist yet and auto_create_table was true (filled on first batch).
    schema: Arc<Mutex<Option<HashMap<String, ColumnInfo>>>>,
}

impl TiDBSink {
    /// Create a new TiDB sink
    pub async fn new(
        connection_string: String,
        table: String,
        max_connections: u32,
        connection_timeout: Duration,
        batch_size: usize,
        auto_create_table: bool,
    ) -> vector::Result<Self> {
        use sqlx::mysql::MySqlPoolOptions;

        // Create connection pool with options
        let pool = MySqlPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(connection_timeout)
            .connect(&connection_string)
            .await
            .map_err(|e| vector::Error::from(format!("Failed to create connection pool: {}", e)))?;

        // Query table schema; if table doesn't exist and auto_create_table, defer to first batch
        let schema = match Self::get_table_schema(&pool, &table).await {
            Ok(s) => {
                info!(
                    message = "TiDB sink initialized with existing table",
                    table = %table,
                    columns = s.len(),
                    max_connections = max_connections,
                    batch_size = batch_size
                );
                Arc::new(Mutex::new(Some(s)))
            }
            Err(e) if auto_create_table && Self::is_table_not_found_error(&e) => {
                info!(
                    message = "TiDB sink initialized, table will be created from first batch",
                    table = %table,
                    max_connections = max_connections,
                    batch_size = batch_size
                );
                Arc::new(Mutex::new(None))
            }
            Err(e) => return Err(e),
        };

        Ok(Self {
            pool,
            table,
            batch_size,
            schema,
        })
    }

    fn is_table_not_found_error(e: &vector::Error) -> bool {
        let msg = e.to_string().to_lowercase();
        msg.contains("doesn't exist") || msg.contains("not found") || msg.contains("1146")
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

    /// Create table from the first event's field structure
    async fn create_table_from_event(
        pool: &MySqlPool,
        table: &str,
        log_event: &LogEvent,
    ) -> vector::Result<()> {
        let mut col_defs: Vec<String> = Vec::new();
        col_defs.push("`id` BIGINT AUTO_INCREMENT PRIMARY KEY".to_string());

        // Prefer _schema_metadata mysql_type when present (e.g. from deltalake/topsql sinks)
        let schema_meta = log_event
            .get("_schema_metadata")
            .and_then(|v| v.as_object())
            .cloned();

        let mut fields_seen = std::collections::HashSet::new();
        if let Some(iter) = log_event.all_event_fields() {
            for (key, value) in iter {
                let name = key.as_ref();
                if name.starts_with('_') || name == "id" {
                    continue;
                }
                if fields_seen.contains(name) {
                    continue;
                }
                fields_seen.insert(name.to_string());

                let mysql_type = schema_meta
                    .as_ref()
                    .and_then(|m| m.get(name))
                    .and_then(|info| info.as_object())
                    .and_then(|obj| obj.get("mysql_type"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| Self::infer_mysql_type(value));

                let col_def = format!("`{}` {}", Self::escape_ident(name), mysql_type);
                col_defs.push(col_def);
            }
        }

        if col_defs.len() <= 1 {
            return Err(vector::Error::from(
                "No insertable fields found in event for auto-create table",
            ));
        }

        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS `{}` ({})",
            table.replace('`', "``"),
            col_defs.join(", ")
        );
        info!(message = "Creating table from first event", table = %table, sql = %create_sql);

        sqlx::query(&create_sql)
            .execute(pool)
            .await
            .map_err(|e| vector::Error::from(format!("Failed to create table: {}", e)))?;

        Ok(())
    }

    /// Infer MySQL column type from Vector Value
    fn infer_mysql_type(value: &Value) -> String {
        match value {
            Value::Integer(_) => "BIGINT",
            Value::Float(_) => "DOUBLE",
            Value::Boolean(_) => "TINYINT(1)",
            Value::Timestamp(_) => "DATETIME(6)",
            Value::Null => "TEXT",
            Value::Object(_) | Value::Array(_) => "JSON",
            Value::Bytes(bytes) => {
                let len = bytes.len();
                if len <= 4096 {
                    "VARCHAR(4096)"
                } else if len <= 65535 {
                    "TEXT"
                } else {
                    "LONGTEXT"
                }
            }
            Value::Regex(_) => "TEXT",
        }
        .to_string()
    }

    fn escape_ident(s: &str) -> String {
        s.replace('`', "``")
    }

    /// Convert boolean-like string to "0" or "1" for TINYINT(1) columns.
    /// Source data (e.g. from Delta Lake/DuckDB) often has "true"/"false" as strings.
    fn convert_bool_string_for_tinyint(value: &str) -> Option<&'static str> {
        let v = value.trim().to_lowercase();
        if v.is_empty() {
            return None;
        }
        match v.as_str() {
            "true" | "t" | "1" | "yes" | "y" => Some("1"),
            "false" | "f" | "0" | "no" | "n" => Some("0"),
            _ => None,
        }
    }

    /// Extract maximum character length from MySQL string-type columns.
    /// Only applies to VARCHAR(n), CHAR(n), etc. — NOT numeric types where (n) is display width.
    fn extract_max_length(data_type: &str) -> Option<usize> {
        let dt_lower = data_type.to_lowercase();
        let is_string_type = dt_lower.starts_with("varchar")
            || dt_lower.starts_with("char")
            || dt_lower.starts_with("binary")
            || dt_lower.starts_with("varbinary");
        if !is_string_type {
            return None;
        }
        if let Some(start) = data_type.find('(') {
            if let Some(end) = data_type.find(')') {
                if let Ok(length) = data_type[start + 1..end].parse::<usize>() {
                    return Some(length);
                }
            }
        }
        None
    }

    /// Sanitize a value for a numeric MySQL column.
    /// Handles NaN, Infinity, empty strings, and float-to-int coercion.
    /// Returns None if the value cannot be represented and should become NULL.
    fn sanitize_numeric_value(value: &str, data_type: &str) -> Option<String> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return None;
        }
        let lower = trimmed.to_lowercase();
        if lower == "nan" || lower == "inf" || lower == "-inf"
            || lower == "infinity" || lower == "-infinity"
            || lower == "none" || lower == "null"
        {
            return None;
        }
        let dt_lower = data_type.to_lowercase();
        let is_integer_type = dt_lower.contains("int") || dt_lower == "serial";
        if is_integer_type {
            if let Ok(i) = trimmed.parse::<i64>() {
                return Some(i.to_string());
            }
            if let Ok(f) = trimmed.parse::<f64>() {
                if f.is_finite() {
                    return Some((f as i64).to_string());
                }
                return None;
            }
            return None;
        }
        if let Ok(f) = trimmed.parse::<f64>() {
            if f.is_finite() {
                return Some(f.to_string());
            }
            return None;
        }
        None
    }

    fn is_numeric_column(data_type: &str) -> bool {
        let dt = data_type.to_lowercase();
        dt.contains("int") || dt.contains("float") || dt.contains("double")
            || dt.contains("decimal") || dt.contains("numeric") || dt == "serial"
    }

    /// Extract value from log event for a given column (case-insensitive match)
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

        // Ensure schema is loaded; create table from first event if needed
        {
            let mut guard = self.schema.lock().await;
            if guard.is_none() {
                let first_log = events.iter().find_map(|e| {
                    if let Event::Log(log) = e {
                        Some(log)
                    } else {
                        None
                    }
                });
                let log_event = first_log.ok_or_else(|| {
                    vector::Error::from("No log events in batch for auto-create table")
                })?;
                Self::create_table_from_event(&self.pool, &self.table, log_event).await?;
                let s = Self::get_table_schema(&self.pool, &self.table).await?;
                *guard = Some(s);
            }
        }

        let schema = {
            let guard = self.schema.lock().await;
            guard.as_ref().unwrap().clone()
        };

        // Build INSERT statement dynamically based on table schema
        let mut columns: Vec<String> = Vec::new();
        for column_info in schema.values() {
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

        // Quote column names with backticks so MySQL accepts identifiers like @timestamp
        let columns_quoted: Vec<String> = columns
            .iter()
            .map(|c| format!("`{}`", c.replace('`', "``")))
            .collect();
        let placeholders: Vec<String> = (0..columns.len()).map(|_| "?".to_string()).collect();
        let query = format!(
            "INSERT INTO `{}` ({}) VALUES ({})",
            self.table.replace('`', "``"),
            columns_quoted.join(", "),
            placeholders.join(", ")
        );

        for event in events {
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

            let mut query_builder = sqlx::query(&query);
            for column_name in &columns {
                let value = self.extract_value_for_column(&log_event, column_name);

                let column_info = schema.get(column_name).unwrap();
                let mut final_value = if column_info.data_type.to_lowercase().contains("datetime")
                    || column_info.data_type.to_lowercase().contains("timestamp")
                {
                    value
                        .as_ref()
                        .map(|v| self.convert_timestamp_to_mysql_format(v))
                } else {
                    value
                };

                // Convert boolean-like strings to "0"/"1" for TINYINT(1)/BOOL columns
                let dt_lower = column_info.data_type.to_lowercase();
                let is_bool_column = dt_lower.contains("tinyint") || dt_lower == "bool"
                    || dt_lower == "boolean";
                if is_bool_column {
                    if let Some(ref v) = final_value {
                        if let Some(normalized) = Self::convert_bool_string_for_tinyint(v) {
                            final_value = Some(normalized.to_string());
                        }
                    }
                }

                // Sanitize values for numeric columns (handle NaN, Infinity, float-to-int, etc.)
                if Self::is_numeric_column(&column_info.data_type) && !is_bool_column {
                    if let Some(ref v) = final_value {
                        match Self::sanitize_numeric_value(v, &column_info.data_type) {
                            Some(sanitized) => final_value = Some(sanitized),
                            None => {
                                warn!(
                                    message = "Invalid numeric value, converting to NULL",
                                    column = %column_name,
                                    value = %v,
                                    data_type = %column_info.data_type,
                                );
                                final_value = None;
                            }
                        }
                    }
                }

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
