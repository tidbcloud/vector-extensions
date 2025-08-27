use std::collections::HashMap;
use std::time::Duration;

use serde_json::Value;
use vector::SourceSender;
use vector_lib::event::{Event, LogEvent};
use sqlx::{Row, Column};

use crate::sources::system_tables::{DatabaseConfig, CollectionConfig, TableConfig, CollectionInterval};

/// Collector for a specific TiDB instance
pub struct Collector {
    instance: String,
    database_config: DatabaseConfig,
    collection_config: CollectionConfig,
    tables: Vec<TableConfig>,
    out: SourceSender,
}

impl Collector {
    /// Create a new collector
    pub fn new(
        instance: String,
        database_config: DatabaseConfig,
        collection_config: CollectionConfig,
        tables: Vec<TableConfig>,
        out: SourceSender,
    ) -> Self {
        Self {
            instance,
            database_config,
            collection_config,
            tables,
            out,
        }
    }

    /// Run the collector for all enabled tables
    pub async fn run(self) {
        let enabled_tables: Vec<&str> = self.tables.iter()
            .filter(|t| t.enabled)
            .map(|t| t.source_table.as_str())
            .collect();

        info!("Starting collector for {} with {} tables: [{}]",
              self.instance, enabled_tables.len(), enabled_tables.join(", "));

        // Start collection tasks for each enabled table
        for table in &self.tables {
            if table.enabled {
                let table_config = table.clone();
                let collection_config = self.collection_config.clone();
                let out = self.out.clone();
                let instance = self.instance.clone();
                let database_config = self.database_config.clone();

                tokio::spawn(async move {
                    Self::collect_table_data(
                        table_config,
                        collection_config,
                        database_config,
                        instance,
                        out,
                    ).await;
                });
            }
        }
    }

    /// Collect data from a specific table
    async fn collect_table_data(
        table: TableConfig,
        collection_config: CollectionConfig,
        database_config: DatabaseConfig,
        instance: String,
        mut out: SourceSender,
    ) {
        let interval = match table.collection_interval.as_str() {
            "short" => collection_config.short_interval,
            "long" => collection_config.long_interval,
            custom if custom.starts_with("custom=") => {
                if let Some(seconds) = custom.strip_prefix("custom=") {
                    seconds.parse::<u64>().unwrap_or(collection_config.short_interval)
                } else {
                    collection_config.short_interval
                }
            }
            _ => collection_config.short_interval,
        };

        info!("Starting collection for table {} with {}s interval from instance {}", table.source_table, interval, instance);

        // Get table schema once at startup
        let column_types = match Self::get_table_schema(&table, &database_config).await {
            Ok(schema) => {
                debug!("Retrieved schema for table {} from instance {}: {} columns", table.source_table, instance, schema.len());
                schema
            }
            Err(e) => {
                error!("Failed to get schema for table {} from instance {}: {}", table.source_table, instance, e);
                return;
            }
        };

        let mut interval_timer = tokio::time::interval(Duration::from_secs(interval));
        
        // Batch processing variables for memory optimization
        let mut event_buffer: Vec<Event> = Vec::with_capacity(100);  // Pre-allocate capacity
        let batch_size = 1000;  // Send batch every 50 events
        let max_buffer_time = Duration::from_secs(15);  // Max wait time 30 seconds
        let mut last_send_time = std::time::Instant::now();
        let mut is_first_batch = true; // Track if this is the first batch for schema inclusion
        
        loop {
            interval_timer.tick().await;
            
            // Query the table data using cached schema for proper type conversion
            match Self::query_table_data(&table, &database_config, &column_types, &instance).await {
                Ok(data) => {
                    if !data.is_empty() {
                        let data_len = data.len();
                        
                        // Buffer events instead of sending immediately
                        for (index, row) in data.into_iter().enumerate() {
                            // Include schema only for the first event of the first batch
                            let include_schema = is_first_batch && index == 0;
                            let event = Self::create_event(&table, &instance, row, &column_types, include_schema);
                            event_buffer.push(event);
                        }
                        
                        debug!("Collected {} rows from table {}, buffer size: {}", 
                               data_len, table.source_table, event_buffer.len());
                        
                        // Check if we should send batch (size or time threshold)
                        let should_send = event_buffer.len() >= batch_size || 
                                        last_send_time.elapsed() >= max_buffer_time;
                        
                        if should_send && !event_buffer.is_empty() {
                            // Send all buffered events in batch
                            let events_to_send: Vec<Event> = event_buffer.drain(..).collect();
                            let batch_size_sent = events_to_send.len();
                            
                            // Use send_batch for efficient bulk transmission
                            if let Err(e) = out.send_batch(events_to_send).await {
                                error!("Failed to send batch for table {} from instance {}: {}", table.source_table, instance, e);
                            } else {
                                info!("Sent batch of {} events for table {} from instance {}", batch_size_sent, table.source_table, instance);
                                last_send_time = std::time::Instant::now();
                                is_first_batch = false; // Schema sent, future batches don't need it
                                
                                // Memory optimization: shrink buffer if it gets too large
                                if event_buffer.capacity() > 200 {
                                    event_buffer.shrink_to(100);
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to query table {}: {}", table.source_table, e);
                }
            }
        }
    }

        /// Get table schema information
    async fn get_table_schema(
        table: &TableConfig,
        database_config: &DatabaseConfig,
    ) -> Result<HashMap<String, (String, bool)>, Box<dyn std::error::Error + Send + Sync>> {
        // Build connection string
        let url = format!(
            "mysql://{}:{}@{}:{}/{}",
            database_config.username,
            database_config.password,
            database_config.host,
            database_config.port,
            database_config.database
        );
        
        // Create connection pool
        let pool = sqlx::mysql::MySqlPoolOptions::new()
            .max_connections(database_config.max_connections.unwrap_or(5))
            .connect(&url)
            .await?;
        
        // Get table schema information
        let schema_sql = format!(
            "SHOW COLUMNS FROM {}.{}",
            table.source_schema, table.source_table
        );
        
        debug!("Getting table schema: {}", schema_sql);
        
        let schema_rows = sqlx::query(&schema_sql).fetch_all(&pool).await?;
        let mut column_types = HashMap::new();
        
        for row in schema_rows {
            let field_name: String = row.try_get("Field")?;
            let field_type: String = row.try_get("Type")?;
            let is_nullable: String = row.try_get("Null")?;
            
            debug!("Column schema: {} -> {} (nullable: {})", field_name, field_type, is_nullable);
            column_types.insert(field_name, (field_type, is_nullable == "YES"));
        }
        
        Ok(column_types)
    }

    /// Query data from a TiDB table using cached schema for proper type conversion
    async fn query_table_data(
        table: &TableConfig,
        database_config: &DatabaseConfig,
        column_types: &HashMap<String, (String, bool)>,
        instance: &str,
    ) -> Result<Vec<HashMap<String, Value>>, Box<dyn std::error::Error + Send + Sync>> {
        // Build connection string
        let url = format!(
            "mysql://{}:{}@{}:{}/{}",
            database_config.username,
            database_config.password,
            database_config.host,
            database_config.port,
            database_config.database
        );
        
        // Create connection pool
        let pool = sqlx::mysql::MySqlPoolOptions::new()
            .max_connections(database_config.max_connections.unwrap_or(5))
            .connect(&url)
            .await?;
        
        // Build SQL query
        let sql = if let Some(where_clause) = &table.where_clause {
            format!("SELECT * FROM {}.{} WHERE {}", table.source_schema, table.source_table, where_clause)
        } else {
            format!("SELECT * FROM {}.{}", table.source_schema, table.source_table)
        };
        
        // Execute query
        let rows = sqlx::query(&sql).fetch_all(&pool).await?;
        debug!("Query returned {} rows for {}.{} from instance {}", rows.len(), table.source_schema, table.source_table, instance);
        
        // Convert rows to HashMap format using cached schema
        let mut result = Vec::new();
        for row in rows.iter() {
            let mut map = HashMap::new();
            
            for (i, column) in row.columns().iter().enumerate() {
                let column_name = column.name().to_string();
                
                // Convert value based on cached MySQL schema
                let value = Self::convert_mysql_value(&row, i, &column_name, column_types)?;
                map.insert(column_name, value);
            }
            result.push(map);
        }
        
        Ok(result)
    }

    /// Convert MySQL row value to JSON Value using schema information
    fn convert_mysql_value(
        row: &sqlx::mysql::MySqlRow,
        column_index: usize,
        column_name: &str,
        column_types: &HashMap<String, (String, bool)>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        if let Some((mysql_type, _is_nullable)) = column_types.get(column_name) {
            let mysql_type_lower = mysql_type.to_lowercase();
            
            // Integer types
            if mysql_type_lower.contains("int") || mysql_type_lower.contains("bigint") {
                if mysql_type_lower.contains("unsigned") {
                    // Unsigned integer
                    match row.try_get::<u64, _>(column_index) {
                        Ok(v) => Ok(Value::Number((v as i64).into())),
                        Err(_) => Self::try_parse_string_as_number(row, column_index),
                    }
                } else {
                    // Signed integer
                    match row.try_get::<i64, _>(column_index) {
                        Ok(v) => Ok(Value::Number(v.into())),
                        Err(_) => Self::try_parse_string_as_number(row, column_index),
                    }
                }
            }
            // Float types
            else if mysql_type_lower.contains("decimal") || mysql_type_lower.contains("float") || 
                     mysql_type_lower.contains("double") || mysql_type_lower.contains("real") {
                match row.try_get::<f64, _>(column_index) {
                    Ok(v) => Ok(Value::Number(
                        serde_json::Number::from_f64(v).unwrap_or_else(|| serde_json::Number::from(0))
                    )),
                    Err(_) => Self::try_parse_string_as_number(row, column_index),
                }
            }
            // Timestamp and datetime types
            else if mysql_type_lower.contains("timestamp") || mysql_type_lower.contains("datetime") {
                // Try NaiveDateTime first (proper type for MySQL TIMESTAMP)
                match row.try_get::<chrono::NaiveDateTime, _>(column_index) {
                    Ok(dt) => {
                        let timestamp_str = dt.format("%Y-%m-%d %H:%M:%S").to_string();
                        Ok(Value::String(timestamp_str))
                    },
                    Err(_) => {
                        // Try as optional NaiveDateTime for nullable fields
                        match row.try_get::<Option<chrono::NaiveDateTime>, _>(column_index) {
                            Ok(Some(dt)) => {
                                let timestamp_str = dt.format("%Y-%m-%d %H:%M:%S").to_string();
                                Ok(Value::String(timestamp_str))
                            },
                            Ok(None) => {
                                Ok(Value::Null)
                            },
                            Err(_) => {
                                // Try DateTime<Utc> for UTC timestamps
                                match row.try_get::<chrono::DateTime<chrono::Utc>, _>(column_index) {
                                    Ok(dt) => {
                                        let timestamp_str = dt.format("%Y-%m-%d %H:%M:%S").to_string();
                                        Ok(Value::String(timestamp_str))
                                    },
                                    Err(_) => {
                                        // Final fallback: try as string if it's actually stored as string
                                        match row.try_get::<String, _>(column_index) {
                                            Ok(v) => {
                                                Ok(Value::String(v))
                                            },
                                            Err(_) => {
                                                warn!("All timestamp retrieval methods failed for column '{}'", column_name);
                                                Ok(Value::Null)
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            // String and other types
            else {
                Self::try_get_as_string_first(row, column_index)
            }
        } else {
            // Fallback if schema not found
            Self::try_simple_conversion(row, column_index)
        }
    }

    /// Try to parse string as number, fallback to string
    fn try_parse_string_as_number(
        row: &sqlx::mysql::MySqlRow,
        column_index: usize,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        match row.try_get::<String, _>(column_index) {
            Ok(s) => {
                if let Ok(int_val) = s.parse::<i64>() {
                    Ok(Value::Number(int_val.into()))
                } else if let Ok(uint_val) = s.parse::<u64>() {
                    Ok(Value::Number((uint_val as i64).into()))
                } else if let Ok(float_val) = s.parse::<f64>() {
                    Ok(Value::Number(
                        serde_json::Number::from_f64(float_val).unwrap_or_else(|| serde_json::Number::from(0))
                    ))
                } else {
                    Ok(Value::String(s))
                }
            },
            Err(_) => Ok(Value::Null),
        }
    }

    /// Try to get as string first, with numeric fallback
    fn try_get_as_string_first(
        row: &sqlx::mysql::MySqlRow,
        column_index: usize,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        match row.try_get::<String, _>(column_index) {
            Ok(v) => Ok(Value::String(v)),
            Err(_) => Self::try_simple_conversion(row, column_index),
        }
    }

    /// Simple type conversion fallback
    fn try_simple_conversion(
        row: &sqlx::mysql::MySqlRow,
        column_index: usize,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        match row.try_get::<i64, _>(column_index) {
            Ok(v) => Ok(Value::Number(v.into())),
            Err(_) => match row.try_get::<f64, _>(column_index) {
                Ok(v) => Ok(Value::Number(
                    serde_json::Number::from_f64(v).unwrap_or_else(|| serde_json::Number::from(0))
                )),
                Err(_) => match row.try_get::<String, _>(column_index) {
                    Ok(v) => Ok(Value::String(v)),
                    Err(_) => Ok(Value::Null),
                },
            },
        }
    }

        /// Convert a JSON value based on MySQL schema information
    fn convert_value_by_schema(
        value: Value,
        mysql_type: &str,
        _is_nullable: bool,
        _field_name: &str,
    ) -> Value {
        // Handle null values
        if value == Value::Null {
            return Value::Null;
        }
        
        // Convert based on MySQL type
        let mysql_type_lower = mysql_type.to_lowercase();
        
        if mysql_type_lower.contains("int") || mysql_type_lower.contains("bigint") {
            // Integer types - try to convert string to int if needed
            match &value {
                Value::String(s) => {
                    if let Ok(i) = s.parse::<i64>() {
                        Value::Number(i.into())
                    } else {
                        value.clone() // Keep original if parsing fails
                    }
                }
                Value::Number(n) => Value::Number(n.clone()),
                _ => value,
            }
        } else if mysql_type_lower.contains("decimal") || mysql_type_lower.contains("float") || 
                  mysql_type_lower.contains("double") || mysql_type_lower.contains("real") {
            // Floating point types - try to convert string to float if needed
            match &value {
                Value::String(s) => {
                    if let Ok(f) = s.parse::<f64>() {
                        Value::Number(serde_json::Number::from_f64(f).unwrap_or_else(|| serde_json::Number::from(0)))
                    } else {
                        value.clone() // Keep original if parsing fails
                    }
                }
                Value::Number(n) => Value::Number(n.clone()),
                _ => value,
            }
        } else {
            // String types and others - keep as is
            value
        }
    }

    /// Convert a database value to JSON Value based on MySQL column type
    fn convert_value_by_type(
        row: &sqlx::mysql::MySqlRow,
        index: usize,
        mysql_type: &str,
        is_nullable: bool,
    ) -> Value {
        // Handle null values first
        if row.try_get::<Option<String>, _>(index).unwrap_or(None).is_none() {
            return Value::Null;
        }
        
        // Convert based on MySQL type
        let mysql_type_lower = mysql_type.to_lowercase();
        
        if mysql_type_lower.contains("int") || mysql_type_lower.contains("bigint") {
            // Integer types
            match row.try_get::<i64, _>(index) {
                Ok(v) => Value::Number(v.into()),
                Err(_) => Value::Null,
            }
        } else if mysql_type_lower.contains("decimal") || mysql_type_lower.contains("float") || 
                  mysql_type_lower.contains("double") || mysql_type_lower.contains("real") {
            // Floating point types
            match row.try_get::<f64, _>(index) {
                Ok(v) => Value::Number(serde_json::Number::from_f64(v).unwrap_or_else(|| serde_json::Number::from(0))),
                Err(_) => Value::Null,
            }
        } else if mysql_type_lower.contains("datetime") || mysql_type_lower.contains("timestamp") {
            // DateTime types
            match row.try_get::<String, _>(index) {
                Ok(v) => Value::String(v),
                Err(_) => Value::Null,
            }
        } else if mysql_type_lower.contains("date") {
            // Date types
            match row.try_get::<String, _>(index) {
                Ok(v) => Value::String(v),
                Err(_) => Value::Null,
            }
        } else if mysql_type_lower.contains("time") {
            // Time types
            match row.try_get::<String, _>(index) {
                Ok(v) => Value::String(v),
                Err(_) => Value::Null,
            }
        } else if mysql_type_lower.contains("json") {
            // JSON types
            match row.try_get::<String, _>(index) {
                Ok(v) => Value::String(v),
                Err(_) => Value::Null,
            }
        } else {
            // Default to string for varchar, text, char, etc.
            match row.try_get::<String, _>(index) {
                Ok(v) => Value::String(v),
                Err(_) => Value::Null,
            }
        }
    }

    /// Create a Vector event from table data
    fn create_event(
        table: &TableConfig,
        instance: &str,
        data: HashMap<String, Value>,
        column_types: &HashMap<String, (String, bool)>,
        include_schema: bool,
    ) -> Event {
        let mut event = Event::Log(LogEvent::default());
        let log = event.as_mut_log();
        
        // Add metadata with Vector prefix (ensure all fields have values)
        log.insert("_vector_table", table.dest_table.clone());
        log.insert("_vector_source_table", table.source_table.clone());
        log.insert("_vector_source_schema", table.source_schema.clone());
        log.insert("_vector_instance", instance.to_string());
        log.insert("_vector_timestamp", chrono::Utc::now().to_rfc3339());
        
        // Only log metadata for first event of each batch to reduce noise
        if include_schema {
            info!("Event metadata: _vector_table={}, _vector_source_table={}, _vector_source_schema={}, _vector_instance={}, _vector_timestamp={}", 
                  table.dest_table, table.source_table, table.source_schema, instance, chrono::Utc::now().to_rfc3339());
        }
        
        // Add schema metadata for sink to use (only on first event of each table)
        if include_schema {
            let mut schema_info = serde_json::Map::new();
            for (column_name, (mysql_type, is_nullable)) in column_types {
                schema_info.insert(column_name.clone(), serde_json::json!({
                    "mysql_type": mysql_type,
                    "is_nullable": is_nullable
                }));
            }
            let schema_field_count = schema_info.len();
            log.insert("_schema_metadata", serde_json::Value::Object(schema_info));
            info!("Included schema metadata for table {} ({} fields)", table.dest_table, schema_field_count);
        }
        
        // Add the actual data with proper type conversion based on schema
        for (key, value) in data {
            // Convert value based on cached schema if available
            let converted_value = if let Some((mysql_type, is_nullable)) = column_types.get(&key) {
                let converted = Self::convert_value_by_schema(value.clone(), mysql_type, *is_nullable, &key);
                
                               // Log timestamp fields at debug level for troubleshooting if needed
               if mysql_type.to_lowercase().contains("timestamp") {
                   debug!("Timestamp field '{}' (type: {}): original={:?}, converted={:?}",
                         key, mysql_type, value, converted);
               }
                
                converted
            } else {
                value // Keep original value if no schema info
            };
            
            log.insert(key.as_str(), converted_value);
        }
        
        event
    }
}
