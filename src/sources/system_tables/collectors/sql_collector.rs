use std::collections::HashMap;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::Value;
use sqlx::{Column, Row};
use tracing::{debug, info, warn};

use crate::sources::system_tables::data_collector::{
    CollectionError, CollectionMetadata, CollectionMethod, CollectionResult, CollectorConfig,
    CollectorConfigType, DataCollector,
};
use crate::sources::system_tables::TableConfig;

/// SQL-based data collector using MySQL protocol
pub struct SqlCollector {
    config: CollectorConfig,
    pool: Option<sqlx::mysql::MySqlPool>,
}

impl SqlCollector {
    /// Create a new SQL collector
    pub fn new(config: CollectorConfig) -> Result<Self, CollectionError> {
        // Validate that we have SQL config
        match &config.config_type {
            CollectorConfigType::Sql { .. } => (),
            _ => {
                return Err(CollectionError::ConfigurationError(
                    "Invalid config type for SqlCollector".to_string(),
                ))
            }
        }

        Ok(Self { config, pool: None })
    }

    /// Build MySQL connection pool
    async fn create_connection_pool(&self) -> Result<sqlx::mysql::MySqlPool, CollectionError> {
        let database_config = match &self.config.config_type {
            CollectorConfigType::Sql { database_config } => database_config,
            _ => {
                return Err(CollectionError::ConfigurationError(
                    "SQL collector requires SQL configuration".to_string(),
                ))
            }
        };

        let mut url = format!(
            "mysql://{}:{}@{}:{}/{}",
            database_config.username,
            database_config.password,
            database_config.host,
            database_config.port,
            database_config.database
        );

        // Add TLS parameters if database TLS is configured
        if let Some(ref tls_config) = database_config.tls {
            let mut tls_params = Vec::new();

            // Set SSL mode based on verification settings
            if tls_config.verify_certificate.unwrap_or(true) {
                if tls_config.verify_hostname.unwrap_or(true) {
                    tls_params.push("ssl-mode=VERIFY_IDENTITY".to_string());
                } else {
                    tls_params.push("ssl-mode=VERIFY_CA".to_string());
                }
            } else {
                tls_params.push("ssl-mode=REQUIRED".to_string());
            }

            // Add CA certificate if provided
            if let Some(ref ca_file) = tls_config.ca_file {
                tls_params.push(format!("ssl-ca={}", ca_file.display()));
            }

            // Add client certificate if provided
            if let Some(ref crt_file) = tls_config.crt_file {
                tls_params.push(format!("ssl-cert={}", crt_file.display()));
            }

            // Add client key if provided
            if let Some(ref key_file) = tls_config.key_file {
                tls_params.push(format!("ssl-key={}", key_file.display()));
            }

            if !tls_params.is_empty() {
                url.push('?');
                url.push_str(&tls_params.join("&"));
            }

            info!("Creating SQL connection pool with TLS enabled");
        } else {
            info!("Creating SQL connection pool without TLS");
        }

        let pool = sqlx::mysql::MySqlPoolOptions::new()
            .max_connections(database_config.max_connections.unwrap_or(10))
            .acquire_timeout(Duration::from_secs(
                database_config.connect_timeout.unwrap_or(30),
            ))
            .connect(&url)
            .await
            .map_err(|e| {
                CollectionError::ConnectionError(format!("Failed to create pool: {}", e))
            })?;

        Ok(pool)
    }

    /// Get table schema information
    async fn get_table_schema(
        &self,
        table: &TableConfig,
        pool: &sqlx::mysql::MySqlPool,
    ) -> Result<HashMap<String, (String, bool)>, CollectionError> {
        let schema_sql = format!(
            "SHOW COLUMNS FROM {}.{}",
            table.source_schema, table.source_table
        );

        debug!("Getting table schema: {}", schema_sql);

        let schema_rows = sqlx::query(&schema_sql)
            .fetch_all(pool)
            .await
            .map_err(|e| CollectionError::QueryError(format!("Schema query failed: {}", e)))?;

        let mut column_types = HashMap::new();

        for row in schema_rows {
            let field_name: String = row.try_get("Field").map_err(|e| {
                CollectionError::ParseError(format!("Failed to get field name: {}", e))
            })?;
            let field_type: String = row.try_get("Type").map_err(|e| {
                CollectionError::ParseError(format!("Failed to get field type: {}", e))
            })?;
            let is_nullable: String = row.try_get("Null").map_err(|e| {
                CollectionError::ParseError(format!("Failed to get nullable info: {}", e))
            })?;

            debug!(
                "Column schema: {} -> {} (nullable: {})",
                field_name, field_type, is_nullable
            );
            column_types.insert(field_name, (field_type, is_nullable == "YES"));
        }

        Ok(column_types)
    }

    /// Query data from a TiDB table
    async fn query_table_data(
        &self,
        table: &TableConfig,
        pool: &sqlx::mysql::MySqlPool,
        column_types: &HashMap<String, (String, bool)>,
    ) -> Result<Vec<HashMap<String, Value>>, CollectionError> {
        // Build SQL query
        let sql = if let Some(where_clause) = &table.where_clause {
            format!(
                "SELECT * FROM {}.{} WHERE {}",
                table.source_schema, table.source_table, where_clause
            )
        } else {
            format!(
                "SELECT * FROM {}.{}",
                table.source_schema, table.source_table
            )
        };

        debug!("Executing query: {}", sql);

        // Execute query
        let rows = sqlx::query(&sql)
            .fetch_all(pool)
            .await
            .map_err(|e| CollectionError::QueryError(format!("Data query failed: {}", e)))?;

        debug!(
            "Query returned {} rows for {}.{}",
            rows.len(),
            table.source_schema,
            table.source_table
        );

        // Convert rows to HashMap format using schema information
        let mut result = Vec::new();
        for row in rows.iter() {
            let mut map = HashMap::new();

            for (i, column) in row.columns().iter().enumerate() {
                let column_name = column.name().to_string();

                // Convert value based on MySQL schema
                let value = self.convert_mysql_value(&row, i, &column_name, column_types)?;
                map.insert(column_name, value);
            }
            result.push(map);
        }

        Ok(result)
    }

    /// Convert MySQL row value to JSON Value using schema information
    fn convert_mysql_value(
        &self,
        row: &sqlx::mysql::MySqlRow,
        column_index: usize,
        column_name: &str,
        column_types: &HashMap<String, (String, bool)>,
    ) -> Result<Value, CollectionError> {
        if let Some((mysql_type, _is_nullable)) = column_types.get(column_name) {
            let mysql_type_lower = mysql_type.to_lowercase();

            // Integer types
            if mysql_type_lower.contains("int") || mysql_type_lower.contains("bigint") {
                if mysql_type_lower.contains("unsigned") {
                    // Unsigned integer
                    match row.try_get::<u64, _>(column_index) {
                        Ok(v) => Ok(Value::Number((v as i64).into())),
                        Err(_) => self.try_parse_string_as_number(row, column_index),
                    }
                } else {
                    // Signed integer
                    match row.try_get::<i64, _>(column_index) {
                        Ok(v) => Ok(Value::Number(v.into())),
                        Err(_) => self.try_parse_string_as_number(row, column_index),
                    }
                }
            }
            // Float types
            else if mysql_type_lower.contains("decimal")
                || mysql_type_lower.contains("float")
                || mysql_type_lower.contains("double")
                || mysql_type_lower.contains("real")
            {
                match row.try_get::<f64, _>(column_index) {
                    Ok(v) => Ok(Value::Number(
                        serde_json::Number::from_f64(v)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    )),
                    Err(_) => self.try_parse_string_as_number(row, column_index),
                }
            }
            // Timestamp and datetime types
            else if mysql_type_lower.contains("timestamp")
                || mysql_type_lower.contains("datetime")
            {
                // Try NaiveDateTime first (proper type for MySQL TIMESTAMP)
                match row.try_get::<chrono::NaiveDateTime, _>(column_index) {
                    Ok(dt) => {
                        let timestamp_str = dt.format("%Y-%m-%d %H:%M:%S").to_string();
                        Ok(Value::String(timestamp_str))
                    }
                    Err(_) => {
                        // Try as optional NaiveDateTime for nullable fields
                        match row.try_get::<Option<chrono::NaiveDateTime>, _>(column_index) {
                            Ok(Some(dt)) => {
                                let timestamp_str = dt.format("%Y-%m-%d %H:%M:%S").to_string();
                                Ok(Value::String(timestamp_str))
                            }
                            Ok(None) => Ok(Value::Null),
                            Err(_) => {
                                // Try DateTime<Utc> for UTC timestamps
                                match row.try_get::<chrono::DateTime<chrono::Utc>, _>(column_index)
                                {
                                    Ok(dt) => {
                                        let timestamp_str =
                                            dt.format("%Y-%m-%d %H:%M:%S").to_string();
                                        Ok(Value::String(timestamp_str))
                                    }
                                    Err(_) => {
                                        // Final fallback: try as string
                                        match row.try_get::<String, _>(column_index) {
                                            Ok(v) => Ok(Value::String(v)),
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
                self.try_get_as_string_first(row, column_index)
            }
        } else {
            // Fallback if schema not found
            self.try_simple_conversion(row, column_index)
        }
    }

    /// Try to parse string as number, fallback to string
    fn try_parse_string_as_number(
        &self,
        row: &sqlx::mysql::MySqlRow,
        column_index: usize,
    ) -> Result<Value, CollectionError> {
        match row.try_get::<String, _>(column_index) {
            Ok(s) => {
                if let Ok(int_val) = s.parse::<i64>() {
                    Ok(Value::Number(int_val.into()))
                } else if let Ok(uint_val) = s.parse::<u64>() {
                    Ok(Value::Number((uint_val as i64).into()))
                } else if let Ok(float_val) = s.parse::<f64>() {
                    Ok(Value::Number(
                        serde_json::Number::from_f64(float_val)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ))
                } else {
                    Ok(Value::String(s))
                }
            }
            Err(_) => Ok(Value::Null),
        }
    }

    /// Try to get as string first, with numeric fallback
    fn try_get_as_string_first(
        &self,
        row: &sqlx::mysql::MySqlRow,
        column_index: usize,
    ) -> Result<Value, CollectionError> {
        match row.try_get::<String, _>(column_index) {
            Ok(v) => Ok(Value::String(v)),
            Err(_) => self.try_simple_conversion(row, column_index),
        }
    }

    /// Simple type conversion fallback
    fn try_simple_conversion(
        &self,
        row: &sqlx::mysql::MySqlRow,
        column_index: usize,
    ) -> Result<Value, CollectionError> {
        match row.try_get::<i64, _>(column_index) {
            Ok(v) => Ok(Value::Number(v.into())),
            Err(_) => match row.try_get::<f64, _>(column_index) {
                Ok(v) => Ok(Value::Number(
                    serde_json::Number::from_f64(v).unwrap_or_else(|| serde_json::Number::from(0)),
                )),
                Err(_) => match row.try_get::<String, _>(column_index) {
                    Ok(v) => Ok(Value::String(v)),
                    Err(_) => Ok(Value::Null),
                },
            },
        }
    }
}

#[async_trait]
impl DataCollector for SqlCollector {
    fn collection_method(&self) -> CollectionMethod {
        CollectionMethod::Sql
    }

    fn can_collect_table(&self, _table: &TableConfig) -> bool {
        // SQL collector can handle any table
        true
    }

    async fn initialize(&mut self) -> Result<(), CollectionError> {
        info!(
            "Initializing SQL collector for instance: {}",
            self.config.instance
        );

        let pool = self.create_connection_pool().await?;
        self.pool = Some(pool);

        info!("SQL collector initialized successfully");
        Ok(())
    }

    async fn collect_table_data(
        &self,
        table: &TableConfig,
    ) -> Result<CollectionResult, CollectionError> {
        let start_time = Instant::now();
        let timestamp = chrono::Utc::now();

        let pool = self.pool.as_ref().ok_or_else(|| {
            CollectionError::ConfigurationError("Pool not initialized".to_string())
        })?;

        // Get table schema
        let column_types = self.get_table_schema(table, pool).await?;

        // Query table data
        let data = self.query_table_data(table, pool, &column_types).await?;

        let duration = start_time.elapsed();
        let row_count = data.len();

        // Create metadata
        let mut extra = HashMap::new();
        extra.insert(
            "schema_columns".to_string(),
            Value::Number(column_types.len().into()),
        );

        let metadata = CollectionMetadata {
            instance: self.config.instance.clone(),
            table_config: table.clone(),
            collection_method: CollectionMethod::Sql,
            timestamp,
            row_count,
            duration_ms: duration.as_millis() as u64,
            extra,
        };

        info!(
            "SQL collection completed for table {}: {} rows in {}ms",
            table.source_table,
            row_count,
            duration.as_millis()
        );

        Ok(CollectionResult { data, metadata })
    }

    async fn health_check(&self) -> Result<(), CollectionError> {
        if let Some(pool) = &self.pool {
            sqlx::query("SELECT 1").fetch_one(pool).await.map_err(|e| {
                CollectionError::ConnectionError(format!("Health check failed: {}", e))
            })?;
            Ok(())
        } else {
            Err(CollectionError::ConfigurationError(
                "Pool not initialized".to_string(),
            ))
        }
    }
}
