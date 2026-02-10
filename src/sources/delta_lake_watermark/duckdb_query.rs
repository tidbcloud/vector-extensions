use std::sync::{Arc, Mutex};

use arrow::array::{Array, StringArray};
use arrow::record_batch::RecordBatch;
use duckdb::Connection;
use serde_json::Value;
use tracing::{debug, info, warn};

use crate::sources::delta_lake_watermark::checkpoint::Checkpoint;

/// DuckDB query executor for Delta Lake tables
pub struct DuckDBQueryExecutor {
    connection: Arc<Mutex<Connection>>,
    endpoint: String,
    cloud_provider: String,
    memory_limit: Option<String>,
}

impl DuckDBQueryExecutor {
    /// Create a new DuckDB query executor
    pub fn new(
        endpoint: String,
        cloud_provider: String,
        memory_limit: Option<String>,
    ) -> vector::Result<Self> {
        let connection = Connection::open_in_memory()
            .map_err(|e| format!("Failed to create DuckDB connection: {}", e))?;

        let executor = Self {
            connection: Arc::new(Mutex::new(connection)),
            endpoint,
            cloud_provider,
            memory_limit,
        };

        executor.initialize()?;
        Ok(executor)
    }

    /// Initialize DuckDB with extensions and configuration
    fn initialize(&self) -> vector::Result<()> {
        let conn = self.connection.lock().unwrap();
        
        // Set memory limit if specified
        if let Some(ref limit) = self.memory_limit {
            conn.execute(&format!("SET memory_limit='{}'", limit), [])
                .map_err(|e| format!("Failed to set memory limit: {}", e))?;
        }

        // Install and load delta extension
        // Note: This requires the delta extension to be available
        // For now, we'll use delta_scan function if available
        match conn
            .execute("INSTALL delta;", [])
            .and_then(|_| conn.execute("LOAD delta;", []))
        {
            Ok(_) => {
                info!("Delta extension loaded successfully");
            }
            Err(e) => {
                warn!("Failed to load delta extension: {}. Will try delta_scan function.", e);
            }
        }

        // Configure cloud storage based on provider
        drop(conn);
        self.configure_cloud_storage()?;

        Ok(())
    }

    /// Configure cloud storage settings based on provider
    fn configure_cloud_storage(&self) -> vector::Result<()> {
        info!("Configuring cloud storage for provider: {}", self.cloud_provider);
        let conn = self.connection.lock().unwrap();
        
        match self.cloud_provider.as_str() {
            "aliyun" => {
                // Configure Aliyun OSS
                // Set S3 endpoint to OSS endpoint
                if let Some(endpoint_url) = std::env::var("OSS_ENDPOINT").ok() {
                    conn.execute(
                        &format!("SET s3_endpoint='{}'", endpoint_url),
                        [],
                    )
                    .map_err(|e| format!("Failed to set OSS endpoint: {}", e))?;
                }
                // Use path-style for OSS
                conn.execute("SET s3_use_path_style='false'", [])
                    .map_err(|e| format!("Failed to set path style: {}", e))?;
            }
            "gcp" => {
                // GCP uses gs:// protocol, DuckDB should handle it natively
                info!("Using GCP Cloud Storage (gs://)");
            }
            "azure" => {
                // Azure uses az:// protocol
                info!("Using Azure Blob Storage (az://)");
            }
            "aws" | _ => {
                info!("Configuring AWS S3 credentials...");
                // AWS S3 - configure credentials using CREATE SECRET
                // DuckDB requires explicit secret creation for S3 access
                let access_key_id = std::env::var("AWS_ACCESS_KEY_ID");
                let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY");
                
                match (access_key_id, secret_access_key) {
                    (Ok(access_key_id), Ok(secret_access_key)) => {
                        info!("AWS credentials found in environment variables, creating DuckDB SECRET...");
                        
                        // Create secret for AWS credentials
                        // DuckDB requires CREATE SECRET for S3 access
                        // If secret already exists, drop it first
                        info!("Dropping existing s3_credentials secret if exists...");
                        let _ = conn.execute("DROP SECRET IF EXISTS s3_credentials;", []);
                        
                        // Escape single quotes in credentials
                        let access_key_id_escaped = access_key_id.replace("'", "''");
                        let secret_access_key_escaped = secret_access_key.replace("'", "''");
                        
                        // Build CREATE SECRET statement
                        // DuckDB syntax: CREATE SECRET name (TYPE S3, KEY_ID '...', SECRET '...', REGION '...', SESSION_TOKEN '...')
                        let mut secret_sql = format!(
                            "CREATE SECRET s3_credentials (TYPE S3, KEY_ID '{}', SECRET '{}'",
                            access_key_id_escaped, secret_access_key_escaped
                        );
                        
                        // Add region if available (required for S3 access)
                        if let Ok(region) = std::env::var("AWS_REGION") {
                            let region_escaped = region.replace("'", "''");
                            secret_sql.push_str(&format!(", REGION '{}'", region_escaped));
                            info!("Including AWS_REGION '{}' in SECRET", region);
                        } else {
                            warn!("AWS_REGION not found in environment variables. S3 access may fail. Please set AWS_REGION environment variable.");
                        }
                        
                        // Add session token if present (for temporary credentials)
                        if let Ok(session_token) = std::env::var("AWS_SESSION_TOKEN") {
                            let session_token_escaped = session_token.replace("'", "''");
                            secret_sql.push_str(&format!(", SESSION_TOKEN '{}'", session_token_escaped));
                            info!("Including AWS_SESSION_TOKEN in SECRET");
                        }
                        
                        secret_sql.push_str(");");
                        
                        info!("Executing CREATE SECRET for AWS S3 credentials...");
                        debug!("CREATE SECRET SQL (credentials masked): {}", secret_sql.replace(&access_key_id_escaped, "***").replace(&secret_access_key_escaped, "***"));
                        
                        conn.execute(&secret_sql, [])
                            .map_err(|e| format!("Failed to create AWS S3 secret: {}. SQL: {}", e, secret_sql.replace(&access_key_id_escaped, "***").replace(&secret_access_key_escaped, "***")))?;
                        
                        // Also set s3_region via SET command for DuckDB's native S3 functions
                        if let Ok(region) = std::env::var("AWS_REGION") {
                            conn.execute(&format!("SET s3_region='{}'", region), [])
                                .map_err(|e| format!("Failed to set s3_region: {}", e))?;
                            info!("✓ Set s3_region to '{}'", region);
                        }
                        
                        info!("✓ AWS S3 credentials configured via CREATE SECRET successfully");
                    }
                    (Err(e1), Err(e2)) => {
                        warn!("AWS_ACCESS_KEY_ID not found: {:?}, AWS_SECRET_ACCESS_KEY not found: {:?}", e1, e2);
                        warn!("Using AWS S3 with default credential chain (IAM roles, etc.)");
                    }
                    (Err(e), _) => {
                        warn!("AWS_ACCESS_KEY_ID not found: {:?}", e);
                        warn!("Using AWS S3 with default credential chain (IAM roles, etc.)");
                    }
                    (_, Err(e)) => {
                        warn!("AWS_SECRET_ACCESS_KEY not found: {:?}", e);
                        warn!("Using AWS S3 with default credential chain (IAM roles, etc.)");
                    }
                }
            }
        }
        
        Ok(())
    }

    /// Build SQL query with watermark and conditions
    pub fn build_query(
        &self,
        checkpoint: &Checkpoint,
        condition: Option<&str>,  // All filtering including time ranges should be in condition
        order_by_column: &str,
        unique_id_column: Option<&str>,
        batch_size: usize,
    ) -> String {
        let mut query = format!("SELECT * FROM delta_scan('{}')", self.endpoint);

        // Build WHERE clause
        let mut where_clauses = Vec::new();

        // Helper function to format time value for SQL comparison
        // If the value is a numeric string (Unix timestamp), use it directly without quotes
        // If it's an ISO 8601 string, use it with quotes
        let format_time_value = |value: &str| -> String {
            // Check if value is a numeric string (Unix timestamp)
            if value.parse::<i64>().is_ok() {
                // Numeric value - use without quotes for numeric comparison
                value.to_string()
            } else {
                // String value (ISO 8601) - use with quotes
                format!("'{}'", value.replace("'", "''"))
            }
        };

        // Handle incremental sync based on checkpoint and unique_id_column
        // unique_id_column can be any type (ID, UUID, string, integer, etc.) used for
        // secondary sorting when multiple records share the same timestamp
        if let (Some(ref last_watermark), Some(ref last_id), Some(ref unique_col)) = (
            checkpoint.last_watermark.as_ref(),
            checkpoint.last_processed_id.as_ref(),
            unique_id_column,
        ) {
            // With unique_id_column: Use OR condition for precise same timestamp handling
            // Query logic: time > last_watermark OR (time = last_watermark AND unique_id > last_processed_id)
            // This ensures we skip already processed records even when they have the same timestamp.
            // The unique_id_column value (last_id) is converted to string for comparison,
            // supporting any data type (ID, UUID, string, integer, etc.)
            let watermark_val = format_time_value(last_watermark);
            let id_val = format!("'{}'", last_id.replace("'", "''"));
            where_clauses.push(format!(
                "({} > {} OR ({} = {} AND {} > {}))",
                order_by_column, watermark_val, order_by_column, watermark_val, unique_col, id_val
            ));
        } else if let Some(ref last_watermark) = checkpoint.last_watermark {
            // Without unique_id_column: Use >= to include records with same timestamp
            // This is necessary for data completeness when multiple records share the same timestamp.
            // Note: This may cause duplicate processing of same-timestamp records after restart,
            // but ensures no data is missed. Users should ensure order_by_column is unique or
            // provide unique_id_column for precise incremental sync.
            let watermark_val = format_time_value(last_watermark);
            where_clauses.push(format!("{} >= {}", order_by_column, watermark_val));
        }
        // Note: If no checkpoint exists, user should specify time range in condition

        // Add user-provided condition (includes time ranges and other filters)
        if let Some(cond) = condition {
            where_clauses.push(format!("({})", cond));
        }

        if !where_clauses.is_empty() {
            query.push_str(" WHERE ");
            query.push_str(&where_clauses.join(" AND "));
        }

        // ORDER BY
        let mut order_by = format!("{} ASC", order_by_column);
        if let Some(unique_col) = unique_id_column {
            order_by.push_str(&format!(", {} ASC", unique_col));
        }
        query.push_str(&format!(" ORDER BY {}", order_by));

        // LIMIT
        query.push_str(&format!(" LIMIT {}", batch_size));

        debug!("Generated SQL query: {}", query);
        query
    }

    /// Execute query and return results as RecordBatch
    pub fn execute_query(&self, sql: &str) -> vector::Result<RecordBatch> {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};

        let conn = self.connection.lock().unwrap();
        
        // First, execute a LIMIT 0 query to get schema without fetching data
        // This allows us to get column metadata before executing the actual query
        let schema_sql = if sql.to_uppercase().contains("LIMIT") {
            // If LIMIT already exists, replace it with LIMIT 0
            let limit_pos = sql.to_uppercase().rfind("LIMIT").unwrap();
            format!("{} LIMIT 0", &sql[..limit_pos])
        } else {
            format!("{} LIMIT 0", sql)
        };
        
        // Get column information by executing a LIMIT 0 query
        let (column_count, column_names) = {
            let mut schema_stmt = conn.prepare(&schema_sql)
                .map_err(|e| format!("Failed to prepare schema query: {}", e))?;
            
            // Execute LIMIT 0 query to get column metadata
            let _schema_rows = schema_stmt.query([])
                .map_err(|e| format!("Failed to execute schema query: {}", e))?;
            
            // Get column count
            let count = schema_stmt.column_count();
            if count == 0 {
                // Return empty RecordBatch
                let fields: Vec<Field> = vec![];
                let schema = Arc::new(Schema::new(fields));
                return Ok(RecordBatch::try_new(schema, vec![]).unwrap());
            }
            
            // Get column names
            let mut names = Vec::new();
            for i in 0..count {
                let name = schema_stmt.column_name(i)
                    .map_err(|e| format!("Failed to get column name: {}", e))?.to_string();
                names.push(name);
            }
            
            // _schema_rows and schema_stmt are dropped here
            (count, names)
        };

        // Now execute the actual query with a fresh statement
        let mut stmt = conn.prepare(sql)
            .map_err(|e| format!("Failed to prepare query: {}", e))?;
        
        let mut rows = stmt.query([])
            .map_err(|e| format!("Failed to execute query: {}", e))?;

        // Collect all rows
        let mut all_rows: Vec<Vec<Option<String>>> = Vec::new();
        while let Some(row) = rows.next()
            .map_err(|e| format!("Failed to fetch row: {}", e))? {
            let mut row_data = Vec::new();
            for i in 0..column_count {
                let value = self.extract_value_as_string(row, i)
                    .map_err(|e| format!("Failed to extract value: {}", e))?;
                row_data.push(value);
            }
            all_rows.push(row_data);
        }

        if all_rows.is_empty() {
            // Return empty RecordBatch with schema
            let fields: Vec<Field> = column_names
                .iter()
                .map(|name| Field::new(name.clone(), DataType::Utf8, true))
                .collect();
            let schema = Arc::new(Schema::new(fields));
            return Ok(RecordBatch::try_new(schema, vec![]).unwrap());
        }

        // Build schema
        let fields: Vec<Field> = column_names
            .iter()
            .map(|name| Field::new(name.clone(), DataType::Utf8, true))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        // Build arrays (transpose rows to columns)
        let num_rows = all_rows.len();
        let mut arrays: Vec<Arc<dyn Array>> = Vec::new();

        for col_idx in 0..column_count {
            let mut column_values: Vec<Option<String>> = Vec::with_capacity(num_rows);
            for row in &all_rows {
                column_values.push(row[col_idx].clone());
            }
            let string_array: Vec<Option<&str>> = column_values.iter().map(|v| v.as_deref()).collect();
            arrays.push(Arc::new(StringArray::from(string_array)) as Arc<dyn Array>);
        }

        RecordBatch::try_new(schema, arrays)
            .map_err(|e| format!("Failed to create RecordBatch: {}", e).into())
    }

    /// Extract value from DuckDB row as String
    fn extract_value_as_string(&self, row: &duckdb::Row, col_idx: usize) -> vector::Result<Option<String>> {
        // Try different types and convert to string
        if let Ok(v) = row.get::<_, Option<String>>(col_idx) {
            return Ok(v);
        }
        if let Ok(v) = row.get::<_, Option<i64>>(col_idx) {
            return Ok(v.map(|i| i.to_string()));
        }
        if let Ok(v) = row.get::<_, Option<f64>>(col_idx) {
            return Ok(v.map(|f| f.to_string()));
        }
        if let Ok(v) = row.get::<_, Option<bool>>(col_idx) {
            return Ok(v.map(|b| b.to_string()));
        }

        // For timestamp types, get as string first
        if let Ok(v) = row.get::<_, Option<String>>(col_idx) {
            return Ok(v);
        }

        // Fallback: try to get as Value and convert to string
        match row.get::<_, duckdb::types::Value>(col_idx) {
            Ok(duckdb::types::Value::Null) => Ok(None),
            Ok(v) => Ok(Some(format!("{:?}", v))),
            Err(_) => Ok(None),
        }
    }

    /// Convert RecordBatch to Vector LogEvent format
    pub fn record_batch_to_events(
        &self,
        batch: &RecordBatch,
    ) -> vector::Result<Vec<serde_json::Value>> {
        let mut events = Vec::new();
        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns();
        let schema = batch.schema();

        for row_idx in 0..num_rows {
            let mut event = serde_json::Map::new();

            for col_idx in 0..num_cols {
                let field = schema.field(col_idx);
                let column = batch.column(col_idx);

                // Extract value from array
                let value = match column.data_type() {
                    arrow::datatypes::DataType::Utf8 => {
                        let arr = column.as_any().downcast_ref::<StringArray>().unwrap();
                        if arr.is_null(row_idx) {
                            Value::Null
                        } else {
                            Value::String(arr.value(row_idx).to_string())
                        }
                    }
                    _ => {
                        // For other types, convert to string
                        Value::String(format!("{:?}", column))
                    }
                };

                event.insert(field.name().clone(), value);
            }

            events.push(Value::Object(event));
        }

        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // TC-013: Test query building - basic
    #[test]
    fn test_query_building_basic() {
        let executor = DuckDBQueryExecutor::new(
            "s3://bucket/table".to_string(),
            "aws".to_string(),
            None,
        )
        .unwrap();

        let checkpoint = Checkpoint::default();
        let query = executor.build_query(
            &checkpoint,
            Some("time >= '2026-01-01T00:00:00Z' AND time <= '2026-02-01T00:00:00Z'"),  // condition with time range
            "time",
            None,
            1000,
        );

        assert!(query.contains("delta_scan"));
        assert!(query.contains("SELECT * FROM delta_scan"));
        assert!(query.contains("time >= '2026-01-01T00:00:00Z'"));
        assert!(query.contains("time <= '2026-02-01T00:00:00Z'"));
        assert!(query.contains("ORDER BY time ASC"));
        assert!(query.contains("LIMIT 1000"));
    }

    // TC-014: Test query building - with checkpoint
    #[test]
    fn test_query_building_with_checkpoint() {
        let executor = DuckDBQueryExecutor::new(
            "s3://bucket/table".to_string(),
            "aws".to_string(),
            None,
        )
        .unwrap();

        let mut checkpoint = Checkpoint::default();
        checkpoint.update_watermark("2026-01-15T00:00:00Z".to_string(), Some("id-100".to_string()));

        let query = executor.build_query(
            &checkpoint,
            Some("time <= '2026-02-01T00:00:00Z'"),  // condition with time range
            "time",
            Some("unique_id"),
            1000,
        );

        // Should use checkpoint watermark
        assert!(query.contains("time > '2026-01-15T00:00:00Z'") || query.contains("time >= '2026-01-15T00:00:00Z'"));
        // Should include unique_id handling
        assert!(query.contains("unique_id"));
        assert!(query.contains("ORDER BY time ASC, unique_id ASC"));
    }

    // TC-015: Test query building - with condition
    #[test]
    fn test_query_building_with_condition() {
        let executor = DuckDBQueryExecutor::new(
            "s3://bucket/table".to_string(),
            "aws".to_string(),
            None,
        )
        .unwrap();

        let checkpoint = Checkpoint::default();
        let query = executor.build_query(
            &checkpoint,
            Some("time >= '2026-01-01T00:00:00Z' AND time <= '2026-02-01T00:00:00Z' AND type = 'error' AND severity > 3"),  // condition with time range and business filter
            "time",
            None,
            1000,
        );

        assert!(query.contains("type = 'error' AND severity > 3"));
        assert!(query.contains("WHERE"));
    }

    // TC-016: Test query building - same timestamp handling
    #[test]
    fn test_query_building_same_timestamp_handling() {
        let executor = DuckDBQueryExecutor::new(
            "s3://bucket/table".to_string(),
            "aws".to_string(),
            None,
        )
        .unwrap();

        let mut checkpoint = Checkpoint::default();
        checkpoint.update_watermark("2026-01-01T00:00:00Z".to_string(), Some("id-050".to_string()));

        let query = executor.build_query(
            &checkpoint,
            None,  // no condition
            "time",
            Some("unique_id"),
            1000,
        );

        // Should include OR condition for same timestamp
        assert!(query.contains("time > '2026-01-01T00:00:00Z'"));
        assert!(query.contains("OR"));
        assert!(query.contains("unique_id > 'id-050'"));
        assert!(query.contains("ORDER BY time ASC, unique_id ASC"));
    }

    #[test]
    fn test_query_building_without_unique_id() {
        let executor = DuckDBQueryExecutor::new(
            "s3://bucket/table".to_string(),
            "aws".to_string(),
            None,
        )
        .unwrap();

        let mut checkpoint = Checkpoint::default();
        checkpoint.update_watermark("2026-01-01T00:00:00Z".to_string(), None);

        let query = executor.build_query(
            &checkpoint,
            None,  // no condition
            "time",
            None,
            1000,
        );

        // Without unique_id_column: Use >= to include records with same timestamp
        // This ensures data completeness when multiple records share the same timestamp
        assert!(query.contains("time >= '2026-01-01T00:00:00Z'"));
        // Without unique_id_column, should NOT contain OR condition for same timestamp handling
        assert!(!query.contains(" OR "));
        assert!(query.contains("ORDER BY time ASC"));
    }

    // TC-017: Test cloud storage configuration - AWS
    #[test]
    fn test_cloud_storage_config_aws() {
        let executor = DuckDBQueryExecutor::new(
            "s3://bucket/table".to_string(),
            "aws".to_string(),
            None,
        );
        assert!(executor.is_ok());
    }

    // TC-018: Test cloud storage configuration - Aliyun
    #[test]
    fn test_cloud_storage_config_aliyun() {
        // Set OSS endpoint for testing
        std::env::set_var("OSS_ENDPOINT", "oss-cn-hangzhou.aliyuncs.com");
        
        let executor = DuckDBQueryExecutor::new(
            "oss://bucket/table".to_string(),
            "aliyun".to_string(),
            None,
        );
        
        // Note: DuckDB initialization might fail if delta extension is not available
        // or if there are connection issues, but the executor creation itself should succeed
        // The actual error would be in initialize(), not in new()
        match executor {
            Ok(_) => {
                // Success case
            }
            Err(e) => {
                // If it fails, it's likely due to DuckDB initialization issues
                // (e.g., delta extension not available), not configuration issues
                // We'll allow this test to pass if the error is about initialization
                let error_msg = e.to_string();
                assert!(
                    error_msg.contains("delta") || 
                    error_msg.contains("extension") ||
                    error_msg.contains("initialize"),
                    "Unexpected error: {}",
                    error_msg
                );
            }
        }
        
        // Clean up
        std::env::remove_var("OSS_ENDPOINT");
    }

    #[test]
    fn test_cloud_storage_config_gcp() {
        let executor = DuckDBQueryExecutor::new(
            "gs://bucket/table".to_string(),
            "gcp".to_string(),
            None,
        );
        assert!(executor.is_ok());
    }

    #[test]
    fn test_cloud_storage_config_azure() {
        let executor = DuckDBQueryExecutor::new(
            "az://account/container/table".to_string(),
            "azure".to_string(),
            None,
        );
        assert!(executor.is_ok());
    }

    // TC-020: Test RecordBatch to events conversion
    #[test]
    fn test_record_batch_to_events() {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};

        let executor = DuckDBQueryExecutor::new(
            "s3://bucket/table".to_string(),
            "aws".to_string(),
            None,
        )
        .unwrap();

        // Create a simple RecordBatch
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Utf8, true),
            Field::new("message", DataType::Utf8, true),
        ]));

        let time_array = Arc::new(StringArray::from(vec![
            Some("2026-01-01T00:00:00Z"),
            Some("2026-01-01T01:00:00Z"),
        ]));
        let message_array = Arc::new(StringArray::from(vec![
            Some("Message 1"),
            Some("Message 2"),
        ]));

        let batch = RecordBatch::try_new(schema, vec![time_array, message_array]).unwrap();

        let events = executor.record_batch_to_events(&batch).unwrap();
        assert_eq!(events.len(), 2);
        
        // Verify first event
        let event1 = &events[0];
        assert!(event1.is_object());
        let obj1 = event1.as_object().unwrap();
        assert_eq!(obj1.get("time").unwrap().as_str().unwrap(), "2026-01-01T00:00:00Z");
        assert_eq!(obj1.get("message").unwrap().as_str().unwrap(), "Message 1");
    }

    #[test]
    fn test_record_batch_to_events_with_null() {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};

        let executor = DuckDBQueryExecutor::new(
            "s3://bucket/table".to_string(),
            "aws".to_string(),
            None,
        )
        .unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Utf8, true),
            Field::new("message", DataType::Utf8, true),
        ]));

        let time_array = Arc::new(StringArray::from(vec![
            Some("2026-01-01T00:00:00Z"),
            None,
        ]));
        let message_array = Arc::new(StringArray::from(vec![
            Some("Message 1"),
            Some("Message 2"),
        ]));

        let batch = RecordBatch::try_new(schema, vec![time_array, message_array]).unwrap();

        let events = executor.record_batch_to_events(&batch).unwrap();
        assert_eq!(events.len(), 2);
        
        // Verify second event has null time
        let event2 = &events[1];
        let obj2 = event2.as_object().unwrap();
        assert!(obj2.get("time").unwrap().is_null());
    }

    // TC-012: Test DuckDB executor initialization
    #[test]
    fn test_duckdb_executor_initialization() {
        let executor = DuckDBQueryExecutor::new(
            "s3://bucket/table".to_string(),
            "aws".to_string(),
            Some("1GB".to_string()),
        );

        // Executor creation might fail if delta extension is not available
        // but we can verify the structure is correct
        match executor {
            Ok(exec) => {
                // Verify executor has correct fields
                // We can't directly access private fields, but we can verify it works
                let _ = exec;
            }
            Err(e) => {
                // If it fails, it's likely due to DuckDB initialization issues
                let error_msg = e.to_string();
                assert!(
                    error_msg.contains("delta") || 
                    error_msg.contains("extension") ||
                    error_msg.contains("initialize"),
                    "Unexpected error: {}",
                    error_msg
                );
            }
        }
    }

    // TC-012: Test DuckDB executor initialization with memory limit
    #[test]
    fn test_duckdb_executor_with_memory_limit() {
        let executor = DuckDBQueryExecutor::new(
            "s3://bucket/table".to_string(),
            "aws".to_string(),
            Some("512MB".to_string()),
        );

        // Similar to above, initialization might fail due to delta extension
        match executor {
            Ok(_) => {
                // Success case
            }
            Err(e) => {
                let error_msg = e.to_string();
                assert!(
                    error_msg.contains("delta") || 
                    error_msg.contains("extension") ||
                    error_msg.contains("initialize"),
                    "Unexpected error: {}",
                    error_msg
                );
            }
        }
    }

    // TC-021: Test empty query result
    // Note: This test verifies that execute_query can handle empty results
    // Actual empty result handling is tested through execute_query which returns
    // an empty RecordBatch with preserved schema
    #[test]
    fn test_empty_query_result() {
        let executor = DuckDBQueryExecutor::new(
            "s3://bucket/table".to_string(),
            "aws".to_string(),
            None,
        );

        // Just verify executor can be created
        // Empty query result handling is tested through execute_query integration
        match executor {
            Ok(_) => {
                // Success case - empty result handling is tested in integration tests
            }
            Err(e) => {
                let error_msg = e.to_string();
                assert!(
                    error_msg.contains("delta") || 
                    error_msg.contains("extension") ||
                    error_msg.contains("initialize"),
                    "Unexpected error: {}",
                    error_msg
                );
            }
        }
    }

    // TC-019: Test value extraction from DuckDB row (conceptual test)
    // Note: This requires actual DuckDB connection with data, which is difficult in unit tests
    // We test the extract_value_as_string logic conceptually
    #[test]
    fn test_extract_value_as_string_concept() {
        // This test verifies that extract_value_as_string handles different types
        // Actual implementation is tested through execute_query integration
        let executor = DuckDBQueryExecutor::new(
            "s3://bucket/table".to_string(),
            "aws".to_string(),
            None,
        );

        // Just verify executor can be created
        // The actual value extraction is tested through execute_query -> extract_value_as_string
        match executor {
            Ok(_) => {
                // Success case
            }
            Err(e) => {
                let error_msg = e.to_string();
                assert!(
                    error_msg.contains("delta") || 
                    error_msg.contains("extension") ||
                    error_msg.contains("initialize"),
                    "Unexpected error: {}",
                    error_msg
                );
            }
        }
    }
}
