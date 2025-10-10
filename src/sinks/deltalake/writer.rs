use std::collections::HashMap;
use std::path::PathBuf;

use deltalake::kernel::{DataType as DeltaDataType, StructField};
use {
    arrow::array::{
        ArrayRef, BooleanBuilder, Float64Builder, Int16Builder, Int32Builder, Int64Builder,
        Int8Builder, StringArray, StringBuilder, UInt32Builder, UInt64Builder,
    },
    arrow::datatypes::{DataType, Field, Schema, TimeUnit},
    arrow::record_batch::RecordBatch,
    deltalake::operations::create::CreateBuilder,
    deltalake::DeltaOps,
};

use vector_lib::event::Event;
use vector_lib::event::{LogEvent, Value as LogValue};

use crate::sinks::deltalake::{DeltaTableConfig, WriteConfig};

/// Delta Lake table writer
pub struct DeltaLakeWriter {
    table_path: PathBuf,
    table_config: DeltaTableConfig,
    #[allow(dead_code)]
    write_config: WriteConfig,
    #[allow(dead_code)]
    storage_options: Option<HashMap<String, String>>,
    schema: Option<Schema>,
    /// Cached schema information from source (table_name -> (field_name -> mysql_type))
    cached_source_schemas: HashMap<String, HashMap<String, String>>,
    /// Fixed Arrow schema for this table to ensure consistency across batches
    fixed_arrow_schema: Option<Schema>,
}

impl DeltaLakeWriter {
    /// Create a new Delta Lake writer
    pub fn new(
        table_path: PathBuf,
        table_config: DeltaTableConfig,
        write_config: WriteConfig,
        storage_options: Option<HashMap<String, String>>,
    ) -> Self {
        // Initialize S3 handlers if this is an S3 path
        if table_path.to_string_lossy().starts_with("s3://") {
            deltalake::aws::register_handlers(None);
            info!(
                "Registered Delta Lake S3 handlers for path: {}",
                table_path.display()
            );
        }

        Self {
            table_path,
            table_config,
            write_config,
            storage_options,
            schema: None,
            cached_source_schemas: HashMap::new(),
            fixed_arrow_schema: None,
        }
    }

    /// Write events to Delta Lake table
    pub async fn write_events(
        &mut self,
        events: Vec<Event>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if events.is_empty() {
            return Ok(());
        }

        // Log batch summary
        info!("Writer processing {} events", events.len());

        // Convert events to Arrow record batch
        let record_batch = self.events_to_record_batch(events)?;

        // Write to Delta Lake
        self.write_to_delta_lake(record_batch).await?;

        Ok(())
    }

    /// Convert Vector events to Arrow record batch
    fn events_to_record_batch(
        &mut self,
        events: Vec<Event>,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        use std::sync::Arc;

        if events.is_empty() {
            return Err("No events to convert".into());
        }

        // Get or create fixed schema for this table
        let schema = if let Some(ref fixed_schema) = self.fixed_arrow_schema {
            fixed_schema.clone()
        } else {
            // Build fixed schema from first event and cache it
            let first_event = &events[0];
            let schema = self.build_fixed_schema(first_event)?;
            self.fixed_arrow_schema = Some(schema.clone());
            self.schema = Some(schema.clone());
            schema
        };

        // Convert events to columns
        let mut columns: Vec<ArrayRef> = Vec::new();

        for field in &schema.fields {
            let column = self.create_column(field, &events)?;
            columns.push(column);
        }

        // Create record batch
        let record_batch = RecordBatch::try_new(Arc::new(schema), columns)?;
        Ok(record_batch)
    }

    /// Build a fixed schema from the first event that will be consistent across all batches
    fn build_fixed_schema(
        &mut self,
        event: &Event,
    ) -> Result<Schema, Box<dyn std::error::Error + Send + Sync>> {
        if let Event::Log(log_event) = event {
            let mut fields = Vec::new();
            let mut added_fields = std::collections::HashSet::new();

            // First, extract and cache the MySQL schema metadata from the event
            self.extract_and_cache_mysql_schema(log_event);

            // Get table name for schema lookup
            let table_name = log_event
                .get("_vector_table")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| "unknown_table".to_string());

            // Build fixed field list based on cached MySQL schema and Vector system fields

            // 1. Add Vector system fields first
            let standard_fields = [
                "_vector_table",
                "_vector_source_table",
                "_vector_source_schema",
                "_vector_instance",
                "_vector_timestamp",
            ];

            for field_name in &standard_fields {
                fields.push(Field::new(*field_name, DataType::Utf8, false));
                added_fields.insert(field_name.to_string());
            }

            // Add date field for partitioning (derived from _vector_timestamp)
            fields.push(Field::new("date", DataType::Utf8, false));
            added_fields.insert("date".to_string());

            // 3. Add all MySQL data fields from cached schema (in deterministic order)
            if let Some(table_schema) = self.cached_source_schemas.get(&table_name) {
                // Sort field names to ensure consistent order
                let mut field_names: Vec<_> = table_schema.keys().collect();
                field_names.sort();

                for field_name in field_names {
                    // Skip if conflicts with Vector system fields or is metadata field
                    if !added_fields.contains(field_name)
                        && !field_name.starts_with("_schema_metadata")
                    {
                        if let Some(mysql_type) = table_schema.get(field_name) {
                            let data_type = self.mysql_type_to_arrow_type(mysql_type);
                            fields.push(Field::new(field_name, data_type, true));
                            added_fields.insert(field_name.to_string());
                        }
                    }
                }
            } else {
                // Fallback: add fields from current event if no schema cache available
                warn!(
                    "No cached schema found for table {}, using fields from current event",
                    table_name
                );
                if let Some(iter) = log_event.all_event_fields() {
                    let mut event_fields: Vec<_> = iter
                        .map(|(key, value)| (key.as_ref().to_string(), value))
                        .collect();
                    event_fields.sort_by_key(|(key, _)| key.clone());

                    for (key_str, value) in event_fields {
                        if !added_fields.contains(&key_str)
                            && !key_str.starts_with("_schema_metadata")
                        {
                            let data_type =
                                self.get_arrow_type_from_schema(log_event, &key_str, value);
                            fields.push(Field::new(&key_str, data_type, true));
                            added_fields.insert(key_str);
                        }
                    }
                }
            }

            info!(
                "Built fixed schema with {} fields for table {}",
                fields.len(),
                table_name
            );
            Ok(Schema::new(fields))
        } else {
            Err("Event is not a log event".into())
        }
    }

    /// Extract MySQL schema metadata from event and cache it
    fn extract_and_cache_mysql_schema(&mut self, log_event: &LogEvent) {
        // Get table name for schema cache key
        let table_name = log_event
            .get("_vector_table")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "unknown_table".to_string());

        // Only extract if not already cached
        if !self.cached_source_schemas.contains_key(&table_name) {
            if let Some(schema_metadata) = log_event.get("_schema_metadata") {
                if let Some(schema_obj) = schema_metadata.as_object() {
                    let mut table_schema = HashMap::new();
                    for (field, info) in schema_obj {
                        if let Some(mysql_type) = info.get("mysql_type").and_then(|v| v.as_str()) {
                            table_schema.insert(field.to_string(), mysql_type.to_string());
                        }
                    }

                    info!(
                        "Cached MySQL schema for table {} with {} fields",
                        table_name,
                        table_schema.len()
                    );
                    self.cached_source_schemas.insert(table_name, table_schema);
                }
            }
        }
    }

    /// Get Arrow data type from cached schema or extract from event and cache
    fn get_arrow_type_from_schema(
        &mut self,
        log_event: &LogEvent,
        field_name: &str,
        value: &LogValue,
    ) -> DataType {
        // Get table name for schema cache key
        let table_name = log_event
            .get("_vector_table")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "unknown_table".to_string());

        // Check if we already have cached schema for this table
        if let Some(table_schema) = self.cached_source_schemas.get(&table_name) {
            if let Some(mysql_type) = table_schema.get(field_name) {
                let arrow_type = self.mysql_type_to_arrow_type(mysql_type);
                // Using cached schema
                return arrow_type;
            }
        }

        // Try to extract and cache schema from this event's _schema_metadata
        if let Some(schema_metadata) = log_event.get("_schema_metadata") {
            if let Some(schema_obj) = schema_metadata.as_object() {
                // Cache the entire schema for this table
                let mut table_schema = HashMap::new();
                for (field, info) in schema_obj {
                    if let Some(mysql_type) = info.get("mysql_type").and_then(|v| v.as_str()) {
                        table_schema.insert(field.to_string(), mysql_type.to_string());
                    }
                }

                // Schema cached successfully
                self.cached_source_schemas
                    .insert(table_name.clone(), table_schema);

                // Now get the type for current field
                if let Some(cached_schema) = self.cached_source_schemas.get(&table_name) {
                    if let Some(mysql_type) = cached_schema.get(field_name) {
                        let arrow_type = self.mysql_type_to_arrow_type(mysql_type);
                        // Using newly cached schema
                        return arrow_type;
                    }
                }
            }
        }

        // Fallback to inference if schema not available
        self.infer_arrow_type(field_name, value)
    }

    /// Convert Arrow Field to Delta StructField
    fn arrow_field_to_delta_field(&self, field: &Field) -> StructField {
        let delta_type = self.arrow_type_to_delta_type(field.data_type());
        StructField::new(field.name().clone(), delta_type, field.is_nullable())
    }

    /// Convert Arrow DataType to Delta DataType
    fn arrow_type_to_delta_type(&self, arrow_type: &DataType) -> DeltaDataType {
        match arrow_type {
            DataType::Boolean => DeltaDataType::BOOLEAN,
            DataType::Int8 => DeltaDataType::BYTE,
            DataType::Int16 => DeltaDataType::SHORT,
            DataType::Int32 => DeltaDataType::INTEGER,
            DataType::Int64 => DeltaDataType::LONG,
            DataType::UInt32 => DeltaDataType::INTEGER, // Delta Lake doesn't have unsigned types
            DataType::UInt64 => DeltaDataType::LONG,
            DataType::Float32 => DeltaDataType::FLOAT,
            DataType::Float64 => DeltaDataType::DOUBLE,
            DataType::Utf8 => DeltaDataType::STRING,
            DataType::LargeUtf8 => DeltaDataType::STRING,
            DataType::Binary => DeltaDataType::BINARY,
            DataType::LargeBinary => DeltaDataType::BINARY,
            DataType::Timestamp(_, _) => DeltaDataType::TIMESTAMP,
            DataType::Date32 => DeltaDataType::DATE,
            DataType::Date64 => DeltaDataType::DATE,
            _ => DeltaDataType::STRING, // Default fallback
        }
    }

    /// Convert MySQL type to Arrow DataType
    fn mysql_type_to_arrow_type(&self, mysql_type: &str) -> DataType {
        let mysql_type_lower = mysql_type.to_lowercase();

        if mysql_type_lower.contains("tinyint(1)") {
            DataType::Boolean
        } else if mysql_type_lower.contains("bigint") {
            if mysql_type_lower.contains("unsigned") {
                DataType::UInt64
            } else {
                DataType::Int64
            }
        } else if mysql_type_lower.contains("tinyint") {
            DataType::Int8
        } else if mysql_type_lower.contains("smallint") {
            DataType::Int16
        } else if mysql_type_lower.contains("mediumint") || mysql_type_lower.contains("int") {
            if mysql_type_lower.contains("unsigned") {
                DataType::UInt32
            } else {
                DataType::Int32
            }
        } else if mysql_type_lower.contains("float") {
            DataType::Float32
        } else if mysql_type_lower.contains("double") || mysql_type_lower.contains("real") {
            DataType::Float64
        } else if mysql_type_lower.contains("decimal") || mysql_type_lower.contains("numeric") {
            // For decimal, we'll use Float64 as a reasonable approximation
            DataType::Float64
        } else if mysql_type_lower.contains("timestamp") {
            // TODO: Change back to Timestamp type when Delta Lake writer features are properly supported
            // Use Utf8 instead of Timestamp to avoid writer feature requirements
            // Original: DataType::Timestamp(TimeUnit::Microsecond, None)
            DataType::Utf8
        } else if mysql_type_lower.contains("datetime") {
            // Use Utf8 for DATETIME columns (they don't have timezone info)
            DataType::Utf8
        } else if mysql_type_lower.contains("date") {
            DataType::Date32
        } else if mysql_type_lower.contains("time") {
            DataType::Time64(arrow::datatypes::TimeUnit::Microsecond)
        } else if mysql_type_lower.contains("longtext")
            || mysql_type_lower.contains("mediumtext")
            || mysql_type_lower.contains("text")
            || mysql_type_lower.contains("varchar")
            || mysql_type_lower.contains("char")
            || mysql_type_lower.contains("blob")
            || mysql_type_lower.contains("longblob")
            || mysql_type_lower.contains("mediumblob")
        {
            // Handle all text and blob types as Utf8
            DataType::Utf8
        } else {
            // Default to Utf8 for any unknown types
            DataType::Utf8
        }
    }

    /// Convert a JSON value to Arrow data type
    fn value_to_arrow_type(&self, value: &LogValue) -> DataType {
        let data_type = match value {
            LogValue::Bytes(_) => DataType::Utf8,
            LogValue::Integer(_) => DataType::Int64,
            LogValue::Float(_) => DataType::Float64,
            LogValue::Boolean(_) => DataType::Boolean,
            LogValue::Null => DataType::Utf8, // Default for null values
            _ => DataType::Utf8,
        };

        // Converting LogValue to Arrow type
        data_type
    }

    /// Infer Arrow data type from field name and value
    /// This function now relies primarily on _schema_metadata for type inference
    fn infer_arrow_type(&self, _field_name: &str, value: &LogValue) -> DataType {
        // If we have a concrete value, use its type
        if !matches!(value, LogValue::Null) {
            return self.value_to_arrow_type(value);
        }

        // For null values, we should rely on _schema_metadata
        // If no schema metadata is available, default to Utf8
        // This is a fallback that should rarely be used with proper schema metadata
        DataType::Utf8
    }

    /// Create a column for a specific field
    fn create_column(
        &self,
        field: &Field,
        events: &[Event],
    ) -> Result<ArrayRef, Box<dyn std::error::Error + Send + Sync>> {
        use std::sync::Arc;

        match field.data_type() {
            DataType::Utf8 => {
                let mut builder = StringBuilder::with_capacity(events.len(), events.len() * 8);
                for event in events.iter() {
                    if let Event::Log(log_event) = event {
                        let value_opt = match field.name().as_str() {
                            "_vector_table" => log_event
                                .get("_vector_table")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string()),
                            "_vector_source_table" => log_event
                                .get("_vector_source_table")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string()),
                            "_vector_source_schema" => log_event
                                .get("_vector_source_schema")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string()),
                            "_vector_instance" => log_event
                                .get("_vector_instance")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string()),
                            "_vector_timestamp" => log_event
                                .get("_vector_timestamp")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string()),
                            "date" => {
                                // Extract date from _vector_timestamp for partitioning
                                let date_str = log_event
                                    .get("_vector_timestamp")
                                    .and_then(|v| v.as_str())
                                    .map(|timestamp_str| {
                                        // Parse ISO 8601 timestamp and extract date part
                                        if let Ok(dt) =
                                            chrono::DateTime::parse_from_rfc3339(&timestamp_str)
                                        {
                                            dt.format("%Y-%m-%d").to_string()
                                        } else {
                                            // Fallback: try to extract date from other timestamp formats
                                            if timestamp_str.len() >= 10 {
                                                timestamp_str[..10].to_string()
                                            } else {
                                                chrono::Utc::now().format("%Y-%m-%d").to_string()
                                            }
                                        }
                                    })
                                    .unwrap_or_else(|| {
                                        // Ensure we always have a date value for consistency
                                        chrono::Utc::now().format("%Y-%m-%d").to_string()
                                    });
                                Some(date_str)
                            }

                            _ => {
                                // For data fields, try exact match first, then case-insensitive match
                                let field_name = field.name();
                                if let Some(value) = log_event.get(field_name.as_str()) {
                                    Some(value.to_string())
                                } else {
                                    // Try case-insensitive match for data fields
                                    if let Some(iter) = log_event.all_event_fields() {
                                        let mut found_value = None;
                                        for (key, value) in iter {
                                            if key.as_ref().to_lowercase()
                                                == field_name.to_lowercase()
                                            {
                                                found_value = Some(value.to_string());
                                                break;
                                            }
                                        }
                                        found_value
                                    } else {
                                        None
                                    }
                                }
                            }
                        };
                        if let Some(s) = value_opt {
                            // Trim quotes from string values to avoid query issues
                            let trimmed = s.trim_matches('"');
                            builder.append_value(trimmed);
                        } else {
                            builder.append_null();
                        }
                    } else {
                        builder.append_null();
                    }
                }
                let array = builder.finish();
                Ok(Arc::new(array))
            }
            DataType::Int64 => {
                let mut builder = Int64Builder::with_capacity(events.len());
                for event in events.iter() {
                    if let Event::Log(log_event) = event {
                        let value_opt = match field.name().as_str() {
                            "_vector_id" => {
                                log_event.get("_vector_id").and_then(|v| v.as_integer())
                            }
                            _ => match log_event.get(field.name().as_str()) {
                                Some(LogValue::Integer(i)) => Some(*i),
                                Some(LogValue::Bytes(bytes)) => {
                                    // Try to parse string as integer
                                    if let Ok(s) = std::str::from_utf8(bytes.as_ref()) {
                                        s.parse::<i64>().ok()
                                    } else {
                                        None
                                    }
                                }
                                // Accept null values gracefully
                                _ => None,
                            },
                        };

                        if let Some(value) = value_opt {
                            builder.append_value(value);
                        } else {
                            builder.append_null();
                        }
                    } else {
                        builder.append_null();
                    }
                }
                let array = builder.finish();
                Ok(Arc::new(array))
            }
            DataType::Int32 => {
                let mut builder = Int32Builder::with_capacity(events.len());
                for event in events.iter() {
                    if let Event::Log(log_event) = event {
                        match log_event.get(field.name().as_str()) {
                            Some(LogValue::Integer(i)) => {
                                if *i >= i32::MIN as i64 && *i <= i32::MAX as i64 {
                                    builder.append_value(*i as i32);
                                } else {
                                    builder.append_null();
                                }
                            }
                            Some(LogValue::Bytes(bytes)) => {
                                // Try to parse string as integer
                                if let Ok(s) = std::str::from_utf8(bytes.as_ref()) {
                                    if let Ok(i) = s.parse::<i32>() {
                                        builder.append_value(i);
                                    } else {
                                        builder.append_null();
                                    }
                                } else {
                                    builder.append_null();
                                }
                            }
                            // Accept null values gracefully
                            _ => builder.append_null(),
                        }
                    } else {
                        builder.append_null();
                    }
                }
                let array = builder.finish();
                Ok(Arc::new(array))
            }
            DataType::UInt32 => {
                let mut builder = UInt32Builder::with_capacity(events.len());
                for event in events.iter() {
                    if let Event::Log(log_event) = event {
                        match log_event.get(field.name().as_str()) {
                            Some(LogValue::Integer(i)) => {
                                if *i >= 0 && *i <= u32::MAX as i64 {
                                    builder.append_value(*i as u32);
                                } else {
                                    builder.append_null();
                                }
                            }
                            Some(LogValue::Bytes(bytes)) => {
                                // Try to parse string as unsigned integer
                                if let Ok(s) = std::str::from_utf8(bytes.as_ref()) {
                                    if let Ok(u) = s.parse::<u32>() {
                                        builder.append_value(u);
                                    } else {
                                        builder.append_null();
                                    }
                                } else {
                                    builder.append_null();
                                }
                            }
                            // Accept null values gracefully
                            _ => builder.append_null(),
                        }
                    } else {
                        builder.append_null();
                    }
                }
                let array = builder.finish();
                Ok(Arc::new(array))
            }
            DataType::Int16 => {
                let mut builder = Int16Builder::with_capacity(events.len());
                for event in events.iter() {
                    if let Event::Log(log_event) = event {
                        match log_event.get(field.name().as_str()) {
                            Some(LogValue::Integer(i)) => {
                                if *i >= i16::MIN as i64 && *i <= i16::MAX as i64 {
                                    builder.append_value(*i as i16);
                                } else {
                                    builder.append_null();
                                }
                            }
                            Some(LogValue::Bytes(bytes)) => {
                                // Try to parse string as integer
                                if let Ok(s) = std::str::from_utf8(bytes.as_ref()) {
                                    if let Ok(i) = s.parse::<i16>() {
                                        builder.append_value(i);
                                    } else {
                                        builder.append_null();
                                    }
                                } else {
                                    builder.append_null();
                                }
                            }
                            // Accept null values gracefully
                            _ => builder.append_null(),
                        }
                    } else {
                        builder.append_null();
                    }
                }
                let array = builder.finish();
                Ok(Arc::new(array))
            }
            DataType::Int8 => {
                let mut builder = Int8Builder::with_capacity(events.len());
                for event in events.iter() {
                    if let Event::Log(log_event) = event {
                        match log_event.get(field.name().as_str()) {
                            Some(LogValue::Integer(i)) => {
                                if *i >= i8::MIN as i64 && *i <= i8::MAX as i64 {
                                    builder.append_value(*i as i8);
                                } else {
                                    builder.append_null();
                                }
                            }
                            Some(LogValue::Bytes(bytes)) => {
                                // Try to parse string as integer
                                if let Ok(s) = std::str::from_utf8(bytes.as_ref()) {
                                    if let Ok(i) = s.parse::<i8>() {
                                        builder.append_value(i);
                                    } else {
                                        builder.append_null();
                                    }
                                } else {
                                    builder.append_null();
                                }
                            }
                            // Accept null values gracefully
                            _ => builder.append_null(),
                        }
                    } else {
                        builder.append_null();
                    }
                }
                let array = builder.finish();
                Ok(Arc::new(array))
            }
            DataType::UInt64 => {
                let mut builder = UInt64Builder::with_capacity(events.len());
                for event in events.iter() {
                    if let Event::Log(log_event) = event {
                        match log_event.get(field.name().as_str()) {
                            Some(LogValue::Integer(i)) => {
                                if *i >= 0 {
                                    builder.append_value(*i as u64);
                                } else {
                                    builder.append_null();
                                }
                            }
                            Some(LogValue::Bytes(bytes)) => {
                                // Try to parse string as unsigned integer
                                if let Ok(s) = std::str::from_utf8(bytes.as_ref()) {
                                    if let Ok(u) = s.parse::<u64>() {
                                        builder.append_value(u);
                                    } else {
                                        builder.append_null();
                                    }
                                } else {
                                    builder.append_null();
                                }
                            }
                            // Accept null values gracefully
                            _ => builder.append_null(),
                        }
                    } else {
                        builder.append_null();
                    }
                }
                let array = builder.finish();
                Ok(Arc::new(array))
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(events.len());
                for event in events.iter() {
                    if let Event::Log(log_event) = event {
                        match log_event.get(field.name().as_str()) {
                            Some(LogValue::Float(f)) => builder.append_value((*f).into_inner()),
                            Some(LogValue::Integer(i)) => builder.append_value(*i as f64),
                            Some(LogValue::Bytes(bytes)) => {
                                // Try to parse string as float
                                if let Ok(s) = std::str::from_utf8(bytes.as_ref()) {
                                    if let Ok(f) = s.parse::<f64>() {
                                        builder.append_value(f);
                                    } else {
                                        builder.append_null();
                                    }
                                } else {
                                    builder.append_null();
                                }
                            }
                            _ => builder.append_null(),
                        }
                    } else {
                        builder.append_null();
                    }
                }
                let array = builder.finish();
                Ok(Arc::new(array))
            }
            DataType::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(events.len());
                for event in events.iter() {
                    if let Event::Log(log_event) = event {
                        match log_event.get(field.name().as_str()) {
                            Some(LogValue::Boolean(b)) => builder.append_value(*b),
                            _ => builder.append_null(),
                        }
                    } else {
                        builder.append_null();
                    }
                }
                let array = builder.finish();
                Ok(Arc::new(array))
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                let mut builder =
                    arrow::array::TimestampMicrosecondBuilder::with_capacity(events.len());
                for event in events.iter() {
                    if let Event::Log(log_event) = event {
                        match log_event.get(field.name().as_str()) {
                            Some(LogValue::Integer(microseconds)) => {
                                // Direct microseconds value from TiDB packed time
                                builder.append_value(*microseconds);
                            }
                            Some(LogValue::Bytes(bytes)) => {
                                // Try to parse timestamp string
                                if let Ok(s) = std::str::from_utf8(bytes.as_ref()) {
                                    if let Ok(timestamp) = chrono::DateTime::parse_from_rfc3339(s) {
                                        let microseconds = timestamp.timestamp_micros();
                                        builder.append_value(microseconds);
                                    } else if let Ok(naive_dt) =
                                        chrono::NaiveDateTime::parse_from_str(
                                            s,
                                            "%Y-%m-%d %H:%M:%S",
                                        )
                                    {
                                        let microseconds = naive_dt.and_utc().timestamp_micros();
                                        builder.append_value(microseconds);
                                    } else {
                                        warn!(
                                            "Failed to parse timestamp '{}' for field '{}'",
                                            s,
                                            field.name()
                                        );
                                        builder.append_null();
                                    }
                                } else {
                                    warn!(
                                        "Failed to decode bytes as UTF-8 for timestamp field '{}'",
                                        field.name()
                                    );
                                    builder.append_null();
                                }
                            }
                            Some(LogValue::Null) => {
                                builder.append_null();
                            }
                            Some(other_value) => {
                                warn!(
                                    "Timestamp field '{}' received unexpected value type: {:?}",
                                    field.name(),
                                    other_value
                                );
                                builder.append_null();
                            }
                            None => {
                                builder.append_null();
                            }
                        }
                    } else {
                        builder.append_null();
                    }
                }
                let array = builder.finish();
                Ok(Arc::new(array))
            }
            _ => {
                // Default to Utf8 representation for any other types
                let values: Vec<Option<String>> = events
                    .iter()
                    .map(|event| {
                        if let Event::Log(log_event) = event {
                            log_event.get(field.name().as_str()).map(|v| {
                                // Trim quotes from string values to avoid query issues
                                let s = v.to_string();
                                s.trim_matches('"').to_string()
                            })
                        } else {
                            None
                        }
                    })
                    .collect();
                let array = StringArray::from(values);
                Ok(Arc::new(array))
            }
        }
    }

    /// Write record batch to Delta Lake
    async fn write_to_delta_lake(
        &self,
        record_batch: RecordBatch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // For local paths, ensure table directory exists
        if !self.table_path.to_string_lossy().starts_with("s3://") {
            std::fs::create_dir_all(&self.table_path)?;
        }

        // Build Delta table URI
        let table_uri = self.table_path.to_string_lossy().to_string();

        info!("Writing to Delta Lake table at: {}", table_uri);

        // Use DeltaOps for improved S3 support, following the successful test pattern
        let table_ops = if let Some(storage_options) = &self.storage_options {
            info!(
                "Using storage options for S3 authentication: {:?}",
                storage_options
            );
            DeltaOps::try_from_uri_with_storage_options(&table_uri, storage_options.clone()).await?
        } else {
            info!("No storage options provided, using default credential chain");
            DeltaOps::try_from_uri(&table_uri).await?
        };

        // Check if table exists first using filesystem check
        info!("Checking if Delta table exists at {}", table_uri);
        
        let table_exists = if table_uri.starts_with("s3://") {
            // For S3, we can't easily check, so we'll try to load and handle errors
            match table_ops.load().await {
                Ok(table) => {
                    info!("✅ Delta table exists at {} (version: {:?})", table_uri, table.0.version());
                    true
                }
                Err(e) => {
                    if e.to_string().contains("does not exist") || e.to_string().contains("not found") {
                        info!("Table doesn't exist at {}, will create new table", table_uri);
                        false
                    } else {
                        error!("Failed to check if Delta table exists: {}", e);
                        return Err(e.into());
                    }
                }
            }
        } else {
            // For local filesystem, check if _delta_log directory exists
            let delta_log_path = std::path::Path::new(&table_uri).join("_delta_log");
            if delta_log_path.exists() {
                info!("✅ Delta table exists at {} (found _delta_log directory)", table_uri);
                true
            } else {
                info!("Table doesn't exist at {}, will create new table", table_uri);
                false
            }
        };

        if table_exists {
            // Table exists, write to it directly
            info!("Writing to existing Delta table at {}", table_uri);
            
            // Recreate table_ops since it was moved in the load() call
            let table_ops = if let Some(storage_options) = &self.storage_options {
                DeltaOps::try_from_uri_with_storage_options(&table_uri, storage_options.clone()).await?
            } else {
                DeltaOps::try_from_uri(&table_uri).await?
            };
            
            let write_result = table_ops.write(vec![record_batch.clone()]).await;
            
            match write_result {
                Ok(table) => {
                    info!("✅ Successfully wrote to existing Delta table at {}", table_uri);
                    info!("Table version: {:?}", table.version());
                    return Ok(());
                }
                Err(e) => {
                    error!("Failed to write to existing Delta table: {}", e);
                    return Err(e.into());
                }
            }
        }

        // If we reach here, table doesn't exist and needs to be created
        // Create new table first
        info!(
            "Creating new Delta table at {} for table {}",
            table_uri, self.table_config.name
        );
        let schema = self.schema.as_ref().ok_or("Schema not available")?;

        let mut create_builder = CreateBuilder::new().with_location(&table_uri).with_columns(
            schema
                .fields()
                .iter()
                .map(|field| self.arrow_field_to_delta_field(field)),
        );

        // Add storage options for S3
        if let Some(storage_options) = &self.storage_options {
            create_builder = create_builder.with_storage_options(storage_options.clone());
        }

        // Add partition columns if configured
        if let Some(partition_cols) = &self.table_config.partition_by {
            info!(
                "Setting partition columns for table {}: {:?}",
                self.table_config.name, partition_cols
            );
            create_builder = create_builder.with_partition_columns(partition_cols.clone());
        } else {
            info!(
                "No partition columns configured for table {}",
                self.table_config.name
            );
        }

        create_builder.await?;
        info!("Successfully created new Delta table");

        // Now write the data using DeltaOps - reload the table_ops to get the created table
        let table_ops = if let Some(storage_options) = &self.storage_options {
            DeltaOps::try_from_uri_with_storage_options(&table_uri, storage_options.clone()).await?
        } else {
            DeltaOps::try_from_uri(&table_uri).await?
        };

        let write_result = table_ops.write(vec![record_batch]).await?;

        info!(
            "Successfully wrote data to Delta Lake table at {}, version: {:?}",
            table_uri,
            write_result.version()
        );

        Ok(())
    }

    /// Check if Delta table exists at the given URI
    #[allow(dead_code)]
    async fn table_exists(
        &self,
        table_uri: &str,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if table_uri.starts_with("s3://") {
            // For S3, we need to check if _delta_log exists
            // This is a simplified check - in practice you'd use the Delta Lake APIs
            // For now, we'll always return false for S3 to trigger table creation logic
            Ok(false)
        } else {
            // For local filesystem
            Ok(self.table_path.join("_delta_log").exists())
        }
    }

    /// Fallback: write events to JSON files
    #[allow(dead_code)]
    async fn write_to_json_files(
        &self,
        events: Vec<Event>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use chrono::Utc;
        use tokio::fs::OpenOptions;
        use tokio::io::AsyncWriteExt;

        // Ensure table directory exists
        std::fs::create_dir_all(&self.table_path)?;

        // Create file with timestamp
        let timestamp = Utc::now().format("%Y%m%d_%H%M%S_%3f");
        let file_path = self.table_path.join(format!("data_{}.json", timestamp));

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path)
            .await?;

        for event in events {
            if let Event::Log(log_event) = event {
                let json_line = serde_json::to_string(&log_event)?;
                file.write_all(json_line.as_bytes()).await?;
                file.write_all(b"\n").await?;
            }
        }

        file.flush().await?;
        Ok(())
    }
}
