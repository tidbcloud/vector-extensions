use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int16Builder, Int32Builder, Int64Builder,
    Int8Builder, RecordBatch, StringArray, StringBuilder, UInt32Builder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use tracing::warn;
use vector_lib::event::{Event, Value as LogValue};

use super::schema::SchemaManager;

/// Event to RecordBatch converter
pub struct EventConverter;

impl EventConverter {
    pub fn new() -> Self {
        Self
    }

    /// Convert events to Arrow RecordBatch
    pub fn events_to_record_batch(
        schema_manager: &mut SchemaManager,
        events: Vec<Event>,
        fixed_schema: &Option<Schema>,
    ) -> Result<(RecordBatch, Schema), Box<dyn std::error::Error + Send + Sync>> {
        if events.is_empty() {
            return Err("No events to convert".into());
        }

        // Get or create fixed schema for this table
        let schema = if let Some(ref fixed_schema) = fixed_schema {
            fixed_schema.clone()
        } else {
            // Build fixed schema from first event and cache it
            let first_event = &events[0];
            schema_manager.build_arrow_schema(first_event)?
        };

        // Convert events to columns
        let mut columns: Vec<ArrayRef> = Vec::new();

        for field in &schema.fields {
            let column = Self::create_column(field, &events)?;
            columns.push(column);
        }

        // Create record batch
        let record_batch = RecordBatch::try_new(Arc::new(schema.clone()), columns)?;
        Ok((record_batch, schema))
    }

    /// Create Arrow column for a field from events
    fn create_column(
        field: &Field,
        events: &[Event],
    ) -> Result<ArrayRef, Box<dyn std::error::Error + Send + Sync>> {
        match field.data_type() {
            DataType::Utf8 => Self::build_string_column(field, events),
            DataType::Int64 => Self::build_int64_column(field, events),
            DataType::Int32 => Self::build_int32_column(field, events),
            DataType::UInt32 => Self::build_uint32_column(field, events),
            DataType::Int16 => Self::build_int16_column(field, events),
            DataType::Int8 => Self::build_int8_column(field, events),
            DataType::UInt64 => Self::build_uint64_column(field, events),
            DataType::Float64 => Self::build_float64_column(field, events),
            DataType::Boolean => Self::build_boolean_column(field, events),
            DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => {
                Self::build_timestamp_with_tz_column(field, events, tz)
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                Self::build_timestamp_column(field, events)
            }
            _ => Self::build_default_column(field, events),
        }
    }

    /// Build string column (handles system fields, date field, and data fields)
    fn build_string_column(
        field: &Field,
        events: &[Event],
    ) -> Result<ArrayRef, Box<dyn std::error::Error + Send + Sync>> {
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
                                if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&timestamp_str)
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
                                    if key.as_ref().to_lowercase() == field_name.to_lowercase() {
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

    /// Build Int64 column
    fn build_int64_column(
        field: &Field,
        events: &[Event],
    ) -> Result<ArrayRef, Box<dyn std::error::Error + Send + Sync>> {
        let mut builder = Int64Builder::with_capacity(events.len());

        for event in events.iter() {
            if let Event::Log(log_event) = event {
                let value_opt = match field.name().as_str() {
                    "_vector_id" => log_event.get("_vector_id").and_then(|v| v.as_integer()),
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

    /// Build Int32 column
    fn build_int32_column(
        field: &Field,
        events: &[Event],
    ) -> Result<ArrayRef, Box<dyn std::error::Error + Send + Sync>> {
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
                    _ => builder.append_null(),
                }
            } else {
                builder.append_null();
            }
        }

        let array = builder.finish();
        Ok(Arc::new(array))
    }

    /// Build UInt32 column
    fn build_uint32_column(
        field: &Field,
        events: &[Event],
    ) -> Result<ArrayRef, Box<dyn std::error::Error + Send + Sync>> {
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
                    _ => builder.append_null(),
                }
            } else {
                builder.append_null();
            }
        }

        let array = builder.finish();
        Ok(Arc::new(array))
    }

    /// Build Int16 column
    fn build_int16_column(
        field: &Field,
        events: &[Event],
    ) -> Result<ArrayRef, Box<dyn std::error::Error + Send + Sync>> {
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
                    _ => builder.append_null(),
                }
            } else {
                builder.append_null();
            }
        }

        let array = builder.finish();
        Ok(Arc::new(array))
    }

    /// Build Int8 column
    fn build_int8_column(
        field: &Field,
        events: &[Event],
    ) -> Result<ArrayRef, Box<dyn std::error::Error + Send + Sync>> {
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
                    _ => builder.append_null(),
                }
            } else {
                builder.append_null();
            }
        }

        let array = builder.finish();
        Ok(Arc::new(array))
    }

    /// Build UInt64 column
    fn build_uint64_column(
        field: &Field,
        events: &[Event],
    ) -> Result<ArrayRef, Box<dyn std::error::Error + Send + Sync>> {
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
                    _ => builder.append_null(),
                }
            } else {
                builder.append_null();
            }
        }

        let array = builder.finish();
        Ok(Arc::new(array))
    }

    /// Build Float64 column
    fn build_float64_column(
        field: &Field,
        events: &[Event],
    ) -> Result<ArrayRef, Box<dyn std::error::Error + Send + Sync>> {
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

    /// Build Boolean column
    fn build_boolean_column(
        field: &Field,
        events: &[Event],
    ) -> Result<ArrayRef, Box<dyn std::error::Error + Send + Sync>> {
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

    /// Build Timestamp column with timezone
    fn build_timestamp_with_tz_column(
        field: &Field,
        events: &[Event],
        tz: &Arc<str>,
    ) -> Result<ArrayRef, Box<dyn std::error::Error + Send + Sync>> {
        // Build a Vec<Option<i64>> of microseconds since epoch, then attach timezone
        let mut values: Vec<Option<i64>> = Vec::with_capacity(events.len());

        for event in events.iter() {
            if let Event::Log(log_event) = event {
                let v = match log_event.get(field.name().as_str()) {
                    Some(LogValue::Integer(microseconds)) => Some(*microseconds),
                    Some(LogValue::Bytes(bytes)) => {
                        if let Ok(s) = std::str::from_utf8(bytes.as_ref()) {
                            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                                Some(dt.timestamp_micros())
                            } else if let Ok(naive_dt) =
                                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                            {
                                Some(naive_dt.and_utc().timestamp_micros())
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                    _ => None,
                };
                values.push(v);
            } else {
                values.push(None);
            }
        }

        let array = arrow::array::TimestampMicrosecondArray::from(values).with_timezone(tz.clone());
        Ok(Arc::new(array))
    }

    /// Build Timestamp column without timezone
    fn build_timestamp_column(
        field: &Field,
        events: &[Event],
    ) -> Result<ArrayRef, Box<dyn std::error::Error + Send + Sync>> {
        let mut builder = arrow::array::TimestampMicrosecondBuilder::with_capacity(events.len());

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
                                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
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

    /// Build default column (fallback to Utf8 for unsupported types)
    fn build_default_column(
        field: &Field,
        events: &[Event],
    ) -> Result<ArrayRef, Box<dyn std::error::Error + Send + Sync>> {
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

impl Default for EventConverter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use vector_lib::event::LogEvent;

    fn create_test_log_event() -> LogEvent {
        let mut log = LogEvent::from(BTreeMap::new());
        log.insert("_vector_table", "test_table");
        log.insert("_vector_timestamp", "2024-01-01T12:00:00Z");
        log.insert("test_string", "hello");
        log.insert("test_int", 42);
        log
    }

    #[test]
    fn test_string_column_system_fields() {
        let events = vec![Event::Log(create_test_log_event())];
        let field = Field::new("_vector_table", DataType::Utf8, false);

        let result = EventConverter::build_string_column(&field, &events);
        assert!(result.is_ok());

        let array = result.unwrap();
        assert_eq!(array.len(), 1);
    }

    #[test]
    fn test_int64_column() {
        let mut log = create_test_log_event();
        log.insert("test_int", 123456789i64);
        let events = vec![Event::Log(log)];
        let field = Field::new("test_int", DataType::Int64, true);

        let result = EventConverter::build_int64_column(&field, &events);
        assert!(result.is_ok());
    }

    #[test]
    fn test_boolean_column() {
        let mut log = create_test_log_event();
        log.insert("test_bool", true);
        let events = vec![Event::Log(log)];
        let field = Field::new("test_bool", DataType::Boolean, true);

        let result = EventConverter::build_boolean_column(&field, &events);
        assert!(result.is_ok());
    }
}
