use std::collections::HashMap;

use arrow::datatypes::{DataType, Field, Schema};
use tracing::{info, warn};
use vector_lib::event::{Event, LogEvent, Value as LogValue};

use super::types::TypeConverter;

/// Schema metadata extracted from events
#[derive(Debug, Clone)]
pub struct SchemaMetadata {
    pub field_types: HashMap<String, String>, // field_name -> mysql_type
    pub partition_by: Option<Vec<String>>,
}

/// Schema manager with caching
pub struct SchemaManager {
    /// Cached schema metadata per table
    cached_schemas: HashMap<String, SchemaMetadata>,
    /// Cached Arrow schemas per table
    cached_arrow_schemas: HashMap<String, Schema>,
    /// Type converter
    type_converter: TypeConverter,
}

impl SchemaManager {
    pub fn new(type_converter: TypeConverter) -> Self {
        Self {
            cached_schemas: HashMap::new(),
            cached_arrow_schemas: HashMap::new(),
            type_converter,
        }
    }

    /// Extract and cache schema metadata from event
    pub fn extract_and_cache(&mut self, log_event: &LogEvent) -> Option<SchemaMetadata> {
        // Get table name for schema cache key
        let table_name = log_event
            .get("_vector_table")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "unknown_table".to_string());

        // Only extract if not already cached
        if !self.cached_schemas.contains_key(&table_name) {
            if let Some(schema_metadata) = log_event.get("_schema_metadata") {
                if let Some(schema_obj) = schema_metadata.as_object() {
                    let mut field_types = HashMap::new();
                    let mut partition_by: Option<Vec<String>> = None;

                    // Extract field types and partition info
                    for (field, info) in schema_obj {
                        if field == "_partition_by" {
                            // Extract partition columns from schema metadata
                            if let Some(partition_str) = info.as_str() {
                                partition_by = Some(
                                    partition_str
                                        .split(',')
                                        .map(|s| s.trim().to_string())
                                        .filter(|s| !s.is_empty())
                                        .collect(),
                                );
                            } else if let Some(partition_array) = info.as_array() {
                                partition_by = Some(
                                    partition_array
                                        .iter()
                                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                        .collect(),
                                );
                            }
                        } else if let Some(mysql_type) =
                            info.get("mysql_type").and_then(|v| v.as_str())
                        {
                            field_types.insert(field.to_string(), mysql_type.to_string());
                        }
                    }

                    info!(
                        "Cached schema for table {} with {} fields and partition_by: {:?}",
                        table_name,
                        field_types.len(),
                        partition_by
                    );

                    let metadata = SchemaMetadata {
                        field_types,
                        partition_by,
                    };

                    self.cached_schemas
                        .insert(table_name.clone(), metadata.clone());
                    return Some(metadata);
                }
            }
        }

        // Return cached metadata if available
        self.cached_schemas.get(&table_name).cloned()
    }

    /// Build Arrow schema from event
    pub fn build_arrow_schema(
        &mut self,
        event: &Event,
    ) -> Result<Schema, Box<dyn std::error::Error + Send + Sync>> {
        if let Event::Log(log_event) = event {
            let mut fields = Vec::new();
            let mut added_fields = std::collections::HashSet::new();

            // First, extract and cache the MySQL schema metadata from the event
            self.extract_and_cache(log_event);

            // Get table name for schema lookup
            let table_name = log_event
                .get("_vector_table")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| "unknown_table".to_string());

            // Build fixed field list based on cached MySQL schema and Vector system fields

            // 1. Add Vector system fields first
            fields.extend(self.add_system_fields());
            for field in &fields {
                added_fields.insert(field.name().to_string());
            }

            // 2. Add date field for partitioning (derived from _vector_timestamp)
            fields.push(Field::new("date", DataType::Utf8, false));
            added_fields.insert("date".to_string());

            // 3. Add all MySQL data fields from cached schema (in deterministic order)
            if let Some(metadata) = self.cached_schemas.get(&table_name) {
                // Sort field names to ensure consistent order
                let mut field_names: Vec<_> = metadata.field_types.keys().collect();
                field_names.sort();

                for field_name in field_names {
                    // Skip if conflicts with Vector system fields or is metadata field
                    if !added_fields.contains(field_name)
                        && !field_name.starts_with("_schema_metadata")
                    {
                        if let Some(mysql_type) = metadata.field_types.get(field_name) {
                            let data_type = self.type_converter.mysql_to_arrow(mysql_type);
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
                                self.get_arrow_type(log_event, &table_name, &key_str, value);
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

    /// Get Arrow data type from cached schema or extract from event
    pub fn get_arrow_type(
        &mut self,
        log_event: &LogEvent,
        table_name: &str,
        field_name: &str,
        value: &LogValue,
    ) -> DataType {
        // Check if we already have cached schema for this table
        if let Some(metadata) = self.cached_schemas.get(table_name) {
            if let Some(mysql_type) = metadata.field_types.get(field_name) {
                return self.type_converter.mysql_to_arrow(mysql_type);
            }
        }

        // Try to extract and cache schema from this event's _schema_metadata
        if let Some(schema_metadata) = log_event.get("_schema_metadata") {
            if let Some(schema_obj) = schema_metadata.as_object() {
                // Cache the entire schema for this table
                let mut field_types = HashMap::new();
                for (field, info) in schema_obj {
                    if let Some(mysql_type) = info.get("mysql_type").and_then(|v| v.as_str()) {
                        field_types.insert(field.to_string(), mysql_type.to_string());
                    }
                }

                // Cache the metadata
                let metadata = SchemaMetadata {
                    field_types,
                    partition_by: None, // Will be extracted separately if needed
                };
                self.cached_schemas.insert(table_name.to_string(), metadata);

                // Now get the type for current field
                if let Some(metadata) = self.cached_schemas.get(table_name) {
                    if let Some(mysql_type) = metadata.field_types.get(field_name) {
                        return self.type_converter.mysql_to_arrow(mysql_type);
                    }
                }
            }
        }

        // Fallback to inference if schema not available
        self.type_converter.infer_arrow_type(field_name, value)
    }

    /// Get cached partition_by for a table
    pub fn get_partition_by(&self, table_name: &str) -> Option<&Vec<String>> {
        self.cached_schemas
            .get(table_name)
            .and_then(|metadata| metadata.partition_by.as_ref())
    }

    /// Get cached Arrow schema for a table
    #[allow(dead_code)]
    pub fn get_cached_arrow_schema(&self, table_name: &str) -> Option<&Schema> {
        self.cached_arrow_schemas.get(table_name)
    }

    /// Cache an Arrow schema for a table
    pub fn cache_arrow_schema(&mut self, table_name: String, schema: Schema) {
        self.cached_arrow_schemas.insert(table_name, schema);
    }

    /// Add Vector system fields
    fn add_system_fields(&self) -> Vec<Field> {
        let standard_fields = [
            "_vector_table",
            "_vector_source_table",
            "_vector_source_schema",
            "_vector_instance",
            "_vector_timestamp",
        ];

        standard_fields
            .iter()
            .map(|name| Field::new(*name, DataType::Utf8, false))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_fields() {
        let type_converter = TypeConverter::new();
        let manager = SchemaManager::new(type_converter);

        let fields = manager.add_system_fields();
        assert_eq!(fields.len(), 5);
        assert_eq!(fields[0].name(), "_vector_table");
        assert_eq!(fields[4].name(), "_vector_timestamp");
    }

    #[test]
    fn test_extract_partition_by_from_event_string() {
        use std::collections::BTreeMap;
        use vector_lib::event::{LogEvent, ObjectMap};

        let type_converter = TypeConverter::new();
        let mut manager = SchemaManager::new(type_converter);

        // Create a log event with partition_by in _schema_metadata
        let mut log = LogEvent::from(BTreeMap::new());
        log.insert("_vector_table", "test_table");

        // Add schema metadata with partition_by as string
        let mut schema_meta = ObjectMap::new();
        schema_meta.insert("_partition_by".into(), LogValue::from("date,hour"));

        let mut field_meta = ObjectMap::new();
        field_meta.insert("mysql_type".into(), LogValue::from("bigint"));
        schema_meta.insert("id".into(), LogValue::Object(field_meta));

        log.insert("_schema_metadata", LogValue::Object(schema_meta));

        // Extract and cache
        let metadata = manager.extract_and_cache(&log);
        assert!(metadata.is_some());

        let metadata = metadata.unwrap();
        assert!(metadata.partition_by.is_some());

        let partition_cols = metadata.partition_by.unwrap();
        assert_eq!(partition_cols.len(), 2);
        assert_eq!(partition_cols[0], "date");
        assert_eq!(partition_cols[1], "hour");
    }

    #[test]
    fn test_extract_partition_by_from_event_array() {
        use std::collections::BTreeMap;
        use vector_lib::event::{LogEvent, ObjectMap};

        let type_converter = TypeConverter::new();
        let mut manager = SchemaManager::new(type_converter);

        // Create a log event with partition_by in _schema_metadata
        let mut log = LogEvent::from(BTreeMap::new());
        log.insert("_vector_table", "test_table");

        // Add schema metadata with partition_by as array
        let mut schema_meta = ObjectMap::new();
        let partition_array = vec![LogValue::from("date"), LogValue::from("hour")];
        schema_meta.insert("_partition_by".into(), LogValue::Array(partition_array));

        let mut field_meta = ObjectMap::new();
        field_meta.insert("mysql_type".into(), LogValue::from("bigint"));
        schema_meta.insert("id".into(), LogValue::Object(field_meta));

        log.insert("_schema_metadata", LogValue::Object(schema_meta));

        // Extract and cache
        let metadata = manager.extract_and_cache(&log);
        assert!(metadata.is_some());

        let metadata = metadata.unwrap();
        assert!(metadata.partition_by.is_some());

        let partition_cols = metadata.partition_by.unwrap();
        assert_eq!(partition_cols.len(), 2);
        assert_eq!(partition_cols[0], "date");
        assert_eq!(partition_cols[1], "hour");
    }

    #[test]
    fn test_get_partition_by() {
        use std::collections::BTreeMap;
        use vector_lib::event::{LogEvent, ObjectMap};

        let type_converter = TypeConverter::new();
        let mut manager = SchemaManager::new(type_converter);

        // Create a log event with partition_by
        let mut log = LogEvent::from(BTreeMap::new());
        log.insert("_vector_table", "test_table");

        let mut schema_meta = ObjectMap::new();
        schema_meta.insert("_partition_by".into(), LogValue::from("date"));
        log.insert("_schema_metadata", LogValue::Object(schema_meta));

        // Extract and cache
        manager.extract_and_cache(&log);

        // Get partition_by
        let partition_by = manager.get_partition_by("test_table");
        assert!(partition_by.is_some());
        assert_eq!(partition_by.unwrap().len(), 1);
        assert_eq!(partition_by.unwrap()[0], "date");

        // Test with non-existent table
        let partition_by = manager.get_partition_by("non_existent");
        assert!(partition_by.is_none());
    }
}
