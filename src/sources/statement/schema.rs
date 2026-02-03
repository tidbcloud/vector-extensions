// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use tracing::{debug, info};

/// Data type for schema fields.
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    String,
    Int64,
    UInt64,
    Float64,
    Bool,
    Bytes,
    Timestamp,
}

/// A field in the schema.
#[derive(Debug, Clone)]
pub struct SchemaField {
    /// Field name
    pub name: String,

    /// Field data type
    pub data_type: DataType,

    /// Whether the field is nullable
    pub nullable: bool,

    /// Field description
    pub description: Option<String>,

    /// Whether this is a core field or extended field
    pub is_extended: bool,
}

/// Schema registry for tracking known fields and auto-discovering new ones.
pub struct SchemaRegistry {
    /// Core fields (from proto definition)
    core_fields: HashMap<String, SchemaField>,

    /// Extended fields (discovered from extended_metrics)
    extended_fields: RwLock<HashMap<String, SchemaField>>,

    /// Set of all known field names for quick lookup
    known_fields: RwLock<HashSet<String>>,
}

impl SchemaRegistry {
    /// Creates a new SchemaRegistry with default core fields.
    pub fn new() -> Self {
        let mut core_fields = HashMap::new();
        let mut known_fields = HashSet::new();

        // Add all core fields from proto
        let fields = vec![
            // Identity fields
            ("digest", DataType::String, false, "SQL statement digest"),
            ("plan_digest", DataType::String, true, "Execution plan digest"),
            ("schema_name", DataType::String, true, "Database schema name"),
            ("normalized_sql", DataType::String, true, "Normalized SQL text"),
            ("table_names", DataType::String, true, "Comma-separated table names"),
            ("stmt_type", DataType::String, true, "Statement type"),

            // Sample data
            ("sample_sql", DataType::String, true, "Sample SQL with parameters"),
            ("sample_plan", DataType::String, true, "Sample execution plan"),
            ("prev_sql", DataType::String, true, "Previous SQL in transaction"),

            // Execution statistics
            ("exec_count", DataType::Int64, false, "Total execution count"),
            ("sum_errors", DataType::Int64, true, "Total error count"),
            ("sum_warnings", DataType::Int64, true, "Total warning count"),

            // Latency metrics
            ("sum_latency_us", DataType::Int64, false, "Total latency (microseconds)"),
            ("max_latency_us", DataType::Int64, true, "Maximum latency"),
            ("min_latency_us", DataType::Int64, true, "Minimum latency"),
            ("avg_latency_us", DataType::Int64, true, "Average latency"),
            ("p50_latency_us", DataType::Int64, true, "P50 latency"),
            ("p95_latency_us", DataType::Int64, true, "P95 latency"),
            ("p99_latency_us", DataType::Int64, true, "P99 latency"),

            // Parse/Compile
            ("sum_parse_latency_us", DataType::Int64, true, "Total parse time"),
            ("max_parse_latency_us", DataType::Int64, true, "Max parse time"),
            ("sum_compile_latency_us", DataType::Int64, true, "Total compile time"),
            ("max_compile_latency_us", DataType::Int64, true, "Max compile time"),

            // Resources
            ("sum_mem_bytes", DataType::Int64, true, "Total memory usage"),
            ("max_mem_bytes", DataType::Int64, true, "Peak memory usage"),
            ("sum_disk_bytes", DataType::Int64, true, "Total disk I/O"),
            ("max_disk_bytes", DataType::Int64, true, "Peak disk I/O"),
            ("sum_tidb_cpu_us", DataType::Int64, true, "Total TiDB CPU time"),
            ("sum_tikv_cpu_us", DataType::Int64, true, "Total TiKV CPU time"),

            // Coprocessor
            ("sum_num_cop_tasks", DataType::Int64, true, "Total coprocessor tasks"),
            ("sum_process_time_us", DataType::Int64, true, "Total TiKV process time"),
            ("max_process_time_us", DataType::Int64, true, "Max TiKV process time"),
            ("sum_wait_time_us", DataType::Int64, true, "Total TiKV wait time"),
            ("max_wait_time_us", DataType::Int64, true, "Max TiKV wait time"),

            // Keys
            ("sum_total_keys", DataType::Int64, true, "Total keys scanned"),
            ("max_total_keys", DataType::Int64, true, "Max keys in single execution"),
            ("sum_processed_keys", DataType::Int64, true, "Total keys processed"),
            ("max_processed_keys", DataType::Int64, true, "Max keys processed"),

            // Transaction
            ("commit_count", DataType::Int64, true, "Number of commits"),
            ("sum_prewrite_time_us", DataType::Int64, true, "Total prewrite time"),
            ("max_prewrite_time_us", DataType::Int64, true, "Max prewrite time"),
            ("sum_commit_time_us", DataType::Int64, true, "Total commit time"),
            ("max_commit_time_us", DataType::Int64, true, "Max commit time"),
            ("sum_write_keys", DataType::Int64, true, "Total write keys"),
            ("max_write_keys", DataType::Int64, true, "Max write keys"),
            ("sum_write_size_bytes", DataType::Int64, true, "Total write size"),
            ("max_write_size_bytes", DataType::Int64, true, "Max write size"),

            // Rows
            ("sum_affected_rows", DataType::Int64, true, "Total affected rows"),
            ("sum_result_rows", DataType::Int64, true, "Total result rows"),
            ("max_result_rows", DataType::Int64, true, "Max result rows"),
            ("min_result_rows", DataType::Int64, true, "Min result rows"),

            // Plan cache
            ("plan_in_cache", DataType::Bool, true, "Is plan cached"),
            ("plan_cache_hits", DataType::Int64, true, "Cache hit count"),

            // Timestamps
            ("first_seen_ms", DataType::Int64, true, "First execution time"),
            ("last_seen_ms", DataType::Int64, true, "Last execution time"),

            // Flags
            ("is_internal", DataType::Bool, true, "Is internal SQL"),
            ("prepared", DataType::Bool, true, "Is prepared statement"),

            // Multi-tenancy
            ("keyspace_name", DataType::String, true, "Keyspace name"),
            ("keyspace_id", DataType::UInt64, true, "Keyspace ID"),
            ("resource_group_name", DataType::String, true, "Resource group"),
        ];

        for (name, dtype, nullable, desc) in fields {
            let field = SchemaField {
                name: name.to_string(),
                data_type: dtype,
                nullable,
                description: Some(desc.to_string()),
                is_extended: false,
            };
            known_fields.insert(name.to_string());
            core_fields.insert(name.to_string(), field);
        }

        info!("Schema registry initialized with {} core fields", core_fields.len());

        Self {
            core_fields,
            extended_fields: RwLock::new(HashMap::new()),
            known_fields: RwLock::new(known_fields),
        }
    }

    /// Registers a new extended field discovered from extended_metrics.
    pub fn register_extended_field(&self, name: String) {
        // Check if already known
        {
            let known = self.known_fields.read().unwrap();
            if known.contains(&name) {
                return;
            }
        }

        // Add new extended field
        {
            let mut known = self.known_fields.write().unwrap();
            let mut extended = self.extended_fields.write().unwrap();

            if !known.contains(&name) {
                info!("Discovered new extended field: {}", name);

                let field = SchemaField {
                    name: name.clone(),
                    data_type: DataType::Float64, // Default to float64 for extended metrics
                    nullable: true,
                    description: Some(format!("Extended metric: {}", name)),
                    is_extended: true,
                };

                known.insert(name.clone());
                extended.insert(name, field);
            }
        }
    }

    /// Returns true if a field is known.
    pub fn is_known_field(&self, name: &str) -> bool {
        self.known_fields.read().unwrap().contains(name)
    }

    /// Returns a field by name.
    pub fn get_field(&self, name: &str) -> Option<SchemaField> {
        if let Some(field) = self.core_fields.get(name) {
            return Some(field.clone());
        }

        self.extended_fields
            .read()
            .unwrap()
            .get(name)
            .cloned()
    }

    /// Returns all known field names.
    pub fn all_field_names(&self) -> Vec<String> {
        self.known_fields.read().unwrap().iter().cloned().collect()
    }

    /// Returns all core fields.
    pub fn core_fields(&self) -> Vec<SchemaField> {
        self.core_fields.values().cloned().collect()
    }

    /// Returns all extended fields.
    pub fn extended_fields(&self) -> Vec<SchemaField> {
        self.extended_fields.read().unwrap().values().cloned().collect()
    }

    /// Returns the total number of known fields.
    pub fn field_count(&self) -> usize {
        self.known_fields.read().unwrap().len()
    }

    /// Generates an Arrow schema for Parquet writing.
    #[cfg(feature = "arrow")]
    pub fn to_arrow_schema(&self) -> arrow::datatypes::Schema {
        use arrow::datatypes::{DataType as ArrowDataType, Field};

        let mut fields = Vec::new();

        // Add core fields
        for field in self.core_fields.values() {
            let arrow_type = match field.data_type {
                DataType::String => ArrowDataType::Utf8,
                DataType::Int64 => ArrowDataType::Int64,
                DataType::UInt64 => ArrowDataType::UInt64,
                DataType::Float64 => ArrowDataType::Float64,
                DataType::Bool => ArrowDataType::Boolean,
                DataType::Bytes => ArrowDataType::Binary,
                DataType::Timestamp => ArrowDataType::Int64, // Store as epoch millis
            };
            fields.push(Field::new(&field.name, arrow_type, field.nullable));
        }

        // Add extended fields
        for field in self.extended_fields.read().unwrap().values() {
            fields.push(Field::new(&field.name, ArrowDataType::Float64, true));
        }

        arrow::datatypes::Schema::new(fields)
    }
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_registry() {
        let registry = SchemaRegistry::new();
        assert!(registry.is_known_field("digest"));
        assert!(registry.is_known_field("exec_count"));
        assert!(!registry.is_known_field("unknown_field"));
    }

    #[test]
    fn test_extended_field_discovery() {
        let registry = SchemaRegistry::new();
        assert!(!registry.is_known_field("sum_network_bytes"));

        registry.register_extended_field("sum_network_bytes".to_string());
        assert!(registry.is_known_field("sum_network_bytes"));

        let field = registry.get_field("sum_network_bytes").unwrap();
        assert!(field.is_extended);
    }
}
