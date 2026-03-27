use std::collections::HashMap;
use std::path::PathBuf;

use arrow::datatypes::Schema;
use serde::{Deserialize, Serialize};
use tracing::info;
use vector_lib::event::Event;

// Module declarations
pub mod converter;
pub mod delta_ops;
pub mod schema;
pub mod types;

// Re-export main types
pub use converter::EventConverter;
pub use delta_ops::DeltaOpsManager;
pub use schema::SchemaManager;
pub use types::TypeConverter;

/// Delta table configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaTableConfig {
    /// Table name
    pub name: String,

    /// Enable schema evolution
    pub schema_evolution: Option<bool>,
}

/// Write configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteConfig {
    /// Batch size for writing
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Write timeout in seconds
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
}

pub const fn default_batch_size() -> usize {
    1000
}

pub const fn default_timeout_secs() -> u64 {
    30
}

/// Delta Lake table writer (refactored version)
pub struct DeltaLakeWriter {
    /// Table path (local, S3, or Azure)
    table_path: PathBuf,
    /// Table configuration
    table_config: DeltaTableConfig,
    /// Write configuration
    #[allow(dead_code)]
    write_config: WriteConfig,
    /// Storage options for S3/Azure cloud storage
    storage_options: Option<HashMap<String, String>>,
    /// Fixed Arrow schema for this table to ensure consistency across batches
    fixed_arrow_schema: Option<Schema>,
    /// Schema manager for caching and schema extraction
    schema_manager: SchemaManager,
    /// Delta operations manager
    delta_ops_manager: DeltaOpsManager,
}

impl DeltaLakeWriter {
    /// Create a new Delta Lake writer
    pub fn new(
        table_path: PathBuf,
        table_config: DeltaTableConfig,
        write_config: WriteConfig,
        storage_options: Option<HashMap<String, String>>,
    ) -> Self {
        Self::new_with_options(
            table_path,
            table_config,
            write_config,
            storage_options,
            true,
        )
    }

    /// Create a new Delta Lake writer with options
    pub fn new_with_options(
        table_path: PathBuf,
        table_config: DeltaTableConfig,
        write_config: WriteConfig,
        storage_options: Option<HashMap<String, String>>,
        enable_standard_fields: bool,
    ) -> Self {
        let path_str = table_path.to_string_lossy();
        if path_str.starts_with("s3://") {
            deltalake::aws::register_handlers(None);
            info!("Registered Delta Lake S3 handlers for path: {}", path_str);
        } else if path_str.starts_with("az://") {
            deltalake::azure::register_handlers(None);
            info!(
                "Registered Delta Lake Azure handlers for path: {}",
                path_str
            );
        }

        let type_converter = TypeConverter::new();
        let schema_manager =
            SchemaManager::new_with_options(type_converter, enable_standard_fields);
        let delta_ops_manager = DeltaOpsManager::new(storage_options.clone());

        Self {
            table_path,
            table_config,
            write_config,
            storage_options,
            fixed_arrow_schema: None,
            schema_manager,
            delta_ops_manager,
        }
    }

    /// Write events to Delta Lake
    pub async fn write_events(
        &mut self,
        events: Vec<Event>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if events.is_empty() {
            return Ok(());
        }

        // Get table name for logging and partition lookup (before moving events)
        let table_name = match events.first() {
            Some(Event::Log(log_event)) => {
                if let Some(table_name) = log_event.get("_vector_table").and_then(|v| v.as_str()) {
                    table_name.to_string()
                } else {
                    self.table_config.name.clone()
                }
            }
            _ => self.table_config.name.clone(),
        };

        // Convert events to RecordBatch
        let (record_batch, schema) = EventConverter::events_to_record_batch(
            &mut self.schema_manager,
            events,
            &self.fixed_arrow_schema,
            Some(&self.table_config.name),
        )?;

        // Cache the schema if not already cached
        if self.fixed_arrow_schema.is_none() {
            self.fixed_arrow_schema = Some(schema.clone());
            // Also cache in schema manager
            self.schema_manager
                .cache_arrow_schema(table_name.clone(), schema);
        }

        // Write to Delta Lake
        self.delta_ops_manager
            .write_to_delta_lake(
                &self.table_path,
                &table_name,
                record_batch,
                &self.schema_manager,
                self.storage_options.as_ref(),
            )
            .await?;

        Ok(())
    }

    /// Get the table path
    #[allow(dead_code)]
    pub fn table_path(&self) -> &PathBuf {
        &self.table_path
    }

    /// Get the table configuration
    #[allow(dead_code)]
    pub fn table_config(&self) -> &DeltaTableConfig {
        &self.table_config
    }

    /// Get the write configuration
    #[allow(dead_code)]
    pub fn write_config(&self) -> &WriteConfig {
        &self.write_config
    }

    /// Get the storage options
    #[allow(dead_code)]
    pub fn storage_options(&self) -> Option<&HashMap<String, String>> {
        self.storage_options.as_ref()
    }

    /// Get the fixed Arrow schema
    #[allow(dead_code)]
    pub fn fixed_arrow_schema(&self) -> Option<&Schema> {
        self.fixed_arrow_schema.as_ref()
    }

    /// Get the schema manager
    #[allow(dead_code)]
    pub fn schema_manager(&self) -> &SchemaManager {
        &self.schema_manager
    }

    /// Get the Delta operations manager
    #[allow(dead_code)]
    pub fn delta_ops_manager(&self) -> &DeltaOpsManager {
        &self.delta_ops_manager
    }
}
