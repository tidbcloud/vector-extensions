use std::collections::HashMap;
use std::path::PathBuf;

use arrow::datatypes::Schema;
use tracing::info;
use vector_lib::event::Event;

use crate::sinks::deltalake::{DeltaTableConfig, WriteConfig};

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

/// Delta Lake table writer (refactored version)
pub struct DeltaLakeWriter {
    /// Table path (local or S3)
    table_path: PathBuf,
    /// Table configuration
    table_config: DeltaTableConfig,
    /// Write configuration
    #[allow(dead_code)]
    write_config: WriteConfig,
    /// Storage options for S3/cloud storage
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
        // Initialize S3 handlers if this is an S3 path
        if table_path.to_string_lossy().starts_with("s3://") {
            deltalake::aws::register_handlers(None);
            info!(
                "Registered Delta Lake S3 handlers for path: {}",
                table_path.to_string_lossy()
            );
        }

        let type_converter = TypeConverter::new();
        let schema_manager = SchemaManager::new(type_converter);
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
