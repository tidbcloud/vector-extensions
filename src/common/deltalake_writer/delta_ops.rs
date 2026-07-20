use std::collections::HashMap;
use std::path::PathBuf;

use arrow::record_batch::RecordBatch;
use deltalake::kernel::TableFeatures;
use deltalake::operations::create::CreateBuilder;
use deltalake::operations::write::WriteBuilder;
use deltalake::DeltaOps;
use tracing::{error, info, warn};
use url::Url;

use super::errors::is_stale_delta_log_error;
use super::schema::SchemaManager;
use super::types::TypeConverter;

/// Delta Lake operations manager
pub struct DeltaOpsManager {
    /// Storage options for S3/cloud storage
    storage_options: Option<HashMap<String, String>>,
    /// Type converter for field conversions
    type_converter: TypeConverter,
}

impl DeltaOpsManager {
    pub fn new(storage_options: Option<HashMap<String, String>>) -> Self {
        Self {
            storage_options,
            type_converter: TypeConverter::new(),
        }
    }

    /// Create DeltaOps from URI with optional storage options
    pub async fn create_delta_ops(
        &self,
        table_uri: &Url,
    ) -> Result<DeltaOps, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(storage_options) = &self.storage_options {
            // Create redacted version for logging
            let mut redacted_options = storage_options.clone();
            if let Some(access_key) = redacted_options.get_mut("AWS_ACCESS_KEY_ID") {
                *access_key = "***".to_string();
            }
            if let Some(secret_key) = redacted_options.get_mut("AWS_SECRET_ACCESS_KEY") {
                *secret_key = "***REDACTED***".to_string();
            }
            if let Some(session_token) = redacted_options.get_mut("AWS_SESSION_TOKEN") {
                *session_token = "***REDACTED***".to_string();
            }
            info!(
                "Using storage options: {:?}",
                redacted_options
            );
            Ok(DeltaOps::try_from_uri_with_storage_options(
                table_uri.clone(),
                storage_options.clone(),
            )
            .await?)
        } else {
            info!("No storage options provided, using default credential chain");
            Ok(DeltaOps::try_from_uri(table_uri.clone()).await?)
        }
    }

    /// Create and configure write builder with partition columns and schema mode
    pub fn configure_write_builder(
        &self,
        table_ops: DeltaOps,
        record_batch: RecordBatch,
        partition_by: Option<&Vec<String>>,
    ) -> WriteBuilder {
        let mut write_builder = table_ops.write(vec![record_batch]);
        // Always pass partition columns on write; for new tables this applies partitioning,
        // for existing tables it validates consistency
        if let Some(partitions) = partition_by {
            write_builder = write_builder.with_partition_columns(partitions.clone());
        }
        // Allow protocol/schema update so timestamp ntz writer feature can be enabled when needed
        write_builder.with_schema_mode(deltalake::operations::write::SchemaMode::Merge)
    }

    /// Create a new Delta table
    pub async fn create_table(
        &self,
        table_uri: &Url,
        table_name: &str,
        schema: &arrow::datatypes::Schema,
        partition_by: Option<&Vec<String>>,
        storage_options: Option<&HashMap<String, String>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!(
            "Creating new Delta table at {} for table {}",
            table_uri, table_name
        );

        let mut create_builder = CreateBuilder::new()
            .with_location(table_uri.to_string())
            .with_columns(
                schema
                    .fields()
                    .iter()
                    .map(|field| self.type_converter.arrow_field_to_delta(field)),
            );

        // Add storage options for S3
        if let Some(storage_options) = storage_options {
            create_builder = create_builder.with_storage_options(storage_options.clone());
        }

        // Add partition columns if configured
        if let Some(partition_cols) = partition_by {
            info!(
                "Setting partition columns for table {}: {:?}",
                table_name, partition_cols
            );
            create_builder = create_builder.with_partition_columns(partition_cols.clone());
        } else {
            info!("No partition columns configured for table {}", table_name);
        }

        create_builder.await?;
        info!("Successfully created new Delta table");

        // Add TimestampWithoutTimezone feature to support Timestamp columns
        info!("Adding TimestampWithoutTimezone feature to Delta table");
        let table_ops_for_feature = self.create_delta_ops(table_uri).await?;

        // Load the table first to ensure state is initialized
        match table_ops_for_feature.load().await {
            Ok((loaded_table, _stream)) => {
                // Now try to add the feature with the loaded table
                match DeltaOps::from(loaded_table)
                    .add_feature()
                    .with_feature(TableFeatures::TimestampWithoutTimezone)
                    .with_allow_protocol_versions_increase(true)
                    .await
                {
                    Ok(_) => {
                        info!(
                            "✅ Successfully added TimestampWithoutTimezone feature to Delta table"
                        );
                    }
                    Err(e) => {
                        warn!("Failed to add TimestampWithoutTimezone feature: {}. Continuing without it.", e);
                    }
                }
            }
            Err(e) => {
                warn!("Failed to load table for feature addition: {}. Continuing without TimestampWithoutTimezone feature.", e);
            }
        }

        Ok(())
    }

    /// Write record batch to Delta Lake (handles both existing and new tables)
    pub async fn write_to_delta_lake(
        &self,
        table_path: &PathBuf,
        table_name: &str,
        record_batch: RecordBatch,
        schema_manager: &SchemaManager,
        storage_options: Option<&HashMap<String, String>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Build Delta table URI as url::Url
        // For non-cloud paths, add file:// protocol prefix (only absolute paths are supported)
        let table_uri = {
            let path_str = table_path.to_string_lossy();
            if path_str.starts_with("s3://")
                || path_str.starts_with("abfss://")
                || path_str.starts_with("gs://")
            {
                // Cloud storage paths already have protocol prefix
                Url::parse(&path_str)?
            } else if path_str.starts_with("file://") {
                // Already has file:// prefix
                Url::parse(&path_str)?
            } else {
                // For local paths, only absolute paths are supported
                if !table_path.is_absolute() {
                    return Err(format!(
                        "Only absolute paths are supported for local file system. Got: {}",
                        path_str
                    )
                    .into());
                }
                // For absolute paths: file:///path (three slashes)
                let file_url = format!("file://{}", path_str);
                Url::parse(&file_url)?
            }
        };

        // For local paths, ensure table directory exists
        if table_uri.scheme() == "file" {
            if let Ok(path) = table_uri.to_file_path() {
                std::fs::create_dir_all(&path)?;
            }
        }

        info!("Writing to Delta Lake table at: {}", table_uri);

        // Get partition columns from schema manager (set via _schema_metadata._partition_by in events)
        let partition_by = schema_manager.get_partition_by(table_name);

        // Try to write directly first (avoid load() which can panic in deltalake-core 0.28.1)
        // Retry logic for transaction conflicts (concurrent writes)
        const MAX_RETRIES: u32 = 3;
        const INITIAL_RETRY_DELAY_MS: u64 = 100;

        for attempt in 0..MAX_RETRIES {
            if attempt > 0 {
                // Exponential backoff: 100ms, 200ms, 400ms
                let delay_ms = INITIAL_RETRY_DELAY_MS * (1 << (attempt - 1));
                let delay = std::time::Duration::from_millis(delay_ms);
                info!(
                    "Retrying write to Delta table (attempt {}/{}) after {:?} delay",
                    attempt + 1,
                    MAX_RETRIES,
                    delay
                );
                tokio::time::sleep(delay).await;
            }

            // Reload table_ops on each attempt to get latest table state
            // Use DeltaOps for improved S3 support, following the successful test pattern
            let table_ops = self.create_delta_ops(&table_uri).await?;

            info!(
                "Attempting to write to Delta table at {} (attempt {}/{})",
                table_uri,
                attempt + 1,
                MAX_RETRIES
            );

            let write_builder =
                self.configure_write_builder(table_ops, record_batch.clone(), partition_by);
            let write_result = write_builder.await;

            match write_result {
                Ok(table) => {
                    info!("✅ Successfully wrote to Delta table at {}", table_uri);
                    info!("Table version: {:?}", table.version());
                    return Ok(());
                }
                Err(e) => {
                    let error_str = e.to_string();

                    // Check if error is due to table not existing
                    if error_str.contains("does not exist")
                        || error_str.contains("not found")
                        || error_str.contains("Not a Delta table")
                    {
                        info!(
                            "Table doesn't exist, will create it. Error was: {}",
                            error_str
                        );
                        // Fall through to table creation below
                        break;
                    }
                    // Check if error is due to transaction conflict (retryable)
                    else if error_str.contains("conflict detected")
                        || error_str.contains("Metadata changed since last commit")
                        || error_str.contains("concurrent modification")
                    {
                        if attempt < MAX_RETRIES - 1 {
                            warn!(
                                "Transaction conflict detected (attempt {}/{}): {}. Will retry...",
                                attempt + 1,
                                MAX_RETRIES,
                                error_str
                            );
                            // Continue to retry
                            continue;
                        } else {
                            error!(
                                "Transaction conflict after {} retries: {}",
                                MAX_RETRIES, error_str
                            );
                            return Err(e.into());
                        }
                    }
                    // Stale log view after external compaction or concurrent writers
                    else if is_stale_delta_log_error(&error_str) {
                        if attempt < MAX_RETRIES - 1 {
                            warn!(
                                "Stale Delta log detected (attempt {}/{}): {}. Reloading table and retrying...",
                                attempt + 1,
                                MAX_RETRIES,
                                error_str
                            );
                            continue;
                        } else {
                            error!(
                                "Stale Delta log after {} retries: {}",
                                MAX_RETRIES, error_str
                            );
                            return Err(e.into());
                        }
                    } else {
                        // Other error, fail immediately
                        error!("Failed to write to Delta table: {}", e);
                        return Err(e.into());
                    }
                }
            }
        }

        // If we reach here, table doesn't exist and needs to be created
        // Create new table first
        self.create_table(
            &table_uri,
            table_name,
            &record_batch.schema(),
            partition_by,
            storage_options,
        )
        .await?;

        // Now write the data using DeltaOps - reload the table_ops to get the created table
        let table_ops = self.create_delta_ops(&table_uri).await?;
        let write_builder = self.configure_write_builder(table_ops, record_batch, partition_by);
        let write_result = write_builder.await?;

        info!(
            "Successfully wrote data to Delta Lake table at {}, version: {:?}",
            table_uri,
            write_result.version()
        );

        Ok(())
    }

    /// Check if Delta table exists at the given URI
    #[allow(dead_code)]
    pub async fn table_exists(
        &self,
        table_uri: &Url,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if table_uri.scheme() == "s3" {
            // For S3, we need to check if _delta_log exists
            // This is a simplified check - in practice you'd use the Delta Lake APIs
            // For now, we'll always return false for S3 to trigger table creation logic
            Ok(false)
        } else {
            // For local filesystem
            if let Ok(path) = table_uri.to_file_path() {
                Ok(path.join("_delta_log").exists())
            } else {
                Ok(false)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    #[allow(dead_code)]
    fn create_test_schema() -> Schema {
        let fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ];
        Schema::new(fields)
    }

    #[test]
    fn test_arrow_field_to_delta_conversion() {
        let manager = DeltaOpsManager::new(None);
        let field = Field::new("test_field", DataType::Int64, false);

        let delta_field = manager.type_converter.arrow_field_to_delta(&field);
        assert_eq!(delta_field.name, "test_field");
        assert_eq!(delta_field.nullable, false);
    }
}
