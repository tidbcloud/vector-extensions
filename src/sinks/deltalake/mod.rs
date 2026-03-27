use std::collections::HashMap;
use std::path::PathBuf;

use vector::{
    aws::{AwsAuthentication, RegionOrEndpoint},
    config::{GenerateConfig, SinkConfig, SinkContext},
    sinks::{
        s3_common::{config::S3Options, service::S3Service},
        Healthcheck,
    },
};

use vector_lib::{
    config::proxy::ProxyConfig,
    config::{AcknowledgementsConfig, DataType, Input},
    configurable::configurable_component,
    sink::VectorSink,
    tls::TlsConfig,
};

use crate::sinks::deltalake::processor::DeltaLakeSink;

use tracing::{error, info, warn};

mod processor;

// Import default functions from common module
use crate::common::deltalake_s3;
use crate::common::deltalake_writer::{default_batch_size, default_timeout_secs};

// Re-export types from common module
pub use crate::common::deltalake_writer::{DeltaLakeWriter, DeltaTableConfig, WriteConfig};

/// Configuration for the deltalake sink
#[configurable_component(sink("deltalake"))]
#[derive(Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct DeltaLakeConfig {
    /// Base path for Delta Lake tables
    pub base_path: String,

    /// Batch size for writing
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Write timeout in seconds
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,

    /// Storage options for cloud storage
    pub storage_options: Option<HashMap<String, String>>,

    /// S3 bucket name for remote storage
    pub bucket: Option<String>,

    /// S3 options
    #[serde(flatten)]
    pub options: Option<S3Options>,

    /// AWS region or endpoint
    #[serde(flatten)]
    pub region: Option<RegionOrEndpoint>,

    /// TLS configuration
    pub tls: Option<TlsConfig>,

    /// AWS authentication
    #[serde(default)]
    pub auth: AwsAuthentication,

    /// Specifies which addressing style to use
    #[serde(default = "default_force_path_style")]
    pub force_path_style: Option<bool>,

    /// Acknowledgments configuration
    #[serde(
        default,
        deserialize_with = "vector::serde::bool_or_struct",
        skip_serializing_if = "vector::serde::is_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,
}

pub fn default_force_path_style() -> Option<bool> {
    None
}

impl GenerateConfig for DeltaLakeConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            base_path: "./delta-tables".to_owned(),
            batch_size: default_batch_size(),
            timeout_secs: default_timeout_secs(),
            storage_options: None,
            bucket: None,
            options: None,
            region: None,
            tls: None,
            auth: AwsAuthentication::default(),
            force_path_style: None,
            acknowledgements: Default::default(),
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "deltalake")]
impl SinkConfig for DeltaLakeConfig {
    async fn build(&self, cx: SinkContext) -> vector::Result<(VectorSink, Healthcheck)> {
        info!(
            "Building Delta Lake sink with bucket: {:?}, base_path: {}",
            self.bucket, self.base_path
        );

        let is_cloud_path = self.base_path.starts_with("s3://")
            || self.base_path.starts_with("abfss://")
            || self.base_path.starts_with("gs://");

        // Create S3 service if bucket is configured (S3/OSS only)
        let s3_service = if self.bucket.is_some() {
            info!("Bucket configured, creating S3 service");
            match self.create_service(&cx.proxy).await {
                Ok(service) => {
                    info!("S3 service created successfully");
                    Some(service)
                }
                Err(e) => {
                    error!(
                        "Failed to create S3 service, falling back to credential-less mode: {}",
                        e
                    );
                    // Don't fail completely, but continue without S3Service
                    // Delta Lake will handle authentication through storage_options
                    None
                }
            }
        } else if is_cloud_path {
            info!(
                "Cloud storage path detected ({}), using storage_options for authentication",
                &self.base_path[..self.base_path.find("://").unwrap_or(0) + 3]
            );
            None
        } else {
            info!("No bucket configured, using local filesystem");
            None
        };

        info!("Building sink processor");
        let sink = self.build_processor(s3_service.as_ref(), cx).await?;

        info!("Building healthcheck");
        let healthcheck = self.build_healthcheck(s3_service.as_ref(), is_cloud_path)?;

        info!("Delta Lake sink build completed successfully");
        Ok((sink, healthcheck))
    }

    fn input(&self) -> Input {
        Input::new(DataType::Log)
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

impl DeltaLakeConfig {
    async fn build_processor(
        &self,
        s3_service: Option<&S3Service>,
        _cx: SinkContext,
    ) -> vector::Result<VectorSink> {
        // For OSS with virtual hosted style, we may need to adjust the base_path format
        // to ensure object_store correctly parses the bucket
        let base_path = if let Some(_endpoint) = self.region.as_ref().and_then(|r| r.endpoint()) {
            // If using custom endpoint (OSS), check if base_path needs adjustment
            // For virtual hosted style, base_path should be: s3://bucket-name/path
            // object_store should construct: http://bucket-name.endpoint/path
            if self.base_path.starts_with("s3://") {
                // Extract bucket from base_path if it's in the correct format
                // Format: s3://bucket-name/path
                let path_without_s3 = self
                    .base_path
                    .strip_prefix("s3://")
                    .unwrap_or(&self.base_path);
                if let Some((bucket, path)) = path_without_s3.split_once('/') {
                    // Verify bucket matches configured bucket
                    if let Some(configured_bucket) = &self.bucket {
                        if bucket != configured_bucket {
                            warn!("Bucket in base_path ({}) doesn't match configured bucket ({}), using configured bucket", 
                                  bucket, configured_bucket);
                        }
                    }
                    info!("Using base_path: s3://{}/{}", bucket, path);
                }
            }
            PathBuf::from(&self.base_path)
        } else {
            PathBuf::from(&self.base_path)
        };

        // Tables are discovered dynamically from events
        // Default partition configuration will be applied to all tables
        let table_configs: Vec<DeltaTableConfig> = Vec::new();

        let write_config = WriteConfig {
            batch_size: self.batch_size,
            timeout_secs: self.timeout_secs,
        };

        let mut storage_options = self.storage_options.clone().unwrap_or_default();

        // Add S3 storage options if S3 service is available
        if let Some(service) = s3_service {
            info!("Applying S3 storage options - S3 service found");
            self.apply_s3_storage_options(&mut storage_options, service)
                .await?;
        } else {
            info!("No S3 service available - using default storage options only");
        }

        let sink = DeltaLakeSink::new(
            base_path,
            table_configs,
            write_config,
            Some(storage_options),
        );

        Ok(VectorSink::from_event_streamsink(sink))
    }

    pub async fn create_service(&self, proxy: &ProxyConfig) -> vector::Result<S3Service> {
        deltalake_s3::create_service(
            self.bucket.as_deref(),
            self.region.as_ref(),
            &self.auth,
            proxy,
            self.tls.as_ref(),
            self.force_path_style,
        )
        .await
    }

    async fn apply_s3_storage_options(
        &self,
        storage_options: &mut HashMap<String, String>,
        _service: &S3Service,
    ) -> vector::Result<()> {
        deltalake_s3::apply_s3_storage_options(
            storage_options,
            self.bucket.as_deref(),
            self.region.as_ref(),
            &self.auth,
            self.force_path_style,
        )
        .await
    }

    fn build_healthcheck(
        &self,
        s3_service: Option<&S3Service>,
        is_cloud_path: bool,
    ) -> vector::Result<Healthcheck> {
        deltalake_s3::build_healthcheck(
            self.bucket.as_deref(),
            &self.base_path,
            s3_service,
            is_cloud_path,
        )
    }
}

#[cfg(test)]
#[allow(clippy::print_stdout)]
#[allow(clippy::print_stderr)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::fs;
    use vector_lib::event::{Event, LogEvent, ObjectMap};

    #[test]
    fn generate_config() {
        vector::test_util::test_generate_config::<DeltaLakeConfig>();
    }

    #[tokio::test]
    async fn test_write_events_to_local_delta_lake() {
        // Create a temporary directory for testing
        let temp_dir = std::env::temp_dir();
        let test_id = format!(
            "delta_test_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let base_path = temp_dir.join(test_id).join("delta-tables");

        // Ensure the base directory exists
        fs::create_dir_all(&base_path).expect("Failed to create base directory");

        // Create test events
        let mut events = Vec::new();

        // Event 1
        let mut log1 = LogEvent::from(BTreeMap::new());
        log1.insert("_vector_table", "test_table_1");
        log1.insert("_vector_source_table", "source_table_1");
        log1.insert("_vector_source_schema", "test_schema");
        log1.insert("_vector_instance", "test_instance_1");
        log1.insert("_vector_timestamp", "2024-01-01T12:00:00Z");
        // Data fields
        log1.insert("id", 1i64);
        log1.insert("name", "Alice");
        log1.insert("age", 30i64);
        log1.insert("active", true);
        // Partition fields
        log1.insert("date", "2024-01-01");
        log1.insert("instance", "test_instance_1");

        // Add schema metadata with multi-level partitioning
        let mut schema_meta = ObjectMap::new();
        schema_meta.insert(
            "_partition_by".into(),
            vector_lib::event::Value::from("date,instance"),
        );

        let mut id_meta = ObjectMap::new();
        id_meta.insert(
            "mysql_type".into(),
            vector_lib::event::Value::from("bigint"),
        );
        schema_meta.insert("id".into(), vector_lib::event::Value::Object(id_meta));

        let mut name_meta = ObjectMap::new();
        name_meta.insert(
            "mysql_type".into(),
            vector_lib::event::Value::from("varchar(255)"),
        );
        schema_meta.insert("name".into(), vector_lib::event::Value::Object(name_meta));

        let mut age_meta = ObjectMap::new();
        age_meta.insert("mysql_type".into(), vector_lib::event::Value::from("int"));
        schema_meta.insert("age".into(), vector_lib::event::Value::Object(age_meta));

        let mut active_meta = ObjectMap::new();
        active_meta.insert(
            "mysql_type".into(),
            vector_lib::event::Value::from("tinyint(1)"),
        );
        schema_meta.insert(
            "active".into(),
            vector_lib::event::Value::Object(active_meta),
        );

        // Add partition fields to schema metadata
        let mut date_meta = ObjectMap::new();
        date_meta.insert("mysql_type".into(), vector_lib::event::Value::from("date"));
        schema_meta.insert("date".into(), vector_lib::event::Value::Object(date_meta));

        let mut instance_meta = ObjectMap::new();
        instance_meta.insert(
            "mysql_type".into(),
            vector_lib::event::Value::from("varchar(100)"),
        );
        schema_meta.insert(
            "instance".into(),
            vector_lib::event::Value::Object(instance_meta),
        );

        log1.insert(
            "_schema_metadata",
            vector_lib::event::Value::Object(schema_meta),
        );

        events.push(Event::Log(log1));

        // Event 2 - different instance to test partitioning
        let mut log2 = LogEvent::from(BTreeMap::new());
        log2.insert("_vector_table", "test_table_1");
        log2.insert("_vector_source_table", "source_table_1");
        log2.insert("_vector_source_schema", "test_schema");
        log2.insert("_vector_instance", "test_instance_2"); // Different instance
        log2.insert("_vector_timestamp", "2024-01-01T12:01:00Z");
        // Data fields
        log2.insert("id", 2i64);
        log2.insert("name", "Bob");
        log2.insert("age", 25i64);
        log2.insert("active", false);
        // Partition fields
        log2.insert("date", "2024-01-01"); // Same date
        log2.insert("instance", "test_instance_2"); // Different instance

        events.push(Event::Log(log2));

        // Create DeltaLakeWriter and write events
        let table_config = DeltaTableConfig {
            name: "test_table_1".to_string(),
            schema_evolution: Some(true),
        };

        let write_config = WriteConfig {
            batch_size: 1000,
            timeout_secs: 30,
        };

        let mut writer = crate::common::deltalake_writer::DeltaLakeWriter::new(
            base_path.clone(),
            table_config,
            write_config,
            None,
        );

        // Write events
        let write_result = writer.write_events(events).await;
        if let Err(e) = &write_result {
            println!("   Write failed: {}", e);
            // Check if base_path exists
            println!("   Base path exists: {}", base_path.exists());
            if base_path.exists() {
                println!("   Base path contents:");
                if let Ok(entries) = fs::read_dir(&base_path) {
                    for entry in entries {
                        if let Ok(entry) = entry {
                            println!("     - {:?}", entry.path());
                        }
                    }
                }
            }
        }
        write_result.expect("Failed to write events");

        // Check if files were generated
        // Note: DeltaLakeWriter uses base_path as the table directory, not base_path/table_name
        let table_path = base_path.clone();
        println!("Checking table path: {:?}", table_path);
        println!("Table path exists: {}", table_path.exists());
        assert!(
            table_path.exists(),
            "Table directory should exist: {:?}",
            table_path
        );

        // Check _delta_log directory
        let delta_log_path = table_path.join("_delta_log");
        println!("Checking delta log path: {:?}", delta_log_path);
        println!("Delta log path exists: {}", delta_log_path.exists());
        assert!(
            delta_log_path.exists(),
            "Delta log directory should exist: {:?}",
            delta_log_path
        );

        // Check for .json files (transaction logs)
        let json_files: Vec<_> = fs::read_dir(&delta_log_path)
            .expect("Failed to read delta log directory")
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if path.extension()? == "json" {
                    Some(path)
                } else {
                    None
                }
            })
            .collect();

        assert!(
            !json_files.is_empty(),
            "Should have at least one .json transaction log file"
        );

        // Check for data files (parquet)
        println!("Checking for parquet files in: {:?}", table_path);
        let entries: Vec<_> = fs::read_dir(&table_path)
            .expect("Failed to read table directory")
            .collect();
        println!("Found {} entries in table directory", entries.len());

        // Collect all parquet files (including those in partition directories)
        let mut parquet_files = Vec::new();

        for entry in &entries {
            let entry = entry.as_ref().expect("Failed to read entry");
            let path = entry.path();
            println!("  Entry: {:?}", path);

            if path.is_dir() && !path.to_string_lossy().contains("_delta_log") {
                // Check partition directory (could be nested for multi-level partitioning)
                println!("    -> This is a directory, checking for parquet files inside");
                if let Ok(sub_entries) = fs::read_dir(&path) {
                    for sub_entry in sub_entries {
                        if let Ok(sub_entry) = sub_entry {
                            let sub_path = sub_entry.path();
                            if sub_path.is_dir() {
                                // Nested directory (e.g., date=2024-01-01/instance=test_instance_1)
                                println!("      Found nested directory: {:?}", sub_path);
                                if let Ok(parquet_entries) = fs::read_dir(&sub_path) {
                                    for parquet_entry in parquet_entries {
                                        if let Ok(parquet_entry) = parquet_entry {
                                            let parquet_path = parquet_entry.path();
                                            if parquet_path
                                                .extension()
                                                .map(|ext| ext == "parquet")
                                                .unwrap_or(false)
                                            {
                                                println!(
                                                    "        Found parquet file: {:?}",
                                                    parquet_path
                                                );
                                                parquet_files.push(parquet_path);
                                            }
                                        }
                                    }
                                }
                            } else if sub_path
                                .extension()
                                .map(|ext| ext == "parquet")
                                .unwrap_or(false)
                            {
                                // Parquet file directly in this directory
                                println!("      Found parquet file: {:?}", sub_path);
                                parquet_files.push(sub_path);
                            }
                        }
                    }
                }
            } else if path
                .extension()
                .map(|ext| ext == "parquet")
                .unwrap_or(false)
            {
                // Parquet file in root directory
                println!("    -> This is a parquet file!");
                parquet_files.push(path);
            }
        }

        println!("Found {} parquet files total", parquet_files.len());
        assert!(
            !parquet_files.is_empty(),
            "Should have at least one .parquet data file"
        );

        // Verify multi-level partitioning
        let partition_dirs: Vec<_> = entries
            .iter()
            .filter_map(|entry| {
                let entry = entry.as_ref().ok()?;
                let path = entry.path();
                if path.is_dir() && !path.to_string_lossy().contains("_delta_log") {
                    Some(path)
                } else {
                    None
                }
            })
            .collect();

        println!("Found {} partition directories:", partition_dirs.len());
        for dir in &partition_dirs {
            println!("  Partition dir: {:?}", dir);

            // Check for nested partition directories (date/instance)
            if dir.is_dir() {
                if let Ok(nested_entries) = fs::read_dir(dir) {
                    for nested_entry in nested_entries {
                        if let Ok(nested_entry) = nested_entry {
                            let nested_path = nested_entry.path();
                            if nested_path.is_dir() {
                                println!("    Nested partition dir: {:?}", nested_path);

                                // Check for parquet files in nested directory
                                if let Ok(parquet_entries) = fs::read_dir(&nested_path) {
                                    for parquet_entry in parquet_entries {
                                        if let Ok(parquet_entry) = parquet_entry {
                                            let parquet_path = parquet_entry.path();
                                            if parquet_path
                                                .extension()
                                                .map(|ext| ext == "parquet")
                                                .unwrap_or(false)
                                            {
                                                println!("      Parquet file: {:?}", parquet_path);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // With multi-level partitioning (date,instance), we should see nested directories
        let has_multi_level = partition_dirs.iter().any(|dir| {
            dir.to_string_lossy().contains("date=")
                && fs::read_dir(dir)
                    .map(|entries| {
                        entries.flatten().any(|entry| {
                            entry.path().is_dir()
                                && entry.path().to_string_lossy().contains("instance=")
                        })
                    })
                    .unwrap_or(false)
        });

        assert!(
            has_multi_level,
            "Should have multi-level partition directories (date=.../instance=...)"
        );

        // Verify file contents (optional)
        for json_file in &json_files {
            let content = fs::read_to_string(json_file).expect("Failed to read JSON file");
            assert!(
                !content.is_empty(),
                "JSON file should not be empty: {:?}",
                json_file
            );
            // Delta Lake JSON files may contain table name or other metadata
            println!(
                "Transaction log content (first 500 chars): {}",
                &content[..content.len().min(500)]
            );
        }

        println!(
            "   Test passed: Successfully wrote events to Delta Lake at {:?}",
            base_path
        );
        println!("   Generated {} transaction log files", json_files.len());
        println!("   Generated {} data files", parquet_files.len());
        println!("   Multi-level partitioning (date/instance) verified");

        // Clean up temporary directory
        let _ = fs::remove_dir_all(base_path.parent().unwrap());
    }

    /// Simulates system_tables source setting partition_by=["date"] via _schema_metadata,
    /// then verifies the DeltaLake table is written with date=... partition directories.
    #[tokio::test]
    async fn test_partition_by_date_from_schema_metadata() {
        let temp_dir = std::env::temp_dir();
        let test_id = format!(
            "delta_partition_test_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let base_path = temp_dir.join(test_id).join("delta-tables");
        fs::create_dir_all(&base_path).expect("Failed to create base directory");

        // Build an event the way system_tables source would, with _partition_by injected
        let mut log = LogEvent::from(BTreeMap::new());
        log.insert("_vector_table", "hist_statements");
        log.insert("_vector_source_table", "CLUSTER_STATEMENTS_SUMMARY");
        log.insert("_vector_source_schema", "information_schema");
        log.insert("_vector_instance", "tidb-0:4000");
        log.insert("_vector_timestamp", "2024-03-15T08:00:00Z");
        log.insert("DIGEST", "abc123");
        log.insert("EXEC_COUNT", 42i64);
        // date field is auto-derived from _vector_timestamp by the converter
        // _partition_by injected by data_collector when TableConfig.partition_by = ["date"]
        let mut schema_meta = ObjectMap::new();
        schema_meta.insert(
            "_partition_by".into(),
            vector_lib::event::Value::from("date"),
        );
        log.insert(
            "_schema_metadata",
            vector_lib::event::Value::Object(schema_meta),
        );

        let table_config = DeltaTableConfig {
            name: "hist_statements".to_string(),
            schema_evolution: Some(true),
        };
        let write_config = WriteConfig {
            batch_size: 1000,
            timeout_secs: 30,
        };

        let mut writer = crate::common::deltalake_writer::DeltaLakeWriter::new(
            base_path.clone(),
            table_config,
            write_config,
            None,
        );

        writer
            .write_events(vec![Event::Log(log)])
            .await
            .expect("Failed to write events");

        // Verify date=2024-03-15 partition directory exists (derived from _vector_timestamp)
        let partition_dirs: Vec<_> = fs::read_dir(&base_path)
            .expect("Failed to read table directory")
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("date="))
            .collect();

        assert!(
            partition_dirs
                .iter()
                .any(|e| e.file_name().to_string_lossy() == "date=2024-03-15"),
            "Expected date=2024-03-15 partition directory, found: {:?}",
            partition_dirs
                .iter()
                .map(|e| e.file_name())
                .collect::<Vec<_>>()
        );

        let _ = fs::remove_dir_all(base_path.parent().unwrap());
    }
}
