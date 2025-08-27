use std::collections::HashMap;
use std::path::PathBuf;

use vector::{
    config::{GenerateConfig, SinkConfig, SinkContext},
    sinks::Healthcheck,
};
use vector_lib::{
    config::{AcknowledgementsConfig, DataType, Input},
    configurable::configurable_component,
    sink::VectorSink,
};
use serde::{Deserialize, Serialize};

use crate::sinks::deltalake::processor::DeltaLakeSink;

mod processor;
mod writer;

/// Configuration for the deltalake sink
#[configurable_component(sink("deltalake"))]
#[derive(Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct DeltaLakeConfig {
    /// Base path for Delta Lake tables
    pub base_path: String,

    /// Table names (comma-separated). Optional; if omitted, tables are discovered dynamically from events.
    #[serde(default)]
    pub table_names: Option<String>,

    /// Batch size for writing
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Write timeout in seconds
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,

    /// Compression format
    #[serde(default = "default_compression")]
    pub compression: String,

    /// Storage options for cloud storage
    pub storage_options: Option<HashMap<String, String>>,

    /// Acknowledgments configuration
    #[serde(
        default,
        deserialize_with = "vector::serde::bool_or_struct",
        skip_serializing_if = "vector::serde::is_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,
}

/// Delta table configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaTableConfig {
    /// Table name
    pub name: String,

    /// Partition columns
    pub partition_by: Option<Vec<String>>,

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

    /// Compression format
    #[serde(default = "default_compression")]
    pub compression: String,
}

/// Compression format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompressionFormat {
    /// Snappy compression
    Snappy,
    /// Gzip compression
    Gzip,
    /// No compression
    None,
}

pub const fn default_batch_size() -> usize {
    1000
}

pub const fn default_timeout_secs() -> u64 {
    30
}

pub fn default_compression() -> String {
    "snappy".to_string()
}

impl GenerateConfig for DeltaLakeConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            base_path: "./delta-tables".to_owned(),
            table_names: None,
            batch_size: default_batch_size(),
            timeout_secs: default_timeout_secs(),
            compression: default_compression(),
            storage_options: None,
            acknowledgements: Default::default(),
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "deltalake")]
impl SinkConfig for DeltaLakeConfig {
    async fn build(&self, cx: SinkContext) -> vector::Result<(VectorSink, Healthcheck)> {
        let sink = self.build_processor(cx)?;
        let healthcheck = self.build_healthcheck()?;
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
    fn build_processor(&self, _cx: SinkContext) -> vector::Result<VectorSink> {
        let base_path = PathBuf::from(&self.base_path);
        
        // Parse table names from comma-separated string when provided; otherwise allow dynamic tables
        let table_configs: Vec<DeltaTableConfig> = match &self.table_names {
            Some(names) => names
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|table_name| DeltaTableConfig {
                    name: table_name.to_string(),
                    partition_by: Some(vec!["date".to_string(), "instance".to_string()]),
                    schema_evolution: Some(true),
                })
                .collect(),
            None => Vec::new(),
        };
        
        let write_config = WriteConfig {
            batch_size: self.batch_size,
            timeout_secs: self.timeout_secs,
            compression: self.compression.clone(),
        };
        
        let storage_options = self.storage_options.clone();

        let sink = DeltaLakeSink::new(
            base_path,
            table_configs,
            write_config,
            storage_options,
        );

        Ok(VectorSink::from_event_streamsink(sink))
    }

    fn build_healthcheck(&self) -> vector::Result<Healthcheck> {
        // Simple healthcheck that verifies the base path is writable
        let base_path = PathBuf::from(&self.base_path);
        
        let healthcheck = Box::pin(async move {
            // Check if directory exists and is writable
            if !base_path.exists() {
                if let Err(e) = std::fs::create_dir_all(&base_path) {
                    return Err(format!("Failed to create directory {}: {}", base_path.display(), e).into());
                }
            }
            
            // Try to create a test file
            let test_file = base_path.join(".healthcheck");
            if let Err(e) = std::fs::write(&test_file, "test") {
                return Err(format!("Failed to write to {}: {}", base_path.display(), e).into());
            }
            
            // Clean up test file
            let _ = std::fs::remove_file(test_file);
            
            Ok(())
        });

        Ok(healthcheck)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_config() {
        vector::test_util::test_generate_config::<DeltaLakeConfig>();
    }
}