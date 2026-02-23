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

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Configuration for the Statement V3 receiver.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementConfig {
    /// gRPC server configuration
    #[serde(default)]
    pub grpc: GrpcConfig,

    /// Storage configuration
    #[serde(default)]
    pub storage: StorageConfig,

    /// Contract configuration
    #[serde(default)]
    pub contract: ContractConfig,

    /// Processing configuration
    #[serde(default)]
    pub processing: ProcessingConfig,

    /// Collection policy pushed to TiDB during Ping handshake
    #[serde(default)]
    pub collection_policy: CollectionPolicy,

    /// Topology discovery configuration for finding TiDB instances
    #[serde(default)]
    pub topology: TopologyConfig,
}

impl Default for StatementConfig {
    fn default() -> Self {
        Self {
            grpc: GrpcConfig::default(),
            storage: StorageConfig::default(),
            contract: ContractConfig::default(),
            processing: ProcessingConfig::default(),
            collection_policy: CollectionPolicy::default(),
            topology: TopologyConfig::default(),
        }
    }
}

/// Collection policy that Vector pushes to TiDB during Ping.
/// TiDB applies these settings to its aggregator and pusher.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionPolicy {
    /// Aggregation window duration in seconds
    #[serde(default = "default_aggregation_window_secs")]
    pub aggregation_window_secs: u32,

    /// Whether to collect internal queries
    #[serde(default)]
    pub enable_internal_query: bool,

    /// Maximum number of statements per push batch
    #[serde(default = "default_push_batch_size")]
    pub push_batch_size: u32,

    /// Push interval in seconds
    #[serde(default = "default_push_interval_secs")]
    pub push_interval_secs: u32,

    /// Push timeout in seconds
    #[serde(default = "default_push_timeout_secs")]
    pub push_timeout_secs: u32,

    /// Maximum unique digests per aggregation window
    #[serde(default = "default_max_digests_per_window")]
    pub max_digests_per_window: u32,

    /// Maximum memory in bytes for aggregation buffer
    #[serde(default = "default_max_memory_bytes")]
    pub max_memory_bytes: u64,

    /// Eviction strategy: "drop_new", "evict_lru", "aggregate_to_other"
    #[serde(default = "default_eviction_strategy")]
    pub eviction_strategy: String,

    /// Early flush threshold (0.0 - 1.0)
    #[serde(default = "default_early_flush_threshold")]
    pub early_flush_threshold: f64,

    /// Maximum retry attempts for push
    #[serde(default = "default_retry_max_attempts")]
    pub retry_max_attempts: u32,

    /// Initial retry delay in milliseconds
    #[serde(default = "default_retry_initial_delay_ms")]
    pub retry_initial_delay_ms: u32,

    /// Maximum retry delay in milliseconds
    #[serde(default = "default_retry_max_delay_ms")]
    pub retry_max_delay_ms: u32,

    /// Configuration version for change detection
    #[serde(default)]
    pub config_version: u64,
}

impl Default for CollectionPolicy {
    fn default() -> Self {
        Self {
            aggregation_window_secs: default_aggregation_window_secs(),
            enable_internal_query: false,
            push_batch_size: default_push_batch_size(),
            push_interval_secs: default_push_interval_secs(),
            push_timeout_secs: default_push_timeout_secs(),
            max_digests_per_window: default_max_digests_per_window(),
            max_memory_bytes: default_max_memory_bytes(),
            eviction_strategy: default_eviction_strategy(),
            early_flush_threshold: default_early_flush_threshold(),
            retry_max_attempts: default_retry_max_attempts(),
            retry_initial_delay_ms: default_retry_initial_delay_ms(),
            retry_max_delay_ms: default_retry_max_delay_ms(),
            config_version: 0,
        }
    }
}

/// gRPC server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcConfig {
    /// Address to bind the gRPC server
    #[serde(default = "default_grpc_address")]
    pub address: String,

    /// Port for the gRPC server
    #[serde(default = "default_grpc_port")]
    pub port: u16,

    /// Maximum number of concurrent connections
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,

    /// TLS configuration
    #[serde(default)]
    pub tls: Option<TlsConfig>,

    /// Request timeout in seconds
    #[serde(default = "default_request_timeout")]
    pub request_timeout_secs: u64,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            address: default_grpc_address(),
            port: default_grpc_port(),
            max_connections: default_max_connections(),
            tls: None,
            request_timeout_secs: default_request_timeout(),
        }
    }
}

/// TLS configuration for gRPC server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// Path to server certificate file
    pub cert_file: String,

    /// Path to server key file
    pub key_file: String,

    /// Path to CA certificate file for client authentication
    pub ca_file: Option<String>,

    /// Whether to require client certificates
    #[serde(default)]
    pub require_client_cert: bool,
}

/// Storage configuration for Parquet/S3.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Storage backend type
    #[serde(default = "default_storage_backend")]
    pub backend: StorageBackend,

    /// S3 configuration (if backend is S3)
    #[serde(default)]
    pub s3: Option<S3Config>,

    /// Local file configuration (if backend is Local)
    #[serde(default)]
    pub local: Option<LocalConfig>,

    /// Buffer flush interval in seconds
    #[serde(default = "default_flush_interval")]
    pub flush_interval_secs: u64,

    /// Maximum buffer size in bytes before forced flush
    #[serde(default = "default_max_buffer_size")]
    pub max_buffer_size_bytes: usize,

    /// Compression codec for Parquet files
    #[serde(default = "default_compression")]
    pub compression: String,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: default_storage_backend(),
            s3: None,
            local: Some(LocalConfig::default()),
            flush_interval_secs: default_flush_interval(),
            max_buffer_size_bytes: default_max_buffer_size(),
            compression: default_compression(),
        }
    }
}

/// Storage backend type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum StorageBackend {
    S3,
    Local,
}

/// S3 storage configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    /// S3 bucket name
    pub bucket: String,

    /// S3 region
    #[serde(default = "default_s3_region")]
    pub region: String,

    /// S3 endpoint (for non-AWS S3-compatible storage)
    pub endpoint: Option<String>,

    /// Access key ID
    pub access_key_id: Option<String>,

    /// Secret access key
    pub secret_access_key: Option<String>,

    /// Path prefix in the bucket
    #[serde(default = "default_s3_prefix")]
    pub prefix: String,
}

/// Local storage configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalConfig {
    /// Base directory for local storage
    #[serde(default = "default_local_path")]
    pub path: String,
}

impl Default for LocalConfig {
    fn default() -> Self {
        Self {
            path: default_local_path(),
        }
    }
}

/// Contract configuration for schema validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractConfig {
    /// Path to the requirements contract file
    #[serde(default)]
    pub contract_path: Option<String>,

    /// Whether to strictly enforce the contract
    #[serde(default)]
    pub strict_validation: bool,

    /// Whether to auto-discover new fields
    #[serde(default = "default_true")]
    pub auto_discover_fields: bool,
}

impl Default for ContractConfig {
    fn default() -> Self {
        Self {
            contract_path: None,
            strict_validation: false,
            auto_discover_fields: true,
        }
    }
}

/// Processing configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingConfig {
    /// Batch size for processing
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Number of worker threads
    #[serde(default = "default_workers")]
    pub workers: usize,

    /// Rate limit (statements per second, 0 = unlimited)
    #[serde(default)]
    pub rate_limit: usize,
}

impl Default for ProcessingConfig {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            workers: default_workers(),
            rate_limit: 0,
        }
    }
}

/// Topology discovery configuration for finding TiDB instances.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyConfig {
    /// PD address for topology discovery
    #[serde(default)]
    pub pd_address: Option<String>,

    /// How often to refresh topology (seconds)
    #[serde(default = "default_topology_fetch_interval")]
    pub fetch_interval_secs: u64,

    /// Enable automatic discovery of TiDB instances
    #[serde(default = "default_true")]
    pub auto_discovery_enabled: bool,
}

impl Default for TopologyConfig {
    fn default() -> Self {
        Self {
            pd_address: None,
            fetch_interval_secs: default_topology_fetch_interval(),
            auto_discovery_enabled: true,
        }
    }
}

// Default value functions
fn default_grpc_address() -> String {
    "0.0.0.0".to_string()
}

fn default_grpc_port() -> u16 {
    50051
}

fn default_max_connections() -> usize {
    100
}

fn default_request_timeout() -> u64 {
    30
}

fn default_storage_backend() -> StorageBackend {
    StorageBackend::Local
}

fn default_flush_interval() -> u64 {
    1800 // 30 minutes
}

fn default_max_buffer_size() -> usize {
    100 * 1024 * 1024 // 100 MB
}

fn default_compression() -> String {
    "snappy".to_string()
}

fn default_s3_region() -> String {
    "us-east-1".to_string()
}

fn default_s3_prefix() -> String {
    "statements/".to_string()
}

fn default_local_path() -> String {
    "/var/lib/vector/statements".to_string()
}

fn default_true() -> bool {
    true
}

fn default_batch_size() -> usize {
    1000
}

fn default_workers() -> usize {
    4
}

fn default_topology_fetch_interval() -> u64 {
    30 // 30 seconds
}

// CollectionPolicy default value functions
fn default_aggregation_window_secs() -> u32 {
    60
}

fn default_push_batch_size() -> u32 {
    1000
}

fn default_push_interval_secs() -> u32 {
    60
}

fn default_push_timeout_secs() -> u32 {
    30
}

fn default_max_digests_per_window() -> u32 {
    10000
}

fn default_max_memory_bytes() -> u64 {
    64 * 1024 * 1024 // 64 MB
}

fn default_eviction_strategy() -> String {
    "aggregate_to_other".to_string()
}

fn default_early_flush_threshold() -> f64 {
    0.8
}

fn default_retry_max_attempts() -> u32 {
    3
}

fn default_retry_initial_delay_ms() -> u32 {
    1000
}

fn default_retry_max_delay_ms() -> u32 {
    30000
}

impl StatementConfig {
    /// Returns the gRPC server bind address.
    pub fn grpc_bind_address(&self) -> String {
        format!("{}:{}", self.grpc.address, self.grpc.port)
    }

    /// Returns the request timeout duration.
    pub fn request_timeout(&self) -> Duration {
        Duration::from_secs(self.grpc.request_timeout_secs)
    }

    /// Returns the flush interval duration.
    pub fn flush_interval(&self) -> Duration {
        Duration::from_secs(self.storage.flush_interval_secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collection_policy_defaults() {
        let policy = CollectionPolicy::default();
        assert_eq!(policy.aggregation_window_secs, 60);
        assert!(!policy.enable_internal_query);
        assert_eq!(policy.push_batch_size, 1000);
        assert_eq!(policy.push_interval_secs, 60);
        assert_eq!(policy.push_timeout_secs, 30);
        assert_eq!(policy.max_digests_per_window, 10000);
        assert_eq!(policy.max_memory_bytes, 64 * 1024 * 1024);
        assert_eq!(policy.eviction_strategy, "aggregate_to_other");
        assert!((policy.early_flush_threshold - 0.8).abs() < f64::EPSILON);
        assert_eq!(policy.retry_max_attempts, 3);
        assert_eq!(policy.retry_initial_delay_ms, 1000);
        assert_eq!(policy.retry_max_delay_ms, 30000);
        assert_eq!(policy.config_version, 0);
    }

    #[test]
    fn test_collection_policy_in_statement_config() {
        let config = StatementConfig::default();
        // CollectionPolicy should be populated with defaults
        assert_eq!(config.collection_policy.aggregation_window_secs, 60);
        assert_eq!(config.collection_policy.push_batch_size, 1000);
        assert_eq!(config.collection_policy.max_memory_bytes, 64 * 1024 * 1024);
    }

    #[test]
    fn test_collection_policy_serde_roundtrip() {
        let policy = CollectionPolicy {
            aggregation_window_secs: 120,
            enable_internal_query: true,
            push_batch_size: 500,
            push_interval_secs: 30,
            push_timeout_secs: 15,
            max_digests_per_window: 5000,
            max_memory_bytes: 128 * 1024 * 1024,
            eviction_strategy: "evict_lru".to_string(),
            early_flush_threshold: 0.9,
            retry_max_attempts: 5,
            retry_initial_delay_ms: 2000,
            retry_max_delay_ms: 60000,
            config_version: 42,
        };

        let json = serde_json::to_string(&policy).unwrap();
        let deserialized: CollectionPolicy = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.aggregation_window_secs, 120);
        assert!(deserialized.enable_internal_query);
        assert_eq!(deserialized.push_batch_size, 500);
        assert_eq!(deserialized.push_interval_secs, 30);
        assert_eq!(deserialized.push_timeout_secs, 15);
        assert_eq!(deserialized.max_digests_per_window, 5000);
        assert_eq!(deserialized.max_memory_bytes, 128 * 1024 * 1024);
        assert_eq!(deserialized.eviction_strategy, "evict_lru");
        assert!((deserialized.early_flush_threshold - 0.9).abs() < f64::EPSILON);
        assert_eq!(deserialized.retry_max_attempts, 5);
        assert_eq!(deserialized.retry_initial_delay_ms, 2000);
        assert_eq!(deserialized.retry_max_delay_ms, 60000);
        assert_eq!(deserialized.config_version, 42);
    }

    #[test]
    fn test_collection_policy_serde_defaults_from_empty() {
        // Deserializing from empty JSON should use serde defaults
        let json = "{}";
        let policy: CollectionPolicy = serde_json::from_str(json).unwrap();

        assert_eq!(policy.aggregation_window_secs, 60);
        assert!(!policy.enable_internal_query);
        assert_eq!(policy.push_batch_size, 1000);
        assert_eq!(policy.push_interval_secs, 60);
        assert_eq!(policy.push_timeout_secs, 30);
        assert_eq!(policy.max_digests_per_window, 10000);
        assert_eq!(policy.max_memory_bytes, 64 * 1024 * 1024);
        assert_eq!(policy.eviction_strategy, "aggregate_to_other");
        assert!((policy.early_flush_threshold - 0.8).abs() < f64::EPSILON);
        assert_eq!(policy.retry_max_attempts, 3);
        assert_eq!(policy.retry_initial_delay_ms, 1000);
        assert_eq!(policy.retry_max_delay_ms, 30000);
        assert_eq!(policy.config_version, 0);
    }

    #[test]
    fn test_collection_policy_partial_serde_override() {
        // Only override some fields, rest should use defaults
        let json = r#"{"aggregation_window_secs": 300, "max_memory_bytes": 268435456}"#;
        let policy: CollectionPolicy = serde_json::from_str(json).unwrap();

        assert_eq!(policy.aggregation_window_secs, 300);
        assert_eq!(policy.max_memory_bytes, 256 * 1024 * 1024);
        // Defaults preserved
        assert_eq!(policy.push_batch_size, 1000);
        assert_eq!(policy.push_interval_secs, 60);
        assert_eq!(policy.eviction_strategy, "aggregate_to_other");
    }

    #[test]
    fn test_statement_config_with_collection_policy_serde() {
        let json = r#"{
            "collection_policy": {
                "aggregation_window_secs": 120,
                "push_batch_size": 500,
                "eviction_strategy": "drop_new"
            }
        }"#;
        let config: StatementConfig = serde_json::from_str(json).unwrap();

        assert_eq!(config.collection_policy.aggregation_window_secs, 120);
        assert_eq!(config.collection_policy.push_batch_size, 500);
        assert_eq!(config.collection_policy.eviction_strategy, "drop_new");
        // Other collection_policy defaults
        assert_eq!(config.collection_policy.push_interval_secs, 60);
        assert_eq!(config.collection_policy.max_digests_per_window, 10000);
    }
}
