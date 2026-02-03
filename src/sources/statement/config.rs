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
}

impl Default for StatementConfig {
    fn default() -> Self {
        Self {
            grpc: GrpcConfig::default(),
            storage: StorageConfig::default(),
            contract: ContractConfig::default(),
            processing: ProcessingConfig::default(),
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
