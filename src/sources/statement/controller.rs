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
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

use super::config::StatementConfig;
use crate::common::topology::{Component, InstanceType};
use crate::common::topology::fetch::TopologyFetcher;

pub mod proto {
    tonic::include_proto!("systemtable.v1");
}

use proto::statement_push_control_client::StatementPushControlClient;
use proto::{CollectionConfig, RegisterPushTargetRequest};

/// Controller manages topology discovery and push target registration for TiDB instances.
pub struct Controller {
    config: Arc<StatementConfig>,

    // Known TiDB instances
    components: Arc<Mutex<HashMap<String, TiDBClient>>>,

    // Shutdown signal
    shutdown: Arc<tokio::sync::Notify>,

    // Vector's own endpoint (where TiDB should push to)
    vector_endpoint: String,
}

/// Represents a connected TiDB client.
#[derive(Clone)]
struct TiDBClient {
    component: Component,
    endpoint: String,
    registered_at: Option<chrono::DateTime<chrono::Utc>>,
    config_version: i64,
}

impl Controller {
    /// Creates a new Controller.
    pub fn new(config: Arc<StatementConfig>, vector_endpoint: String) -> Self {
        Self {
            config,
            components: Arc::new(Mutex::new(HashMap::new())),
            shutdown: Arc::new(tokio::sync::Notify::new()),
            vector_endpoint,
        }
    }

    /// Starts the controller.
    pub fn start(self) -> JoinHandle<()> {
        let fetch_interval = Duration::from_secs(self.config.topology.fetch_interval_secs);

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(fetch_interval);
            ticker.tick().await; // Skip first tick

            // Initial discovery
            if let Err(e) = self.discover_and_update().await {
                error!("Initial topology discovery failed: {}", e);
            }

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Err(e) = self.discover_and_update().await {
                            error!("Topology discovery failed: {}", e);
                        }
                    }
                    _ = self.shutdown.notified() => {
                        info!("Controller shutting down");
                        self.unregister_all().await;
                        break;
                    }
                }
            }
        })
    }

    /// Stops the controller.
    pub fn stop(&self) {
        self.shutdown.notify_one();
    }

    /// Discovers TiDB instances and updates registrations.
    async fn discover_and_update(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let Some(ref pd_addr) = self.config.topology.pd_address else {
            debug!("Topology discovery disabled (no PD address)");
            return Ok(());
        };

        // Create topology fetcher
        let mut fetcher = TopologyFetcher::new(
            Some(pd_addr.clone()),
            None, // tls_config
            &Default::default(), // proxy_config
            None, // tidb_group
            None, // label_k8s_instance
        ).await?;

        // Fetch current topology
        let mut components = HashSet::new();
        fetcher.get_up_components(&mut components).await?;

        // Filter for TiDB instances only
        let tidb_components: Vec<Component> = components.into_iter()
            .filter(|c| c.instance_type == InstanceType::TiDB)
            .collect();

        debug!("Discovered {} TiDB instances", tidb_components.len());

        let mut clients = self.components.lock().await;

        // Process each TiDB instance
        for component in &tidb_components {
            let key = format!("{}:{}", component.host, component.secondary_port);

            if let Some(client) = clients.get_mut(&key) {
                // Check if this is the same instance
                if client.component.host != component.host ||
                   client.component.secondary_port != component.secondary_port {
                    // Component changed, update it
                    debug!("TiDB component changed, updating: {}", key);
                    client.component = component.clone();
                    client.endpoint = format!("http://{}:{}", component.host, component.secondary_port);
                    let updated = self.register_client(&key, client).await?;
                    clients.insert(key.clone(), updated);
                }
            } else {
                // New TiDB instance
                info!("Discovered new TiDB instance: {}", key);
                let client = TiDBClient {
                    component: component.clone(),
                    endpoint: format!("http://{}:{}", component.host, component.secondary_port),
                    registered_at: None,
                    config_version: 0,
                };
                let updated = self.register_client(&key, &client).await?;
                clients.insert(key, updated);
            }
        }

        // TODO: Detect removed instances and unregister

        Ok(())
    }

    /// Registers this Vector instance with a TiDB.
    async fn register_client(&self, key: &str, client: &TiDBClient) -> Result<TiDBClient, Box<dyn std::error::Error + Send + Sync>> {
        // Connect to TiDB's status port
        let mut grpc_client = match StatementPushControlClient::connect(client.endpoint.clone()).await {
            Ok(c) => c,
            Err(e) => {
                warn!("Failed to connect to TiDB at {}: {}", client.endpoint, e);
                return Err(e.into());
            }
        };

        // Build collection config from StatementConfig
        let collection_config = self.build_collection_config();

        // Build registration request
        let request = RegisterPushTargetRequest {
            vector_endpoint: self.vector_endpoint.clone(),
            collection_config: Some(collection_config),
            vector_instance_id: format!("vector-{}", std::process::id()),
            vector_version: env!("CARGO_PKG_VERSION").to_string(),
            tls_config: None, // TODO: add TLS support
        };

        // Register with TiDB
        let mut updated_client = client.clone();
        match grpc_client.register_push_target(request).await {
            Ok(response) => {
                let resp = response.into_inner();
                if resp.success {
                    info!("Successfully registered with TiDB {}: {}", key, resp.message);
                    updated_client.registered_at = Some(chrono::Utc::now());
                    if let Some(cc) = resp.applied_config {
                        updated_client.config_version = cc.config_version;
                    }
                } else {
                    warn!("Registration with TiDB {} failed: {}", key, resp.message);
                }
            }
            Err(e) => {
                error!("Failed to register with TiDB {}: {}", key, e);
                return Err(e.into());
            }
        }

        Ok(updated_client)
    }

    /// Builds a CollectionConfig from StatementConfig.
    fn build_collection_config(&self) -> CollectionConfig {
        let policy = &self.config.collection_policy;

        CollectionConfig {
            aggregation_window_secs: policy.aggregation_window_secs as i32,
            enable_internal_query: policy.enable_internal_query,
            push_batch_size: policy.push_batch_size as i32,
            push_interval_secs: policy.push_interval_secs as i32,
            push_timeout_secs: policy.push_timeout_secs as i32,
            max_digests_per_window: policy.max_digests_per_window as i32,
            max_memory_bytes: policy.max_memory_bytes as i64,
            eviction_strategy: policy.eviction_strategy.clone(),
            early_flush_threshold: policy.early_flush_threshold,
            retry_max_attempts: policy.retry_max_attempts as i32,
            retry_initial_delay_ms: policy.retry_initial_delay_ms as i32,
            retry_max_delay_ms: policy.retry_max_delay_ms as i32,
            extended_metrics: Vec::new(), // TODO: add extended metrics
            config_version: 0,
        }
    }

    /// Unregisters from all TiDB instances during shutdown.
    async fn unregister_all(&self) {
        // Collect endpoints and keys first to avoid holding the lock across await
        let items: Vec<(String, String)> = {
            let clients = self.components.lock().await;
            clients.iter().map(|(k, v)| (k.clone(), v.endpoint.clone())).collect()
        };

        for (key, addr) in items {
            let mut grpc_client = match StatementPushControlClient::connect(addr.clone()).await {
                Ok(c) => c,
                Err(e) => {
                    error!("Failed to connect to TiDB {} for unregistration: {}", key, e);
                    continue;
                }
            };

            let request = tonic::Request::new(proto::UnregisterPushTargetRequest {
                vector_instance_id: format!("vector-{}", std::process::id()),
                stop_immediately: true,
            });

            match grpc_client.unregister_push_target(request).await {
                Ok(response) => {
                    let resp = response.into_inner();
                    if resp.success {
                        info!("Successfully unregistered from TiDB {}", key);
                    } else {
                        warn!("Unregistration from TiDB {} failed: {}", key, resp.message);
                    }
                }
                Err(e) => {
                    error!("Failed to unregister from TiDB {}: {}", key, e);
                }
            }
        }

        self.components.lock().await.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_controller_creation() {
        let config = Arc::new(StatementConfig::default());
        let controller = Controller::new(config, "localhost:50051".to_string());
        assert_eq!(controller.vector_endpoint, "localhost:50051");
    }
}
