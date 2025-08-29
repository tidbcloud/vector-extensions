use std::collections::{HashMap, HashSet};
use std::time::Duration;

use vector::shutdown::ShutdownSignal;
use vector::SourceSender;
use vector_lib::config::proxy::ProxyConfig;
use vector_lib::tls::TlsConfig;

use crate::common::features::is_nextgen_mode;
use crate::common::topology::{Component, FetchError, InstanceType, TopologyFetcher};
use crate::sources::system_tables::{
    collector::Collector, CollectionConfig, DatabaseConfig, TableConfig,
};

/// Main controller for system_tables source
pub struct Controller {
    topology_fetch_interval: Duration,
    topology_fetcher: TopologyFetcher,
    tidb_components: HashSet<Component>,
    running_collectors: HashMap<String, tokio::task::JoinHandle<()>>,
    database_config: DatabaseConfig,
    collection_config: CollectionConfig,
    tables: Vec<TableConfig>,
    #[allow(dead_code)]
    proxy_config: ProxyConfig,
    out: SourceSender,
    shared_pool: Option<sqlx::mysql::MySqlPool>,
}

impl Controller {
    /// Create a new controller
    pub async fn new(
        pd_address: Option<String>,
        tidb_group: Option<String>,
        label_k8s_instance: Option<String>,
        topology_fetch_interval: Duration,
        database_config: DatabaseConfig,
        collection_config: CollectionConfig,
        tables: Vec<TableConfig>,
        pd_tls: Option<TlsConfig>,
        proxy_config: &ProxyConfig,
        out: SourceSender,
    ) -> vector::Result<Self> {
        // Create topology fetcher based on nextgen mode and configuration
        let topology_fetcher = if is_nextgen_mode() {
            // Nextgen mode: use K8s-based topology fetching
            info!("Using nextgen mode for topology discovery");
            if tidb_group.is_none() && label_k8s_instance.is_none() {
                return Err(
                    "In nextgen mode, either tidb_group or label_k8s_instance must be specified"
                        .into(),
                );
            }
            TopologyFetcher::new(
                String::new(), // Empty PD address for nextgen mode
                None,          // No TLS needed for nextgen mode (uses K8s API)
                proxy_config,
                tidb_group.clone(),
                label_k8s_instance.clone(),
            )
            .await
            .map_err(|e| format!("Failed to create nextgen topology fetcher: {}", e))?
        } else {
            // Legacy mode: use PD/etcd-based topology fetching
            info!("Using legacy mode for topology discovery");
            let pd_addr = pd_address.ok_or("In legacy mode, pd_address must be specified")?;

            // Log TLS configuration for debugging
            if let Some(ref tls_config) = pd_tls {
                info!("Legacy mode using TLS configuration for PD/etcd connections");
                if tls_config.ca_file.is_some() {
                    info!("  CA file configured: {:?}", tls_config.ca_file);
                }
                if tls_config.crt_file.is_some() && tls_config.key_file.is_some() {
                    info!("  Client certificate and key configured");
                }
            } else {
                info!("Legacy mode using insecure connections to PD/etcd");
            }

            TopologyFetcher::new(
                pd_addr,
                pd_tls.clone(), // Use the pd_tls parameter
                proxy_config,
                tidb_group.clone(),
                label_k8s_instance.clone(),
            )
            .await
            .map_err(|e| format!("Failed to create legacy topology fetcher: {}", e))?
        };

        Ok(Self {
            topology_fetch_interval,
            topology_fetcher,
            tidb_components: HashSet::new(),
            running_collectors: HashMap::new(),
            database_config,
            collection_config,
            tables,
            proxy_config: proxy_config.clone(),
            out,
            shared_pool: None,
        })
    }

    /// Get or create shared connection pool
    async fn get_shared_pool(
        &mut self,
    ) -> Result<&sqlx::mysql::MySqlPool, Box<dyn std::error::Error + Send + Sync>> {
        if self.shared_pool.is_none() {
            let mut url = format!(
                "mysql://{}:{}@{}:{}/{}",
                self.database_config.username,
                &self.database_config.password,
                self.database_config.host,
                self.database_config.port,
                self.database_config.database
            );

            // Add TLS parameters if database TLS is configured
            if let Some(ref tls_config) = self.database_config.tls {
                let mut tls_params = Vec::new();

                // Set SSL mode based on verification settings
                if tls_config.verify_certificate.unwrap_or(true) {
                    if tls_config.verify_hostname.unwrap_or(true) {
                        tls_params.push("ssl-mode=VERIFY_IDENTITY".to_string());
                    } else {
                        tls_params.push("ssl-mode=VERIFY_CA".to_string());
                    }
                } else {
                    tls_params.push("ssl-mode=REQUIRED".to_string());
                }

                // Add CA certificate if provided
                if let Some(ref ca_file) = tls_config.ca_file {
                    tls_params.push(format!("ssl-ca={}", ca_file.display()));
                }

                // Add client certificate if provided
                if let Some(ref crt_file) = tls_config.crt_file {
                    tls_params.push(format!("ssl-cert={}", crt_file.display()));
                }

                // Add client key if provided
                if let Some(ref key_file) = tls_config.key_file {
                    tls_params.push(format!("ssl-key={}", key_file.display()));
                }

                if !tls_params.is_empty() {
                    url.push('?');
                    url.push_str(&tls_params.join("&"));
                }

                info!("Creating shared connection pool with TLS enabled");
            } else {
                info!("Creating shared connection pool without TLS");
            }

            let pool = sqlx::mysql::MySqlPoolOptions::new()
                .max_connections(self.database_config.max_connections.unwrap_or(10)) // 增加连接数，因为是共享的
                .acquire_timeout(std::time::Duration::from_secs(
                    self.database_config.connect_timeout.unwrap_or(30) as u64,
                ))
                .connect(&url)
                .await?;

            self.shared_pool = Some(pool);
        }

        Ok(self.shared_pool.as_ref().unwrap())
    }

    /// Run the main controller loop
    pub async fn run(mut self, mut shutdown: ShutdownSignal) {
        info!("System Tables Controller starting...");

        tokio::select! {
            _ = self.run_loop() => {},
            _ = &mut shutdown => {},
        }

        info!("System Tables Controller shutting down...");
        self.shutdown_all_collectors().await;
    }

    /// Main control loop
    async fn run_loop(&mut self) {
        loop {
            // Fetch TiDB instances and update collectors
            if let Err(e) = self.fetch_and_update_tidb_instances().await {
                error!("Failed to fetch TiDB instances: {}", e);
            }

            tokio::time::sleep(self.topology_fetch_interval).await;
        }
    }

    /// Fetch TiDB instances and update collectors
    async fn fetch_and_update_tidb_instances(&mut self) -> Result<(), FetchError> {
        let mut new_components = HashSet::new();

        // Fetch topology from PD/etcd or K8s
        self.topology_fetcher
            .get_up_components(&mut new_components)
            .await?;

        // Filter only TiDB components
        let tidb_components: HashSet<Component> = new_components
            .into_iter()
            .filter(|c| c.instance_type == InstanceType::TiDB)
            .collect();

        // Only log if there are changes in TiDB components
        if tidb_components != self.tidb_components {
            info!(
                "TiDB topology changed: {} components discovered",
                tidb_components.len()
            );
            for component in &tidb_components {
                info!(
                    "  TiDB instance: {}:{}",
                    component.host, component.primary_port
                );
            }
        } else {
            debug!(
                "TiDB topology unchanged: {} components",
                tidb_components.len()
            );
        }

        // Update collectors based on component changes
        self.update_collectors(tidb_components).await;

        Ok(())
    }

    /// Update collectors based on new TiDB components
    async fn update_collectors(&mut self, new_components: HashSet<Component>) {
        // Clone tables first to avoid borrowing issues
        let tables = self.tables.clone();

        // Separate tables into cluster-level and instance-level
        let (cluster_tables, instance_tables): (Vec<_>, Vec<_>) = tables
            .iter()
            .partition(|table| table.source_table.starts_with("CLUSTER_"));

        debug!(
            "Table classification: {} cluster tables, {} instance tables",
            cluster_tables.len(),
            instance_tables.len()
        );

        // For cluster-level tables, only start one collector on the primary instance
        if !cluster_tables.is_empty() {
            let primary_component = new_components.iter().next().cloned();
            if let Some(primary_component) = primary_component {
                let cluster_collector_key = format!(
                    "{}:{}_cluster",
                    primary_component.host, primary_component.primary_port
                );
                if !self.running_collectors.contains_key(&cluster_collector_key) {
                    let cluster_tables_owned: Vec<TableConfig> =
                        cluster_tables.into_iter().cloned().collect();
                    self.start_collector_with_tables(
                        &primary_component,
                        cluster_tables_owned,
                        &cluster_collector_key,
                    )
                    .await;
                }
            }
        }

        // For instance-level tables, start collectors on all instances
        if !instance_tables.is_empty() {
            for component in &new_components {
                let instance_collector_key =
                    format!("{}:{}_instance", component.host, component.primary_port);
                if !self
                    .running_collectors
                    .contains_key(&instance_collector_key)
                {
                    let instance_tables_owned: Vec<TableConfig> =
                        instance_tables.iter().map(|t| (*t).clone()).collect();
                    self.start_collector_with_tables(
                        component,
                        instance_tables_owned,
                        &instance_collector_key,
                    )
                    .await;
                }
            }
        }

        // Stop collectors for removed instances
        let current_component_keys: HashSet<_> = self
            .tidb_components
            .iter()
            .map(|c| format!("{}:{}", c.host, c.primary_port))
            .collect();
        let new_component_keys: HashSet<_> = new_components
            .iter()
            .map(|c| format!("{}:{}", c.host, c.primary_port))
            .collect();

        for removed_key in current_component_keys.difference(&new_component_keys) {
            self.stop_collector_by_instance(removed_key).await;
        }

        // Update the component set
        self.tidb_components = new_components;
    }

    /// Start a collector for a specific TiDB component with specific tables
    async fn start_collector_with_tables(
        &mut self,
        component: &Component,
        tables: Vec<TableConfig>,
        collector_key: &str,
    ) {
        let table_names: Vec<&str> = tables.iter().map(|t| t.source_table.as_str()).collect();
        info!(
            "Starting collector for {}:{} with tables: [{}]",
            component.host,
            component.primary_port,
            table_names.join(", ")
        );

        // Create a database config specific to this TiDB instance
        let mut instance_db_config = self.database_config.clone();
        instance_db_config.host = component.host.clone();
        instance_db_config.port = component.primary_port;

        // Get shared connection pool
        let shared_pool = match self.get_shared_pool().await {
            Ok(pool) => pool.clone(),
            Err(e) => {
                error!("Failed to get shared connection pool: {}", e);
                return;
            }
        };

        let collector = Collector::new(
            format!("{}:{}", component.host, component.primary_port),
            instance_db_config,
            self.collection_config.clone(),
            tables,
            self.out.clone(),
            shared_pool,
        );

        let handle = tokio::spawn(async move {
            collector.run().await;
        });

        self.running_collectors
            .insert(collector_key.to_string(), handle);
    }

    /// Stop a collector for a specific TiDB instance
    async fn stop_collector(&mut self, collector_key: &str) {
        if let Some(handle) = self.running_collectors.remove(collector_key) {
            info!("Stopping collector with key: {}", collector_key);
            handle.abort();
            info!("Stopped collector with key: {}", collector_key);
        }
    }

    /// Stop all collectors for a specific instance
    async fn stop_collector_by_instance(&mut self, instance: &str) {
        let keys_to_remove: Vec<String> = self
            .running_collectors
            .keys()
            .filter(|key| key.starts_with(instance))
            .cloned()
            .collect();

        for key in keys_to_remove {
            self.stop_collector(&key).await;
        }
    }

    /// Shutdown all collectors
    async fn shutdown_all_collectors(&mut self) {
        for (collector_key, handle) in self.running_collectors.drain() {
            info!("Shutting down collector with key: {}", collector_key);
            handle.abort();
        }
        info!("All collectors shut down");
    }
}
