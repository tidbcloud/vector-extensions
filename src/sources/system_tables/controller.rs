use std::collections::{HashMap, HashSet};
use std::time::Duration;

use vector::shutdown::ShutdownSignal;
use vector::SourceSender;
use vector_lib::config::proxy::ProxyConfig;
use vector_lib::tls::TlsConfig;

use crate::sources::system_tables::{
    collector::Collector, DatabaseConfig, CollectionConfig, TableConfig,
};
use crate::sources::topsql::topology::{Component, FetchError, InstanceType, TopologyFetcher};

/// Main controller for system_tables source
pub struct Controller {
    topology_fetch_interval: Duration,
    topology_fetcher: TopologyFetcher,
    tidb_components: HashSet<Component>,
    running_collectors: HashMap<String, tokio::task::JoinHandle<()>>,
    database_config: DatabaseConfig,
    collection_config: CollectionConfig,
    tables: Vec<TableConfig>,
    tls: Option<TlsConfig>,
    proxy_config: ProxyConfig,
    out: SourceSender,
}

impl Controller {
    /// Create a new controller
    pub async fn new(
        pd_address: Option<String>,
        tidb_group: Option<String>,
        topology_fetch_interval: Duration,
        database_config: DatabaseConfig,
        collection_config: CollectionConfig,
        tables: Vec<TableConfig>,
        tls: Option<TlsConfig>,
        proxy_config: &ProxyConfig,
        out: SourceSender,
    ) -> vector::Result<Self> {
        // Create topology fetcher based on configuration
        let topology_fetcher = if let Some(pd_addr) = pd_address {
            TopologyFetcher::new(
                pd_addr,
                tls.clone(),
                proxy_config,
                tidb_group,
                None, // label_k8s_instance not used for system_tables
            )
            .await
            .map_err(|e| format!("Failed to create topology fetcher: {}", e))?
        } else {
            return Err("PD address is required for system_tables source".into());
        };

        Ok(Self {
            topology_fetch_interval,
            topology_fetcher,
            tidb_components: HashSet::new(),
            running_collectors: HashMap::new(),
            database_config,
            collection_config,
            tables,
            tls,
            proxy_config: proxy_config.clone(),
            out,
        })
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
            info!("TiDB topology changed: {} components discovered", tidb_components.len());
            for component in &tidb_components {
                info!("  TiDB instance: {}:{}", component.host, component.primary_port);
            }
        } else {
            debug!("TiDB topology unchanged: {} components", tidb_components.len());
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

        debug!("Table classification: {} cluster tables, {} instance tables",
               cluster_tables.len(), instance_tables.len());

        // For cluster-level tables, only start one collector on the primary instance
        if !cluster_tables.is_empty() {
            let primary_component = new_components.iter().next().cloned();
            if let Some(primary_component) = primary_component {
                let cluster_collector_key = format!("{}:{}_cluster", primary_component.host, primary_component.primary_port);
                if !self.running_collectors.contains_key(&cluster_collector_key) {
                    let cluster_tables_owned: Vec<TableConfig> = cluster_tables.into_iter().cloned().collect();
                    self.start_collector_with_tables(&primary_component, cluster_tables_owned, &cluster_collector_key).await;
                }
            }
        }

        // For instance-level tables, start collectors on all instances
        if !instance_tables.is_empty() {
            for component in &new_components {
                let instance_collector_key = format!("{}:{}_instance", component.host, component.primary_port);
                if !self.running_collectors.contains_key(&instance_collector_key) {
                    let instance_tables_owned: Vec<TableConfig> = instance_tables.iter().map(|t| (*t).clone()).collect();
                    self.start_collector_with_tables(component, instance_tables_owned, &instance_collector_key).await;
                }
            }
        }

        // Stop collectors for removed instances
        let current_component_keys: HashSet<_> = self.tidb_components.iter()
            .map(|c| format!("{}:{}", c.host, c.primary_port))
            .collect();
        let new_component_keys: HashSet<_> = new_components.iter()
            .map(|c| format!("{}:{}", c.host, c.primary_port))
            .collect();

        for removed_key in current_component_keys.difference(&new_component_keys) {
            self.stop_collector_by_instance(removed_key).await;
        }

        // Update the component set
        self.tidb_components = new_components;
    }
    
    /// Start a collector for a specific TiDB component with specific tables
    async fn start_collector_with_tables(&mut self, component: &Component, tables: Vec<TableConfig>, collector_key: &str) {
        let table_names: Vec<&str> = tables.iter().map(|t| t.source_table.as_str()).collect();
        info!("Starting collector for {}:{} with tables: [{}]",
              component.host, component.primary_port, table_names.join(", "));

        // Create a database config specific to this TiDB instance
        let mut instance_db_config = self.database_config.clone();
        instance_db_config.host = component.host.clone();
        instance_db_config.port = component.primary_port;

        let collector = Collector::new(
            format!("{}:{}", component.host, component.primary_port),
            instance_db_config,
            self.collection_config.clone(),
            tables,
            self.out.clone(),
        );

        let handle = tokio::spawn(async move {
            collector.run().await;
        });

        self.running_collectors.insert(collector_key.to_string(), handle);
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
        let keys_to_remove: Vec<String> = self.running_collectors.keys()
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
