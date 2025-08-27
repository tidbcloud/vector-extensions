use std::collections::{HashMap, HashSet};
use std::time::Duration;

use vector::shutdown::ShutdownSignal;
use vector::SourceSender;
use vector_lib::config::proxy::ProxyConfig;
use vector_lib::tls::TlsConfig;

use crate::sources::system_tables::{
    collector::Collector, DatabaseConfig, CollectionConfig, TableConfig,
};

/// Main controller for system_tables source
pub struct Controller {
    topology_fetch_interval: Duration,
    tidb_instances: HashSet<String>,
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
        Ok(Self {
            topology_fetch_interval,
            tidb_instances: HashSet::new(),
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
    async fn fetch_and_update_tidb_instances(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let mut new_instances = HashSet::new();

        // For now, use the configured database as the primary instance
        // In a real implementation, you would fetch from PD or K8s
        let primary_instance = format!("{}:{}", self.database_config.host, self.database_config.port);
        new_instances.insert(primary_instance);

        // If PD address is configured, try to fetch additional instances
        // TODO: Implement PD-based TiDB discovery
        // For now, just use the primary instance

        // Update collectors based on instance changes
        self.update_collectors(new_instances).await;

        Ok(())
    }

    /// Fetch TiDB instances from PD (simplified implementation)
    async fn fetch_tidb_from_pd(&self, _pd_address: &str) -> Result<HashSet<String>, Box<dyn std::error::Error>> {
        // Simplified: return empty set for now
        // In a real implementation, you would query PD API to get TiDB instances
        Ok(HashSet::new())
    }

    /// Update collectors based on new TiDB instances
    async fn update_collectors(&mut self, new_instances: HashSet<String>) {
        // Clone tables first to avoid borrowing issues
        let tables = self.tables.clone();
        
        // Separate tables into cluster-level and instance-level
        let (cluster_tables, instance_tables): (Vec<_>, Vec<_>) = tables
            .iter()
            .partition(|table| table.source_table.starts_with("CLUSTER_"));

        info!("Table classification: {} cluster tables, {} instance tables", 
              cluster_tables.len(), instance_tables.len());

        // For cluster-level tables, only start one collector on the primary instance
        if !cluster_tables.is_empty() {
            let primary_instance = new_instances.iter().next().cloned();
            if let Some(primary_instance) = primary_instance {
                let cluster_collector_key = format!("{}_cluster", primary_instance);
                if !self.running_collectors.contains_key(&cluster_collector_key) {
                    let cluster_tables_owned: Vec<TableConfig> = cluster_tables.into_iter().cloned().collect();
                    self.start_collector_with_tables(&primary_instance, cluster_tables_owned, &cluster_collector_key).await;
                }
            }
        }

        // For instance-level tables, start collectors on all instances
        if !instance_tables.is_empty() {
            for instance in &new_instances {
                let instance_collector_key = format!("{}_instance", instance);
                if !self.running_collectors.contains_key(&instance_collector_key) {
                    let instance_tables_owned: Vec<TableConfig> = instance_tables.iter().map(|t| (*t).clone()).collect();
                    self.start_collector_with_tables(instance, instance_tables_owned, &instance_collector_key).await;
                }
            }
        }

        // Stop collectors for removed instances
        let current_instances: HashSet<_> = self.running_collectors.keys()
            .filter_map(|key| {
                if key.ends_with("_cluster") || key.ends_with("_instance") {
                    key.split('_').next().map(|s| s.to_string())
                } else {
                    Some(key.clone())
                }
            })
            .collect();
        
        for instance in current_instances.difference(&new_instances) {
            self.stop_collector_by_instance(instance).await;
        }

        // Update the instance set
        self.tidb_instances = new_instances;
    }
    
    /// Start a collector for a specific TiDB instance with specific tables
    async fn start_collector_with_tables(&mut self, instance: &str, tables: Vec<TableConfig>, collector_key: &str) {
        info!("Starting collector for TiDB instance: {} with {} tables (key: {})", instance, tables.len(), collector_key);

        let collector = Collector::new(
            instance.to_string(),
            self.database_config.clone(),
            self.collection_config.clone(),
            tables,
            self.out.clone(),
        );

        let handle = tokio::spawn(async move {
            collector.run().await;
        });

        self.running_collectors.insert(collector_key.to_string(), handle);
        info!("Started collector for TiDB instance: {} (key: {})", instance, collector_key);
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
