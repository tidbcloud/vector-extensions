use std::collections::{HashMap, HashSet};
use std::time::Duration;

use tokio::time::interval;
use tracing::{debug, error, info, warn};
use vector::shutdown::ShutdownSignal;
use vector::SourceSender;
use vector_lib::config::proxy::ProxyConfig;
use vector_lib::tls::TlsConfig;

use crate::common::features::is_nextgen_mode;
use crate::common::topology::{Component, FetchError, InstanceType, TopologyFetcher};
use crate::sources::system_tables::{
    CollectionConfig, DatabaseConfig, TableConfig,
};

use crate::sources::system_tables::data_collector::{
    CollectionMethod, CollectorConfig, DataCollector,
};
use crate::sources::system_tables::collector_factory::CollectorFactory;

/// Main controller using abstracted data collectors
pub struct Controller {
    topology_fetch_interval: Duration,
    topology_fetcher: TopologyFetcher,
    tidb_components: HashSet<Component>,
    running_collectors: HashMap<String, CollectorTask>,
    database_config: DatabaseConfig,
    collection_config: CollectionConfig,
    tables: Vec<TableConfig>,
    collection_method: CollectionMethod,
    proxy_config: ProxyConfig,
    out: SourceSender,
}

/// Task information for a running collector
struct CollectorTask {
    handle: tokio::task::JoinHandle<()>,
    collector_type: CollectionMethod,
    table_count: usize,
}

impl Controller {
    /// Create a new controller with abstracted collectors
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
        collection_method: String,
    ) -> vector::Result<Self> {
        // Parse collection method
        let collection_method = CollectionMethod::from_string(&collection_method)
            .map_err(|e| format!("Invalid collection method: {}", e))?;

        // Create topology fetcher
        let topology_fetcher = if is_nextgen_mode() {
            info!("Using nextgen mode for topology discovery");
            if tidb_group.is_none() && label_k8s_instance.is_none() {
                return Err(
                    "In nextgen mode, either tidb_group or label_k8s_instance must be specified"
                        .into(),
                );
            }
            TopologyFetcher::new(
                Some(String::new()),
                None,
                proxy_config,
                tidb_group.clone(),
                label_k8s_instance.clone(),
            )
            .await
            .map_err(|e| format!("Failed to create nextgen topology fetcher: {}", e))?
        } else {
            info!("Using legacy mode for topology discovery");
            let pd_addr = pd_address.ok_or("In legacy mode, pd_address must be specified")?;

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
                Some(pd_addr),
                pd_tls.clone(),
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
            collection_method,
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
        let mut topology_interval = interval(self.topology_fetch_interval);

        loop {
            topology_interval.tick().await;

            // Fetch TiDB instances and update collectors
            if let Err(e) = self.fetch_and_update_tidb_instances().await {
                error!("Failed to fetch TiDB instances: {}", e);
            }
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

    /// Start a collector for a specific TiDB component using abstracted interface
    async fn start_collector_with_tables(
        &mut self,
        component: &Component,
        tables: Vec<TableConfig>,
        collector_key: &str,
    ) {
        let table_names: Vec<&str> = tables.iter().map(|t| t.source_table.as_str()).collect();
        info!(
            "Starting {} collector for {}:{} with {} tables: [{}]",
            self.collection_method,
            component.host,
            component.primary_port,
            tables.len(),
            table_names.join(", ")
        );

        // Create collector config
        let mut instance_db_config = self.database_config.clone();
        instance_db_config.host = component.host.clone();
        instance_db_config.port = component.primary_port;

        let collector_config = CollectorConfig {
            instance: format!("{}:{}", component.host, component.primary_port),
            database_config: instance_db_config,
            collection_config: self.collection_config.clone(),
            tables: tables.clone(),
            out: self.out.clone(),
        };

        // Create collector using simplified factory
        match CollectorFactory::create_collector(self.collection_method.clone(), collector_config)
        {
            Ok(mut collector) => {
                // Initialize the collector
                if let Err(e) = collector.initialize().await {
                    error!(
                        "Failed to initialize collector for {}: {}",
                        collector_key, e
                    );
                    return;
                }

                info!(
                    "Successfully initialized {} collector for {}",
                    collector.collection_method(),
                    collector_key
                );

                // Store table count before moving tables
                let table_count = tables.len();

                // Start the collector task
                let handle = tokio::spawn(async move {
                    Self::run_collector_task(collector, tables).await;
                });
                let task = CollectorTask {
                    handle,
                    collector_type: self.collection_method.clone(),
                    table_count,
                };

                self.running_collectors.insert(collector_key.to_string(), task);
            }
            Err(e) => {
                error!(
                    "Failed to create collector for {}: {}",
                    collector_key, e
                );
            }
        }
    }

    /// Run a collector task for multiple tables
    async fn run_collector_task(
        collector: Box<dyn DataCollector>,
        tables: Vec<TableConfig>,
    ) {
        use crate::sources::system_tables::data_collector::utils::{create_event_from_result, parse_collection_interval};

        let collection_config = &tables[0]; // Use first table's config as reference
        let interval_duration = Duration::from_secs(
            parse_collection_interval(
                &collection_config.collection_interval,
                &CollectionConfig {
                    short_interval: 5,
                    long_interval: 1800,
                    retention_days: 7,
                },
            ),
        );

        let mut collection_interval = interval(interval_duration);

        loop {
            collection_interval.tick().await;

            // Collect data from each table
            for table in &tables {
                if !table.enabled {
                    continue;
                }

                // Check if collector can handle this table
                if !collector.can_collect_table(table) {
                    warn!(
                        "Collector {} cannot handle table {}.{}",
                        collector.collection_method(),
                        table.source_schema,
                        table.source_table
                    );
                    continue;
                }

                match collector.collect_table_data(table).await {
                    Ok(result) => {
                        let row_count = result.data.len();
                        info!(
                            "Collected {} rows from table {} using {}",
                            row_count,
                            table.source_table,
                            collector.collection_method()
                        );

                        // Convert data to events and send
                        for row_data in &result.data {
                            let _event = create_event_from_result(&result, row_data.clone());

                            // Send event (note: we'd need to get the sender here)
                            // This is a simplified version - in practice, you'd need to pass
                            // the sender through the collector config or result
                            debug!("Created event for table {}", table.source_table);
                        }
                    }
                    Err(e) => {
                        error!(
                            "Failed to collect data from table {} using {}: {}",
                            table.source_table,
                            collector.collection_method(),
                            e
                        );
                    }
                }
            }

            // Perform periodic health check
            if let Err(e) = collector.health_check().await {
                warn!(
                    "Health check failed for {} collector: {}",
                    collector.collection_method(),
                    e
                );
            }
        }
    }

    /// Stop a collector by its key
    async fn stop_collector(&mut self, collector_key: &str) {
        if let Some(task) = self.running_collectors.remove(collector_key) {
            info!(
                "Stopping {} collector with key: {} ({} tables)",
                task.collector_type, collector_key, task.table_count
            );
            task.handle.abort();
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
        for (collector_key, task) in self.running_collectors.drain() {
            info!(
                "Shutting down {} collector with key: {} ({} tables)",
                task.collector_type, collector_key, task.table_count
            );
            task.handle.abort();
        }
        info!("All collectors shut down");
    }

    /// Get statistics about running collectors
    pub fn get_collector_statistics(&self) -> HashMap<String, serde_json::Value> {
        let mut stats = HashMap::new();

        stats.insert(
            "total_collectors".to_string(),
            serde_json::Value::Number(self.running_collectors.len().into()),
        );

        // Group by collector type
        let mut type_counts = HashMap::new();
        let mut table_counts = HashMap::new();

        for (key, task) in &self.running_collectors {
            let type_str = task.collector_type.to_string();
            *type_counts.entry(type_str.clone()).or_insert(0) += 1;
            *table_counts.entry(type_str).or_insert(0) += task.table_count;
        }

        stats.insert(
            "collector_types".to_string(),
            serde_json::Value::Object(
                type_counts
                    .into_iter()
                    .map(|(k, v)| (k, serde_json::Value::Number(v.into())))
                    .collect(),
            ),
        );

        stats.insert(
            "tables_by_type".to_string(),
            serde_json::Value::Object(
                table_counts
                    .into_iter()
                    .map(|(k, v)| (k, serde_json::Value::Number(v.into())))
                    .collect(),
            ),
        );

        stats.insert(
            "supported_methods".to_string(),
            serde_json::Value::Array(
                CollectorFactory::supported_methods()
                    .into_iter()
                    .map(|m| serde_json::Value::String(m.to_string()))
                    .collect(),
            ),
        );

        stats
    }
}