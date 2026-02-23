use std::collections::{HashMap, HashSet};
use std::time::Duration;

use tokio::time::{interval, sleep_until, Instant};
use tracing::{debug, error, info, warn};
use vector::shutdown::ShutdownSignal;
use vector::SourceSender;
use vector_lib::config::proxy::ProxyConfig;
use vector_lib::tls::TlsConfig;

use crate::common::features::is_nextgen_mode;
use crate::common::topology::{Component, FetchError, InstanceType, TopologyFetcher};
use crate::sources::system_tables::{CollectionConfig, DatabaseConfig, TableConfig};

use crate::sources::system_tables::collector_factory::CollectorFactory;
use crate::sources::system_tables::data_collector::{
    CollectionMethod, CollectorConfig, DataCollector,
};

/// Main controller using abstracted data collectors
#[allow(dead_code)]
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
        let default_method = self.collection_method.clone();

        // Separate tables into cluster-level and instance-level, then by collection method
        let (cluster_tables, instance_tables): (Vec<_>, Vec<_>) = tables
            .iter()
            .partition(|table| table.source_table.starts_with("CLUSTER_"));

        debug!(
            "Table classification: {} cluster tables, {} instance tables",
            cluster_tables.len(),
            instance_tables.len()
        );

        // Helper to get collection method for a table
        let get_method = |table: &TableConfig| -> CollectionMethod {
            if let Some(ref method) = table.collection_method {
                CollectionMethod::from_string(method).unwrap_or_else(|_| default_method.clone())
            } else {
                default_method.clone()
            }
        };

        // For cluster-level tables, group by collection method
        // Each group gets its own collector
        if !cluster_tables.is_empty() {
            let primary_component = new_components.iter().next().cloned();
            if let Some(primary_component) = primary_component {
                // Group cluster tables by collection method
                let mut method_groups: HashMap<CollectionMethod, Vec<TableConfig>> = HashMap::new();
                for table in &cluster_tables {
                    let method = get_method(table);
                    method_groups
                        .entry(method)
                        .or_insert_with(Vec::new)
                        .push((*table).clone());
                }

                for (method, group_tables) in method_groups {
                    let collector_key = format!(
                        "{}:{}_cluster_{}",
                        primary_component.host,
                        primary_component.primary_port,
                        method.to_string()
                    );
                    if !self.running_collectors.contains_key(&collector_key) {
                        self.start_collector_with_tables(
                            &primary_component,
                            group_tables,
                            &collector_key,
                            method,
                        )
                        .await;
                    }
                }
            }
        }

        // For instance-level tables, group by collection method
        if !instance_tables.is_empty() {
            // Group instance tables by collection method
            let mut method_groups: HashMap<CollectionMethod, Vec<TableConfig>> = HashMap::new();
            for table in &instance_tables {
                let method = get_method(table);
                method_groups
                    .entry(method)
                    .or_insert_with(Vec::new)
                    .push((*table).clone());
            }

            for component in &new_components {
                for (method, group_tables) in &method_groups {
                    let collector_key = format!(
                        "{}:{}_instance_{}",
                        component.host,
                        component.primary_port,
                        method.to_string()
                    );
                    if !self.running_collectors.contains_key(&collector_key) {
                        self.start_collector_with_tables(
                            component,
                            group_tables.clone(),
                            &collector_key,
                            method.clone(),
                        )
                        .await;
                    }
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
        collection_method: CollectionMethod,
    ) {
        // Validate table compatibility with collection method
        if collection_method == CollectionMethod::Coprocessor {
            for table in &tables {
                if !table.source_table.starts_with("CLUSTER_") {
                    error!(
                        "Table {} is not a cluster table and cannot be collected using coprocessor method. Only CLUSTER_* tables are supported for coprocessor collection.",
                        table.source_table
                    );
                    return;
                }
            }
        }

        let table_names: Vec<&str> = tables.iter().map(|t| t.source_table.as_str()).collect();
        info!(
            "Starting {} collector for {}:{} with {} tables: [{}]",
            collection_method,
            component.host,
            component.primary_port,
            tables.len(),
            table_names.join(", ")
        );

        // Create collector config based on collection method
        let instance = format!("{}:{}", component.host, component.primary_port);
        let collector_config = match collection_method {
            CollectionMethod::Coprocessor => {
                // For coprocessor method, use coprocessor-specific config
                // Pass database TLS config for HTTP schema fetching
                CollectorConfig::for_coprocessor(
                    instance,
                    component.host.clone(),
                    component.primary_port,
                    Some(30), // grpc_timeout_secs
                    Some(3),  // max_retries
                    self.database_config.tls.clone(),
                )
            }
            CollectionMethod::Sql => {
                // For SQL method, use database config
                let mut instance_db_config = self.database_config.clone();
                instance_db_config.host = component.host.clone();
                instance_db_config.port = component.primary_port;

                CollectorConfig::for_sql(instance, instance_db_config)
            }
            CollectionMethod::HttpApi => {
                // For HTTP API method, use HTTP-specific config
                CollectorConfig::for_http_api(
                    instance,
                    component.host.clone(),
                    component.primary_port,
                    Some(30), // timeout_secs
                    Some(3),  // max_retries
                )
            }
            CollectionMethod::CustomGrpc => {
                // For custom gRPC, fallback to coprocessor config for now
                CollectorConfig::for_coprocessor(
                    instance,
                    component.host.clone(),
                    component.primary_port,
                    Some(30),
                    Some(3),
                    self.database_config.tls.clone(),
                )
            }
            CollectionMethod::GrpcPush => {
                // For gRPC push, use secondary_port (status port) for registration
                // and start a gRPC server on Vector side to receive push data
                CollectorConfig::for_grpc_push(
                    instance,
                    component.host.clone(),
                    component.secondary_port,
                    "0.0.0.0".to_string(),
                    50051, // Vector gRPC port
                    Some(30),
                    Some(3),
                )
            }
            CollectionMethod::GrpcPull => {
                // For gRPC pull, use secondary_port (status port) to query TiDB
                // via SystemTablePullService::QueryTable
                CollectorConfig::for_grpc_pull(
                    instance,
                    component.host.clone(),
                    component.secondary_port,
                    Some(30),
                    Some(3),
                )
            }
        };

        // Create collector using simplified factory
        // For GrpcPush, we need to pass the first table's config
        let table_config_for_v3 = tables.first().cloned();
        match CollectorFactory::create_collector(collection_method.clone(), collector_config, table_config_for_v3) {
            Ok(mut collector) => {
                // For GrpcPush, set output sender so it can send events directly
                // instead of relying on the fixed-interval loop
                if collection_method == CollectionMethod::GrpcPush {
                    collector.set_output_sender(self.out.clone());
                }

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

                // For GrpcPush, the collector handles sending internally via set_output_sender
                // No need to start the run_collector_task loop
                if collection_method == CollectionMethod::GrpcPush {
                    info!(
                        "GrpcPush collector running in push mode - handles sending internally"
                    );
                    // Keep the task alive indefinitely (collector runs until shutdown)
                    let task = CollectorTask {
                        handle: tokio::spawn(async move {
                            // Wait forever - the collector's background tasks will keep running
                            loop {
                                tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
                            }
                        }),
                        collector_type: self.collection_method.clone(),
                        table_count,
                    };
                    self.running_collectors
                        .insert(collector_key.to_string(), task);
                    return;
                }

                // Start the collector task for pull-based collectors
                let out_clone = self.out.clone();
                let collection_config_clone = self.collection_config.clone();
                let handle = tokio::spawn(async move {
                    Self::run_collector_task(collector, tables, out_clone, collection_config_clone)
                        .await;
                });
                let task = CollectorTask {
                    handle,
                    collector_type: self.collection_method.clone(),
                    table_count,
                };

                self.running_collectors
                    .insert(collector_key.to_string(), task);
            }
            Err(e) => {
                error!("Failed to create collector for {}: {}", collector_key, e);
            }
        }
    }

    /// Run a collector task for multiple tables
    async fn run_collector_task(
        collector: Box<dyn DataCollector>,
        tables: Vec<TableConfig>,
        mut out: SourceSender,
        collection_config: CollectionConfig,
    ) {
        use crate::sources::system_tables::data_collector::utils::{
            create_event_from_result, parse_collection_interval,
        };

        let table_config = &tables[0]; // Use first table's config as reference
        let table_names: Vec<String> = tables.iter().map(|t| t.source_table.clone()).collect();

        // Special AUTO scheduling for coprocessor + STATEMENTS_SUMMARY tables
        let is_copr = matches!(collector.collection_method(), CollectionMethod::Coprocessor);
        let has_statements_summary = tables
            .iter()
            .any(|t| t.source_table.contains("STATEMENTS_SUMMARY"));
        let auto_interval_secs = if is_copr && table_config.collection_interval.starts_with("auto(")
        {
            table_config
                .collection_interval
                .trim_start_matches("auto(")
                .trim_end_matches(')')
                .parse::<u64>()
                .unwrap_or(300)
        } else {
            0
        };

        if is_copr && has_statements_summary && auto_interval_secs > 0 {
            info!(
                "📊 Starting AUTO aligned collection for tables: [{}], TiDB rotate={}s (pull at rotate-20s)",
                table_names.join(", "),
                auto_interval_secs
            );

            // Main loop aligned to TiDB rotate boundary: floor(now/interval)*interval + interval - 20s
            loop {
                let now_secs = chrono::Utc::now().timestamp() as u64;
                let begin_for_cur = (now_secs / auto_interval_secs) * auto_interval_secs;
                let rotate_at = begin_for_cur + auto_interval_secs;
                // target time is 20s before rotate; if already passed, use next interval
                let mut target = rotate_at.saturating_sub(20);
                if now_secs >= target {
                    let next_begin = rotate_at;
                    let next_rotate = next_begin + auto_interval_secs;
                    target = next_rotate.saturating_sub(20);
                }

                let sleep_secs = target.saturating_sub(now_secs);
                info!(
                    "⏳ Waiting {}s until next aligned pull at t={} (rotate-20s)",
                    sleep_secs, target
                );
                let wake_at = Instant::now() + Duration::from_secs(sleep_secs);
                sleep_until(wake_at).await;

                info!(
                    "🔄 AUTO collection cycle starting for tables: [{}]",
                    table_names.join(", ")
                );

                // Collect with up to 5 retries to adapt around rotate jitter
                for table in &tables {
                    if !table.enabled {
                        continue;
                    }
                    if !collector.can_collect_table(table) {
                        warn!(
                            "Collector {} cannot handle table {}.{}",
                            collector.collection_method(),
                            table.source_schema,
                            table.source_table
                        );
                        continue;
                    }

                    let mut attempts = 0u8;
                    loop {
                        attempts += 1;
                        match collector.collect_table_data(table).await {
                            Ok(result) => {
                                let row_count = result.data.len();
                                info!(
                                    "Collected {} rows from table {} using {} (attempt {}/{})",
                                    row_count,
                                    table.source_table,
                                    collector.collection_method(),
                                    attempts,
                                    5
                                );

                                for row_data in &result.data {
                                    let event = create_event_from_result(&result, row_data.clone());
                                    if let Err(e) = out.send_event(event).await {
                                        error!(
                                            "Failed to send event for table {}: {}",
                                            table.source_table, e
                                        );
                                    } else {
                                        debug!(
                                            "Successfully sent event for table {}",
                                            table.source_table
                                        );
                                    }
                                }
                                break;
                            }
                            Err(e) => {
                                if attempts >= 5 {
                                    error!(
                                        "Failed to collect data from table {} using {} after {} attempts: {}",
                                        table.source_table,
                                        collector.collection_method(),
                                        attempts,
                                        e
                                    );
                                    break;
                                } else {
                                    warn!(
                                        "Collect failed for table {} (attempt {}/{}): {}. Retrying in 3s...",
                                        table.source_table,
                                        attempts,
                                        5,
                                        e
                                    );
                                    tokio::time::sleep(Duration::from_secs(3)).await;
                                }
                            }
                        }
                    }
                }

                if let Err(e) = collector.health_check().await {
                    warn!(
                        "Health check failed for {} collector: {}",
                        collector.collection_method(),
                        e
                    );
                }
            }
        } else {
            // Default fixed-interval scheduling
            let interval_seconds =
                parse_collection_interval(&table_config.collection_interval, &collection_config);
            let interval_duration = Duration::from_secs(interval_seconds);

            info!(
                "📊 Starting collection loop for tables: [{}] with interval: {}s ({}) [config: short={}s, long={}s]",
                table_names.join(", "),
                interval_seconds,
                &table_config.collection_interval,
                collection_config.short_interval,
                collection_config.long_interval
            );

            let mut collection_interval = interval(interval_duration);

            loop {
                collection_interval.tick().await;

                info!(
                    "🔄 Collection cycle starting - interval: {}s, tables: [{}]",
                    interval_seconds,
                    table_names.join(", ")
                );

                for table in &tables {
                    if !table.enabled {
                        continue;
                    }
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
                            if row_count > 0 {
                                info!(
                                    "Collected {} rows from table {} using {}",
                                    row_count,
                                    table.source_table,
                                    collector.collection_method()
                                );
                            }
                            for row_data in &result.data {
                                let event = create_event_from_result(&result, row_data.clone());
                                if let Err(e) = out.send_event(event).await {
                                    error!(
                                        "Failed to send event for table {}: {}",
                                        table.source_table, e
                                    );
                                } else {
                                    debug!(
                                        "Successfully sent event for table {}",
                                        table.source_table
                                    );
                                }
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

                if let Err(e) = collector.health_check().await {
                    warn!(
                        "Health check failed for {} collector: {}",
                        collector.collection_method(),
                        e
                    );
                }
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
}
