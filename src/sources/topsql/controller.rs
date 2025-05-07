use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use rand::seq::SliceRandom;
use rand::thread_rng;
use tracing::instrument::Instrument;
use vector::shutdown::ShutdownSignal;
use vector::SourceSender;
use vector_lib::{config::proxy::ProxyConfig, tls::TlsConfig};

use crate::sources::topsql::schema_cache::{SchemaCache, SchemaManager};
use crate::sources::topsql::shutdown::{pair, ShutdownNotifier, ShutdownSubscriber};
use crate::sources::topsql::topology::{Component, FetchError, InstanceType, TopologyFetcher};
use crate::sources::topsql::upstream::TopSQLSource;

pub struct Controller {
    topo_fetch_interval: Duration,
    topo_fetcher: TopologyFetcher,

    components: HashSet<Component>,
    running_components: HashMap<Component, ShutdownNotifier>,

    shutdown_notifier: ShutdownNotifier,
    shutdown_subscriber: ShutdownSubscriber,

    tls: Option<TlsConfig>,
    init_retry_delay: Duration,
    top_n: usize,
    downsampling_interval: u32,

    schema_cache: Option<Arc<SchemaCache>>,
    schema_update_interval: Duration,
    schema_manager_tidb: Option<Component>,

    out: SourceSender,
}

impl Controller {
    pub async fn new(
        pd_address: String,
        topo_fetch_interval: Duration,
        init_retry_delay: Duration,
        top_n: usize,
        downsampling_interval: u32,
        schema_update_interval: Duration,
        tls_config: Option<TlsConfig>,
        proxy_config: &ProxyConfig,
        out: SourceSender,
    ) -> vector::Result<Self> {
        let topo_fetcher =
            TopologyFetcher::new(pd_address, tls_config.clone(), proxy_config).await?;
        let (shutdown_notifier, shutdown_subscriber) = pair();
        Ok(Self {
            topo_fetch_interval,
            topo_fetcher,
            components: HashSet::new(),
            running_components: HashMap::new(),
            shutdown_notifier,
            shutdown_subscriber,
            tls: tls_config,
            init_retry_delay,
            top_n,
            downsampling_interval,
            schema_cache: None,
            schema_update_interval,
            schema_manager_tidb: None,
            out,
        })
    }

    pub async fn run(mut self, mut shutdown: ShutdownSignal) {
        tokio::select! {
            _ = self.run_loop() => {},
            _ = &mut shutdown => {},
        }

        info!("TopSQL PubSub Controller is shutting down.");
        self.shutdown_all_components().await;
    }

    async fn run_loop(&mut self) {
        loop {
            let res = self.fetch_and_update().await;
            match res {
                Ok(has_change) if has_change => {
                    info!(message = "Topology has changed.", latest_components = ?self.components);
                }
                Err(error) => {
                    error!(message = "Failed to fetch topology.", error = %error);
                }
                _ => {}
            }

            tokio::time::sleep(self.topo_fetch_interval).await;
        }
    }

    async fn fetch_and_update(&mut self) -> Result<bool, FetchError> {
        let mut has_change = false;
        let mut latest_components = HashSet::new();
        self.topo_fetcher
            .get_up_components(&mut latest_components)
            .await?;

        let prev_components = self.components.clone();
        let newcomers = latest_components.difference(&prev_components);
        let leavers = prev_components.difference(&latest_components);

        for newcomer in newcomers {
            if self.start_component(newcomer) {
                has_change = true;
                self.components.insert(newcomer.clone());
            }
        }
        for leaver in leavers {
            if self.stop_component(leaver).await {
                has_change = true;
                self.components.remove(leaver);
            }
        }

        // Check if the TiDB instance used by the current schema manager is no longer available
        let need_update_schema_manager = match &self.schema_manager_tidb {
            Some(tidb) => !latest_components.contains(tidb),
            None => true, // Schema manager has never been started
        };

        // If we need to update the schema manager, find an available TiDB instance
        if need_update_schema_manager {
            self.update_schema_manager(&latest_components).await;
        }

        Ok(has_change)
    }

    async fn update_schema_manager(&mut self, available_components: &HashSet<Component>) {
        // If there is a running schema manager, clear the reference
        if let Some(_tidb) = &self.schema_manager_tidb {
            self.schema_manager_tidb = None;
        }

        // Find all available TiDB instances
        let mut tidb_components: Vec<_> = available_components
            .iter()
            .filter(|c| c.instance_type == InstanceType::TiDB)
            .collect();

        if tidb_components.is_empty() {
            info!(message = "No TiDB component available for schema manager");
            return;
        }

        // Shuffle the TiDB instances to distribute load
        tidb_components.shuffle(&mut thread_rng());
        info!(
            message = "Shuffled TiDB instances for load balancing",
            instance_count = tidb_components.len()
        );

        // Try each TiDB instance until one succeeds
        for tidb in tidb_components {
            info!(message = "Trying schema manager with TiDB instance", instance = %tidb);

            let tidb_address = format!("{}:{}", tidb.host, tidb.secondary_port);

            // Use the async constructor with TLS configuration
            let schema_manager = match SchemaManager::new(
                tidb_address,
                self.schema_update_interval,
                self.tls.clone(),
            )
            .await
            {
                Ok(manager) => manager,
                Err(err) => {
                    error!(message = "Failed to create schema manager with this TiDB instance, trying next", instance = %tidb, %err);
                    continue; // Try the next TiDB instance
                }
            };

            // Get cache for stats and store it
            let cache = schema_manager.get_cache();
            self.schema_cache = Some(cache.clone());

            // Convert ShutdownSubscriber to broadcast::Receiver<()>
            let shutdown = self.shutdown_subscriber.subscribe();

            // Clone the etcd client for the schema manager
            let etcd_client = self.topo_fetcher.etcd_client.clone();

            tokio::spawn(
                schema_manager
                    .run_update_loop_with_etcd(shutdown, etcd_client)
                    .instrument(tracing::info_span!("topsql_schema_manager")),
            );

            info!(
                message = "Started schema manager successfully",
                instance = %tidb,
                initial_entries = cache.entry_count(),
                initial_memory_usage_bytes = cache.memory_usage(),
                initial_memory_usage_kb = cache.memory_usage() / 1024
            );

            // Store the running schema manager and the TiDB instance it uses
            self.running_components
                .insert(tidb.clone(), self.shutdown_notifier.clone());
            self.schema_manager_tidb = Some(tidb.clone());

            // Successfully started, exit the loop
            return;
        }

        // If we get here, all TiDB instances failed
        error!(message = "Failed to start schema manager with any available TiDB instance");
    }

    fn start_component(&mut self, component: &Component) -> bool {
        let source = TopSQLSource::new(
            component.clone(),
            self.tls.clone(),
            self.out.clone(),
            self.init_retry_delay,
            self.top_n,
            self.downsampling_interval,
            self.schema_cache.clone(),
        );
        let source = match source {
            Some(source) => source,
            None => return false,
        };

        let (shutdown_notifier, shutdown_subscriber) = self.shutdown_subscriber.extend();
        tokio::spawn(
            source
                .run(shutdown_subscriber)
                .instrument(tracing::info_span!("topsql_source", topsql_source = %component)),
        );
        info!(message = "Started TopSQL source.", topsql_source = %component);
        self.running_components
            .insert(component.clone(), shutdown_notifier);

        true
    }

    async fn stop_component(&mut self, component: &Component) -> bool {
        let shutdown_notifier = self.running_components.remove(component);
        let shutdown_notifier = match shutdown_notifier {
            Some(shutdown_notifier) => shutdown_notifier,
            None => return false,
        };
        shutdown_notifier.shutdown();
        shutdown_notifier.wait_for_exit().await;
        info!(message = "Stopped TopSQL source.", topsql_source = %component);

        // If the component being stopped is the current TiDB instance used by the schema manager, clear the reference
        if let Some(tidb) = &self.schema_manager_tidb {
            if tidb == component {
                // Print memory usage stats before clearing schema manager reference
                if let Some(cache) = &self.schema_cache {
                    info!(
                        message = "Schema cache stats when stopping TiDB instance",
                        instance = %tidb,
                        entries = cache.entry_count(),
                        memory_usage_bytes = cache.memory_usage(),
                        memory_usage_kb = cache.memory_usage() / 1024
                    );
                }
                self.schema_manager_tidb = None;
            }
        }

        true
    }

    async fn shutdown_all_components(self) {
        for (component, shutdown_notifier) in self.running_components {
            info!(message = "Shutting down TopSQL source.", topsql_source = %component);
            shutdown_notifier.shutdown();
            shutdown_notifier.wait_for_exit().await;
        }

        drop(self.shutdown_subscriber);
        self.shutdown_notifier.shutdown();
        self.shutdown_notifier.wait_for_exit().await;
        info!(message = "All TopSQL sources have been shut down.");
    }
}
