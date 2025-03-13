use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tracing::instrument::Instrument;
use vector::shutdown::ShutdownSignal;
use vector::SourceSender;
use vector_lib::{config::proxy::ProxyConfig, tls::TlsConfig};

use crate::sources::topsql::schema_cache::{SchemaCache, SchemaManager};
use crate::sources::topsql::shutdown::{pair, ShutdownNotifier, ShutdownSubscriber};
use crate::sources::topsql::topology::{Component, FetchError, TopologyFetcher};
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
            out,
        })
    }

    pub async fn run(mut self, mut shutdown: ShutdownSignal) {
        // Start schema manager if we have a TiDB component
        self.start_schema_manager_if_needed().await;

        tokio::select! {
            _ = self.run_loop() => {},
            _ = &mut shutdown => {},
        }

        info!("TopSQL PubSub Controller is shutting down.");
        self.shutdown_all_components().await;
    }

    async fn start_schema_manager_if_needed(&mut self) {
        // First fetch to see if we have any TiDB components
        let mut components = HashSet::new();
        if let Err(e) = self.topo_fetcher.get_up_components(&mut components).await {
            error!(message = "Failed to fetch initial topology for schema manager", error = %e);
            return;
        }

        // Find a TiDB component to use for schema fetching
        let tidb_component = components
            .iter()
            .find(|c| c.instance_type == crate::sources::topsql::topology::InstanceType::TiDB);

        if let Some(tidb) = tidb_component {
            info!(message = "Starting schema manager with TiDB instance", instance = %tidb);

            let https = self.tls.is_some();
            let tidb_address = format!("{}:{}", tidb.host, tidb.secondary_port);

            let schema_manager =
                SchemaManager::new(tidb_address, https, self.schema_update_interval);
            self.schema_cache = Some(schema_manager.get_cache());

            // Convert ShutdownSubscriber to broadcast::Receiver<()>
            let shutdown = self.shutdown_subscriber.subscribe();

            // Clone the etcd client for the schema manager
            let etcd_client = self.topo_fetcher.etcd_client.clone();

            tokio::spawn(
                schema_manager
                    .run_update_loop_with_etcd(shutdown, etcd_client)
                    .instrument(tracing::info_span!("topsql_schema_manager")),
            );

            info!(message = "Started schema manager");
            self.running_components
                .insert(tidb.clone(), self.shutdown_notifier.clone());
        } else {
            info!(message = "No TiDB component found for schema manager");
        }
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

        Ok(has_change)
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
