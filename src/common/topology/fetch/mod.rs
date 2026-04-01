mod models;
mod pd;
mod store;
mod tidb;
mod tidb_manager;
mod utils;

pub mod tidb_nextgen;
mod tikv_nextgen;

#[cfg(test)]
mod mock;

use crate::common::topology::Component;
use snafu::{ResultExt, Snafu};
use std::collections::HashSet;

// Import dependencies
use kube;
use std::fs::read;
use vector::config::ProxyConfig;
use vector::http::HttpClient;
use vector::tls::{MaybeTlsSettings, TlsConfig};

#[derive(Debug, Snafu)]
pub enum FetchError {
    #[snafu(display("Failed to build TLS settings: {}", source))]
    BuildTlsSettings { source: vector::tls::TlsError },
    #[snafu(display("Failed to read ca file: {}", source))]
    ReadCaFile { source: std::io::Error },
    #[snafu(display("Failed to read crt file: {}", source))]
    ReadCrtFile { source: std::io::Error },
    #[snafu(display("Failed to read key file: {}", source))]
    ReadKeyFile { source: std::io::Error },
    #[snafu(display("Failed to parse address: {}", source))]
    ParseAddress { source: http::uri::InvalidUri },
    #[snafu(display("Failed to build HTTP client: {}", source))]
    BuildHttpClient { source: vector::http::HttpError },
    #[snafu(display("Failed to build etcd client: {}", source))]
    BuildEtcdClient { source: etcd_client::Error },
    #[snafu(display("Failed to build kubernetes client: {}", source))]
    BuildKubeClient { source: kube::Error },
    #[snafu(display("Failed to fetch pd topology: {}", source))]
    FetchPDTopology { source: pd::FetchError },
    #[snafu(display("Failed to fetch tidb topology: {}", source))]
    FetchTiDBTopology { source: tidb::FetchError },
    #[snafu(display("Failed to fetch tidb topology from manager server: {}", source))]
    FetchTiDBFromManagerServerTopology { source: tidb_manager::FetchError },
    #[snafu(display("Failed to fetch store topology: {}", source))]
    FetchStoreTopology { source: store::FetchError },
    #[snafu(display("Failed to fetch tidb nextgen topology: {}", source))]
    FetchTiDBNextGenTopology { source: tidb_nextgen::FetchError },
    #[snafu(display("Failed to fetch tikv nextgen topology: {}", source))]
    FetchTiKVNextGenTopology { source: tikv_nextgen::FetchError },
    #[snafu(display("Configuration error: {}", message))]
    ConfigurationError { message: String },
}

// Legacy topology fetcher
pub struct LegacyTopologyFetcher {
    pd_address: String,
    manager_server_address: Option<String>,
    tidb_namespace: Option<String>,
    http_client: HttpClient<hyper::Body>,
    pub etcd_client: etcd_client::Client,
}

impl LegacyTopologyFetcher {
    pub async fn new(
        pd_address: String,
        manager_server_address: Option<String>,
        tidb_namespace: Option<String>,
        tls_config: Option<TlsConfig>,
        proxy_config: &ProxyConfig,
    ) -> Result<Self, FetchError> {
        let pd_address = Self::polish_address(pd_address, &tls_config)?;
        let manager_server_address = manager_server_address
            .map(Self::polish_manager_server_address)
            .transpose()?;
        let http_client = Self::build_http_client(tls_config.as_ref(), proxy_config)?;
        let etcd_client = Self::build_etcd_client(&pd_address, &tls_config).await?;

        Ok(Self {
            pd_address,
            manager_server_address,
            tidb_namespace,
            http_client,
            etcd_client,
        })
    }

    pub async fn get_up_components(
        &mut self,
        components: &mut HashSet<Component>,
    ) -> Result<(), FetchError> {
        pd::PDTopologyFetcher::new(&self.pd_address, &self.http_client)
            .get_up_pds(components)
            .await
            .context(FetchPDTopologySnafu)?;
        if let Some(manager_server_address) = self.manager_server_address.as_deref() {
            if self
                .tidb_namespace
                .as_deref()
                .map(str::trim)
                .is_none_or(str::is_empty)
            {
                info!(
                    message =
                        "Skipping manager-based TiDB discovery because tidb namespace is empty",
                    manager_server_address
                );
            }
            tidb_manager::TiDBManagerTopologyFetcher::new(
                manager_server_address,
                self.tidb_namespace.as_deref(),
                &self.http_client,
            )
            .get_up_tidbs(components)
            .await
            .context(FetchTiDBFromManagerServerTopologySnafu)?;
        } else {
            tidb::TiDBTopologyFetcher::new(&mut self.etcd_client)
                .get_up_tidbs(components)
                .await
                .context(FetchTiDBTopologySnafu)?;
        }
        store::StoreTopologyFetcher::new(&self.pd_address, &self.http_client)
            .get_up_stores(components)
            .await
            .context(FetchStoreTopologySnafu)?;
        Ok(())
    }

    fn polish_address(
        mut address: String,
        tls_config: &Option<TlsConfig>,
    ) -> Result<String, FetchError> {
        let uri: hyper::Uri = address.parse().context(ParseAddressSnafu)?;
        if uri.scheme().is_none() {
            address = if tls_config.is_some() {
                format!("https://{address}")
            } else {
                format!("http://{address}")
            };
        }
        if address.ends_with('/') {
            address.pop();
        }
        Ok(address)
    }

    fn polish_manager_server_address(mut address: String) -> Result<String, FetchError> {
        let uri: hyper::Uri = address.parse().context(ParseAddressSnafu)?;
        if uri.scheme().is_none() {
            address = format!("http://{address}");
        }
        if address.ends_with('/') {
            address.pop();
        }
        Ok(address)
    }

    fn build_http_client(
        tls_config: Option<&TlsConfig>,
        proxy_config: &ProxyConfig,
    ) -> Result<HttpClient<hyper::Body>, FetchError> {
        let tls_settings =
            MaybeTlsSettings::tls_client(tls_config).context(BuildTlsSettingsSnafu)?;

        let http_client =
            HttpClient::new(tls_settings, proxy_config).context(BuildHttpClientSnafu)?;
        Ok(http_client)
    }

    async fn build_etcd_client(
        pd_address: &str,
        tls_config: &Option<TlsConfig>,
    ) -> Result<etcd_client::Client, FetchError> {
        let etcd_connect_opt = Self::build_etcd_connect_opt(tls_config)?;
        let etcd_client: etcd_client::Client =
            etcd_client::Client::connect(&[pd_address], etcd_connect_opt)
                .await
                .context(BuildEtcdClientSnafu)?;
        Ok(etcd_client)
    }

    fn build_etcd_connect_opt(
        tls_config: &Option<TlsConfig>,
    ) -> Result<Option<etcd_client::ConnectOptions>, FetchError> {
        let conn_opt = if let Some(tls_config) = tls_config.as_ref() {
            let mut tls_options = etcd_client::TlsOptions::new();

            if let Some(ca_file) = tls_config.ca_file.as_ref() {
                let cacert = read(ca_file).context(ReadCaFileSnafu)?;
                tls_options = tls_options.ca_certificate(etcd_client::Certificate::from_pem(cacert))
            }

            if let (Some(crt_file), Some(key_file)) =
                (tls_config.crt_file.as_ref(), tls_config.key_file.as_ref())
            {
                let cert = read(crt_file).context(ReadCrtFileSnafu)?;
                let key = read(key_file).context(ReadKeyFileSnafu)?;
                tls_options = tls_options.identity(etcd_client::Identity::from_pem(cert, key));
            }
            Some(etcd_client::ConnectOptions::new().with_tls(tls_options))
        } else {
            None
        };

        Ok(conn_opt)
    }
}

// Nextgen topology fetcher
pub struct NextgenTopologyFetcher {
    tidb_group: Option<String>,
    label_k8s_instance: Option<String>,
    kube_client: kube::Client,
}

impl NextgenTopologyFetcher {
    pub async fn new(
        tidb_group: Option<String>,
        label_k8s_instance: Option<String>,
    ) -> Result<Self, FetchError> {
        let kube_client = Self::build_kube_client().await?;

        Ok(Self {
            tidb_group,
            label_k8s_instance,
            kube_client,
        })
    }

    pub async fn get_up_components(
        &mut self,
        components: &mut HashSet<Component>,
    ) -> Result<(), FetchError> {
        if let Some(tidb_group) = &self.tidb_group {
            tidb_nextgen::TiDBNextGenTopologyFetcher::new(
                self.kube_client.clone(),
                tidb_group.clone(),
            )
            .get_up_tidbs(components)
            .await
            .context(FetchTiDBNextGenTopologySnafu)?;
        }
        if let Some(label_k8s_instance) = &self.label_k8s_instance {
            tikv_nextgen::TiKVNextGenTopologyFetcher::new(
                self.kube_client.clone(),
                label_k8s_instance.clone(),
            )
            .get_up_tikvs(components)
            .await
            .context(FetchTiKVNextGenTopologySnafu)?;
        }
        Ok(())
    }

    async fn build_kube_client() -> Result<kube::Client, FetchError> {
        let client = kube::Client::try_default()
            .await
            .context(BuildKubeClientSnafu)?;
        Ok(client)
    }
}

// Unified topology fetcher that abstracts over both implementations
pub struct TopologyFetcher {
    inner: TopologyFetcherImpl,
}

// Internal enum to handle different implementations
enum TopologyFetcherImpl {
    Legacy(Box<LegacyTopologyFetcher>),
    Nextgen(NextgenTopologyFetcher),
}

impl TopologyFetcher {
    /// Create a new topology fetcher based on the current feature configuration
    pub async fn new(
        pd_address: Option<String>,
        manager_server_address: Option<String>,
        tidb_namespace: Option<String>,
        tls_config: Option<TlsConfig>,
        proxy_config: &ProxyConfig,
        tidb_group: Option<String>,
        label_k8s_instance: Option<String>,
    ) -> Result<Self, FetchError> {
        // Use runtime mode to determine which implementation to use
        use crate::common::features::is_nextgen_mode;

        if is_nextgen_mode() {
            let fetcher = NextgenTopologyFetcher::new(tidb_group, label_k8s_instance).await?;
            Ok(Self {
                inner: TopologyFetcherImpl::Nextgen(fetcher),
            })
        } else {
            // In legacy mode, pd_address is required
            let pd_address = pd_address.ok_or_else(|| FetchError::ConfigurationError {
                message: "PD address is required in legacy mode".to_string(),
            })?;
            let fetcher = LegacyTopologyFetcher::new(
                pd_address,
                manager_server_address,
                tidb_namespace,
                tls_config,
                proxy_config,
            )
            .await?;
            Ok(Self {
                inner: TopologyFetcherImpl::Legacy(Box::new(fetcher)),
            })
        }
    }

    /// Fetch topology components and populate the provided set
    pub async fn get_up_components(
        &mut self,
        components: &mut HashSet<Component>,
    ) -> Result<(), FetchError> {
        match &mut self.inner {
            TopologyFetcherImpl::Legacy(fetcher) => fetcher.get_up_components(components).await,
            TopologyFetcherImpl::Nextgen(fetcher) => fetcher.get_up_components(components).await,
        }
    }

    /// Get the etcd client (only available in legacy mode)
    pub fn etcd_client(&self) -> Option<&etcd_client::Client> {
        match &self.inner {
            TopologyFetcherImpl::Legacy(fetcher) => Some(&fetcher.etcd_client),
            TopologyFetcherImpl::Nextgen(_) => None,
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use vector::tls::TlsConfig;

//     use super::*;

//     #[tokio::test]
//     async fn t() {
//         let tls_config = Some(TlsConfig {
//             ca_file: Some("/home/zhongzc/.tiup/storage/cluster/clusters/tmp/tls/ca.crt".into()),
//             crt_file: Some(
//                 "/home/zhongzc/.tiup/storage/cluster/clusters/tmp/tls/client.crt".into(),
//             ),
//             key_file: Some(
//                 "/home/zhongzc/.tiup/storage/cluster/clusters/tmp/tls/client.pem".into(),
//             ),
//             ..Default::default()
//         });

//         let proxy_config = ProxyConfig::from_env();

//         let mut topo = TopologyFetcher::new(
//             "127.0.0.1:2379".to_string(),
//             tls_config,
//             &proxy_config,
//         )
//         .await
//         .unwrap();

//         let mut components = HashSet::new();
//         topo.get_up_components(&mut components).await.unwrap();
//         println!("{:?}", components);
//     }
// }
