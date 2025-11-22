mod lightning;
mod models;
mod pd;
mod store;
mod tidb;
mod tiproxy;
mod utils;

use crate::common::features::is_nextgen_mode;
use crate::common::topology::fetch::tidb_nextgen::{TiDBNextGenTopologyFetcher, FetchError as TiDBNextGenFetchError};
use crate::sources::conprof::topology::Component;

#[cfg(test)]
mod mock;

use std::collections::HashSet;
use std::fs::read;

use snafu::{ResultExt, Snafu};
use vector::config::ProxyConfig;
use vector::http::HttpClient;
use vector::tls::{MaybeTlsSettings, TlsConfig};

#[derive(Debug, Snafu)]
pub enum FetchError {
    #[snafu(display("Failed to build TLS settings: {}", source))]
    BuildTlsSettings { source: vector::tls::TlsError },
    #[snafu(display("Failed to build kubernetes client: {}", source))]
    BuildKubeClient { source: kube::Error },
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
    #[snafu(display("Configuration error: {}", message))]
    ConfigurationError { message: String },
    #[snafu(display("Failed to fetch pd topology: {}", source))]
    FetchPDTopology { source: pd::FetchError },
    #[snafu(display("Failed to fetch tidb topology: {}", source))]
    FetchTiDBTopology { source: tidb::FetchError },
    #[snafu(display("Failed to fetch tidb nextgen topology: {}", source))]
    FetchTiDBNextGenTopology { source: TiDBNextGenFetchError },
    #[snafu(display("Failed to fetch store topology: {}", source))]
    FetchStoreTopology { source: store::FetchError },
    #[snafu(display("Failed to fetch tiproxy topology: {}", source))]
    FetchTiProxyTopology { source: tiproxy::FetchError },
    #[snafu(display("Failed to fetch lightning topology: {}", source))]
    FetchLightningTopology { source: lightning::FetchError },
}

pub struct TopologyFetcher {
    pd_address: String,
    http_client: HttpClient<hyper::Body>,
    etcd_client: etcd_client::Client,
    kube_client: kube::Client,
}

impl TopologyFetcher {
    pub async fn new(
        pd_address: String,
        tls_config: Option<TlsConfig>,
        proxy_config: &ProxyConfig,
    ) -> Result<Self, FetchError> {
        let pd_address = Self::polish_address(pd_address, &tls_config)?;
        let http_client = Self::build_http_client(tls_config.as_ref(), proxy_config)?;
        let etcd_client = Self::build_etcd_client(&pd_address, &tls_config).await?;
        let kube_client = Self::build_kube_client().await?;

        Ok(Self {
            pd_address,
            http_client,
            etcd_client,
            kube_client,
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
        if is_nextgen_mode() {
            // Use nextgen topology fetcher
            // Note: For conprof, we need to determine how to get tidb_group
            // This might need to be passed in or configured differently
            // For now, using a placeholder approach
            let tidb_group = std::env::var("TIDB_GROUP").unwrap_or_default();
            
            // Create temporary HashSet for common::topology::Component
            let mut temp_components = std::collections::HashSet::new();
            
            TiDBNextGenTopologyFetcher::new(
                self.kube_client.clone(),
                tidb_group,
            )
            .get_up_tidbs(&mut temp_components)
            .await
            .context(FetchTiDBNextGenTopologySnafu)?;
            
            // Convert common::topology::Component to conprof::topology::Component
            for common_comp in temp_components {
                let instance_type = match common_comp.instance_type {
                    crate::common::topology::InstanceType::PD => crate::sources::conprof::topology::InstanceType::PD,
                    crate::common::topology::InstanceType::TiDB => crate::sources::conprof::topology::InstanceType::TiDB,
                    crate::common::topology::InstanceType::TiKV => crate::sources::conprof::topology::InstanceType::TiKV,
                    crate::common::topology::InstanceType::TiFlash => crate::sources::conprof::topology::InstanceType::TiFlash,
                };
                
                let conprof_comp = crate::sources::conprof::topology::Component {
                    instance_type,
                    host: common_comp.host,
                    primary_port: common_comp.primary_port,
                    secondary_port: common_comp.secondary_port,
                };
                
                components.insert(conprof_comp);
            }
        } else {
            // Use legacy topology fetcher
            tidb::TiDBTopologyFetcher::new(&mut self.etcd_client)
                .get_up_tidbs(components)
                .await
                .context(FetchTiDBTopologySnafu)?;
        }
        store::StoreTopologyFetcher::new(&self.pd_address, &self.http_client)
            .get_up_stores(components)
            .await
            .context(FetchStoreTopologySnafu)?;
        tiproxy::TiProxyTopologyFetcher::new(&mut self.etcd_client)
            .get_up_tiproxys(components)
            .await
            .context(FetchTiProxyTopologySnafu)?;
        lightning::KubeLightningTopologyFetcher::new(self.kube_client.clone())
            .get_up_lightnings(components)
            .await
            .context(FetchLightningTopologySnafu)?;
        Ok(())
    }

    fn polish_address(
        mut address: String,
        tls_config: &Option<TlsConfig>,
    ) -> Result<String, FetchError> {
        let uri: hyper::Uri = address.parse().context(ParseAddressSnafu)?;
        if uri.scheme().is_none() {
            if tls_config.is_some() {
                address = format!("https://{}", address);
            } else {
                address = format!("http://{}", address);
            }
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

    async fn build_kube_client() -> Result<kube::Client, FetchError> {
        kube::Client::try_default()
            .await
            .context(BuildKubeClientSnafu)
    }

    async fn build_etcd_client(
        pd_address: &str,
        tls_config: &Option<TlsConfig>,
    ) -> Result<etcd_client::Client, FetchError> {
        let etcd_connect_opt = Self::build_etcd_connect_opt(tls_config)?;
        let etcd_client = etcd_client::Client::connect(&[pd_address], etcd_connect_opt)
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
//         let mut fetcher =
//             TopologyFetcher::new("localhost:2379".to_owned(), tls_config, &proxy_config)
//                 .await
//                 .unwrap();
//         let mut components = HashSet::new();
//         fetcher.get_up_components(&mut components).await.unwrap();
//         // println!("{:#?}", components);
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_fetch_error_display() {
        let error = FetchError::ConfigurationError {
            message: "test error".to_string(),
        };
        assert_eq!(format!("{}", error), "Configuration error: test error");
    }
}