mod models;
mod pd;
mod store;
mod tidb;
mod utils;

#[cfg(test)]
mod mock;

use std::{collections::HashSet, fs::read};

use snafu::{ResultExt, Snafu};

use vector::{
    config::ProxyConfig,
    http::HttpClient,
    tls::{MaybeTlsSettings, TlsConfig},
};

use crate::topology::Component;

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
    #[snafu(display("Failed to fetch pd topology: {}", source))]
    FetchPDTopology { source: pd::FetchError },
    #[snafu(display("Failed to fetch tidb topology: {}", source))]
    FetchTiDBTopology { source: tidb::FetchError },
    #[snafu(display("Failed to fetch store topology: {}", source))]
    FetchStoreTopology { source: store::FetchError },
}

pub struct TopologyFetcher {
    pd_address: String,
    http_client: HttpClient<hyper::Body>,
    etcd_client: etcd_client::Client,
}

impl TopologyFetcher {
    pub async fn new(
        pd_address: String,
        tls_config: Option<TlsConfig>,
        proxy_config: &ProxyConfig,
    ) -> Result<Self, FetchError> {
        let pd_address = Self::polish_address(pd_address, &tls_config)?;
        let http_client = Self::build_http_client(&tls_config, proxy_config)?;
        let etcd_client = Self::build_etcd_client(&pd_address, &tls_config).await?;

        Ok(Self {
            pd_address,
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
        tidb::TiDBTopologyFetcher::new(&mut self.etcd_client)
            .get_up_tidbs(components)
            .await
            .context(FetchTiDBTopologySnafu)?;
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
            if tls_config.is_some() {
                address = format!("https://{}", address);
            } else {
                address = format!("http://{}", address);
            }
        }

        if address.chars().last() == Some('/') {
            address.pop();
        }

        Ok(address)
    }

    fn build_http_client(
        tls_config: &Option<TlsConfig>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use vector::tls::TlsConfig;

    #[tokio::test]
    async fn t() {
        let tls_config = Some(TlsConfig {
            ca_file: Some("/home/zhongzc/.tiup/storage/cluster/clusters/tmp/tls/ca.crt".into()),
            crt_file: Some(
                "/home/zhongzc/.tiup/storage/cluster/clusters/tmp/tls/client.crt".into(),
            ),
            key_file: Some(
                "/home/zhongzc/.tiup/storage/cluster/clusters/tmp/tls/client.pem".into(),
            ),
            ..Default::default()
        });

        let proxy_config = ProxyConfig::from_env();
        let mut fetcher =
            TopologyFetcher::new("localhost:2379".to_owned(), tls_config, &proxy_config)
                .await
                .unwrap();
        let mut components = HashSet::new();
        fetcher.get_up_components(&mut components).await.unwrap();
        println!("{:#?}", components);
    }
}
