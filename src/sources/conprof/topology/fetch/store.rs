use std::collections::HashSet;

use snafu::{ResultExt, Snafu};
use vector::http::HttpClient;

use crate::sources::conprof::topology::fetch::{models, utils};
use crate::sources::conprof::topology::{Component, InstanceType};

#[derive(Debug, Snafu)]
pub enum FetchError {
    #[snafu(display("Failed to build request: {}", source))]
    BuildRequest { source: http::Error },
    #[snafu(display("Failed to get stores: {}", source))]
    GetStores { source: vector::http::HttpError },
    #[snafu(display("Failed to get stores text: {}", source))]
    GetStoresBytes { source: hyper::Error },
    #[snafu(display("Failed to parse stores JSON text: {}", source))]
    StoresJsonFromStr { source: serde_json::Error },
    #[snafu(display("Failed to parse store address: {}", source))]
    ParseStoreAddress { source: utils::ParseError },
}

pub struct StoreTopologyFetcher<'a> {
    stores_path: &'static str,

    pd_address: &'a str,
    http_client: &'a HttpClient<hyper::Body>,
}

impl<'a> StoreTopologyFetcher<'a> {
    pub fn new(pd_address: &'a str, http_client: &'a HttpClient<hyper::Body>) -> Self {
        Self {
            stores_path: "/pd/api/v1/stores",
            pd_address,
            http_client,
        }
    }

    pub async fn get_up_stores(
        &mut self,
        components: &mut HashSet<Component>,
    ) -> Result<(), FetchError> {
        let stores_resp = self.fetch_stores().await?;

        for models::StoreItem { store } in stores_resp.stores {
            if !Self::is_up(&store) {
                continue;
            }

            let (host, primary_port) =
                utils::parse_host_port(&store.address).context(ParseStoreAddressSnafu)?;
            let (_, secondary_port) =
                utils::parse_host_port(&store.status_address).context(ParseStoreAddressSnafu)?;
            let instance_type = Self::parse_instance_type(&store);

            components.insert(Component {
                instance_type,
                host,
                primary_port,
                secondary_port,
            });
        }

        Ok(())
    }

    async fn fetch_stores(&mut self) -> Result<models::StoresResponse, FetchError> {
        let req = http::Request::get(format!("{}{}", self.pd_address, self.stores_path))
            .body(hyper::Body::empty())
            .context(BuildRequestSnafu)?;

        let res = self.http_client.send(req).await.context(GetStoresSnafu)?;

        let body = res.into_body();
        let bytes = hyper::body::to_bytes(body)
            .await
            .context(GetStoresBytesSnafu)?;

        let stores_resp = serde_json::from_slice::<models::StoresResponse>(&bytes)
            .context(StoresJsonFromStrSnafu)?;

        Ok(stores_resp)
    }

    fn is_up(store: &models::StoreInfo) -> bool {
        store.state_name.to_lowercase().as_str() == "up"
    }

    fn parse_instance_type(store: &models::StoreInfo) -> InstanceType {
        if store
            .labels
            .iter()
            .any(|models::LabelItem { key, value }| key == "engine" && value.to_lowercase().contains("tiflash"))
        {
            InstanceType::TiFlash
        } else {
            InstanceType::TiKV
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_up() {
        let store = models::StoreInfo {
            address: "127.0.0.1:20160".to_string(),
            status_address: "127.0.0.1:20180".to_string(),
            state_name: "Up".to_string(),
            labels: vec![],
        };
        assert!(StoreTopologyFetcher::is_up(&store));

        let store = models::StoreInfo {
            address: "127.0.0.1:20160".to_string(),
            status_address: "127.0.0.1:20180".to_string(),
            state_name: "up".to_string(),
            labels: vec![],
        };
        assert!(StoreTopologyFetcher::is_up(&store));

        let store = models::StoreInfo {
            address: "127.0.0.1:20160".to_string(),
            status_address: "127.0.0.1:20180".to_string(),
            state_name: "DOWN".to_string(),
            labels: vec![],
        };
        assert!(!StoreTopologyFetcher::is_up(&store));

        let store = models::StoreInfo {
            address: "127.0.0.1:20160".to_string(),
            status_address: "127.0.0.1:20180".to_string(),
            state_name: "Offline".to_string(),
            labels: vec![],
        };
        assert!(!StoreTopologyFetcher::is_up(&store));
    }

    #[test]
    fn test_parse_instance_type_tikv() {
        let store = models::StoreInfo {
            address: "127.0.0.1:20160".to_string(),
            status_address: "127.0.0.1:20180".to_string(),
            state_name: "Up".to_string(),
            labels: vec![],
        };
        assert_eq!(
            StoreTopologyFetcher::parse_instance_type(&store),
            InstanceType::TiKV
        );

        let store = models::StoreInfo {
            address: "127.0.0.1:20160".to_string(),
            status_address: "127.0.0.1:20180".to_string(),
            state_name: "Up".to_string(),
            labels: vec![models::LabelItem {
                key: "engine".to_string(),
                value: "tikv".to_string(),
            }],
        };
        assert_eq!(
            StoreTopologyFetcher::parse_instance_type(&store),
            InstanceType::TiKV
        );
    }

    #[test]
    fn test_parse_instance_type_tiflash() {
        let store = models::StoreInfo {
            address: "127.0.0.1:20160".to_string(),
            status_address: "127.0.0.1:20180".to_string(),
            state_name: "Up".to_string(),
            labels: vec![models::LabelItem {
                key: "engine".to_string(),
                value: "tiflash".to_string(),
            }],
        };
        assert_eq!(
            StoreTopologyFetcher::parse_instance_type(&store),
            InstanceType::TiFlash
        );

        let store = models::StoreInfo {
            address: "127.0.0.1:20160".to_string(),
            status_address: "127.0.0.1:20180".to_string(),
            state_name: "Up".to_string(),
            labels: vec![models::LabelItem {
                key: "engine".to_string(),
                value: "TiFlash".to_string(),
            }],
        };
        assert_eq!(
            StoreTopologyFetcher::parse_instance_type(&store),
            InstanceType::TiFlash
        );

        let store = models::StoreInfo {
            address: "127.0.0.1:20160".to_string(),
            status_address: "127.0.0.1:20180".to_string(),
            state_name: "Up".to_string(),
            labels: vec![models::LabelItem {
                key: "engine".to_string(),
                value: "TIFLASH".to_string(),
            }],
        };
        assert_eq!(
            StoreTopologyFetcher::parse_instance_type(&store),
            InstanceType::TiFlash
        );

        let store = models::StoreInfo {
            address: "127.0.0.1:20160".to_string(),
            status_address: "127.0.0.1:20180".to_string(),
            state_name: "Up".to_string(),
            labels: vec![models::LabelItem {
                key: "engine".to_string(),
                value: "tiflash-cluster".to_string(),
            }],
        };
        assert_eq!(
            StoreTopologyFetcher::parse_instance_type(&store),
            InstanceType::TiFlash
        );
    }
}
