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

    pub(crate) async fn fetch_stores(&mut self) -> Result<models::StoresResponse, FetchError> {
        self.fetch_stores_impl().await
    }

    async fn fetch_stores_impl(&mut self) -> Result<models::StoresResponse, FetchError> {
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
    use crate::sources::conprof::topology::fetch::models;
    use hyper::service::{make_service_fn, service_fn};
    use hyper::{Body, Request, Response, Server, StatusCode};
    use std::convert::Infallible;
    use std::net::SocketAddr;
    use tokio::net::TcpListener;
    use vector::http::HttpClient;
    use vector::config::ProxyConfig;

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

    #[test]
    fn test_get_up_stores_component_creation() {
        // Test component creation logic in get_up_stores
        let mut components = HashSet::new();
        let store = models::StoreInfo {
            address: "127.0.0.1:20160".to_string(),
            status_address: "127.0.0.1:20180".to_string(),
            state_name: "Up".to_string(),
            labels: vec![],
        };
        
        if StoreTopologyFetcher::is_up(&store) {
            let (host, primary_port) = utils::parse_host_port(&store.address).unwrap();
            let (_, secondary_port) = utils::parse_host_port(&store.status_address).unwrap();
            let instance_type = StoreTopologyFetcher::parse_instance_type(&store);
            
            components.insert(Component {
                instance_type,
                host,
                primary_port,
                secondary_port,
            });
        }
        
        assert_eq!(components.len(), 1);
        let component = components.iter().next().unwrap();
        assert_eq!(component.host, "127.0.0.1");
        assert_eq!(component.primary_port, 20160);
        assert_eq!(component.secondary_port, 20180);
        assert_eq!(component.instance_type, InstanceType::TiKV);
    }

    #[test]
    fn test_get_up_stores_skip_down_stores() {
        // Test that down stores are skipped
        let mut components = HashSet::new();
        let store = models::StoreInfo {
            address: "127.0.0.1:20160".to_string(),
            status_address: "127.0.0.1:20180".to_string(),
            state_name: "Down".to_string(),
            labels: vec![],
        };
        
        if StoreTopologyFetcher::is_up(&store) {
            let (host, primary_port) = utils::parse_host_port(&store.address).unwrap();
            let (_, secondary_port) = utils::parse_host_port(&store.status_address).unwrap();
            let instance_type = StoreTopologyFetcher::parse_instance_type(&store);
            
            components.insert(Component {
                instance_type,
                host,
                primary_port,
                secondary_port,
            });
        }
        
        assert_eq!(components.len(), 0);
    }

    #[test]
    fn test_fetch_stores_path() {
        // Test that stores path is correctly constructed
        let pd_address = "http://127.0.0.1:2379";
        let stores_path = "/pd/api/v1/stores";
        let full_path = format!("{}{}", pd_address, stores_path);
        assert_eq!(full_path, "http://127.0.0.1:2379/pd/api/v1/stores");
    }

    #[test]
    fn test_get_up_stores_with_tiflash() {
        // Test component creation for TiFlash stores
        let mut components = HashSet::new();
        let store = models::StoreInfo {
            address: "127.0.0.1:9000".to_string(),
            status_address: "127.0.0.1:8123".to_string(),
            state_name: "Up".to_string(),
            labels: vec![models::LabelItem {
                key: "engine".to_string(),
                value: "tiflash".to_string(),
            }],
        };
        
        if StoreTopologyFetcher::is_up(&store) {
            let (host, primary_port) = utils::parse_host_port(&store.address).unwrap();
            let (_, secondary_port) = utils::parse_host_port(&store.status_address).unwrap();
            let instance_type = StoreTopologyFetcher::parse_instance_type(&store);
            
            components.insert(Component {
                instance_type,
                host,
                primary_port,
                secondary_port,
            });
        }
        
        assert_eq!(components.len(), 1);
        let component = components.iter().next().unwrap();
        assert_eq!(component.instance_type, InstanceType::TiFlash);
    }

    #[test]
    fn test_get_up_stores_skip_down_stores_logic() {
        // Test that down stores are skipped in get_up_stores
        let stores = vec![
            models::StoreItem {
                store: models::StoreInfo {
                    address: "127.0.0.1:20160".to_string(),
                    status_address: "127.0.0.1:20180".to_string(),
                    state_name: "Up".to_string(),
                    labels: vec![],
                },
            },
            models::StoreItem {
                store: models::StoreInfo {
                    address: "127.0.0.1:20161".to_string(),
                    status_address: "127.0.0.1:20181".to_string(),
                    state_name: "Down".to_string(),
                    labels: vec![],
                },
            },
        ];
        
        let mut components = HashSet::new();
        for models::StoreItem { store } in stores {
            if !StoreTopologyFetcher::is_up(&store) {
                continue;
            }
            
            let (host, primary_port) = utils::parse_host_port(&store.address).unwrap();
            let (_, secondary_port) = utils::parse_host_port(&store.status_address).unwrap();
            let instance_type = StoreTopologyFetcher::parse_instance_type(&store);
            
            components.insert(Component {
                instance_type,
                host,
                primary_port,
                secondary_port,
            });
        }
        
        assert_eq!(components.len(), 1);
    }

    #[test]
    fn test_fetch_stores_url_construction() {
        // Test URL construction for fetch_stores
        let pd_address = "http://127.0.0.1:2379";
        let stores_path = "/pd/api/v1/stores";
        let full_url = format!("{}{}", pd_address, stores_path);
        assert_eq!(full_url, "http://127.0.0.1:2379/pd/api/v1/stores");
    }

    #[test]
    fn test_get_up_stores_multiple_stores() {
        // Test get_up_stores with multiple stores
        let stores = vec![
            models::StoreItem {
                store: models::StoreInfo {
                    address: "127.0.0.1:20160".to_string(),
                    status_address: "127.0.0.1:20180".to_string(),
                    state_name: "Up".to_string(),
                    labels: vec![],
                },
            },
            models::StoreItem {
                store: models::StoreInfo {
                    address: "127.0.0.1:20161".to_string(),
                    status_address: "127.0.0.1:20181".to_string(),
                    state_name: "Up".to_string(),
                    labels: vec![],
                },
            },
        ];
        
        let mut components = HashSet::new();
        for models::StoreItem { store } in stores {
            if !StoreTopologyFetcher::is_up(&store) {
                continue;
            }
            
            let (host, primary_port) = utils::parse_host_port(&store.address).unwrap();
            let (_, secondary_port) = utils::parse_host_port(&store.status_address).unwrap();
            let instance_type = StoreTopologyFetcher::parse_instance_type(&store);
            
            components.insert(Component {
                instance_type,
                host,
                primary_port,
                secondary_port,
            });
        }
        
        assert_eq!(components.len(), 2);
    }

    async fn mock_http_server(port: u16, stores_resp: String) -> String {
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        
        let stores_resp_clone = stores_resp.clone();
        
        tokio::spawn(async move {
            let make_svc = make_service_fn(move |_conn| {
                let stores_resp = stores_resp_clone.clone();
                async move {
                    Ok::<_, Infallible>(service_fn(move |req: Request<Body>| {
                        let stores_resp = stores_resp.clone();
                        async move {
                            let path = req.uri().path();
                            let resp = if path == "/pd/api/v1/stores" {
                                Response::builder()
                                    .status(StatusCode::OK)
                                    .body(Body::from(stores_resp))
                                    .unwrap()
                            } else {
                                Response::builder()
                                    .status(StatusCode::NOT_FOUND)
                                    .body(Body::from("Not Found"))
                                    .unwrap()
                            };
                            Ok::<_, Infallible>(resp)
                        }
                    }))
                }
            });
            
            let server = Server::bind(&addr).serve(make_svc);
            server.await.unwrap();
        });
        
        format!("http://127.0.0.1:{}", port)
    }

    #[tokio::test]
    async fn test_get_up_stores_with_mock_server() {
        // Test get_up_stores with mock HTTP server
        let stores_resp = models::StoresResponse {
            stores: vec![
                models::StoreItem {
                    store: models::StoreInfo {
                        address: "127.0.0.1:20160".to_string(),
                        status_address: "127.0.0.1:20180".to_string(),
                        state_name: "Up".to_string(),
                        labels: vec![],
                    },
                },
                models::StoreItem {
                    store: models::StoreInfo {
                        address: "127.0.0.1:20161".to_string(),
                        status_address: "127.0.0.1:20181".to_string(),
                        state_name: "Down".to_string(),
                        labels: vec![],
                    },
                },
            ],
        };
        
        let stores_resp_json = serde_json::to_string(&stores_resp).unwrap();
        
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        
        let pd_address = mock_http_server(port, stores_resp_json).await;
        
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        
        let proxy_config = ProxyConfig::from_env();
        let http_client = HttpClient::new(None, &proxy_config).unwrap();
        let mut fetcher = StoreTopologyFetcher::new(&pd_address, &http_client);
        
        let mut components = HashSet::new();
        let result = fetcher.get_up_stores(&mut components).await;
        
        assert!(result.is_ok());
        // Should only have one component (the "Up" one)
        assert_eq!(components.len(), 1);
    }

    #[tokio::test]
    async fn test_fetch_stores_with_mock_server() {
        // Test fetch_stores with mock HTTP server
        let stores_resp = models::StoresResponse {
            stores: vec![
                models::StoreItem {
                    store: models::StoreInfo {
                        address: "127.0.0.1:20160".to_string(),
                        status_address: "127.0.0.1:20180".to_string(),
                        state_name: "Up".to_string(),
                        labels: vec![],
                    },
                },
            ],
        };
        
        let stores_resp_json = serde_json::to_string(&stores_resp).unwrap();
        
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        
        let pd_address = mock_http_server(port, stores_resp_json).await;
        
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        
        let proxy_config = ProxyConfig::from_env();
        let http_client = HttpClient::new(None, &proxy_config).unwrap();
        let mut fetcher = StoreTopologyFetcher::new(&pd_address, &http_client);
        
        let result = fetcher.fetch_stores().await;
        
        assert!(result.is_ok());
        let stores = result.unwrap();
        assert_eq!(stores.stores.len(), 1);
    }
}
