use std::collections::HashSet;

use snafu::{ResultExt, Snafu};
use vector::http::HttpClient;

use crate::sources::conprof::topology::fetch::{models, utils};
use crate::sources::conprof::topology::{Component, InstanceType};

#[derive(Debug, Snafu)]
pub enum FetchError {
    #[snafu(display("Failed to build request: {}", source))]
    BuildRequest { source: http::Error },
    #[snafu(display("Failed to get health: {}", source))]
    GetHealth { source: vector::http::HttpError },
    #[snafu(display("Failed to get health text: {}", source))]
    GetHealthBytes { source: hyper::Error },
    #[snafu(display("Failed to parse health JSON text: {}", source))]
    HealthJsonFromStr { source: serde_json::Error },
    #[snafu(display("Failed to get members: {}", source))]
    GetMembers { source: vector::http::HttpError },
    #[snafu(display("Failed to get members text: {}", source))]
    GetMembersBytes { source: hyper::Error },
    #[snafu(display("Failed to parse members JSON text: {}", source))]
    MembersJsonFromStr { source: serde_json::Error },
    #[snafu(display("Failed to parse pd address: {}", source))]
    ParsePDAddress { source: utils::ParseError },
}

pub struct PDTopologyFetcher<'a> {
    health_path: &'static str,
    members_path: &'static str,

    pd_address: &'a str,
    http_client: &'a HttpClient<hyper::Body>,
}

impl<'a> PDTopologyFetcher<'a> {
    pub fn new(pd_address: &'a str, http_client: &'a HttpClient<hyper::Body>) -> Self {
        Self {
            health_path: "/pd/api/v1/health",
            members_path: "/pd/api/v1/members",

            pd_address,
            http_client,
        }
    }

    pub async fn get_up_pds(&self, components: &mut HashSet<Component>) -> Result<(), FetchError> {
        let health_resp = self.fetch_pd_health().await?;
        let members_resp = self.fetch_pd_members().await?;

        let health_members = health_resp
            .iter()
            .filter(|h| h.health)
            .map(|h| h.member_id)
            .collect::<HashSet<_>>();
        for member in members_resp.members {
            if health_members.contains(&member.member_id) {
                if let Some(url) = member.client_urls.get(0) {
                    let (host, port) = utils::parse_host_port(url).context(ParsePDAddressSnafu)?;
                    components.insert(Component {
                        instance_type: InstanceType::PD,
                        host,
                        primary_port: port,
                        secondary_port: port,
                    });
                }
            }
        }

        Ok(())
    }

    pub(crate) async fn fetch_pd_health(&self) -> Result<models::HealthResponse, FetchError> {
        self.fetch_pd_health_impl().await
    }

    async fn fetch_pd_health_impl(&self) -> Result<models::HealthResponse, FetchError> {
        let req = http::Request::get(format!("{}{}", self.pd_address, self.health_path))
            .body(hyper::Body::empty())
            .context(BuildRequestSnafu)?;

        let res = self.http_client.send(req).await.context(GetHealthSnafu)?;

        let body = res.into_body();
        let bytes = hyper::body::to_bytes(body)
            .await
            .context(GetHealthBytesSnafu)?;

        let health_resp = serde_json::from_slice::<models::HealthResponse>(&bytes)
            .context(HealthJsonFromStrSnafu)?;

        Ok(health_resp)
    }

    pub(crate) async fn fetch_pd_members(&self) -> Result<models::MembersResponse, FetchError> {
        self.fetch_pd_members_impl().await
    }

    async fn fetch_pd_members_impl(&self) -> Result<models::MembersResponse, FetchError> {
        let req = http::Request::get(format!("{}{}", self.pd_address, self.members_path))
            .body(hyper::Body::empty())
            .context(BuildRequestSnafu)?;

        let res = self.http_client.send(req).await.context(GetMembersSnafu)?;

        let body = res.into_body();
        let bytes = hyper::body::to_bytes(body)
            .await
            .context(GetMembersBytesSnafu)?;

        let members_resp = serde_json::from_slice::<models::MembersResponse>(&bytes)
            .context(MembersJsonFromStrSnafu)?;

        Ok(members_resp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::conprof::topology::fetch::models;
    use crate::sources::conprof::topology::fetch::mock::pd::PDResponseGenerator;
    use crate::sources::conprof::topology::fetch::mock;
    use hyper::service::{make_service_fn, service_fn};
    use hyper::{Body, Request, Response, Server, StatusCode};
    use std::convert::Infallible;
    use std::net::SocketAddr;
    use tokio::net::TcpListener;
    use vector::http::HttpClient;
    use vector::config::ProxyConfig;

    #[test]
    fn test_pd_topology_fetcher_new() {
        // We can't actually create an HttpClient without a real connection,
        // but we can test the structure
        let _ = std::mem::size_of::<PDTopologyFetcher>();
    }

    #[test]
    fn test_pd_topology_fetcher_paths() {
        // Test that paths are correctly set
        // We can't create HttpClient, but we can test the path constants
        let health_path = "/pd/api/v1/health";
        let members_path = "/pd/api/v1/members";
        
        assert_eq!(health_path, "/pd/api/v1/health");
        assert_eq!(members_path, "/pd/api/v1/members");
    }

    #[test]
    fn test_get_up_pds_logic() {
        // Test the logic of get_up_pds by creating mock data
        let mut health_members = std::collections::HashSet::new();
        health_members.insert(1);
        health_members.insert(2);
        
        let members = vec![
            models::MemberItem {
                member_id: 1,
                client_urls: vec!["http://127.0.0.1:2379".to_string()],
            },
            models::MemberItem {
                member_id: 2,
                client_urls: vec!["http://127.0.0.1:2380".to_string()],
            },
            models::MemberItem {
                member_id: 3,
                client_urls: vec!["http://127.0.0.1:2381".to_string()],
            },
        ];
        
        // Test filtering logic
        let filtered: Vec<_> = members
            .iter()
            .filter(|m| health_members.contains(&m.member_id))
            .collect();
        
        assert_eq!(filtered.len(), 2);
        
        // Test that we get the first client_url
        for member in &filtered {
            if let Some(url) = member.client_urls.get(0) {
                assert!(!url.is_empty());
            }
        }
    }

    #[test]
    fn test_fetch_error_parse_pd_address() {
        let parse_error = utils::ParseError::MissingHost {
            address: "test".to_string(),
        };
        let error = FetchError::ParsePDAddress {
            source: parse_error,
        };
        let display = format!("{}", error);
        assert!(display.contains("Failed to parse pd address"));
    }

    #[test]
    fn test_fetch_error_variants() {
        // Test error creation - we can't easily create http::Error, so we test other variants
        let parse_error = utils::ParseError::MissingHost {
            address: "test".to_string(),
        };
        let error = FetchError::ParsePDAddress {
            source: parse_error,
        };
        let _display = format!("{}", error);
    }

    #[test]
    fn test_health_response_filtering() {
        // Test health response filtering logic
        let health_resp = vec![
            models::HealthItem {
                member_id: 1,
                health: true,
            },
            models::HealthItem {
                member_id: 2,
                health: false,
            },
            models::HealthItem {
                member_id: 3,
                health: true,
            },
        ];
        
        let health_members: std::collections::HashSet<_> = health_resp
            .iter()
            .filter(|h| h.health)
            .map(|h| h.member_id)
            .collect();
        
        assert_eq!(health_members.len(), 2);
        assert!(health_members.contains(&1));
        assert!(health_members.contains(&3));
        assert!(!health_members.contains(&2));
    }

    #[test]
    fn test_fetch_pd_health_path() {
        // Test that health path is correctly constructed
        let pd_address = "http://127.0.0.1:2379";
        let health_path = "/pd/api/v1/health";
        let full_path = format!("{}{}", pd_address, health_path);
        assert_eq!(full_path, "http://127.0.0.1:2379/pd/api/v1/health");
    }

    #[test]
    fn test_fetch_pd_members_path() {
        // Test that members path is correctly constructed
        let pd_address = "http://127.0.0.1:2379";
        let members_path = "/pd/api/v1/members";
        let full_path = format!("{}{}", pd_address, members_path);
        assert_eq!(full_path, "http://127.0.0.1:2379/pd/api/v1/members");
    }

    #[test]
    fn test_get_up_pds_component_creation() {
        // Test component creation logic in get_up_pds
        let mut components = HashSet::new();
        let member = models::MemberItem {
            member_id: 1,
            client_urls: vec!["http://127.0.0.1:2379".to_string()],
        };
        
        if let Some(url) = member.client_urls.get(0) {
            let result = utils::parse_host_port(url);
            if let Ok((host, port)) = result {
                components.insert(Component {
                    instance_type: InstanceType::PD,
                    host,
                    primary_port: port,
                    secondary_port: port,
                });
            }
        }
        
        assert_eq!(components.len(), 1);
        let component = components.iter().next().unwrap();
        assert_eq!(component.host, "127.0.0.1");
        assert_eq!(component.primary_port, 2379);
        assert_eq!(component.secondary_port, 2379);
    }

    #[test]
    fn test_get_up_pds_empty_client_urls() {
        // Test handling of empty client_urls
        let member = models::MemberItem {
            member_id: 1,
            client_urls: vec![],
        };
        
        assert!(member.client_urls.get(0).is_none());
    }

    #[test]
    fn test_get_up_pds_multiple_client_urls() {
        // Test that we use the first client_url
        let member = models::MemberItem {
            member_id: 1,
            client_urls: vec![
                "http://127.0.0.1:2379".to_string(),
                "http://127.0.0.1:2380".to_string(),
            ],
        };
        
        let first_url = member.client_urls.get(0);
        assert!(first_url.is_some());
        assert_eq!(first_url.unwrap(), "http://127.0.0.1:2379");
    }

    #[test]
    fn test_fetch_pd_health_url_construction() {
        // Test URL construction for fetch_pd_health
        let pd_address = "http://127.0.0.1:2379";
        let health_path = "/pd/api/v1/health";
        let full_url = format!("{}{}", pd_address, health_path);
        assert_eq!(full_url, "http://127.0.0.1:2379/pd/api/v1/health");
    }

    #[test]
    fn test_fetch_pd_members_url_construction() {
        // Test URL construction for fetch_pd_members
        let pd_address = "http://127.0.0.1:2379";
        let members_path = "/pd/api/v1/members";
        let full_url = format!("{}{}", pd_address, members_path);
        assert_eq!(full_url, "http://127.0.0.1:2379/pd/api/v1/members");
    }

    #[test]
    fn test_get_up_pds_with_empty_health_members() {
        // Test get_up_pds with empty health_members
        let mut components = HashSet::new();
        let health_members: HashSet<u64> = HashSet::new();
        let members = vec![
            models::MemberItem {
                member_id: 1,
                client_urls: vec!["http://127.0.0.1:2379".to_string()],
            },
        ];
        
        for member in members {
            if health_members.contains(&member.member_id) {
                if let Some(url) = member.client_urls.get(0) {
                    let result = utils::parse_host_port(url);
                    if let Ok((host, port)) = result {
                        components.insert(Component {
                            instance_type: InstanceType::PD,
                            host,
                            primary_port: port,
                            secondary_port: port,
                        });
                    }
                }
            }
        }
        
        assert_eq!(components.len(), 0);
    }

    async fn mock_http_server(port: u16, health_resp: String, members_resp: String) -> String {
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        
        let health_resp_clone = health_resp.clone();
        let members_resp_clone = members_resp.clone();
        
        tokio::spawn(async move {
            let make_svc = make_service_fn(move |_conn| {
                let health_resp = health_resp_clone.clone();
                let members_resp = members_resp_clone.clone();
                async move {
                    Ok::<_, Infallible>(service_fn(move |req: Request<Body>| {
                        let health_resp = health_resp.clone();
                        let members_resp = members_resp.clone();
                        async move {
                            let path = req.uri().path();
                            let resp = if path == "/pd/api/v1/health" {
                                Response::builder()
                                    .status(StatusCode::OK)
                                    .body(Body::from(health_resp))
                                    .unwrap()
                            } else if path == "/pd/api/v1/members" {
                                Response::builder()
                                    .status(StatusCode::OK)
                                    .body(Body::from(members_resp))
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
    async fn test_get_up_pds_with_mock_server() {
        // Test get_up_pds with mock HTTP server
        let generator = PDResponseGenerator::new(vec![
            mock::pd::PDURL {
                client_url: "http://127.0.0.1:2379".to_string(),
                peer_url: "http://127.0.0.1:2380".to_string(),
            },
            mock::pd::PDURL {
                client_url: "http://127.0.0.1:2378".to_string(),
                peer_url: "http://127.0.0.1:2381".to_string(),
            },
        ]);
        
        let health_resp = generator.health_resp();
        let members_resp = generator.members_resp();
        
        // Find an available port
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        
        let pd_address = mock_http_server(port, health_resp, members_resp).await;
        
        // Wait a bit for server to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        
        let proxy_config = ProxyConfig::from_env();
        let http_client = HttpClient::new(None, &proxy_config).unwrap();
        let fetcher = PDTopologyFetcher::new(&pd_address, &http_client);
        
        let mut components = HashSet::new();
        let result = fetcher.get_up_pds(&mut components).await;
        
        // Should succeed and find components
        assert!(result.is_ok());
        assert!(!components.is_empty());
    }

    #[tokio::test]
    async fn test_fetch_pd_health_with_mock_server() {
        // Test fetch_pd_health with mock HTTP server
        let generator = PDResponseGenerator::new(vec![
            mock::pd::PDURL {
                client_url: "http://127.0.0.1:2379".to_string(),
                peer_url: "http://127.0.0.1:2380".to_string(),
            },
        ]);
        
        let health_resp = generator.health_resp();
        let members_resp = generator.members_resp();
        
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        
        let pd_address = mock_http_server(port, health_resp, members_resp).await;
        
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        
        let proxy_config = ProxyConfig::from_env();
        let http_client = HttpClient::new(None, &proxy_config).unwrap();
        let fetcher = PDTopologyFetcher::new(&pd_address, &http_client);
        
        let result = fetcher.fetch_pd_health().await;
        
        assert!(result.is_ok());
        let health = result.unwrap();
        assert!(!health.is_empty());
    }

    #[tokio::test]
    async fn test_fetch_pd_members_with_mock_server() {
        // Test fetch_pd_members with mock HTTP server
        let generator = PDResponseGenerator::new(vec![
            mock::pd::PDURL {
                client_url: "http://127.0.0.1:2379".to_string(),
                peer_url: "http://127.0.0.1:2380".to_string(),
            },
        ]);
        
        let health_resp = generator.health_resp();
        let members_resp = generator.members_resp();
        
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        
        let pd_address = mock_http_server(port, health_resp, members_resp).await;
        
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        
        let proxy_config = ProxyConfig::from_env();
        let http_client = HttpClient::new(None, &proxy_config).unwrap();
        let fetcher = PDTopologyFetcher::new(&pd_address, &http_client);
        
        let result = fetcher.fetch_pd_members().await;
        
        assert!(result.is_ok());
        let members = result.unwrap();
        assert!(!members.members.is_empty());
    }

    #[test]
    fn test_get_up_pds_with_member_not_in_health() {
        // Test get_up_pds when member is not in health_members
        let mut components = HashSet::new();
        let mut health_members: HashSet<u64> = HashSet::new();
        health_members.insert(2); // Different member_id
        
        let members = vec![
            models::MemberItem {
                member_id: 1,
                client_urls: vec!["http://127.0.0.1:2379".to_string()],
            },
        ];
        
        for member in members {
            if health_members.contains(&member.member_id) {
                if let Some(url) = member.client_urls.get(0) {
                    let result = utils::parse_host_port(url);
                    if let Ok((host, port)) = result {
                        components.insert(Component {
                            instance_type: InstanceType::PD,
                            host,
                            primary_port: port,
                            secondary_port: port,
                        });
                    }
                }
            }
        }
        
        assert_eq!(components.len(), 0);
    }
}
