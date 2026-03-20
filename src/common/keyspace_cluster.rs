use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::time::Duration;

use reqwest::{Certificate, Client, Identity, StatusCode};
use serde::Deserialize;
use tokio::sync::Mutex;
use url::form_urlencoded::byte_serialize;
use vector_lib::tls::TlsConfig;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

const ORG_ID_KEYS: &[&str] = &["tenant_id", "TenantID", "org_id", "organization_id"];
const CLUSTER_ID_KEYS: &[&str] = &["cluster_id", "ClusterId", "tidb_cluster_id"];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyspaceRoute {
    pub org_id: String,
    pub cluster_id: String,
}

#[derive(Clone)]
pub struct PdKeyspaceResolver {
    base_url: String,
    client: Client,
    cache: Arc<Mutex<HashMap<String, KeyspaceRoute>>>,
}

#[derive(Debug, Deserialize)]
struct PdKeyspaceMetadata {
    config: Option<HashMap<String, String>>,
}

impl PdKeyspaceResolver {
    pub fn new(pd_address: impl Into<String>, pd_tls: Option<TlsConfig>) -> Result<Self, BoxError> {
        let client = build_http_client(pd_tls.as_ref())?;
        Ok(Self::new_with_client(pd_address, pd_tls.as_ref(), client))
    }

    pub fn new_with_client(
        pd_address: impl Into<String>,
        pd_tls: Option<&TlsConfig>,
        client: Client,
    ) -> Self {
        Self {
            base_url: normalize_pd_address(&pd_address.into(), pd_tls.is_some()),
            client,
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn resolve_keyspace(
        &self,
        keyspace_name: &str,
    ) -> Result<Option<KeyspaceRoute>, BoxError> {
        if keyspace_name.is_empty() {
            return Ok(None);
        }

        if let Some(cached) = self.cache.lock().await.get(keyspace_name).cloned() {
            return Ok(Some(cached));
        }

        let encoded_keyspace = byte_serialize(keyspace_name.as_bytes()).collect::<String>();
        let response = self
            .client
            .get(format!(
                "{}/pd/api/v2/keyspaces/{}",
                self.base_url, encoded_keyspace
            ))
            .send()
            .await?;

        match response.status() {
            StatusCode::NOT_FOUND => return Ok(None),
            status if !status.is_success() => {
                let body = response.text().await.unwrap_or_default();
                if is_not_found_body(&body) {
                    return Ok(None);
                }
                return Err(format!(
                    "pd keyspace lookup failed for {} with status {}: {}",
                    keyspace_name, status, body
                )
                .into());
            }
            _ => {}
        }

        let metadata: PdKeyspaceMetadata = response.json().await?;
        let route = metadata.config.as_ref().and_then(extract_route_from_config);

        if let Some(route) = route.clone() {
            self.cache
                .lock()
                .await
                .insert(keyspace_name.to_string(), route);
        }

        Ok(route)
    }
}

fn build_http_client(pd_tls: Option<&TlsConfig>) -> Result<Client, BoxError> {
    let mut builder = Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .connect_timeout(CONNECT_TIMEOUT);

    if let Some(tls) = pd_tls {
        builder = builder
            .danger_accept_invalid_certs(!tls.verify_certificate.unwrap_or(true))
            .danger_accept_invalid_hostnames(!tls.verify_hostname.unwrap_or(true));

        if let Some(ca_file) = tls.ca_file.as_ref() {
            let ca = fs::read(ca_file)?;
            builder = builder.add_root_certificate(Certificate::from_pem(&ca)?);
        }

        match (tls.crt_file.as_ref(), tls.key_file.as_ref()) {
            (Some(crt_file), Some(key_file)) => {
                let crt = fs::read(crt_file)?;
                let key = fs::read(key_file)?;
                builder = builder.identity(Identity::from_pkcs8_pem(&crt, &key)?);
            }
            (None, None) => {}
            _ => {
                return Err(
                    "pd_tls.crt_file and pd_tls.key_file must both be set when client TLS is enabled"
                        .into(),
                );
            }
        }
    }

    Ok(builder.build()?)
}

fn normalize_pd_address(pd_address: &str, use_tls: bool) -> String {
    let trimmed = pd_address.trim().trim_end_matches('/');
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_string()
    } else if use_tls {
        format!("https://{}", trimmed)
    } else {
        format!("http://{}", trimmed)
    }
}

fn is_not_found_body(body: &str) -> bool {
    let lower = body.to_ascii_lowercase();
    lower.contains("not found")
}

fn extract_route_from_config(config: &HashMap<String, String>) -> Option<KeyspaceRoute> {
    let org_id = find_config_value(config, ORG_ID_KEYS)?;
    let cluster_id = find_config_value(config, CLUSTER_ID_KEYS)?;

    if org_id.is_empty() || cluster_id.is_empty() {
        return None;
    }

    Some(KeyspaceRoute {
        org_id: org_id.to_string(),
        cluster_id: cluster_id.to_string(),
    })
}

fn find_config_value<'a>(config: &'a HashMap<String, String>, keys: &[&str]) -> Option<&'a str> {
    keys.iter()
        .find_map(|key| config.get(*key))
        .map(String::as_str)
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use hyper::service::{make_service_fn, service_fn};
    use hyper::{Body, Request, Response, Server, StatusCode as HyperStatusCode};

    use super::*;

    #[test]
    fn normalize_pd_address_adds_expected_scheme() {
        assert_eq!(normalize_pd_address("pd:2379/", false), "http://pd:2379");
        assert_eq!(normalize_pd_address("pd:2379/", true), "https://pd:2379");
        assert_eq!(
            normalize_pd_address("https://pd:2379", false),
            "https://pd:2379"
        );
    }

    #[test]
    fn extract_route_from_config_supports_expected_aliases() {
        let mut config = HashMap::new();
        config.insert("tenant_id".to_string(), "30018".to_string());
        config.insert(
            "tidb_cluster_id".to_string(),
            "10762701230946915645".to_string(),
        );

        assert_eq!(
            extract_route_from_config(&config),
            Some(KeyspaceRoute {
                org_id: "30018".to_string(),
                cluster_id: "10762701230946915645".to_string(),
            })
        );
    }

    #[tokio::test]
    async fn resolve_keyspace_uses_pd_keyspace_api_and_caches_result() {
        let request_count = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&request_count);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = Server::from_tcp(listener)
            .unwrap()
            .serve(make_service_fn(move |_| {
                let counter = Arc::clone(&counter);
                async move {
                    Ok::<_, Infallible>(service_fn(move |request: Request<Body>| {
                        let counter = Arc::clone(&counter);
                        async move {
                            counter.fetch_add(1, Ordering::SeqCst);
                            assert_eq!(request.uri().path(), "/pd/api/v2/keyspaces/test_keyspace");
                            Ok::<_, Infallible>(Response::new(Body::from(
                                r#"{"config":{"tenant_id":"30018","cluster_id":"10762701230946915645"}}"#,
                            )))
                        }
                    }))
                }
            }));
        let server_handle = tokio::spawn(server);

        let client = Client::builder().no_proxy().build().unwrap();
        let resolver =
            PdKeyspaceResolver::new_with_client(format!("http://{}", address), None, client);

        let first = resolver.resolve_keyspace("test_keyspace").await.unwrap();
        let second = resolver.resolve_keyspace("test_keyspace").await.unwrap();

        assert_eq!(
            first,
            Some(KeyspaceRoute {
                org_id: "30018".to_string(),
                cluster_id: "10762701230946915645".to_string(),
            })
        );
        assert_eq!(second, first);
        assert_eq!(request_count.load(Ordering::SeqCst), 1);

        server_handle.abort();
    }

    #[tokio::test]
    async fn resolve_keyspace_returns_none_for_missing_route() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server =
            Server::from_tcp(listener)
                .unwrap()
                .serve(make_service_fn(move |_| async move {
                    Ok::<_, Infallible>(service_fn(move |_request: Request<Body>| async move {
                        Ok::<_, Infallible>(Response::new(Body::from(
                            r#"{"config":{"tenant_id":"30018"}}"#,
                        )))
                    }))
                }));
        let server_handle = tokio::spawn(server);

        let client = Client::builder().no_proxy().build().unwrap();
        let resolver =
            PdKeyspaceResolver::new_with_client(format!("http://{}", address), None, client);
        let route = resolver.resolve_keyspace("test_keyspace").await.unwrap();

        assert_eq!(route, None);

        server_handle.abort();
    }

    #[tokio::test]
    async fn resolve_keyspace_treats_not_found_error_body_as_empty_result() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server =
            Server::from_tcp(listener)
                .unwrap()
                .serve(make_service_fn(move |_| async move {
                    Ok::<_, Infallible>(service_fn(move |_request: Request<Body>| async move {
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(HyperStatusCode::INTERNAL_SERVER_ERROR)
                                .body(Body::from("keyspace not found"))
                                .unwrap(),
                        )
                    }))
                }));
        let server_handle = tokio::spawn(server);

        let client = Client::builder().no_proxy().build().unwrap();
        let resolver =
            PdKeyspaceResolver::new_with_client(format!("http://{}", address), None, client);
        let route = resolver.resolve_keyspace("missing_keyspace").await.unwrap();

        assert_eq!(route, None);

        server_handle.abort();
    }
}
