use std::collections::HashMap;
use std::fs;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use lru::LruCache;
use reqwest::{Certificate, Client, Identity, StatusCode};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::Mutex;
use url::form_urlencoded::byte_serialize;
use vector_lib::tls::TlsConfig;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const DEFAULT_KEYSPACE_ROUTE_CACHE_CAPACITY: usize = 10_000;

const ROUTE_RESOLUTION_BASE_DELAY: Duration = Duration::from_secs(5);
const ROUTE_RESOLUTION_MAX_DELAY: Duration = Duration::from_secs(60);
pub const MAX_ROUTE_RESOLUTION_RETRIES: usize = 5;

/// Exponential backoff delay for keyspace route resolution retries.
pub fn route_resolution_retry_delay(retry_count: usize) -> Duration {
    let multiplier = 1u64 << retry_count.saturating_sub(1).min(6);
    let delay_secs = ROUTE_RESOLUTION_BASE_DELAY
        .as_secs()
        .saturating_mul(multiplier)
        .min(ROUTE_RESOLUTION_MAX_DELAY.as_secs());
    Duration::from_secs(delay_secs)
}

const ORG_ID_KEYS: &[&str] = &["serverless_tenant_id"];
const CLUSTER_ID_KEYS: &[&str] = &["serverless_cluster_id"];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyspaceRoute {
    pub org_id: String,
    pub cluster_id: String,
}

// Path segments use the exact prefixes `org=` and `cluster=` as a convention.
// These are Hive-style partition keys chosen for the storage path layout;
// callers must follow this convention when constructing base_path templates.
pub fn path_contains_keyspace_route_segments(path: &str) -> bool {
    let mut has_org_segment = false;
    let mut has_cluster_segment = false;
    for segment in path.split('/') {
        if segment.starts_with("org=") {
            has_org_segment = true;
        } else if segment.starts_with("cluster=") {
            has_cluster_segment = true;
        }
    }

    has_org_segment && has_cluster_segment
}

pub fn validate_keyspace_route_template(path: &str) -> Result<(), String> {
    if path_contains_keyspace_route_segments(path) {
        return Ok(());
    }

    Err(format!(
        "base_path must contain both `org=` and `cluster=` path segments when enable_keyspace_cluster_mapping is true; expected something like `.../org=xxx/cluster=xxx/...`, got: {}",
        path
    ))
}

pub fn replace_keyspace_route_segments(base_path: &PathBuf, route: &KeyspaceRoute) -> PathBuf {
    debug_assert!(
        !route.org_id.contains('/') && !route.cluster_id.contains('/'),
        "org_id and cluster_id must not contain path separators"
    );
    let path = base_path.to_string_lossy();
    let replaced = path
        .split('/')
        .map(|segment| {
            if segment.starts_with("org=") {
                format!("org={}", route.org_id)
            } else if segment.starts_with("cluster=") {
                format!("cluster={}", route.cluster_id)
            } else {
                segment.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("/");

    PathBuf::from(replaced)
}

#[derive(Clone)]
pub struct PdKeyspaceResolver {
    base_url: String,
    client: Client,
    cache: Arc<Mutex<LruCache<String, KeyspaceRoute>>>,
    /// Per-keyspace locks to deduplicate concurrent HTTP requests for the same keyspace.
    keyspace_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
}

#[derive(Debug, Deserialize)]
struct PdKeyspaceMetadata {
    config: Option<HashMap<String, String>>,
}

impl PdKeyspaceResolver {
    pub fn new(pd_address: impl Into<String>, pd_tls: Option<&TlsConfig>) -> Result<Self, BoxError> {
        let client = build_http_client(pd_tls)?;
        Ok(Self::new_with_client(pd_address, pd_tls, client))
    }

    pub fn new_with_client(
        pd_address: impl Into<String>,
        pd_tls: Option<&TlsConfig>,
        client: Client,
    ) -> Self {
        Self::new_with_client_and_capacity(
            pd_address,
            pd_tls,
            client,
            DEFAULT_KEYSPACE_ROUTE_CACHE_CAPACITY,
        )
    }

    fn new_with_client_and_capacity(
        pd_address: impl Into<String>,
        pd_tls: Option<&TlsConfig>,
        client: Client,
        cache_capacity: usize,
    ) -> Self {
        Self {
            base_url: normalize_pd_address(&pd_address.into(), pd_tls.is_some()),
            client,
            cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(cache_capacity.max(1)).unwrap(),
            ))),
            keyspace_locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn resolve_keyspace(
        &self,
        keyspace_name: &str,
    ) -> Result<Option<KeyspaceRoute>, BoxError> {
        if keyspace_name.is_empty() {
            return Ok(None);
        }

        // Fast path: read lock on cache (via short-lived Mutex guard).
        {
            let mut cache = self.cache.lock().await;
            if let Some(cached) = cache.get(keyspace_name).cloned() {
                return Ok(Some(cached));
            }
        }

        // Acquire a per-keyspace lock so only one request hits PD for the same keyspace.
        let ks_lock = {
            let mut locks = self.keyspace_locks.lock().await;
            locks
                .entry(keyspace_name.to_string())
                .or_default()
                .clone()
        };
        let _guard = ks_lock.lock().await;

        let result: Result<Option<KeyspaceRoute>, BoxError> = async {
            // Double-check: another request may have populated the cache while we waited.
            {
                let mut cache = self.cache.lock().await;
                if let Some(cached) = cache.get(keyspace_name).cloned() {
                    return Ok(Some(cached));
                }
            }

            let route = self.fetch_keyspace_from_pd(keyspace_name).await?;

            if let Some(route) = route.clone() {
                self.cache
                    .lock()
                    .await
                    .put(keyspace_name.to_string(), route);
            }
            // Intentionally do not cache misses or transient failures so a later retry can
            // recover once PD metadata becomes visible.

            Ok(route)
        }
        .await;

        // Clean up the per-keyspace lock if no other task is waiting on it.
        // Runs on every exit path: cache hit, fetch success, and fetch error.
        drop(_guard);
        {
            let mut locks = self.keyspace_locks.lock().await;
            if let Some(lock) = locks.get(keyspace_name) {
                // The HashMap holds one Arc and we cloned one into `ks_lock` (still alive).
                // If strong_count == 2, no other task is queued, safe to remove.
                if Arc::strong_count(lock) <= 2 {
                    locks.remove(keyspace_name);
                }
            }
        }

        result
    }

    async fn fetch_keyspace_from_pd(
        &self,
        keyspace_name: &str,
    ) -> Result<Option<KeyspaceRoute>, BoxError> {
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
        Ok(metadata.config.as_ref().and_then(extract_route_from_config))
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
    if body.to_ascii_lowercase().contains("keyspace not found") {
        return true;
    }

    let Ok(value) = serde_json::from_str::<Value>(body) else {
        return false;
    };

    extract_error_message(&value)
        .map(|message| message.to_ascii_lowercase().contains("keyspace not found"))
        .unwrap_or(false)
}

fn extract_error_message(value: &Value) -> Option<&str> {
    value
        .get("message")
        .and_then(Value::as_str)
        .or_else(|| value.get("error").and_then(Value::as_str))
        .or_else(|| {
            value
                .get("error")
                .and_then(|error| error.get("message"))
                .and_then(Value::as_str)
        })
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
    fn extract_route_from_config_uses_serverless_route_keys() {
        let mut serverless_config = HashMap::new();
        serverless_config.insert("serverless_tenant_id".to_string(), "30018".to_string());
        serverless_config.insert(
            "serverless_cluster_id".to_string(),
            "10155668891296301432".to_string(),
        );

        assert_eq!(
            extract_route_from_config(&serverless_config),
            Some(KeyspaceRoute {
                org_id: "30018".to_string(),
                cluster_id: "10155668891296301432".to_string(),
            })
        );
    }

    #[test]
    fn extract_route_from_config_ignores_legacy_route_keys() {
        let mut legacy_config = HashMap::new();
        legacy_config.insert("tenant_id".to_string(), "30018".to_string());
        legacy_config.insert(
            "tidb_cluster_id".to_string(),
            "10762701230946915645".to_string(),
        );

        assert_eq!(extract_route_from_config(&legacy_config), None);
    }

    #[test]
    fn is_not_found_body_only_matches_keyspace_errors() {
        assert!(is_not_found_body("keyspace not found"));
        assert!(is_not_found_body(r#"{"message":"keyspace not found"}"#));
        assert!(!is_not_found_body("certificate not found"));
        assert!(!is_not_found_body(r#"{"message":"PD server not found"}"#));
    }

    #[test]
    fn path_contains_keyspace_route_segments_requires_both_segments() {
        assert!(path_contains_keyspace_route_segments(
            "s3://bucket/deltalake/org=xxx/cluster=xxx/type=topsql"
        ));
        assert!(path_contains_keyspace_route_segments(
            "/tmp/deltalake/org=xxx/cluster=xxx/type=topsql"
        ));
        assert!(!path_contains_keyspace_route_segments(
            "s3://bucket/deltalake/org=xxx/type=topsql"
        ));
        assert!(!path_contains_keyspace_route_segments(
            "/tmp/deltalake/type=topsql"
        ));
    }

    #[test]
    fn replace_keyspace_route_segments_rewrites_template_values() {
        let replaced = replace_keyspace_route_segments(
            &PathBuf::from("s3://bucket/deltalake/org=xxx/cluster=xxx/type=topsql"),
            &KeyspaceRoute {
                org_id: "30018".to_string(),
                cluster_id: "10155668891296301432".to_string(),
            },
        );

        assert_eq!(
            replaced,
            PathBuf::from(
                "s3://bucket/deltalake/org=30018/cluster=10155668891296301432/type=topsql"
            )
        );
    }

    #[test]
    fn validate_keyspace_route_template_requires_org_and_cluster_segments() {
        assert!(
            validate_keyspace_route_template("/tmp/deltalake/org=xxx/cluster=xxx/type=topsql")
                .is_ok()
        );

        let error = validate_keyspace_route_template("/tmp/deltalake/type=topsql").unwrap_err();
        assert!(error.contains("org="));
        assert!(error.contains("cluster="));
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
                                r#"{"config":{"serverless_tenant_id":"30018","serverless_cluster_id":"10762701230946915645"}}"#,
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

    #[tokio::test]
    async fn resolve_keyspace_cache_is_bounded() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = Server::from_tcp(listener)
            .unwrap()
            .serve(make_service_fn(move |_| async move {
                Ok::<_, Infallible>(service_fn(move |request: Request<Body>| async move {
                    let keyspace_name = request
                        .uri()
                        .path()
                        .trim_start_matches("/pd/api/v2/keyspaces/");
                    let body = format!(
                        r#"{{"config":{{"serverless_tenant_id":"30018","serverless_cluster_id":"{}"}}}}"#,
                        keyspace_name
                    );
                    Ok::<_, Infallible>(Response::new(Body::from(body)))
                }))
            }));
        let server_handle = tokio::spawn(server);

        let client = Client::builder().no_proxy().build().unwrap();
        let resolver = PdKeyspaceResolver::new_with_client_and_capacity(
            format!("http://{}", address),
            None,
            client,
            2,
        );

        let _ = resolver.resolve_keyspace("ks-1").await.unwrap();
        let _ = resolver.resolve_keyspace("ks-2").await.unwrap();
        let _ = resolver.resolve_keyspace("ks-3").await.unwrap();

        assert_eq!(resolver.cache.lock().await.len(), 2);

        server_handle.abort();
    }

    #[test]
    fn route_resolution_retry_delay_caps_at_maximum() {
        assert_eq!(route_resolution_retry_delay(1), Duration::from_secs(5));
        assert_eq!(route_resolution_retry_delay(2), Duration::from_secs(10));
        assert_eq!(route_resolution_retry_delay(5), Duration::from_secs(60));
        assert_eq!(route_resolution_retry_delay(8), Duration::from_secs(60));
    }
}
