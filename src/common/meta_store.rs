use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use reqwest::{Client, StatusCode};
use serde::Deserialize;
use tokio::sync::Mutex;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyspaceRoute {
    pub org_id: String,
    pub cluster_id: String,
}

#[derive(Clone)]
pub struct MetaStoreResolver {
    base_url: String,
    client: Client,
    cache: Arc<Mutex<HashMap<String, KeyspaceRoute>>>,
}

#[derive(Debug, Deserialize)]
struct MetaStoreKeyspaceMetadata {
    #[serde(rename = "ClusterId", alias = "cluster_id")]
    cluster_id: String,
    #[serde(rename = "TenantID", alias = "tenant_id")]
    tenant_id: String,
}

impl MetaStoreResolver {
    pub fn new(meta_store_addr: impl Into<String>) -> Result<Self, BoxError> {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(3))
            .build()?;
        Ok(Self::new_with_client(meta_store_addr, client))
    }

    pub fn new_with_client(meta_store_addr: impl Into<String>, client: Client) -> Self {
        Self {
            base_url: normalize_meta_store_addr(&meta_store_addr.into()),
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

        let response = self
            .client
            .get(format!("{}/api/v2/meta", self.base_url))
            .query(&[("keyspace_name", keyspace_name)])
            .send()
            .await?;

        match response.status() {
            StatusCode::NOT_FOUND => return Ok(None),
            status if !status.is_success() => {
                return Err(format!(
                    "meta-store lookup failed for keyspace {} with status {}",
                    keyspace_name, status
                )
                .into());
            }
            _ => {}
        }

        let metadata: Vec<MetaStoreKeyspaceMetadata> = response.json().await?;
        let Some(first) = metadata.into_iter().next() else {
            return Ok(None);
        };

        if first.cluster_id.is_empty() || first.tenant_id.is_empty() {
            return Ok(None);
        }

        let route = KeyspaceRoute {
            org_id: first.tenant_id,
            cluster_id: first.cluster_id,
        };
        self.cache
            .lock()
            .await
            .insert(keyspace_name.to_string(), route.clone());

        Ok(Some(route))
    }
}

fn normalize_meta_store_addr(meta_store_addr: &str) -> String {
    let trimmed = meta_store_addr.trim().trim_end_matches('/');
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_string()
    } else {
        format!("http://{}", trimmed)
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use hyper::service::{make_service_fn, service_fn};
    use hyper::{Body, Request, Response, Server};

    use super::*;

    #[test]
    fn normalize_meta_store_addr_adds_scheme() {
        assert_eq!(
            normalize_meta_store_addr("meta-store:9088/"),
            "http://meta-store:9088"
        );
        assert_eq!(
            normalize_meta_store_addr("https://meta-store:9088"),
            "https://meta-store:9088"
        );
    }

    #[tokio::test]
    async fn resolve_keyspace_uses_tenant_as_org_id_and_caches_result() {
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
                            assert_eq!(request.uri().path(), "/api/v2/meta");
                            assert!(
                                request
                                    .uri()
                                    .query()
                                    .unwrap_or_default()
                                    .contains("keyspace_name=test_keyspace"),
                                "query should contain keyspace_name=test_keyspace"
                            );
                            Ok::<_, Infallible>(Response::new(Body::from(
                                r#"[{"ClusterId":"10110362358366286743","TenantID":"1369847559692509642"}]"#,
                            )))
                        }
                    }))
                }
            }));
        let server_handle = tokio::spawn(server);

        let resolver = MetaStoreResolver::new(format!("http://{}", address)).unwrap();

        let first = resolver.resolve_keyspace("test_keyspace").await.unwrap();
        let second = resolver.resolve_keyspace("test_keyspace").await.unwrap();

        assert_eq!(
            first,
            Some(KeyspaceRoute {
                org_id: "1369847559692509642".to_string(),
                cluster_id: "10110362358366286743".to_string(),
            })
        );
        assert_eq!(second, first);
        assert_eq!(request_count.load(Ordering::SeqCst), 1);

        server_handle.abort();
    }
}
