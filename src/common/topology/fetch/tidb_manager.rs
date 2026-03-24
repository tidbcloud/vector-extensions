use std::{collections::HashSet, env};

use hyper::body::HttpBody;
use serde_json::{Map, Value};
use snafu::{ResultExt, Snafu};
use vector::http::HttpClient;

use super::normalize_namespace_list;
use crate::common::topology::fetch::utils;
use crate::common::topology::{Component, InstanceType};

const GET_ACTIVE_TIDB_PATH: &str = "/api/tidb/get_active_tidb";
const DEFAULT_TIDB_PRIMARY_PORT: u16 = 4000;
const DEFAULT_TIDB_STATUS_PORT: u16 = 10080;
const MAX_RESPONSE_DEPTH: usize = 8;
const MAX_MANAGER_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const VECTOR_STS_REPLICA_COUNT_ENV: &str = "VECTOR_STS_REPLICA_COUNT";
const VECTOR_STS_ID_ENV: &str = "VECTOR_STS_ID";
const FNV1A_64_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV1A_64_PRIME: u64 = 0x100000001b3;

#[derive(Debug, Snafu)]
pub enum FetchError {
    #[snafu(display("Failed to build request: {}", source))]
    BuildRequest { source: http::Error },
    #[snafu(display("Failed to get active tidb addresses from manager server: {}", source))]
    GetActiveTiDBs { source: vector::http::HttpError },
    #[snafu(display("Failed to read active tidb response bytes: {}", source))]
    GetActiveTiDBsBytes { source: hyper::Error },
    #[snafu(display("Manager active tidb response exceeds limit of {} bytes", limit_bytes))]
    ActiveTiDBResponseTooLarge { limit_bytes: usize },
    #[snafu(display("Failed to parse active tidb response JSON text: {}", source))]
    ActiveTiDBJsonFromStr { source: serde_json::Error },
    #[snafu(display("Invalid manager server response: {}", message))]
    InvalidManagerResponse { message: String },
    #[snafu(display("Invalid manager keyspace shard config: {}", message))]
    InvalidShardConfig { message: String },
    #[snafu(display("Failed to parse tidb host from manager response: {}", source))]
    ParseTiDBHost { source: utils::ParseError },
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct ActiveTiDBAddress {
    host: String,
    port: Option<u16>,
    status_port: Option<u16>,
    hostname: Option<String>,
    keyspace_name: Option<String>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) struct ManagerShardConfig {
    replica_count: u64,
    sts_id: u64,
}

pub struct TiDBManagerTopologyFetcher<'a> {
    manager_server_address: &'a str,
    tidb_namespace: Option<&'a str>,
    http_client: &'a HttpClient<hyper::Body>,
    shard_config: Option<ManagerShardConfig>,
}

impl<'a> TiDBManagerTopologyFetcher<'a> {
    pub fn new(
        manager_server_address: &'a str,
        tidb_namespace: Option<&'a str>,
        http_client: &'a HttpClient<hyper::Body>,
        shard_config: Option<ManagerShardConfig>,
    ) -> Self {
        Self {
            manager_server_address,
            tidb_namespace,
            http_client,
            shard_config,
        }
    }

    pub async fn get_up_tidbs(
        &self,
        components: &mut HashSet<Component>,
    ) -> Result<(), FetchError> {
        let active_tidb_addresses = Self::filter_active_tidb_addresses(
            self.fetch_active_tidb_addresses().await?,
            self.shard_config,
        )?;
        if active_tidb_addresses.is_empty() {
            info!(
                message = "No active TiDB instances selected from manager server",
                manager_server_address = self.manager_server_address,
                tidb_namespace = ?self.tidb_namespace,
                tidb_count = 0,
                shard_config = ?self.shard_config
            );
            return Ok(());
        }

        info!(
            message = "Fetched active TiDB instances from manager server",
            manager_server_address = self.manager_server_address,
            tidb_namespace = ?self.tidb_namespace,
            tidb_count = active_tidb_addresses.len()
        );

        for active_tidb in active_tidb_addresses {
            let (host, primary_port) =
                Self::parse_tidb_host_and_primary(&active_tidb.host, active_tidb.port)?;
            let secondary_port = active_tidb.status_port.unwrap_or(DEFAULT_TIDB_STATUS_PORT);

            components.insert(Component {
                instance_type: InstanceType::TiDB,
                host,
                primary_port,
                secondary_port,
                instance_name: active_tidb.hostname.filter(|name| !name.trim().is_empty()),
            });
        }

        Ok(())
    }

    async fn fetch_active_tidb_addresses(&self) -> Result<Vec<ActiveTiDBAddress>, FetchError> {
        let Some(endpoint_url) = self.active_tidb_endpoint_url() else {
            return Ok(Vec::new());
        };

        let req = http::Request::get(endpoint_url)
            .body(hyper::Body::empty())
            .context(BuildRequestSnafu)?;

        let res = self
            .http_client
            .send(req)
            .await
            .context(GetActiveTiDBsSnafu)?;
        let bytes = Self::read_response_body_with_limit(res.into_body()).await?;

        Self::parse_active_tidb_addresses_response(&bytes)
    }

    fn active_tidb_endpoint_url(&self) -> Option<String> {
        let namespaces = normalize_namespace_list(self.tidb_namespace)?;
        Some(Self::build_active_tidb_endpoint_url(
            self.manager_server_address,
            &namespaces,
        ))
    }

    fn normalize_namespaces(namespaces: Option<&str>) -> Option<String> {
        normalize_namespace_list(namespaces)
    }

    fn build_active_tidb_endpoint_url(manager_server_address: &str, namespaces: &str) -> String {
        let mut endpoint = if manager_server_address.ends_with(GET_ACTIVE_TIDB_PATH) {
            manager_server_address.to_owned()
        } else {
            format!("{manager_server_address}{GET_ACTIVE_TIDB_PATH}")
        };
        endpoint.push_str("?namespace=");
        endpoint.push_str(namespaces);
        endpoint
    }

    fn parse_tidb_host_and_primary(
        host_or_address: &str,
        explicit_port: Option<u16>,
    ) -> Result<(String, u16), FetchError> {
        let host_or_address = host_or_address.trim_end_matches('/');
        if let Ok((host, parsed_port)) = utils::parse_host_port(host_or_address) {
            return Ok((host, explicit_port.unwrap_or(parsed_port)));
        }

        let default_address = format!("{host_or_address}:{DEFAULT_TIDB_PRIMARY_PORT}");
        let (host, _) = utils::parse_host_port(&default_address).context(ParseTiDBHostSnafu)?;
        Ok((host, explicit_port.unwrap_or(DEFAULT_TIDB_PRIMARY_PORT)))
    }

    fn parse_active_tidb_addresses_response(
        bytes: &[u8],
    ) -> Result<Vec<ActiveTiDBAddress>, FetchError> {
        let value = serde_json::from_slice::<Value>(bytes).context(ActiveTiDBJsonFromStrSnafu)?;
        let addresses = Self::extract_active_tidb_addresses(&value, 0)?;

        if addresses.is_empty() {
            return Err(FetchError::InvalidManagerResponse {
                message: "no active tidb addresses found".to_owned(),
            });
        }

        Ok(addresses)
    }

    fn extract_active_tidb_addresses(
        value: &Value,
        depth: usize,
    ) -> Result<Vec<ActiveTiDBAddress>, FetchError> {
        if depth > MAX_RESPONSE_DEPTH {
            return Err(FetchError::InvalidManagerResponse {
                message: "response nesting is too deep".to_owned(),
            });
        }

        match value {
            Value::String(host) => Ok(vec![ActiveTiDBAddress {
                host: host.clone(),
                port: None,
                status_port: None,
                hostname: None,
                keyspace_name: None,
            }]),
            Value::Array(items) => {
                let mut addresses = Vec::new();
                for item in items {
                    addresses.extend(Self::extract_active_tidb_addresses(item, depth + 1)?);
                }
                Ok(addresses)
            }
            Value::Object(obj) => {
                if let Some(address) = Self::extract_active_tidb_address_from_object(obj) {
                    return Ok(vec![address]);
                }

                for key in [
                    "data",
                    "result",
                    "active_tidb_addresses",
                    "tidb_addresses",
                    "active_tidbs",
                    "tidbs",
                    "addresses",
                    "instances",
                    "items",
                    "nodes",
                    "list",
                ] {
                    if let Some(next_value) = obj.get(key) {
                        let addresses = Self::extract_active_tidb_addresses(next_value, depth + 1)?;
                        if !addresses.is_empty() {
                            return Ok(addresses);
                        }
                    }
                }

                Ok(Vec::new())
            }
            _ => Ok(Vec::new()),
        }
    }

    fn extract_active_tidb_address_from_object(
        obj: &Map<String, Value>,
    ) -> Option<ActiveTiDBAddress> {
        let host = Self::extract_string_field(
            obj,
            &["host", "address", "tidb_address", "active_tidb_address"],
        )?;
        let port = Self::extract_u16_field(obj, &["port", "primary_port"]);
        let status_port = Self::extract_u16_field(obj, &["status_port", "secondary_port"]);
        let hostname = Self::extract_string_field(obj, &["hostname", "pod_name", "instance_name"]);
        let keyspace_name =
            Self::extract_string_field(obj, &["keyspace_name", "keyspaceName", "keyspace"]);

        Some(ActiveTiDBAddress {
            host,
            port,
            status_port,
            hostname,
            keyspace_name,
        })
    }

    fn extract_string_field(obj: &Map<String, Value>, keys: &[&str]) -> Option<String> {
        keys.iter()
            .find_map(|key| obj.get(*key).and_then(Value::as_str).map(str::to_owned))
    }

    fn extract_u16_field(obj: &Map<String, Value>, keys: &[&str]) -> Option<u16> {
        keys.iter().find_map(|key| {
            obj.get(*key)
                .and_then(Value::as_u64)
                .and_then(|raw| u16::try_from(raw).ok())
        })
    }

    fn filter_active_tidb_addresses(
        active_tidb_addresses: Vec<ActiveTiDBAddress>,
        shard_config: Option<ManagerShardConfig>,
    ) -> Result<Vec<ActiveTiDBAddress>, FetchError> {
        let Some(shard_config) = shard_config else {
            return Ok(active_tidb_addresses);
        };

        let total_tidb_count = active_tidb_addresses.len();
        let mut filtered_tidbs = Vec::new();
        let mut skipped_missing_keyspace_count = 0usize;

        for active_tidb in active_tidb_addresses {
            let Some(keyspace_name) = active_tidb
                .keyspace_name
                .as_deref()
                .map(str::trim)
                .filter(|name| !name.is_empty())
            else {
                skipped_missing_keyspace_count += 1;
                warn!(
                    message = "Skipping manager active TiDB without keyspace_name while keyspace sharding is enabled",
                    host = active_tidb.host,
                    port = ?active_tidb.port,
                    status_port = ?active_tidb.status_port,
                    hostname = ?active_tidb.hostname,
                    replica_count = shard_config.replica_count,
                    sts_id = shard_config.sts_id
                );
                continue;
            };

            let shard = Self::hash_keyspace_name(keyspace_name) % shard_config.replica_count;
            if shard == shard_config.sts_id {
                filtered_tidbs.push(active_tidb);
            }
        }

        info!(
            message = "Applied manager keyspace sharding to active TiDB instances",
            replica_count = shard_config.replica_count,
            sts_id = shard_config.sts_id,
            total_tidb_count,
            selected_tidb_count = filtered_tidbs.len(),
            skipped_missing_keyspace_count
        );

        Ok(filtered_tidbs)
    }

    fn hash_keyspace_name(keyspace_name: &str) -> u64 {
        // FNV-1a keeps sharding deterministic across process restarts and languages.
        // We intentionally avoid std::hash because it is not stable across runs, and
        // any service that shards keyspaces the same way must reuse this exact contract.
        let mut hash = FNV1A_64_OFFSET_BASIS;
        for byte in keyspace_name.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV1A_64_PRIME);
        }
        hash
    }

    async fn read_response_body_with_limit(mut body: hyper::Body) -> Result<Vec<u8>, FetchError> {
        if body
            .size_hint()
            .upper()
            .is_some_and(|upper| upper > MAX_MANAGER_RESPONSE_BYTES as u64)
        {
            return Err(FetchError::ActiveTiDBResponseTooLarge {
                limit_bytes: MAX_MANAGER_RESPONSE_BYTES,
            });
        }

        let mut bytes = Vec::new();
        while let Some(chunk) = body.data().await {
            let chunk = chunk.context(GetActiveTiDBsBytesSnafu)?;
            if bytes.len().saturating_add(chunk.len()) > MAX_MANAGER_RESPONSE_BYTES {
                return Err(FetchError::ActiveTiDBResponseTooLarge {
                    limit_bytes: MAX_MANAGER_RESPONSE_BYTES,
                });
            }
            bytes.extend_from_slice(&chunk);
        }

        Ok(bytes)
    }
}

pub(super) fn read_manager_shard_config_from_env() -> Result<Option<ManagerShardConfig>, FetchError>
{
    ManagerShardConfig::from_env_values(
        env::var(VECTOR_STS_REPLICA_COUNT_ENV).ok().as_deref(),
        env::var(VECTOR_STS_ID_ENV).ok().as_deref(),
    )
}

impl ManagerShardConfig {
    fn from_env_values(
        replica_count: Option<&str>,
        sts_id: Option<&str>,
    ) -> Result<Option<Self>, FetchError> {
        let replica_count = replica_count
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let sts_id = sts_id.map(str::trim).filter(|value| !value.is_empty());

        match (replica_count, sts_id) {
            (None, None) => Ok(None),
            (Some(_), None) => Err(FetchError::InvalidShardConfig {
                message: format!(
                    "{VECTOR_STS_REPLICA_COUNT_ENV} is set but {VECTOR_STS_ID_ENV} is missing"
                ),
            }),
            (None, Some(_)) => Err(FetchError::InvalidShardConfig {
                message: format!(
                    "{VECTOR_STS_ID_ENV} is set but {VECTOR_STS_REPLICA_COUNT_ENV} is missing"
                ),
            }),
            (Some(replica_count), Some(sts_id)) => {
                let replica_count =
                    Self::parse_u64_env(VECTOR_STS_REPLICA_COUNT_ENV, replica_count)?;
                let sts_id = Self::parse_u64_env(VECTOR_STS_ID_ENV, sts_id)?;

                if replica_count == 0 {
                    return Err(FetchError::InvalidShardConfig {
                        message: format!("{VECTOR_STS_REPLICA_COUNT_ENV} must be greater than 0"),
                    });
                }

                if sts_id >= replica_count {
                    return Err(FetchError::InvalidShardConfig {
                        message: format!(
                            "{VECTOR_STS_ID_ENV} ({sts_id}) must be smaller than {VECTOR_STS_REPLICA_COUNT_ENV} ({replica_count})"
                        ),
                    });
                }

                Ok(Some(Self {
                    replica_count,
                    sts_id,
                }))
            }
        }
    }

    fn parse_u64_env(env_name: &str, raw_value: &str) -> Result<u64, FetchError> {
        raw_value
            .parse::<u64>()
            .map_err(|_| FetchError::InvalidShardConfig {
                message: format!("{env_name} must be a non-negative integer, got {raw_value}"),
            })
    }
}

#[cfg(test)]
mod tests {
    use hyper::Body;

    use super::*;

    #[test]
    fn parse_response_new_schema() {
        let bytes = br#"[
            {"host":"10.0.0.1","port":4000,"status_port":10080,"hostname":"tidb-0","keyspace_name":"tenant-a"},
            {"host":"10.0.0.2","port":4000,"status_port":10080,"hostname":"tidb-1","keyspace_name":"tenant-b"}
        ]"#;
        let addresses =
            TiDBManagerTopologyFetcher::parse_active_tidb_addresses_response(bytes).unwrap();

        assert_eq!(
            addresses,
            vec![
                ActiveTiDBAddress {
                    host: "10.0.0.1".to_owned(),
                    port: Some(4000),
                    status_port: Some(10080),
                    hostname: Some("tidb-0".to_owned()),
                    keyspace_name: Some("tenant-a".to_owned()),
                },
                ActiveTiDBAddress {
                    host: "10.0.0.2".to_owned(),
                    port: Some(4000),
                    status_port: Some(10080),
                    hostname: Some("tidb-1".to_owned()),
                    keyspace_name: Some("tenant-b".to_owned()),
                }
            ]
        );
    }

    #[test]
    fn parse_response_supports_keyspace_aliases() {
        let bytes = br#"[
            {"host":"10.0.0.1","keyspace":"tenant-a"},
            {"host":"10.0.0.2","keyspaceName":"tenant-b"}
        ]"#;
        let addresses =
            TiDBManagerTopologyFetcher::parse_active_tidb_addresses_response(bytes).unwrap();

        assert_eq!(addresses[0].keyspace_name.as_deref(), Some("tenant-a"));
        assert_eq!(addresses[1].keyspace_name.as_deref(), Some("tenant-b"));
    }

    #[test]
    fn parse_response_invalid_format() {
        let bytes = br#"{"code":0,"message":"ok"}"#;
        let err = TiDBManagerTopologyFetcher::parse_active_tidb_addresses_response(bytes)
            .expect_err("expected invalid manager response");
        assert!(matches!(err, FetchError::InvalidManagerResponse { .. }));
    }

    #[test]
    fn parse_tidb_host_and_primary_with_address() {
        let (host, primary_port) =
            TiDBManagerTopologyFetcher::parse_tidb_host_and_primary("10.0.0.1:4100", None).unwrap();
        assert_eq!(host, "10.0.0.1");
        assert_eq!(primary_port, 4100);
    }

    #[test]
    fn parse_tidb_host_and_primary_with_host_only() {
        let (host, primary_port) =
            TiDBManagerTopologyFetcher::parse_tidb_host_and_primary("10.0.0.1", None).unwrap();
        assert_eq!(host, "10.0.0.1");
        assert_eq!(primary_port, DEFAULT_TIDB_PRIMARY_PORT);
    }

    #[test]
    fn parse_tidb_host_and_primary_with_explicit_port() {
        let (host, primary_port) =
            TiDBManagerTopologyFetcher::parse_tidb_host_and_primary("10.0.0.1", Some(4200))
                .unwrap();
        assert_eq!(host, "10.0.0.1");
        assert_eq!(primary_port, 4200);
    }

    #[test]
    fn build_endpoint_url_with_namespaces() {
        let endpoint = TiDBManagerTopologyFetcher::build_active_tidb_endpoint_url(
            "http://manager:8080",
            "super-vip-tidb-pool,canary-super-vip-tidb-pool",
        );
        assert_eq!(
            endpoint,
            "http://manager:8080/api/tidb/get_active_tidb?namespace=super-vip-tidb-pool,canary-super-vip-tidb-pool"
        );
    }

    #[test]
    fn build_endpoint_url_with_full_path() {
        let endpoint = TiDBManagerTopologyFetcher::build_active_tidb_endpoint_url(
            "http://manager:8080/api/tidb/get_active_tidb",
            "super-vip-tidb-pool,canary-super-vip-tidb-pool",
        );
        assert_eq!(
            endpoint,
            "http://manager:8080/api/tidb/get_active_tidb?namespace=super-vip-tidb-pool,canary-super-vip-tidb-pool"
        );
    }

    #[test]
    fn normalize_namespaces_none_or_empty() {
        assert_eq!(TiDBManagerTopologyFetcher::normalize_namespaces(None), None);
        assert_eq!(
            TiDBManagerTopologyFetcher::normalize_namespaces(Some("")),
            None
        );
        assert_eq!(
            TiDBManagerTopologyFetcher::normalize_namespaces(Some("  ,   ")),
            None
        );
    }

    #[test]
    fn normalize_namespaces_trim_and_filter() {
        let normalized = TiDBManagerTopologyFetcher::normalize_namespaces(Some(
            " super-vip-tidb-pool, canary-super-vip-tidb-pool , ",
        ));
        assert_eq!(
            normalized.as_deref(),
            Some("super-vip-tidb-pool,canary-super-vip-tidb-pool")
        );
    }

    #[test]
    fn manager_shard_config_is_disabled_when_envs_are_missing() {
        assert_eq!(
            ManagerShardConfig::from_env_values(None, None).unwrap(),
            None
        );
    }

    #[test]
    fn manager_shard_config_requires_both_envs() {
        let err = ManagerShardConfig::from_env_values(Some("3"), None)
            .expect_err("expected missing sts id to fail");
        assert!(matches!(err, FetchError::InvalidShardConfig { .. }));

        let err = ManagerShardConfig::from_env_values(None, Some("1"))
            .expect_err("expected missing replica count to fail");
        assert!(matches!(err, FetchError::InvalidShardConfig { .. }));
    }

    #[test]
    fn manager_shard_config_validates_values() {
        let err = ManagerShardConfig::from_env_values(Some("0"), Some("0"))
            .expect_err("expected zero replica count to fail");
        assert!(matches!(err, FetchError::InvalidShardConfig { .. }));

        let err = ManagerShardConfig::from_env_values(Some("2"), Some("2"))
            .expect_err("expected sts id overflow to fail");
        assert!(matches!(err, FetchError::InvalidShardConfig { .. }));

        let err = ManagerShardConfig::from_env_values(Some("abc"), Some("1"))
            .expect_err("expected invalid replica count to fail");
        assert!(matches!(err, FetchError::InvalidShardConfig { .. }));
    }

    #[test]
    fn hash_keyspace_name_is_stable() {
        assert_eq!(
            TiDBManagerTopologyFetcher::hash_keyspace_name("tenant-a"),
            14046587775414411003
        );
        assert_eq!(
            TiDBManagerTopologyFetcher::hash_keyspace_name("tenant-b"),
            14046588874926039214
        );
    }

    #[test]
    fn filter_active_tidb_addresses_by_keyspace_shard() {
        let addresses = vec![
            ActiveTiDBAddress {
                host: "10.0.0.1".to_owned(),
                port: Some(4000),
                status_port: Some(10080),
                hostname: Some("tidb-0".to_owned()),
                keyspace_name: Some("tenant-a".to_owned()),
            },
            ActiveTiDBAddress {
                host: "10.0.0.2".to_owned(),
                port: Some(4000),
                status_port: Some(10080),
                hostname: Some("tidb-1".to_owned()),
                keyspace_name: Some("tenant-b".to_owned()),
            },
            ActiveTiDBAddress {
                host: "10.0.0.3".to_owned(),
                port: Some(4000),
                status_port: Some(10080),
                hostname: Some("tidb-2".to_owned()),
                keyspace_name: None,
            },
        ];

        let filtered = TiDBManagerTopologyFetcher::filter_active_tidb_addresses(
            addresses,
            Some(ManagerShardConfig {
                replica_count: 4,
                sts_id: 2,
            }),
        )
        .unwrap();

        assert_eq!(
            filtered,
            vec![ActiveTiDBAddress {
                host: "10.0.0.2".to_owned(),
                port: Some(4000),
                status_port: Some(10080),
                hostname: Some("tidb-1".to_owned()),
                keyspace_name: Some("tenant-b".to_owned()),
            }]
        );
    }

    #[tokio::test]
    async fn read_response_body_with_limit_rejects_oversized_response() {
        let body = Body::from(vec![b'x'; MAX_MANAGER_RESPONSE_BYTES + 1]);
        let err = TiDBManagerTopologyFetcher::read_response_body_with_limit(body)
            .await
            .expect_err("expected oversized response to fail");

        assert!(matches!(err, FetchError::ActiveTiDBResponseTooLarge { .. }));
    }
}
