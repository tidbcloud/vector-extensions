use std::collections::HashSet;

use serde_json::{Map, Value};
use snafu::{ResultExt, Snafu};
use vector::http::HttpClient;

use crate::common::topology::fetch::utils;
use crate::common::topology::{Component, InstanceType};

const GET_ACTIVE_TIDB_PATH: &str = "/api/tidb/get_active_tidb";
const DEFAULT_TIDB_PRIMARY_PORT: u16 = 4000;
const DEFAULT_TIDB_STATUS_PORT: u16 = 10080;
const MAX_RESPONSE_DEPTH: usize = 8;

#[derive(Debug, Snafu)]
pub enum FetchError {
    #[snafu(display("Failed to build request: {}", source))]
    BuildRequest { source: http::Error },
    #[snafu(display("Failed to get active tidb addresses from manager server: {}", source))]
    GetActiveTiDBs { source: vector::http::HttpError },
    #[snafu(display("Failed to read active tidb response bytes: {}", source))]
    GetActiveTiDBsBytes { source: hyper::Error },
    #[snafu(display("Failed to parse active tidb response JSON text: {}", source))]
    ActiveTiDBJsonFromStr { source: serde_json::Error },
    #[snafu(display("Invalid manager server response: {}", message))]
    InvalidManagerResponse { message: String },
    #[snafu(display("Failed to parse tidb host from manager response: {}", source))]
    ParseTiDBHost { source: utils::ParseError },
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct ActiveTiDBAddress {
    host: String,
    port: Option<u16>,
    status_port: Option<u16>,
}

pub struct TiDBManagerTopologyFetcher<'a> {
    manager_server_address: &'a str,
    tidb_namespace: Option<&'a str>,
    http_client: &'a HttpClient<hyper::Body>,
}

impl<'a> TiDBManagerTopologyFetcher<'a> {
    pub fn new(
        manager_server_address: &'a str,
        tidb_namespace: Option<&'a str>,
        http_client: &'a HttpClient<hyper::Body>,
    ) -> Self {
        Self {
            manager_server_address,
            tidb_namespace,
            http_client,
        }
    }

    pub async fn get_up_tidbs(
        &self,
        components: &mut HashSet<Component>,
    ) -> Result<(), FetchError> {
        let active_tidb_addresses = self.fetch_active_tidb_addresses().await?;
        if !active_tidb_addresses.is_empty() {
            info!(
                message = "Fetched active TiDB instances from manager server",
                manager_server_address = self.manager_server_address,
                tidb_namespace = ?self.tidb_namespace,
                tidb_count = active_tidb_addresses.len()
            );
        }

        for active_tidb in active_tidb_addresses {
            let (host, primary_port) =
                Self::parse_tidb_host_and_primary(&active_tidb.host, active_tidb.port)?;
            let secondary_port = active_tidb.status_port.unwrap_or(DEFAULT_TIDB_STATUS_PORT);

            components.insert(Component {
                instance_type: InstanceType::TiDB,
                host,
                primary_port,
                secondary_port,
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
        let bytes = hyper::body::to_bytes(res.into_body())
            .await
            .context(GetActiveTiDBsBytesSnafu)?;

        Self::parse_active_tidb_addresses_response(&bytes)
    }

    fn active_tidb_endpoint_url(&self) -> Option<String> {
        let namespaces = Self::normalize_namespaces(self.tidb_namespace)?;
        Some(Self::build_active_tidb_endpoint_url(
            self.manager_server_address,
            &namespaces,
        ))
    }

    fn normalize_namespaces(namespaces: Option<&str>) -> Option<String> {
        let namespaces = namespaces?;
        let normalized = namespaces
            .split(',')
            .map(str::trim)
            .filter(|ns| !ns.is_empty())
            .collect::<Vec<_>>();
        if normalized.is_empty() {
            None
        } else {
            Some(normalized.join(","))
        }
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

        Some(ActiveTiDBAddress {
            host,
            port,
            status_port,
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_response_new_schema() {
        let bytes = br#"[
            {"host":"10.0.0.1","port":4000,"status_port":10080},
            {"host":"10.0.0.2","port":4000,"status_port":10080}
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
                },
                ActiveTiDBAddress {
                    host: "10.0.0.2".to_owned(),
                    port: Some(4000),
                    status_port: Some(10080),
                }
            ]
        );
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
}
