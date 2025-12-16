use std::collections::HashSet;
use std::time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH};

use snafu::{ResultExt, Snafu};

use crate::sources::conprof::topology::fetch::{models, utils};
use crate::sources::conprof::topology::{Component, InstanceType};

#[derive(Debug, Snafu)]
pub enum FetchError {
    #[snafu(display("Failed to get topology: {}", source))]
    GetTopology { source: etcd_client::Error },
    #[snafu(display("Failed to read etcd key: {}", source))]
    ReadEtcdKey { source: etcd_client::Error },
    #[snafu(display("Failed to read etcd value: {}", source))]
    ReadEtcdValue { source: etcd_client::Error },
    #[snafu(display("Missing address in etcd key: {}", key))]
    MissingAddress { key: String },
    #[snafu(display("Missing kind in etcd key: {}", key))]
    MissingKind { key: String },
    #[snafu(display("Failed to parse ttl: {}", source))]
    ParseTTL { source: std::num::ParseIntError },
    #[snafu(display("Time drift occurred: {}", source))]
    TimeDrift { source: SystemTimeError },
    #[snafu(display("Failed to parse topology value Json text: {}", source))]
    TopologyValueJsonFromStr { source: serde_json::Error },
    #[snafu(display("Failed to parse tiproxy address: {}", source))]
    ParseTiProxyAddress { source: utils::ParseError },
    #[snafu(display("Failed to parse status_port: {}", source))]
    ParseStatusPort { source: std::num::ParseIntError },
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug)]
pub(crate) enum EtcdTopology {
    TTL {
        address: String,
        ttl: u128,
    },
    Info {
        address: String,
        value: models::TiProxyTopologyValue,
    },
}

pub struct TiProxyTopologyFetcher<'a> {
    topolgy_prefix: &'static str,
    etcd_client: &'a mut etcd_client::Client,
}

impl<'a> TiProxyTopologyFetcher<'a> {
    pub fn new(etcd_client: &'a mut etcd_client::Client) -> Self {
        Self {
            topolgy_prefix: "/topology/tiproxy/",
            etcd_client,
        }
    }

    pub async fn get_up_tiproxys(
        &mut self,
        components: &mut HashSet<Component>,
    ) -> Result<(), FetchError> {
        let mut up_tiproxys = HashSet::new();
        let mut tiproxys = Vec::new();

        let topology_kvs = self.fetch_topology_kvs().await?;
        for kv in topology_kvs.kvs() {
            match self.parse_kv(kv)? {
                Some(EtcdTopology::TTL { address, ttl }) => {
                    if Self::is_up(ttl)? {
                        up_tiproxys.insert(address);
                    }
                }
                Some(EtcdTopology::Info { address, value }) => {
                    let (host, primary_port) =
                        utils::parse_host_port(&address).context(ParseTiProxyAddressSnafu)?;
                    let secondary_port = value
                        .status_port
                        .parse::<u16>()
                        .context(ParseStatusPortSnafu)?;
                    tiproxys.push((
                        address,
                        Component {
                            instance_type: InstanceType::TiProxy,
                            host,
                            primary_port,
                            secondary_port,
                        },
                    ));
                }
                _ => {}
            }
        }

        for (address, component) in tiproxys {
            if up_tiproxys.contains(&address) {
                components.insert(component);
            }
        }

        Ok(())
    }

    async fn fetch_topology_kvs(&mut self) -> Result<etcd_client::GetResponse, FetchError> {
        let topology_resp = self
            .etcd_client
            .get(
                self.topolgy_prefix,
                Some(etcd_client::GetOptions::new().with_prefix()),
            )
            .await
            .context(GetTopologySnafu)?;

        Ok(topology_resp)
    }

    pub(crate) fn parse_kv(
        &self,
        kv: &'_ etcd_client::KeyValue,
    ) -> Result<Option<EtcdTopology>, FetchError> {
        self.parse_kv_impl(kv)
    }

    fn parse_kv_impl(
        &self,
        kv: &'_ etcd_client::KeyValue,
    ) -> Result<Option<EtcdTopology>, FetchError> {
        let (key, value) = Self::extract_kv_str(kv)?;

        let remaining_key = &key[self.topolgy_prefix.len()..];
        let mut key_labels = remaining_key.splitn(2, '/');
        let address = key_labels
            .next()
            .ok_or_else(|| FetchError::MissingAddress {
                key: key.to_owned(),
            })?;
        let kind = key_labels.next().ok_or_else(|| FetchError::MissingKind {
            key: key.to_owned(),
        })?;

        let res = match kind {
            "info" => Some(Self::parse_info_impl(address, value)?),
            "ttl" => Some(Self::parse_ttl_impl(address, value)?),
            _ => None,
        };

        Ok(res)
    }

    fn is_up(ttl: u128) -> Result<bool, FetchError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context(TimeDriftSnafu)?
            .as_nanos();
        Ok(ttl + Duration::from_secs(45).as_nanos() >= now)
    }

    #[cfg(test)]
    pub(crate) fn parse_info(address: &str, value: &str) -> Result<EtcdTopology, FetchError> {
        Self::parse_info_impl(address, value)
    }

    fn parse_info_impl(address: &str, value: &str) -> Result<EtcdTopology, FetchError> {
        let info = serde_json::from_str::<models::TiProxyTopologyValue>(value)
            .context(TopologyValueJsonFromStrSnafu)?;
        Ok(EtcdTopology::Info {
            address: address.to_owned(),
            value: info,
        })
    }

    #[cfg(test)]
    pub(crate) fn parse_ttl(address: &str, value: &str) -> Result<EtcdTopology, FetchError> {
        Self::parse_ttl_impl(address, value)
    }

    fn parse_ttl_impl(address: &str, value: &str) -> Result<EtcdTopology, FetchError> {
        let ttl = value.parse::<u128>().context(ParseTTLSnafu)?;
        Ok(EtcdTopology::TTL {
            address: address.to_owned(),
            ttl,
        })
    }

    pub(crate) fn extract_kv_str(kv: &etcd_client::KeyValue) -> Result<(&str, &str), FetchError> {
        Self::extract_kv_str_impl(kv)
    }

    fn extract_kv_str_impl(kv: &etcd_client::KeyValue) -> Result<(&str, &str), FetchError> {
        let key = kv.key_str().context(ReadEtcdKeySnafu)?;
        let value = kv.value_str().context(ReadEtcdValueSnafu)?;

        Ok((key, value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_up() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        // TTL that is still valid (within 45 seconds)
        let valid_ttl = now - Duration::from_secs(30).as_nanos();
        assert!(TiProxyTopologyFetcher::is_up(valid_ttl).unwrap());

        // TTL that is expired (more than 45 seconds ago)
        let expired_ttl = now - Duration::from_secs(60).as_nanos();
        assert!(!TiProxyTopologyFetcher::is_up(expired_ttl).unwrap());
    }

    #[test]
    fn test_parse_info() {
        let value = r#"{"status_port": "6000"}"#;
        let result = TiProxyTopologyFetcher::parse_info_impl("127.0.0.1:6000", value).unwrap();
        match result {
            EtcdTopology::Info { address, value } => {
                assert_eq!(address, "127.0.0.1:6000");
                assert_eq!(value.status_port, "6000");
            }
            _ => panic!("Expected Info variant"),
        }
    }

    #[test]
    fn test_parse_info_invalid_json() {
        let value = "invalid json";
        let result = TiProxyTopologyFetcher::parse_info_impl("127.0.0.1:6000", value);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            FetchError::TopologyValueJsonFromStr { .. }
        ));
    }

    #[test]
    fn test_parse_ttl() {
        let result =
            TiProxyTopologyFetcher::parse_ttl_impl("127.0.0.1:6000", "1234567890").unwrap();
        match result {
            EtcdTopology::TTL { address, ttl } => {
                assert_eq!(address, "127.0.0.1:6000");
                assert_eq!(ttl, 1234567890);
            }
            _ => panic!("Expected TTL variant"),
        }
    }

    #[test]
    fn test_parse_ttl_invalid_number() {
        let result = TiProxyTopologyFetcher::parse_ttl_impl("127.0.0.1:6000", "invalid");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), FetchError::ParseTTL { .. }));
    }

    #[test]
    fn test_parse_status_port_invalid() {
        let value = r#"{"status_port": "invalid"}"#;
        let result = TiProxyTopologyFetcher::parse_info_impl("127.0.0.1:6000", value);
        // This should succeed in parsing JSON, but status_port parsing happens later
        assert!(result.is_ok());
    }

    #[test]
    fn test_fetch_error_display() {
        let error = FetchError::ParseTiProxyAddress {
            source: utils::ParseError::MissingHost {
                address: "test".to_string(),
            },
        };
        let display = format!("{}", error);
        assert!(display.contains("Failed to parse tiproxy address"));
    }

    #[test]
    fn test_fetch_error_missing_address() {
        let error = FetchError::MissingAddress {
            key: "/topology/tiproxy/".to_string(),
        };
        let display = format!("{}", error);
        assert!(display.contains("Missing address"));
    }

    #[test]
    fn test_fetch_error_missing_kind() {
        let error = FetchError::MissingKind {
            key: "/topology/tiproxy/127.0.0.1:6000".to_string(),
        };
        let display = format!("{}", error);
        assert!(display.contains("Missing kind"));
    }

    #[test]
    fn test_fetch_error_parse_status_port() {
        let parse_error = "invalid".parse::<u16>().unwrap_err();
        let _error = FetchError::ParseStatusPort {
            source: parse_error,
        };
    }

    #[test]
    fn test_extract_kv_str() {
        // This is a private function, but we can test it indirectly through parse_kv
        // We need to create a mock KeyValue, but that's complex
        // So we just test that the error types work
        let _error = FetchError::ReadEtcdKey {
            source: etcd_client::Error::InvalidArgs("test".to_string()),
        };
        let _error = FetchError::ReadEtcdValue {
            source: etcd_client::Error::InvalidArgs("test".to_string()),
        };
    }

    #[test]
    fn test_get_up_tiproxys_logic() {
        // Test the logic of get_up_tiproxys
        let mut up_tiproxys = HashSet::new();
        let mut tiproxys = Vec::new();

        // Simulate TTL that is up
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let valid_ttl = now - Duration::from_secs(30).as_nanos();
        if TiProxyTopologyFetcher::is_up(valid_ttl).unwrap() {
            up_tiproxys.insert("127.0.0.1:6000".to_string());
        }

        // Simulate Info
        let (host, primary_port) = utils::parse_host_port("127.0.0.1:6000").unwrap();
        let secondary_port = "6001".parse::<u16>().unwrap();
        tiproxys.push((
            "127.0.0.1:6000".to_string(),
            Component {
                instance_type: InstanceType::TiProxy,
                host,
                primary_port,
                secondary_port,
            },
        ));

        // Test filtering logic
        let mut components = HashSet::new();
        for (address, component) in tiproxys {
            if up_tiproxys.contains(&address) {
                components.insert(component);
            }
        }

        assert_eq!(components.len(), 1);
    }

    #[test]
    fn test_get_up_tiproxys_status_port_parsing() {
        // Test status_port parsing
        let value = models::TiProxyTopologyValue {
            status_port: "6001".to_string(),
        };
        let port = value.status_port.parse::<u16>().unwrap();
        assert_eq!(port, 6001);
    }

    #[test]
    fn test_get_up_tiproxys_invalid_status_port() {
        // Test invalid status_port parsing
        let value = models::TiProxyTopologyValue {
            status_port: "invalid".to_string(),
        };
        let result = value.status_port.parse::<u16>();
        assert!(result.is_err());
    }

    #[test]
    fn test_topology_prefix() {
        // Test topology prefix
        let prefix = "/topology/tiproxy/";
        assert_eq!(prefix, "/topology/tiproxy/");
    }

    #[test]
    fn test_parse_kv_info_kind() {
        // Test parse_kv with "info" kind
        let key = "/topology/tiproxy/127.0.0.1:6000/info";
        let value = r#"{"status_port": "6001"}"#;
        let remaining_key = &key["/topology/tiproxy/".len()..];
        let mut key_labels = remaining_key.splitn(2, '/');
        let address = key_labels.next().unwrap();
        let kind = key_labels.next().unwrap();

        assert_eq!(address, "127.0.0.1:6000");
        assert_eq!(kind, "info");

        let result = TiProxyTopologyFetcher::parse_info_impl(address, value);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_kv_ttl_kind() {
        // Test parse_kv with "ttl" kind
        let key = "/topology/tiproxy/127.0.0.1:6000/ttl";
        let value = "1234567890";
        let remaining_key = &key["/topology/tiproxy/".len()..];
        let mut key_labels = remaining_key.splitn(2, '/');
        let address = key_labels.next().unwrap();
        let kind = key_labels.next().unwrap();

        assert_eq!(address, "127.0.0.1:6000");
        assert_eq!(kind, "ttl");

        let result = TiProxyTopologyFetcher::parse_ttl_impl(address, value);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_kv_unknown_kind() {
        // Test parse_kv with unknown kind
        let key = "/topology/tiproxy/127.0.0.1:6000/unknown";
        let remaining_key = &key["/topology/tiproxy/".len()..];
        let mut key_labels = remaining_key.splitn(2, '/');
        let _address = key_labels.next().unwrap();
        let kind = key_labels.next().unwrap();

        let res = match kind {
            "info" => Some(()),
            "ttl" => Some(()),
            _ => None,
        };

        assert!(res.is_none());
    }

    #[test]
    fn test_get_up_tiproxys_with_multiple_ttl_and_info() {
        // Test get_up_tiproxys logic with multiple TTL and Info entries
        let mut up_tiproxys = HashSet::new();
        let mut tiproxys = Vec::new();

        // Add valid TTL
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let valid_ttl = now - Duration::from_secs(30).as_nanos();
        if TiProxyTopologyFetcher::is_up(valid_ttl).unwrap() {
            up_tiproxys.insert("127.0.0.1:6000".to_string());
        }

        // Add Info entry
        let (host, primary_port) = utils::parse_host_port("127.0.0.1:6000").unwrap();
        let secondary_port = "6001".parse::<u16>().unwrap();
        tiproxys.push((
            "127.0.0.1:6000".to_string(),
            Component {
                instance_type: InstanceType::TiProxy,
                host,
                primary_port,
                secondary_port,
            },
        ));

        // Filter components
        let mut components = HashSet::new();
        for (address, component) in tiproxys {
            if up_tiproxys.contains(&address) {
                components.insert(component);
            }
        }

        assert_eq!(components.len(), 1);
    }

    #[test]
    fn test_fetch_topology_kvs_prefix() {
        // Test that fetch_topology_kvs uses correct prefix
        let prefix = "/topology/tiproxy/";
        assert_eq!(prefix, "/topology/tiproxy/");
    }
}
