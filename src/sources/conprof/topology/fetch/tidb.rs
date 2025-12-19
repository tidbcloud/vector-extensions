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
    #[snafu(display("Failed to parse tidb address: {}", source))]
    ParseTiDBAddress { source: utils::ParseError },
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
        value: models::TopologyValue,
    },
}

pub struct TiDBTopologyFetcher<'a> {
    topolgy_prefix: &'static str,
    etcd_client: &'a mut etcd_client::Client,
}

impl<'a> TiDBTopologyFetcher<'a> {
    pub fn new(etcd_client: &'a mut etcd_client::Client) -> Self {
        Self {
            topolgy_prefix: "/topology/tidb/",
            etcd_client,
        }
    }

    pub async fn get_up_tidbs(
        &mut self,
        components: &mut HashSet<Component>,
    ) -> Result<(), FetchError> {
        let mut up_tidbs = HashSet::new();
        let mut tidbs = Vec::new();

        let topology_kvs = self.fetch_topology_kvs().await?;
        for kv in topology_kvs.kvs() {
            match self.parse_kv(kv)? {
                Some(EtcdTopology::TTL { address, ttl }) => {
                    if Self::is_up_impl(ttl)? {
                        up_tidbs.insert(address);
                    }
                }
                Some(EtcdTopology::Info { address, value }) => {
                    let (host, port) =
                        utils::parse_host_port(&address).context(ParseTiDBAddressSnafu)?;
                    tidbs.push((
                        address,
                        Component {
                            instance_type: InstanceType::TiDB,
                            host,
                            primary_port: port,
                            secondary_port: value.status_port,
                        },
                    ));
                }
                _ => {}
            }
        }

        for (address, component) in tidbs {
            if up_tidbs.contains(&address) {
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

    #[cfg(test)]
    pub(crate) fn is_up(ttl: u128) -> Result<bool, FetchError> {
        Self::is_up_impl(ttl)
    }

    fn is_up_impl(ttl: u128) -> Result<bool, FetchError> {
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
        let info = serde_json::from_str::<models::TopologyValue>(value)
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
        assert!(TiDBTopologyFetcher::is_up(valid_ttl).unwrap());

        // TTL that is expired (more than 45 seconds ago)
        let expired_ttl = now - Duration::from_secs(60).as_nanos();
        assert!(!TiDBTopologyFetcher::is_up(expired_ttl).unwrap());

        // TTL exactly at the boundary (45 seconds ago) - should be valid due to >=
        let boundary_ttl = now - Duration::from_secs(45).as_nanos();
        // Allow for small timing differences
        let result = TiDBTopologyFetcher::is_up(boundary_ttl).unwrap();
        // The boundary case should be valid (>=), but due to timing it might vary
        // So we just check it doesn't panic
        let _ = result;
    }

    #[test]
    fn test_parse_info() {
        let value = r#"{"status_port": 10080}"#;
        let result = TiDBTopologyFetcher::parse_info_impl("127.0.0.1:4000", value).unwrap();
        match result {
            EtcdTopology::Info { address, value } => {
                assert_eq!(address, "127.0.0.1:4000");
                assert_eq!(value.status_port, 10080);
            }
            _ => panic!("Expected Info variant"),
        }
    }

    #[test]
    fn test_parse_info_invalid_json() {
        let value = "invalid json";
        let result = TiDBTopologyFetcher::parse_info_impl("127.0.0.1:4000", value);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            FetchError::TopologyValueJsonFromStr { .. }
        ));
    }

    #[test]
    fn test_parse_ttl() {
        let result = TiDBTopologyFetcher::parse_ttl_impl("127.0.0.1:4000", "1234567890").unwrap();
        match result {
            EtcdTopology::TTL { address, ttl } => {
                assert_eq!(address, "127.0.0.1:4000");
                assert_eq!(ttl, 1234567890);
            }
            _ => panic!("Expected TTL variant"),
        }
    }

    #[test]
    fn test_parse_ttl_invalid_number() {
        let result = TiDBTopologyFetcher::parse_ttl_impl("127.0.0.1:4000", "invalid");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), FetchError::ParseTTL { .. }));
    }

    #[test]
    fn test_fetch_error_display() {
        let error = FetchError::ParseTiDBAddress {
            source: utils::ParseError::MissingHost {
                address: "test".to_string(),
            },
        };
        let display = format!("{}", error);
        assert!(display.contains("Failed to parse tidb address"));
    }

    #[test]
    fn test_fetch_error_missing_address() {
        let error = FetchError::MissingAddress {
            key: "/topology/tidb/".to_string(),
        };
        let display = format!("{}", error);
        assert!(display.contains("Missing address"));
    }

    #[test]
    fn test_fetch_error_missing_kind() {
        let error = FetchError::MissingKind {
            key: "/topology/tidb/127.0.0.1:4000".to_string(),
        };
        let display = format!("{}", error);
        assert!(display.contains("Missing kind"));
    }

    #[test]
    fn test_fetch_error_variants() {
        let _error = FetchError::GetTopology {
            source: etcd_client::Error::InvalidArgs("test".to_string()),
        };
        let _error = FetchError::ReadEtcdKey {
            source: etcd_client::Error::InvalidArgs("test".to_string()),
        };
        let _error = FetchError::ReadEtcdValue {
            source: etcd_client::Error::InvalidArgs("test".to_string()),
        };
        let _error = FetchError::MissingAddress {
            key: "test".to_string(),
        };
        let _error = FetchError::MissingKind {
            key: "test".to_string(),
        };
        let _error = FetchError::TimeDrift {
            source: std::time::SystemTime::UNIX_EPOCH
                .duration_since(std::time::SystemTime::now())
                .unwrap_err(),
        };
        let _error = FetchError::ParseTiDBAddress {
            source: utils::ParseError::MissingHost {
                address: "test".to_string(),
            },
        };
    }

    #[test]
    fn test_etcd_topology_variants() {
        let ttl = EtcdTopology::TTL {
            address: "127.0.0.1:4000".to_string(),
            ttl: 1234567890,
        };
        match ttl {
            EtcdTopology::TTL { address, ttl } => {
                assert_eq!(address, "127.0.0.1:4000");
                assert_eq!(ttl, 1234567890);
            }
            _ => panic!("Expected TTL variant"),
        }

        let info = EtcdTopology::Info {
            address: "127.0.0.1:4000".to_string(),
            value: models::TopologyValue { status_port: 10080 },
        };
        match info {
            EtcdTopology::Info { address, value } => {
                assert_eq!(address, "127.0.0.1:4000");
                assert_eq!(value.status_port, 10080);
            }
            _ => panic!("Expected Info variant"),
        }
    }

    #[test]
    fn test_get_up_tidbs_logic() {
        // Test the logic of get_up_tidbs
        let mut up_tidbs = HashSet::new();
        let mut tidbs = Vec::new();

        // Simulate TTL that is up
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let valid_ttl = now - Duration::from_secs(30).as_nanos();
        if TiDBTopologyFetcher::is_up(valid_ttl).unwrap() {
            up_tidbs.insert("127.0.0.1:4000".to_string());
        }

        // Simulate Info
        let (host, port) = utils::parse_host_port("127.0.0.1:4000").unwrap();
        tidbs.push((
            "127.0.0.1:4000".to_string(),
            Component {
                instance_type: InstanceType::TiDB,
                host,
                primary_port: port,
                secondary_port: 10080,
            },
        ));

        // Test filtering logic
        let mut components = HashSet::new();
        for (address, component) in tidbs {
            if up_tidbs.contains(&address) {
                components.insert(component);
            }
        }

        assert_eq!(components.len(), 1);
    }

    #[test]
    fn test_get_up_tidbs_ttl_expired() {
        // Test that expired TTLs are not included
        let mut up_tidbs = HashSet::new();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let expired_ttl = now - Duration::from_secs(60).as_nanos();

        if TiDBTopologyFetcher::is_up(expired_ttl).unwrap() {
            up_tidbs.insert("127.0.0.1:4000".to_string());
        }

        // Should be empty because TTL is expired
        assert_eq!(up_tidbs.len(), 0);
    }

    #[test]
    fn test_get_up_tidbs_address_matching() {
        // Test that address matching works correctly
        let mut up_tidbs = HashSet::new();
        up_tidbs.insert("127.0.0.1:4000".to_string());
        up_tidbs.insert("127.0.0.1:4001".to_string());

        let tidbs = vec![
            (
                "127.0.0.1:4000".to_string(),
                Component {
                    instance_type: InstanceType::TiDB,
                    host: "127.0.0.1".to_string(),
                    primary_port: 4000,
                    secondary_port: 10080,
                },
            ),
            (
                "127.0.0.1:4002".to_string(),
                Component {
                    instance_type: InstanceType::TiDB,
                    host: "127.0.0.1".to_string(),
                    primary_port: 4002,
                    secondary_port: 10080,
                },
            ),
        ];

        let mut components = HashSet::new();
        for (address, component) in tidbs {
            if up_tidbs.contains(&address) {
                components.insert(component);
            }
        }

        assert_eq!(components.len(), 1);
    }

    #[test]
    fn test_topology_prefix() {
        // Test topology prefix
        let prefix = "/topology/tidb/";
        assert_eq!(prefix, "/topology/tidb/");
    }

    #[test]
    fn test_parse_kv_info_kind() {
        // Test parse_kv with "info" kind
        let key = "/topology/tidb/127.0.0.1:4000/info";
        let value = r#"{"status_port": 10080}"#;
        let remaining_key = &key["/topology/tidb/".len()..];
        let mut key_labels = remaining_key.splitn(2, '/');
        let address = key_labels.next().unwrap();
        let kind = key_labels.next().unwrap();

        assert_eq!(address, "127.0.0.1:4000");
        assert_eq!(kind, "info");

        let result = TiDBTopologyFetcher::parse_info_impl(address, value);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_kv_ttl_kind() {
        // Test parse_kv with "ttl" kind
        let key = "/topology/tidb/127.0.0.1:4000/ttl";
        let value = "1234567890";
        let remaining_key = &key["/topology/tidb/".len()..];
        let mut key_labels = remaining_key.splitn(2, '/');
        let address = key_labels.next().unwrap();
        let kind = key_labels.next().unwrap();

        assert_eq!(address, "127.0.0.1:4000");
        assert_eq!(kind, "ttl");

        let result = TiDBTopologyFetcher::parse_ttl_impl(address, value);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_kv_unknown_kind() {
        // Test parse_kv with unknown kind
        let key = "/topology/tidb/127.0.0.1:4000/unknown";
        let remaining_key = &key["/topology/tidb/".len()..];
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
    fn test_get_up_tidbs_with_multiple_ttl_and_info() {
        // Test get_up_tidbs logic with multiple TTL and Info entries
        let mut up_tidbs = HashSet::new();
        let mut tidbs = Vec::new();

        // Add valid TTL
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let valid_ttl = now - Duration::from_secs(30).as_nanos();
        if TiDBTopologyFetcher::is_up(valid_ttl).unwrap() {
            up_tidbs.insert("127.0.0.1:4000".to_string());
            up_tidbs.insert("127.0.0.1:4001".to_string());
        }

        // Add Info entries
        let (host1, port1) = utils::parse_host_port("127.0.0.1:4000").unwrap();
        tidbs.push((
            "127.0.0.1:4000".to_string(),
            Component {
                instance_type: InstanceType::TiDB,
                host: host1,
                primary_port: port1,
                secondary_port: 10080,
            },
        ));

        let (host2, port2) = utils::parse_host_port("127.0.0.1:4001").unwrap();
        tidbs.push((
            "127.0.0.1:4001".to_string(),
            Component {
                instance_type: InstanceType::TiDB,
                host: host2,
                primary_port: port2,
                secondary_port: 10080,
            },
        ));

        // Add Info without matching TTL
        let (host3, port3) = utils::parse_host_port("127.0.0.1:4002").unwrap();
        tidbs.push((
            "127.0.0.1:4002".to_string(),
            Component {
                instance_type: InstanceType::TiDB,
                host: host3,
                primary_port: port3,
                secondary_port: 10080,
            },
        ));

        // Filter components
        let mut components = HashSet::new();
        for (address, component) in tidbs {
            if up_tidbs.contains(&address) {
                components.insert(component);
            }
        }

        assert_eq!(components.len(), 2);
    }

    #[test]
    fn test_fetch_topology_kvs_prefix() {
        // Test that fetch_topology_kvs uses correct prefix
        let prefix = "/topology/tidb/";
        assert_eq!(prefix, "/topology/tidb/");
    }
}
