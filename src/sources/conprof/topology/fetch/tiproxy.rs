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
enum EtcdTopology {
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

    fn parse_kv(&self, kv: &'_ etcd_client::KeyValue) -> Result<Option<EtcdTopology>, FetchError> {
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
            "info" => Some(Self::parse_info(address, value)?),
            "ttl" => Some(Self::parse_ttl(address, value)?),
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

    fn parse_info(address: &str, value: &str) -> Result<EtcdTopology, FetchError> {
        let info = serde_json::from_str::<models::TiProxyTopologyValue>(value)
            .context(TopologyValueJsonFromStrSnafu)?;
        Ok(EtcdTopology::Info {
            address: address.to_owned(),
            value: info,
        })
    }

    fn parse_ttl(address: &str, value: &str) -> Result<EtcdTopology, FetchError> {
        let ttl = value.parse::<u128>().context(ParseTTLSnafu)?;
        Ok(EtcdTopology::TTL {
            address: address.to_owned(),
            ttl,
        })
    }

    fn extract_kv_str(kv: &etcd_client::KeyValue) -> Result<(&str, &str), FetchError> {
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
        let result = TiProxyTopologyFetcher::parse_info("127.0.0.1:6000", value).unwrap();
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
        let result = TiProxyTopologyFetcher::parse_info("127.0.0.1:6000", value);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), FetchError::TopologyValueJsonFromStr { .. }));
    }

    #[test]
    fn test_parse_ttl() {
        let result = TiProxyTopologyFetcher::parse_ttl("127.0.0.1:6000", "1234567890").unwrap();
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
        let result = TiProxyTopologyFetcher::parse_ttl("127.0.0.1:6000", "invalid");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), FetchError::ParseTTL { .. }));
    }
}
