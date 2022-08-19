use std::{
    collections::HashSet,
    time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH},
};

use snafu::{ResultExt, Snafu};

use crate::topology::{
    fetch::{models, utils},
    Component, InstanceType,
};

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

enum EtcdTopology {
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
                    if Self::is_up(ttl)? {
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

    fn parse_kv<'b>(
        &self,
        kv: &'b etcd_client::KeyValue,
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
        let info = serde_json::from_str::<models::TopologyValue>(value)
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

    fn extract_kv_str<'b>(kv: &'b etcd_client::KeyValue) -> Result<(&'b str, &'b str), FetchError> {
        let key = kv.key_str().context(ReadEtcdKeySnafu)?;
        let value = kv.value_str().context(ReadEtcdValueSnafu)?;

        Ok((key, value))
    }
}
