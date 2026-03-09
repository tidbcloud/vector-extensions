pub mod fetch;

use std::fmt;

pub use fetch::{FetchError, TopologyFetcher};

#[derive(Debug, Copy, Clone, Eq, Hash, PartialEq)]
pub enum InstanceType {
    PD,
    TiDB,
    TiKV,
    TiFlash,
}

impl fmt::Display for InstanceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InstanceType::PD => write!(f, "pd"),
            InstanceType::TiDB => write!(f, "tidb"),
            InstanceType::TiKV => write!(f, "tikv"),
            InstanceType::TiFlash => write!(f, "tiflash"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Component {
    pub instance_type: InstanceType,
    pub host: String,
    pub primary_port: u16,
    pub secondary_port: u16,
    /// Optional display/upload identifier. When set (e.g. K8s pod name), used for instance
    /// identification in metrics instead of host:port. Connection still uses host.
    pub instance_name: Option<String>,
}

impl PartialEq for Component {
    fn eq(&self, other: &Self) -> bool {
        self.instance_type == other.instance_type
            && self.host == other.host
            && self.primary_port == other.primary_port
            && self.secondary_port == other.secondary_port
    }
}

impl Eq for Component {}

impl std::hash::Hash for Component {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.instance_type.hash(state);
        self.host.hash(state);
        self.primary_port.hash(state);
        self.secondary_port.hash(state);
    }
}

impl Component {
    pub fn topsql_address(&self) -> Option<String> {
        match self.instance_type {
            InstanceType::TiDB => Some(format!("{}:{}", self.host, self.secondary_port)),
            InstanceType::TiKV => Some(format!("{}:{}", self.host, self.primary_port)),
            _ => None,
        }
    }

    /// Instance identifier for metrics/tags. Uses instance_name when set (e.g. K8s pod name),
    /// otherwise falls back to topsql_address (host:port).
    pub fn instance_id(&self) -> String {
        self.instance_name
            .clone()
            .unwrap_or_else(|| self.topsql_address().unwrap_or_default())
    }
}

impl fmt::Display for Component {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}({}:{}, {}:{})",
            self.instance_type, self.host, self.primary_port, self.host, self.secondary_port
        )
    }
}
