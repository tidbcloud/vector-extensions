mod fetch;

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

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct Component {
    pub instance_type: InstanceType,
    pub host: String,
    pub primary_port: u16,
    pub secondary_port: u16,
}

impl Component {
    pub fn topsql_address(&self) -> Option<String> {
        match self.instance_type {
            InstanceType::TiDB => Some(format!("{}:{}", self.host, self.secondary_port)),
            InstanceType::TiKV => Some(format!("{}:{}", self.host, self.primary_port)),
            _ => None,
        }
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
