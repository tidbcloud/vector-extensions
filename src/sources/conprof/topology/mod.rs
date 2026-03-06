pub mod fetch;

use std::fmt;
use std::str::FromStr;

pub use fetch::FetchError;

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub enum InstanceType {
    PD,
    TiDB,
    TiKV,
    TiFlash,
    TiProxy,
    Lightning,
    /// TiKV worker (separate profile config from TiKV).
    TikvWorker,
    /// Coprocessor worker (separate profile config from TiKV).
    CoprocessorWorker,
    /// Unknown component label (e.g. from K8s). Uses default profile; type name is for display only.
    Other(String),
}

impl fmt::Display for InstanceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InstanceType::PD => write!(f, "pd"),
            InstanceType::TiDB => write!(f, "tidb"),
            InstanceType::TiKV => write!(f, "tikv"),
            InstanceType::TiFlash => write!(f, "tiflash"),
            InstanceType::TiProxy => write!(f, "tiproxy"),
            InstanceType::Lightning => write!(f, "lightning"),
            InstanceType::TikvWorker => write!(f, "tikv_worker"),
            InstanceType::CoprocessorWorker => write!(f, "coprocessor_worker"),
            InstanceType::Other(s) => write!(f, "{}", s),
        }
    }
}

impl FromStr for InstanceType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let normalized = s.to_lowercase().replace('-', "_");
        match normalized.as_str() {
            "pd" => Ok(InstanceType::PD),
            "tidb" => Ok(InstanceType::TiDB),
            "tikv" => Ok(InstanceType::TiKV),
            "tiflash" => Ok(InstanceType::TiFlash),
            "tiproxy" => Ok(InstanceType::TiProxy),
            "lightning" => Ok(InstanceType::Lightning),
            "tikv_worker" => Ok(InstanceType::TikvWorker),
            "coprocessor_worker" => Ok(InstanceType::CoprocessorWorker),
            _ => Err(()),
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
    /// identification in filenames and metadata instead of host:port. Connection still uses host.
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
    pub fn conprof_address(&self) -> Option<String> {
        match &self.instance_type {
            InstanceType::PD => Some(format!("{}:{}", self.host, self.primary_port)),
            InstanceType::TiDB
            | InstanceType::TiKV
            | InstanceType::TiFlash
            | InstanceType::TiProxy
            | InstanceType::Lightning
            | InstanceType::TikvWorker
            | InstanceType::CoprocessorWorker
            | InstanceType::Other(_) => Some(format!("{}:{}", self.host, self.secondary_port)),
        }
    }

    /// Instance identifier for filenames and upload metadata. Uses instance_name when set
    /// (e.g. K8s pod name), otherwise falls back to conprof_address (host:port).
    pub fn instance_id(&self) -> String {
        self.instance_name
            .clone()
            .unwrap_or_else(|| self.conprof_address().unwrap_or_default())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_instance_type_display() {
        assert_eq!(InstanceType::PD.to_string(), "pd");
        assert_eq!(InstanceType::TiDB.to_string(), "tidb");
        assert_eq!(InstanceType::TiKV.to_string(), "tikv");
        assert_eq!(InstanceType::TiFlash.to_string(), "tiflash");
        assert_eq!(InstanceType::TiProxy.to_string(), "tiproxy");
        assert_eq!(InstanceType::Lightning.to_string(), "lightning");
        assert_eq!(InstanceType::TikvWorker.to_string(), "tikv_worker");
        assert_eq!(InstanceType::CoprocessorWorker.to_string(), "coprocessor_worker");
        assert_eq!(
            InstanceType::Other("compute-tiflash".to_string()).to_string(),
            "compute-tiflash"
        );
    }

    #[test]
    fn test_component_display() {
        let component = Component {
            instance_type: InstanceType::TiDB,
            host: "127.0.0.1".to_string(),
            primary_port: 4000,
            secondary_port: 10080,
            instance_name: None,
        };
        assert_eq!(
            component.to_string(),
            "tidb(127.0.0.1:4000, 127.0.0.1:10080)"
        );
    }

    #[test]
    fn test_component_conprof_address_pd() {
        let component = Component {
            instance_type: InstanceType::PD,
            host: "127.0.0.1".to_string(),
            primary_port: 2379,
            secondary_port: 10080,
            instance_name: None,
        };
        assert_eq!(
            component.conprof_address(),
            Some("127.0.0.1:2379".to_string())
        );
    }

    #[test]
    fn test_component_conprof_address_tidb() {
        let component = Component {
            instance_type: InstanceType::TiDB,
            host: "127.0.0.1".to_string(),
            primary_port: 4000,
            secondary_port: 10080,
            instance_name: None,
        };
        assert_eq!(
            component.conprof_address(),
            Some("127.0.0.1:10080".to_string())
        );
    }

    #[test]
    fn test_component_conprof_address_tikv() {
        let component = Component {
            instance_type: InstanceType::TiKV,
            host: "127.0.0.1".to_string(),
            primary_port: 20160,
            secondary_port: 20180,
            instance_name: None,
        };
        assert_eq!(
            component.conprof_address(),
            Some("127.0.0.1:20180".to_string())
        );
    }

    #[test]
    fn test_component_conprof_address_tiflash() {
        let component = Component {
            instance_type: InstanceType::TiFlash,
            host: "127.0.0.1".to_string(),
            primary_port: 9000,
            secondary_port: 8123,
            instance_name: None,
        };
        assert_eq!(
            component.conprof_address(),
            Some("127.0.0.1:8123".to_string())
        );
    }

    #[test]
    fn test_component_conprof_address_tiproxy() {
        let component = Component {
            instance_type: InstanceType::TiProxy,
            host: "127.0.0.1".to_string(),
            primary_port: 6000,
            secondary_port: 10080,
            instance_name: None,
        };
        assert_eq!(
            component.conprof_address(),
            Some("127.0.0.1:10080".to_string())
        );
    }

    #[test]
    fn test_component_conprof_address_lightning() {
        let component = Component {
            instance_type: InstanceType::Lightning,
            host: "127.0.0.1".to_string(),
            primary_port: 8287,
            secondary_port: 8286,
            instance_name: None,
        };
        assert_eq!(
            component.conprof_address(),
            Some("127.0.0.1:8286".to_string())
        );
    }

    #[test]
    fn test_component_conprof_address_tikv_worker() {
        let component = Component {
            instance_type: InstanceType::TikvWorker,
            host: "127.0.0.1".to_string(),
            primary_port: 20160,
            secondary_port: 20180,
            instance_name: None,
        };
        assert_eq!(
            component.conprof_address(),
            Some("127.0.0.1:20180".to_string())
        );
    }

    #[test]
    fn test_component_conprof_address_other() {
        let component = Component {
            instance_type: InstanceType::Other("compute-tiflash".to_string()),
            host: "127.0.0.1".to_string(),
            primary_port: 10080,
            secondary_port: 10080,
            instance_name: None,
        };
        assert_eq!(
            component.conprof_address(),
            Some("127.0.0.1:10080".to_string())
        );
    }

    #[test]
    fn test_component_equality() {
        let component1 = Component {
            instance_type: InstanceType::TiDB,
            host: "127.0.0.1".to_string(),
            primary_port: 4000,
            secondary_port: 10080,
            instance_name: None,
        };
        let component2 = Component {
            instance_type: InstanceType::TiDB,
            host: "127.0.0.1".to_string(),
            primary_port: 4000,
            secondary_port: 10080,
            instance_name: None,
        };
        let component3 = Component {
            instance_type: InstanceType::TiDB,
            host: "127.0.0.1".to_string(),
            primary_port: 4001,
            secondary_port: 10080,
            instance_name: None,
        };
        assert_eq!(component1, component2);
        assert_ne!(component1, component3);
    }

    #[test]
    fn test_component_hash() {
        use std::collections::HashSet;
        let component1 = Component {
            instance_type: InstanceType::TiDB,
            host: "127.0.0.1".to_string(),
            primary_port: 4000,
            secondary_port: 10080,
            instance_name: None,
        };
        let component2 = Component {
            instance_type: InstanceType::TiDB,
            host: "127.0.0.1".to_string(),
            primary_port: 4000,
            secondary_port: 10080,
            instance_name: None,
        };
        let mut set = HashSet::new();
        set.insert(component1.clone());
        set.insert(component2.clone());
        assert_eq!(set.len(), 1);
    }
}
