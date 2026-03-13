// We need to make the topology module public to access its components in tests
use vector_extensions::sources::conprof::topology::fetch::{FetchError, TopologyFetcher};
use vector_extensions::sources::conprof::topology::{Component, InstanceType};

#[cfg(test)]
mod topology_tests {
    use super::*;

    #[test]
    fn test_instance_type_display() {
        assert_eq!(InstanceType::PD.to_string(), "pd");
        assert_eq!(InstanceType::TiDB.to_string(), "tidb");
        assert_eq!(InstanceType::TiKV.to_string(), "tikv");
        assert_eq!(InstanceType::TiFlash.to_string(), "tiflash");
        assert_eq!(InstanceType::TiProxy.to_string(), "tiproxy");
        assert_eq!(InstanceType::Lightning.to_string(), "lightning");
    }

    #[test]
    fn test_component_conprof_address() {
        // Test PD component
        let pd_component = Component {
            instance_type: InstanceType::PD,
            host: "127.0.0.1".to_string(),
            primary_port: 2379,
            secondary_port: 2380,
            instance_name: None,
        };
        assert_eq!(
            pd_component.conprof_address(),
            Some("127.0.0.1:2379".to_string())
        );

        // Test TiDB component
        let tidb_component = Component {
            instance_type: InstanceType::TiDB,
            host: "127.0.0.1".to_string(),
            primary_port: 4000,
            secondary_port: 10080,
            instance_name: None,
        };
        assert_eq!(
            tidb_component.conprof_address(),
            Some("127.0.0.1:10080".to_string())
        );

        // Test TiKV component
        let tikv_component = Component {
            instance_type: InstanceType::TiKV,
            host: "127.0.0.1".to_string(),
            primary_port: 20160,
            secondary_port: 20180,
            instance_name: None,
        };
        assert_eq!(
            tikv_component.conprof_address(),
            Some("127.0.0.1:20180".to_string())
        );

        // Test TiFlash component
        let tiflash_component = Component {
            instance_type: InstanceType::TiFlash,
            host: "127.0.0.1".to_string(),
            primary_port: 9000,
            secondary_port: 8123,
            instance_name: None,
        };
        assert_eq!(
            tiflash_component.conprof_address(),
            Some("127.0.0.1:8123".to_string())
        );

        // Test TiProxy component
        let tiproxy_component = Component {
            instance_type: InstanceType::TiProxy,
            host: "127.0.0.1".to_string(),
            primary_port: 6000,
            secondary_port: 6001,
            instance_name: None,
        };
        assert_eq!(
            tiproxy_component.conprof_address(),
            Some("127.0.0.1:6001".to_string())
        );

        // Test Lightning component
        let lightning_component = Component {
            instance_type: InstanceType::Lightning,
            host: "127.0.0.1".to_string(),
            primary_port: 8287,
            secondary_port: 8286,
            instance_name: None,
        };
        assert_eq!(
            lightning_component.conprof_address(),
            Some("127.0.0.1:8286".to_string())
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
