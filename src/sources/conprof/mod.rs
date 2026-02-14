use std::time::Duration;

use serde::{Deserialize, Serialize};
use vector::config::{GenerateConfig, SourceConfig, SourceContext};
use vector_config::Configurable;
use vector_lib::{
    config::{DataType, LogNamespace, SourceOutput},
    configurable::configurable_component,
    source::Source,
    tls::TlsConfig,
};

use crate::sources::conprof::controller::Controller;

mod controller;
mod shutdown;
mod tools;
pub mod topology;
mod upstream;

/// Topology discovery mode: PD+etcd (default) or Kubernetes pod labels (for quick rollback).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, Configurable)]
#[serde(rename_all = "lowercase")]
pub enum TopologyMode {
    /// Discover via PD API and etcd (TiDB/TiProxy from etcd, TiKV/TiFlash from PD stores).
    #[default]
    Pd,
    /// Discover via Kubernetes: list pods with the configured component label and map label value to instance type.
    K8s,
}

/// K8s topology config. Used when `topology_mode = "k8s"`.
/// Which components to collect and which instance_type (profile) to use is fully configurable via `component_label_to_instance_type`.
#[configurable_component]
#[derive(Debug, Clone)]
pub struct TopologyK8sConfig {
    /// Label key used to read component from each pod (e.g. `pingcap.com/component` or `tags.tidbcloud.com/component`).
    #[serde(default = "default_topology_k8s_component_label_key")]
    pub component_label_key: String,

    /// Namespace to list pods in. If unset, uses the pod's own namespace (from service account).
    pub namespace: Option<String>,

    /// Map: component label value -> instance_type. Only pods whose label value is a key in this map are collected; the value selects which profile config to use (e.g. `tidb`, `tikv`, `tikv_worker`, `coprocessor_worker`). Any label name is allowed as key.
    /// Example: `"worker-tidb" = "tidb"`, `"tikv-worker" = "tikv_worker"`, `"coprocessor-worker" = "coprocessor_worker"`.
    #[serde(default)]
    pub component_label_to_instance_type: std::collections::HashMap<String, String>,
}

fn default_topology_k8s_component_label_key() -> String {
    "pingcap.com/component".to_string()
}

/// PLACEHOLDER
#[configurable_component(source("conprof"))]
#[derive(Debug, Clone)]
pub struct ConprofConfig {
    /// PLACEHOLDER
    pub pd_address: String,

    /// PLACEHOLDER
    pub tls: Option<TlsConfig>,

    /// How to discover instances to profile: `pd` (PD API + etcd) or `k8s` (Kubernetes pod labels). Use `k8s` for quick rollback when PD/etcd is unavailable.
    #[serde(default)]
    pub topology_mode: TopologyMode,

    /// Required when `topology_mode = "k8s"`. Ignored otherwise.
    pub topology_k8s: Option<TopologyK8sConfig>,

    /// PLACEHOLDER
    #[serde(default = "default_topology_fetch_interval")]
    pub topology_fetch_interval_seconds: f64,

    /// PLACEHOLDER
    #[serde(default = "default_components_profile_types")]
    pub components_profile_types: ComponentsProfileTypes,
}

/// PLACEHOLDER
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Configurable)]
pub struct ComponentsProfileTypes {
    /// PLACEHOLDER
    pub pd: ProfileTypes,
    /// PLACEHOLDER
    pub tidb: ProfileTypes,
    /// PLACEHOLDER
    pub tikv: ProfileTypes,
    /// PLACEHOLDER
    pub tiflash: ProfileTypes,
    /// PLACEHOLDER
    pub tiproxy: ProfileTypes,
    /// PLACEHOLDER
    pub lightning: ProfileTypes,
    /// K8s label e.g. tikv-worker: profile config for this component.
    #[serde(default = "default_tikv_worker_profile_types")]
    pub tikv_worker: ProfileTypes,
    /// K8s label e.g. coprocessor-worker: profile config for this component.
    #[serde(default = "default_coprocessor_worker_profile_types")]
    pub coprocessor_worker: ProfileTypes,
}

impl ComponentsProfileTypes {
    /// Returns the profile types for the given instance type (e.g. which profiles to collect).
    pub fn for_instance(&self, t: topology::InstanceType) -> ProfileTypes {
        match t {
            topology::InstanceType::PD => self.pd,
            topology::InstanceType::TiDB => self.tidb,
            topology::InstanceType::TiKV => self.tikv,
            topology::InstanceType::TiFlash => self.tiflash,
            topology::InstanceType::TiProxy => self.tiproxy,
            topology::InstanceType::Lightning => self.lightning,
            topology::InstanceType::TikvWorker => self.tikv_worker,
            topology::InstanceType::CoprocessorWorker => self.coprocessor_worker,
        }
    }
}

/// PLACEHOLDER
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Configurable)]
pub struct ProfileTypes {
    /// PLACEHOLDER
    pub cpu: bool,
    /// Collect heap via HTTP (pprof).
    pub heap: bool,
    /// TiKV only: collect heap via perl+jeprof (jemalloc). Can be used with or without heap; typically one of heap or jeheap for TiKV.
    #[serde(default)]
    pub jeheap: bool,
    /// PLACEHOLDER
    pub mutex: bool,
    /// PLACEHOLDER
    pub goroutine: bool,
}

pub const fn default_topology_fetch_interval() -> f64 {
    30.0
}

pub const fn default_tikv_worker_profile_types() -> ProfileTypes {
    default_tikv_profile_types()
}

pub const fn default_coprocessor_worker_profile_types() -> ProfileTypes {
    default_tikv_profile_types()
}

pub const fn default_components_profile_types() -> ComponentsProfileTypes {
    ComponentsProfileTypes {
        pd: default_go_profile_types(),
        tidb: default_go_profile_types(),
        tikv: default_tikv_profile_types(),
        tiflash: default_tiflash_profile_types(),
        tiproxy: default_go_profile_types(),
        lightning: default_go_profile_types(),
        tikv_worker: default_tikv_worker_profile_types(),
        coprocessor_worker: default_coprocessor_worker_profile_types(),
    }
}

pub const fn default_go_profile_types() -> ProfileTypes {
    ProfileTypes {
        cpu: true,
        heap: true,
        jeheap: false,
        mutex: true,
        goroutine: true,
    }
}

pub const fn default_tikv_profile_types() -> ProfileTypes {
    ProfileTypes {
        cpu: false,
        heap: true,
        jeheap: false,
        mutex: false,
        goroutine: false,
    }
}

pub const fn default_tiflash_profile_types() -> ProfileTypes {
    ProfileTypes {
        cpu: false,
        heap: false,
        jeheap: false,
        mutex: false,
        goroutine: false,
    }
}

impl GenerateConfig for ConprofConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            pd_address: "127.0.0.1:2379".to_owned(),
            tls: None,
            topology_mode: TopologyMode::Pd,
            topology_k8s: None,
            topology_fetch_interval_seconds: default_topology_fetch_interval(),
            components_profile_types: default_components_profile_types(),
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "conprof")]
impl SourceConfig for ConprofConfig {
    async fn build(&self, cx: SourceContext) -> vector::Result<Source> {
        self.validate()?;

        let pd_address = self.pd_address.clone();
        let tls = self.tls.clone();
        let topology_mode = self.topology_mode;
        let topology_k8s = self.topology_k8s.clone();
        let topology_fetch_interval = Duration::from_secs_f64(self.topology_fetch_interval_seconds);
        let components_profile_types = self.components_profile_types;
        let proxy = cx.proxy.clone();
        let out = cx.out;
        let shutdown = cx.shutdown;
        Ok(Box::pin(async move {
            let topo_fetcher = match topology_mode {
                TopologyMode::Pd => {
                    let f = match crate::sources::conprof::topology::fetch::TopologyFetcher::new(
                        pd_address,
                        tls.clone(),
                        &proxy,
                    )
                    .await
                    {
                        Ok(x) => x,
                        Err(e) => {
                            error!(message = "Failed to create PD topology fetcher.", %e);
                            return Err(());
                        }
                    };
                    crate::sources::conprof::topology::fetch::TopologyFetcherKind::Pd(f)
                }
                TopologyMode::K8s => {
                    let k8s_config = match topology_k8s {
                        Some(c) => c,
                        None => {
                            error!(message = "topology_k8s is required when topology_mode = \"k8s\"");
                            return Err(());
                        }
                    };
                    let f = match crate::sources::conprof::topology::fetch::K8sTopologyFetcher::new(
                        k8s_config,
                    )
                    .await
                    {
                        Ok(x) => x,
                        Err(e) => {
                            error!(message = "Failed to create K8s topology fetcher.", %e);
                            return Err(());
                        }
                    };
                    crate::sources::conprof::topology::fetch::TopologyFetcherKind::K8s(f)
                }
            };
            let controller = match Controller::new_with_topo_fetcher(
                topo_fetcher,
                topology_fetch_interval,
                components_profile_types,
                tls,
                out,
            ) {
                Ok(c) => c,
                Err(e) => {
                    error!(message = "Failed to create controller.", %e);
                    return Err(());
                }
            };
            controller.run(shutdown).await;
            Ok(())
        }))
    }

    fn outputs(&self, _: LogNamespace) -> Vec<SourceOutput> {
        vec![SourceOutput {
            port: None,
            ty: DataType::Log,
            schema_definition: None,
        }]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

impl ConprofConfig {
    fn validate(&self) -> vector::Result<()> {
        if self.topology_mode == TopologyMode::K8s && self.topology_k8s.is_none() {
            return Err("topology_k8s is required when topology_mode = \"k8s\".".into());
        }
        self.validate_tls()?;
        Ok(())
    }

    fn validate_tls(&self) -> vector::Result<()> {
        if self.tls.is_none() {
            return Ok(());
        }

        let tls = self.tls.as_ref().unwrap();
        if (tls.ca_file.is_some() || tls.crt_file.is_some() || tls.key_file.is_some())
            && (tls.ca_file.is_none() || tls.crt_file.is_none() || tls.key_file.is_none())
        {
            return Err("ca, cert and private key should be all configured.".into());
        }

        Self::check_key_file("ca key", &tls.ca_file)?;
        Self::check_key_file("cert key", &tls.crt_file)?;
        Self::check_key_file("private key", &tls.key_file)?;

        Ok(())
    }

    fn check_key_file(
        tag: &str,
        path: &Option<std::path::PathBuf>,
    ) -> vector::Result<Option<std::fs::File>> {
        if path.is_none() {
            return Ok(None);
        }
        match std::fs::File::open(path.as_ref().unwrap()) {
            Err(e) => Err(format!("failed to open {:?} to load {}: {:?}", path, tag, e).into()),
            Ok(f) => Ok(Some(f)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use vector_lib::tls::TlsConfig;

    #[test]
    fn generate_config() {
        vector::test_util::test_generate_config::<ConprofConfig>();
    }

    #[test]
    fn test_default_topology_fetch_interval() {
        assert_eq!(default_topology_fetch_interval(), 30.0);
    }

    #[test]
    fn test_default_components_profile_types_tikv_heap() {
        assert!(default_components_profile_types().tikv.heap);
    }

    #[test]
    fn test_outputs() {
        let config = ConprofConfig {
            pd_address: "127.0.0.1:2379".to_owned(),
            tls: None,
            topology_mode: TopologyMode::Pd,
            topology_k8s: None,
            topology_fetch_interval_seconds: 30.0,
            components_profile_types: default_components_profile_types(),
        };
        let outputs = config.outputs(LogNamespace::Legacy);
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].ty, DataType::Log);
        assert_eq!(outputs[0].port, None);
    }

    #[test]
    fn test_can_acknowledge() {
        let config = ConprofConfig {
            pd_address: "127.0.0.1:2379".to_owned(),
            tls: None,
            topology_mode: TopologyMode::Pd,
            topology_k8s: None,
            topology_fetch_interval_seconds: 30.0,
            components_profile_types: default_components_profile_types(),
        };
        assert_eq!(config.can_acknowledge(), false);
    }

    #[test]
    fn test_validate_tls_none() {
        let config = ConprofConfig {
            pd_address: "127.0.0.1:2379".to_owned(),
            tls: None,
            topology_mode: TopologyMode::Pd,
            topology_k8s: None,
            topology_fetch_interval_seconds: 30.0,
            components_profile_types: default_components_profile_types(),
        };
        assert!(config.validate_tls().is_ok());
    }

    #[test]
    fn test_validate_tls_all_none() {
        let config = ConprofConfig {
            pd_address: "127.0.0.1:2379".to_owned(),
            tls: Some(TlsConfig::default()),
            topology_mode: TopologyMode::Pd,
            topology_k8s: None,
            topology_fetch_interval_seconds: 30.0,
            components_profile_types: default_components_profile_types(),
        };
        assert!(config.validate_tls().is_ok());
    }

    #[test]
    fn test_validate_tls_all_some() {
        let temp_dir = TempDir::new().unwrap();
        let ca_file = temp_dir.path().join("ca.crt");
        let crt_file = temp_dir.path().join("client.crt");
        let key_file = temp_dir.path().join("client.key");

        fs::write(&ca_file, "ca content").unwrap();
        fs::write(&crt_file, "cert content").unwrap();
        fs::write(&key_file, "key content").unwrap();

        let config = ConprofConfig {
            pd_address: "127.0.0.1:2379".to_owned(),
            tls: Some(TlsConfig {
                ca_file: Some(ca_file.clone()),
                crt_file: Some(crt_file.clone()),
                key_file: Some(key_file.clone()),
                ..Default::default()
            }),
            topology_mode: TopologyMode::Pd,
            topology_k8s: None,
            topology_fetch_interval_seconds: 30.0,
            components_profile_types: default_components_profile_types(),
        };
        assert!(config.validate_tls().is_ok());
    }

    #[test]
    fn test_validate_tls_partial_config_ca_only() {
        let temp_dir = TempDir::new().unwrap();
        let ca_file = temp_dir.path().join("ca.crt");
        fs::write(&ca_file, "ca content").unwrap();

        let config = ConprofConfig {
            pd_address: "127.0.0.1:2379".to_owned(),
            tls: Some(TlsConfig {
                ca_file: Some(ca_file),
                crt_file: None,
                key_file: None,
                ..Default::default()
            }),
            topology_mode: TopologyMode::Pd,
            topology_k8s: None,
            topology_fetch_interval_seconds: 30.0,
            components_profile_types: default_components_profile_types(),
        };
        assert!(config.validate_tls().is_err());
        let err = config.validate_tls().unwrap_err();
        assert!(err
            .to_string()
            .contains("ca, cert and private key should be all configured"));
    }

    #[test]
    fn test_validate_tls_partial_config_crt_only() {
        let temp_dir = TempDir::new().unwrap();
        let crt_file = temp_dir.path().join("client.crt");
        fs::write(&crt_file, "cert content").unwrap();

        let config = ConprofConfig {
            pd_address: "127.0.0.1:2379".to_owned(),
            tls: Some(TlsConfig {
                ca_file: None,
                crt_file: Some(crt_file),
                key_file: None,
                ..Default::default()
            }),
            topology_mode: TopologyMode::Pd,
            topology_k8s: None,
            topology_fetch_interval_seconds: 30.0,
            components_profile_types: default_components_profile_types(),
        };
        assert!(config.validate_tls().is_err());
    }

    #[test]
    fn test_validate_tls_partial_config_key_only() {
        let temp_dir = TempDir::new().unwrap();
        let key_file = temp_dir.path().join("client.key");
        fs::write(&key_file, "key content").unwrap();

        let config = ConprofConfig {
            pd_address: "127.0.0.1:2379".to_owned(),
            tls: Some(TlsConfig {
                ca_file: None,
                crt_file: None,
                key_file: Some(key_file),
                ..Default::default()
            }),
            topology_mode: TopologyMode::Pd,
            topology_k8s: None,
            topology_fetch_interval_seconds: 30.0,
            components_profile_types: default_components_profile_types(),
        };
        assert!(config.validate_tls().is_err());
    }

    #[test]
    fn test_validate_tls_partial_config_ca_and_crt() {
        let temp_dir = TempDir::new().unwrap();
        let ca_file = temp_dir.path().join("ca.crt");
        let crt_file = temp_dir.path().join("client.crt");
        fs::write(&ca_file, "ca content").unwrap();
        fs::write(&crt_file, "cert content").unwrap();

        let config = ConprofConfig {
            pd_address: "127.0.0.1:2379".to_owned(),
            tls: Some(TlsConfig {
                ca_file: Some(ca_file),
                crt_file: Some(crt_file),
                key_file: None,
                ..Default::default()
            }),
            topology_mode: TopologyMode::Pd,
            topology_k8s: None,
            topology_fetch_interval_seconds: 30.0,
            components_profile_types: default_components_profile_types(),
        };
        assert!(config.validate_tls().is_err());
    }

    #[test]
    fn test_validate_tls_missing_file() {
        let config = ConprofConfig {
            pd_address: "127.0.0.1:2379".to_owned(),
            tls: Some(TlsConfig {
                ca_file: Some(PathBuf::from("/nonexistent/ca.crt")),
                crt_file: Some(PathBuf::from("/nonexistent/client.crt")),
                key_file: Some(PathBuf::from("/nonexistent/client.key")),
                ..Default::default()
            }),
            topology_mode: TopologyMode::Pd,
            topology_k8s: None,
            topology_fetch_interval_seconds: 30.0,
            components_profile_types: default_components_profile_types(),
        };
        assert!(config.validate_tls().is_err());
        let err = config.validate_tls().unwrap_err();
        assert!(err.to_string().contains("failed to open"));
    }

    #[test]
    fn test_check_key_file_none() {
        let result = ConprofConfig::check_key_file("test", &None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_check_key_file_exists() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.key");
        fs::write(&test_file, "test content").unwrap();

        let result = ConprofConfig::check_key_file("test", &Some(test_file)).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_check_key_file_not_exists() {
        let result =
            ConprofConfig::check_key_file("test", &Some(PathBuf::from("/nonexistent/test.key")));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("failed to open"));
        assert!(err.to_string().contains("test"));
    }
}
