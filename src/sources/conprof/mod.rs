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

/// PLACEHOLDER
#[configurable_component(source("conprof"))]
#[derive(Debug, Clone)]
pub struct ConprofConfig {
    /// PLACEHOLDER
    pub pd_address: String,

    /// PLACEHOLDER
    pub tls: Option<TlsConfig>,

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
}

/// PLACEHOLDER
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Configurable)]
pub struct ProfileTypes {
    /// PLACEHOLDER
    pub cpu: bool,
    /// PLACEHOLDER
    pub heap: bool,
    /// PLACEHOLDER
    pub mutex: bool,
    /// PLACEHOLDER
    pub goroutine: bool,
}

pub const fn default_topology_fetch_interval() -> f64 {
    30.0
}

pub const fn default_components_profile_types() -> ComponentsProfileTypes {
    ComponentsProfileTypes {
        pd: default_go_profile_types(),
        tidb: default_go_profile_types(),
        tikv: default_tikv_profile_types(),
        tiflash: default_tiflash_profile_types(),
        tiproxy: default_go_profile_types(),
        lightning: default_go_profile_types(),
    }
}

pub const fn default_go_profile_types() -> ProfileTypes {
    ProfileTypes {
        cpu: true,
        heap: true,
        mutex: true,
        goroutine: true,
    }
}

pub const fn default_tikv_profile_types() -> ProfileTypes {
    ProfileTypes {
        cpu: false,
        heap: true,
        mutex: false,
        goroutine: false,
    }
}

pub const fn default_tiflash_profile_types() -> ProfileTypes {
    ProfileTypes {
        cpu: false,
        heap: false,
        mutex: false,
        goroutine: false,
    }
}

impl GenerateConfig for ConprofConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            pd_address: "127.0.0.1:2379".to_owned(),
            tls: None,
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
        self.validate_tls()?;

        let pd_address = self.pd_address.clone();
        let tls = self.tls.clone();
        let topology_fetch_interval = Duration::from_secs_f64(self.topology_fetch_interval_seconds);
        let components_profile_types = self.components_profile_types;
        Ok(Box::pin(async move {
            Controller::new(
                pd_address,
                topology_fetch_interval,
                components_profile_types,
                tls,
                &cx.proxy,
                cx.out,
            )
            .await
            .map_err(|error| error!(message = "Source failed.", %error))?
            .run(cx.shutdown)
            .await;
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
    fn test_default_enable_tikv_heap_profile() {
        assert_eq!(default_enable_tikv_heap_profile(), false);
    }

    #[test]
    fn test_outputs() {
        let config = ConprofConfig {
            pd_address: "127.0.0.1:2379".to_owned(),
            tls: None,
            topology_fetch_interval_seconds: 30.0,
            enable_tikv_heap_profile: false,
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
            topology_fetch_interval_seconds: 30.0,
            enable_tikv_heap_profile: false,
        };
        assert_eq!(config.can_acknowledge(), false);
    }

    #[test]
    fn test_validate_tls_none() {
        let config = ConprofConfig {
            pd_address: "127.0.0.1:2379".to_owned(),
            tls: None,
            topology_fetch_interval_seconds: 30.0,
            enable_tikv_heap_profile: false,
        };
        assert!(config.validate_tls().is_ok());
    }

    #[test]
    fn test_validate_tls_all_none() {
        let config = ConprofConfig {
            pd_address: "127.0.0.1:2379".to_owned(),
            tls: Some(TlsConfig::default()),
            topology_fetch_interval_seconds: 30.0,
            enable_tikv_heap_profile: false,
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
            topology_fetch_interval_seconds: 30.0,
            enable_tikv_heap_profile: false,
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
            topology_fetch_interval_seconds: 30.0,
            enable_tikv_heap_profile: false,
        };
        assert!(config.validate_tls().is_err());
        let err = config.validate_tls().unwrap_err();
        assert!(err.to_string().contains("ca, cert and private key should be all configured"));
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
            topology_fetch_interval_seconds: 30.0,
            enable_tikv_heap_profile: false,
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
            topology_fetch_interval_seconds: 30.0,
            enable_tikv_heap_profile: false,
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
            topology_fetch_interval_seconds: 30.0,
            enable_tikv_heap_profile: false,
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
            topology_fetch_interval_seconds: 30.0,
            enable_tikv_heap_profile: false,
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
        let result = ConprofConfig::check_key_file(
            "test",
            &Some(PathBuf::from("/nonexistent/test.key")),
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("failed to open"));
        assert!(err.to_string().contains("test"));
    }
}
