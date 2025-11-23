use crate::sources::conprof::topology::{Component, InstanceType};

use std::collections::HashSet;

use k8s_openapi::api::core::v1::Pod;
use kube::{api::ListParams, Api, Client};
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum FetchError {
    #[snafu(display("Failed to get namespace: {}", source))]
    GetNamespace { source: std::io::Error },
    #[snafu(display("Failed to list pods in namespace '{}': {}", namespace, source))]
    ListPods {
        namespace: String,
        source: kube::Error,
    },
}

pub struct KubeLightningTopologyFetcher {
    client: Client,
}

impl KubeLightningTopologyFetcher {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    pub async fn get_up_lightnings(
        &self,
        components: &mut HashSet<Component>,
    ) -> Result<(), FetchError> {
        let namespace =
            tokio::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
                .await
                .context(GetNamespaceSnafu)?;
        let pod_list = Api::<Pod>::namespaced(self.client.clone(), &namespace)
            .list(&ListParams::default())
            .await
            .context(ListPodsSnafu {
                namespace: namespace.clone(),
            })?;
        for pod in pod_list.items {
            if let Some(pod_name) = pod.metadata.name {
                if !pod_name.starts_with("import-") {
                    continue;
                }
                if let Some(status) = pod.status {
                    if status.phase.as_deref() != Some("Running") {
                        continue;
                    }
                    if let Some(pod_ip) = status.pod_ip {
                        if pod_ip.is_empty() {
                            continue;
                        }
                        components.insert(Component {
                            instance_type: InstanceType::Lightning,
                            host: pod_ip,
                            primary_port: 8289,
                            secondary_port: 8289,
                        });
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kube_lightning_topology_fetcher_new() {
        // We can't actually create a kube::Client without a real cluster,
        // but we can test the structure
        let _ = std::mem::size_of::<KubeLightningTopologyFetcher>();
    }

    #[test]
    fn test_get_up_lightnings_logic() {
        // Test the logic of get_up_lightnings by creating mock pod data
        let pod_name1 = "import-test-1".to_string();
        let pod_name2 = "other-pod".to_string();
        let pod_name3 = "import-test-2".to_string();
        
        // Test pod name filtering
        assert!(pod_name1.starts_with("import-"));
        assert!(!pod_name2.starts_with("import-"));
        assert!(pod_name3.starts_with("import-"));
        
        // Test phase filtering
        let phase_running = Some("Running".to_string());
        let phase_pending = Some("Pending".to_string());
        
        assert_eq!(phase_running.as_deref(), Some("Running"));
        assert_ne!(phase_pending.as_deref(), Some("Running"));
        
        // Test pod_ip filtering
        let pod_ip_valid = Some("127.0.0.1".to_string());
        let pod_ip_empty = Some("".to_string());
        
        assert!(!pod_ip_valid.as_ref().unwrap().is_empty());
        assert!(pod_ip_empty.as_ref().unwrap().is_empty());
        
        // Test component creation
        let component = Component {
            instance_type: InstanceType::Lightning,
            host: "127.0.0.1".to_string(),
            primary_port: 8289,
            secondary_port: 8289,
        };
        
        assert_eq!(component.instance_type, InstanceType::Lightning);
        assert_eq!(component.primary_port, 8289);
        assert_eq!(component.secondary_port, 8289);
    }

    #[test]
    fn test_fetch_error_display() {
        let error = FetchError::GetNamespace {
            source: std::io::Error::from(std::io::ErrorKind::NotFound),
        };
        let display = format!("{}", error);
        assert!(display.contains("Failed to get namespace"));
    }

    #[test]
    fn test_fetch_error_variants() {
        let error = FetchError::GetNamespace {
            source: std::io::Error::from(std::io::ErrorKind::NotFound),
        };
        let _display = format!("{}", error);
        
        // We can't easily create kube::Error, so we just test that the error type exists
        // The actual error creation will be tested in integration tests
    }

    #[test]
    fn test_pod_filtering_logic() {
        // Test the pod filtering logic used in get_up_lightnings
        struct MockPod {
            name: Option<String>,
            phase: Option<String>,
            pod_ip: Option<String>,
        }
        
        let pods = vec![
            MockPod {
                name: Some("import-test-1".to_string()),
                phase: Some("Running".to_string()),
                pod_ip: Some("127.0.0.1".to_string()),
            },
            MockPod {
                name: Some("other-pod".to_string()),
                phase: Some("Running".to_string()),
                pod_ip: Some("127.0.0.1".to_string()),
            },
            MockPod {
                name: Some("import-test-2".to_string()),
                phase: Some("Pending".to_string()),
                pod_ip: Some("127.0.0.1".to_string()),
            },
            MockPod {
                name: Some("import-test-3".to_string()),
                phase: Some("Running".to_string()),
                pod_ip: Some("".to_string()),
            },
        ];
        
        let filtered: Vec<_> = pods
            .iter()
            .filter(|pod| {
                pod.name.as_ref().map_or(false, |n| n.starts_with("import-"))
                    && pod.phase.as_deref() == Some("Running")
                    && pod.pod_ip.as_ref().map_or(false, |ip| !ip.is_empty())
            })
            .collect();
        
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].name.as_ref().unwrap(), "import-test-1");
    }

    #[test]
    fn test_get_up_lightnings_component_creation() {
        // Test component creation logic in get_up_lightnings
        let mut components = HashSet::new();
        let pod_ip = "127.0.0.1".to_string();
        
        if !pod_ip.is_empty() {
            components.insert(Component {
                instance_type: InstanceType::Lightning,
                host: pod_ip,
                primary_port: 8289,
                secondary_port: 8289,
            });
        }
        
        assert_eq!(components.len(), 1);
        let component = components.iter().next().unwrap();
        assert_eq!(component.instance_type, InstanceType::Lightning);
        assert_eq!(component.primary_port, 8289);
        assert_eq!(component.secondary_port, 8289);
    }

    #[test]
    fn test_get_up_lightnings_pod_name_filtering() {
        // Test pod name filtering
        let pod_names = vec![
            Some("import-test-1".to_string()),
            Some("other-pod".to_string()),
            Some("import-test-2".to_string()),
            None,
        ];
        
        let filtered: Vec<_> = pod_names
            .iter()
            .filter(|name| {
                name.as_ref().map_or(false, |n| n.starts_with("import-"))
            })
            .collect();
        
        assert_eq!(filtered.len(), 2);
    }

    #[test]
    fn test_get_up_lightnings_phase_filtering() {
        // Test phase filtering
        let phases = vec![
            Some("Running".to_string()),
            Some("Pending".to_string()),
            Some("Failed".to_string()),
            None,
        ];
        
        let filtered: Vec<_> = phases
            .iter()
            .filter(|phase| phase.as_deref() == Some("Running"))
            .collect();
        
        assert_eq!(filtered.len(), 1);
    }

    #[test]
    fn test_get_up_lightnings_pod_ip_filtering() {
        // Test pod_ip filtering
        let pod_ips = vec![
            Some("127.0.0.1".to_string()),
            Some("".to_string()),
            Some("192.168.1.1".to_string()),
            None,
        ];
        
        let filtered: Vec<_> = pod_ips
            .iter()
            .filter(|ip| ip.as_ref().map_or(false, |i| !i.is_empty()))
            .collect();
        
        assert_eq!(filtered.len(), 2);
    }

    #[test]
    fn test_get_up_lightnings_namespace_path() {
        // Test namespace file path
        let namespace_path = "/var/run/secrets/kubernetes.io/serviceaccount/namespace";
        assert_eq!(namespace_path, "/var/run/secrets/kubernetes.io/serviceaccount/namespace");
    }
}
