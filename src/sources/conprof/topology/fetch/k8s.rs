//! Topology discovery via Kubernetes pod labels (e.g. `pingcap.com/component`).
//! Used when `topology_mode = "k8s"`. Which components to collect and which instance_type (profile) to use is fully configurable via `component_label_to_instance_type`.

use std::collections::HashSet;
use std::str::FromStr;

use k8s_openapi::api::core::v1::Pod;
use kube::api::ListParams;
use kube::Api;
use kube::Client;
use snafu::{ResultExt, Snafu};

use crate::sources::conprof::TopologyK8sConfig;
use crate::sources::conprof::topology::{Component, InstanceType};

#[derive(Debug, Snafu)]
pub enum FetchError {
    #[snafu(display("Failed to build Kubernetes client: {}", source))]
    BuildKubeClient { source: kube::Error },
    #[snafu(display("Failed to get namespace: {}", source))]
    GetNamespace { source: std::io::Error },
    #[snafu(display("Failed to list pods in namespace '{}': {}", namespace, source))]
    ListPods {
        namespace: String,
        source: kube::Error,
    },
}

/// Default status/conprof port per instance type (same as PD/etcd discovery).
fn default_port_for_instance_type(t: InstanceType) -> u16 {
    match t {
        InstanceType::PD => 2379,
        InstanceType::TiDB => 10080,
        InstanceType::TiKV => 20180,
        InstanceType::TiFlash => 20292,
        InstanceType::TiProxy => 8286,
        InstanceType::Lightning => 8289,
        InstanceType::TikvWorker | InstanceType::CoprocessorWorker => 20180,
    }
}

pub struct K8sTopologyFetcher {
    client: Client,
    config: TopologyK8sConfig,
}

impl K8sTopologyFetcher {
    pub async fn new(config: TopologyK8sConfig) -> Result<Self, FetchError> {
        let client = Client::try_default()
            .await
            .context(BuildKubeClientSnafu)?;
        Ok(Self { client, config })
    }

    pub async fn get_up_components(
        &mut self,
        components: &mut HashSet<Component>,
    ) -> Result<(), FetchError> {
        let namespace = match &self.config.namespace {
            Some(ns) => ns.clone(),
            None => tokio::fs::read_to_string(
                "/var/run/secrets/kubernetes.io/serviceaccount/namespace",
            )
            .await
            .context(GetNamespaceSnafu)?,
        };

        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &namespace);
        let list_params = ListParams::default();
        let pod_list = pods.list(&list_params).await.context(ListPodsSnafu {
            namespace: namespace.clone(),
        })?;

        let key = &self.config.component_label_key;
        let label_to_instance = &self.config.component_label_to_instance_type;
        for pod in pod_list.items {
            let labels = match &pod.metadata.labels {
                Some(l) => l,
                None => continue,
            };
            let value = match labels.get(key) {
                Some(v) => v.as_str(),
                None => continue,
            };
            let instance_type_key = match label_to_instance.get(value) {
                Some(k) => k.as_str(),
                None => continue,
            };
            let instance_type = match InstanceType::from_str(instance_type_key) {
                Ok(t) => t,
                Err(_) => continue,
            };
            let status = match &pod.status {
                Some(s) => s,
                None => continue,
            };
            if status.phase.as_deref() != Some("Running") {
                continue;
            }
            let pod_ip = match &status.pod_ip {
                Some(ip) if !ip.is_empty() => ip.clone(),
                _ => continue,
            };
            let port = default_port_for_instance_type(instance_type);
            components.insert(Component {
                instance_type,
                host: pod_ip,
                primary_port: port,
                secondary_port: port,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_instance_type_from_str() {
        assert_eq!(InstanceType::from_str("pd").ok(), Some(InstanceType::PD));
        assert_eq!(InstanceType::from_str("tikv_worker").ok(), Some(InstanceType::TikvWorker));
        assert_eq!(InstanceType::from_str("coprocessor_worker").ok(), Some(InstanceType::CoprocessorWorker));
        assert!(InstanceType::from_str("unknown").is_err());
    }

    #[test]
    fn test_default_ports() {
        assert_eq!(default_port_for_instance_type(InstanceType::PD), 2379);
        assert_eq!(default_port_for_instance_type(InstanceType::TiDB), 10080);
        assert_eq!(default_port_for_instance_type(InstanceType::TiKV), 20180);
        assert_eq!(default_port_for_instance_type(InstanceType::TiFlash), 20292);
        assert_eq!(default_port_for_instance_type(InstanceType::TikvWorker), 20180);
        assert_eq!(default_port_for_instance_type(InstanceType::CoprocessorWorker), 20180);
    }
}
