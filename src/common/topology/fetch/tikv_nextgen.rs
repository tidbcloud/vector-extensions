use crate::common::topology::{Component, InstanceType};

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
        label_k8s_instance: String,
        source: kube::Error,
    },
}

const K8S_COMPONENT_LABEL: &str = "app.kubernetes.io/component";
const K8S_INSTANCE_LABEL: &str = "app.kubernetes.io/instance";

const TIKV_COMPONENT: &str = "tikv";
const COPROCESSOR_WORKER_COMPONENT: &str = "coprocessor-worker";

const TIKV_GRPC_PORT: u16 = 20160;
const TIKV_STATUS_PORT: u16 = 20180;
const COPROCESSOR_WORKER_GRPC_PORT: u16 = 9500;
const COPROCESSOR_WORKER_HTTP_PORT: u16 = 19000;

pub struct TiKVNextGenTopologyFetcher {
    client: Client,
    label_k8s_instance: String,
}

impl TiKVNextGenTopologyFetcher {
    pub fn new(client: Client, label_k8s_instance: String) -> Self {
        Self {
            client,
            label_k8s_instance,
        }
    }

    pub async fn get_up_tikvs(
        &self,
        components: &mut HashSet<Component>,
    ) -> Result<(), FetchError> {
        let namespace =
            tokio::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
                .await
                .context(GetNamespaceSnafu)?;
        let label_selector = build_label_selector(&self.label_k8s_instance);
        let pod_list = Api::<Pod>::namespaced(self.client.clone(), &namespace)
            .list(&ListParams::default().labels(&label_selector))
            .await
            .context(ListPodsSnafu {
                namespace: namespace.clone(),
                label_k8s_instance: self.label_k8s_instance.clone(),
            })?;
        for pod in pod_list.items {
            if let Some(component) = component_from_pod(&pod) {
                components.insert(component);
            }
        }
        Ok(())
    }
}

fn build_label_selector(label_k8s_instance: &str) -> String {
    let component_values = std::iter::once(TIKV_COMPONENT)
        .chain(std::iter::once(COPROCESSOR_WORKER_COMPONENT))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{K8S_COMPONENT_LABEL} in ({component_values}),{K8S_INSTANCE_LABEL}={label_k8s_instance}"
    )
}

fn component_from_pod(pod: &Pod) -> Option<Component> {
    let status = pod.status.as_ref()?;
    if status.phase.as_deref() != Some("Running") {
        return None;
    }

    let pod_ip = status.pod_ip.as_deref()?;
    if pod_ip.trim().is_empty() {
        return None;
    }

    let component_label = pod
        .metadata
        .labels
        .as_ref()
        .and_then(|labels| labels.get(K8S_COMPONENT_LABEL))
        .map(String::as_str)
        .unwrap_or(TIKV_COMPONENT);

    let (primary_port, secondary_port) = if is_coprocessor_worker_component(component_label) {
        (COPROCESSOR_WORKER_GRPC_PORT, COPROCESSOR_WORKER_HTTP_PORT)
    } else {
        (TIKV_GRPC_PORT, TIKV_STATUS_PORT)
    };

    Some(Component {
        instance_type: InstanceType::TiKV,
        host: pod_ip.to_string(),
        primary_port,
        secondary_port,
        instance_name: pod.metadata.name.clone(),
    })
}

fn is_coprocessor_worker_component(component: &str) -> bool {
    component == COPROCESSOR_WORKER_COMPONENT
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use k8s_openapi::{api::core::v1::PodStatus, apimachinery::pkg::apis::meta::v1::ObjectMeta};

    use super::*;

    fn pod(name: &str, component: &str, phase: &str, pod_ip: &str) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                labels: Some(BTreeMap::from([(
                    K8S_COMPONENT_LABEL.to_string(),
                    component.to_string(),
                )])),
                ..Default::default()
            },
            status: Some(PodStatus {
                phase: Some(phase.to_string()),
                pod_ip: Some(pod_ip.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn builds_selector_with_coprocessor_worker_component() {
        let selector = build_label_selector("demo-cluster");

        assert!(selector.contains("app.kubernetes.io/component in ("));
        assert!(selector.contains("tikv"));
        assert!(selector.contains("coprocessor-worker"));
        assert!(selector.contains("app.kubernetes.io/instance=demo-cluster"));
    }

    #[test]
    fn builds_tikv_component_from_tikv_pod() {
        let component = component_from_pod(&pod("tikv-0", "tikv", "Running", "10.0.0.1")).unwrap();

        assert_eq!(component.instance_type, InstanceType::TiKV);
        assert_eq!(component.host, "10.0.0.1");
        assert_eq!(component.primary_port, TIKV_GRPC_PORT);
        assert_eq!(component.secondary_port, TIKV_STATUS_PORT);
        assert_eq!(component.instance_name.as_deref(), Some("tikv-0"));
    }

    #[test]
    fn builds_tikv_component_from_coprocessor_worker_pod() {
        let component = component_from_pod(&pod(
            "coprocessor-worker-0",
            "coprocessor-worker",
            "Running",
            "10.0.0.2",
        ))
        .unwrap();

        assert_eq!(component.instance_type, InstanceType::TiKV);
        assert_eq!(component.host, "10.0.0.2");
        assert_eq!(component.primary_port, COPROCESSOR_WORKER_GRPC_PORT);
        assert_eq!(component.secondary_port, COPROCESSOR_WORKER_HTTP_PORT);
        assert_eq!(
            component.instance_name.as_deref(),
            Some("coprocessor-worker-0")
        );
    }

    #[test]
    fn skips_non_running_pod() {
        assert!(component_from_pod(&pod("tikv-1", "tikv", "Pending", "10.0.0.3")).is_none());
    }

    #[test]
    fn does_not_treat_other_worker_aliases_as_coprocessor_worker() {
        let component =
            component_from_pod(&pod("tikv-worker-0", "tikv-worker", "Running", "10.0.0.4"))
                .unwrap();

        assert_eq!(component.primary_port, TIKV_GRPC_PORT);
        assert_eq!(component.secondary_port, TIKV_STATUS_PORT);
    }
}
