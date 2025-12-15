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
        let label_selector = format!(
            "app.kubernetes.io/component=tikv,app.kubernetes.io/instance={}",
            self.label_k8s_instance
        );
        let pod_list = Api::<Pod>::namespaced(self.client.clone(), &namespace)
            .list(&ListParams::default().labels(&label_selector))
            .await
            .context(ListPodsSnafu {
                namespace: namespace.clone(),
                label_k8s_instance: self.label_k8s_instance.clone(),
            })?;
        for pod in pod_list.items {
            if let Some(status) = pod.status {
                if status.phase.as_deref() != Some("Running") {
                    continue;
                }
                if let Some(pod_ip) = status.pod_ip {
                    if pod_ip.is_empty() {
                        continue;
                    }
                    components.insert(Component {
                        instance_type: InstanceType::TiKV,
                        host: pod_ip,
                        primary_port: 20160,
                        secondary_port: 20180,
                    });
                }
            }
        }
        Ok(())
    }
}
