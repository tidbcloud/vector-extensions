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
