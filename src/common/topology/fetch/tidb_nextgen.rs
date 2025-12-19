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
        tidb_group: String,
        source: kube::Error,
    },
}

pub struct TiDBNextGenTopologyFetcher {
    client: Client,
    tidb_group: String,
}

impl TiDBNextGenTopologyFetcher {
    pub fn new(client: Client, tidb_group: String) -> Self {
        Self { client, tidb_group }
    }

    pub async fn get_up_tidbs(
        &self,
        components: &mut HashSet<Component>,
    ) -> Result<(), FetchError> {
        let namespace =
            tokio::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
                .await
                .context(GetNamespaceSnafu)?;

        let label_selector = if self.tidb_group.is_empty() {
            "app.kubernetes.io/component=tidb".to_string()
        } else {
            format!(
                "app.kubernetes.io/component=tidb,tags.tidbcloud.com/tidb-group={}",
                self.tidb_group
            )
        };
        let pod_list = Api::<Pod>::namespaced(self.client.clone(), &namespace)
            .list(&ListParams::default().labels(&label_selector))
            .await
            .context(ListPodsSnafu {
                namespace: namespace.clone(),
                tidb_group: self.tidb_group.clone(),
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
                        instance_type: InstanceType::TiDB,
                        host: pod_ip,
                        primary_port: 4000,
                        secondary_port: 10080,
                    });
                }
            }
        }
        Ok(())
    }
}
