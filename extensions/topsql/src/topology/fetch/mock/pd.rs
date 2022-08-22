use rand::prelude::SliceRandom;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
pub struct PDURL {
    pub client_url: String,
    pub peer_url: String,
}

#[derive(Clone, Debug)]
pub struct PDInfo {
    pub pd_url: PDURL,
    pub name: String,
    pub member_id: u64,
    pub deploy_path: String,
    pub binary_version: String,
    pub git_hash: String,
}

#[derive(Clone, Debug)]
pub struct PDResponseGenerator {
    pub cluster_id: u64,
    pub pd_infos: Vec<PDInfo>,
    pub leader: PDInfo,
}

pub type Health = Vec<HealthItem>;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct HealthItem {
    pub name: String,
    pub member_id: u64,
    pub client_urls: Vec<String>,
    pub health: bool,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Members {
    pub header: Header,
    pub members: Vec<MemberItem>,
    pub leader: MemberItem,
    pub etcd_leader: MemberItem,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Header {
    pub cluster_id: u64,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MemberItem {
    pub name: String,
    pub member_id: u64,
    pub peer_urls: Vec<String>,
    pub client_urls: Vec<String>,
    pub deploy_path: String,
    pub binary_version: String,
    pub git_hash: String,
}

impl PDResponseGenerator {
    pub fn new(pd_urls: Vec<PDURL>) -> Self {
        let binary_version = "v6.1.0";
        let git_hash = "d82f4fab6cf37cd1eca9c3574984e12a7ae27c42";

        let pd_infos = pd_urls
            .into_iter()
            .map(|url| {
                let uri: hyper::Uri = url.client_url.parse().unwrap();

                PDInfo {
                    pd_url: url,
                    name: format!("pd-{}-{}", uri.host().unwrap(), uri.port().unwrap()),
                    member_id: rand::random::<u64>(),
                    deploy_path: format!("/deploy/pd-{}/bin", uri.port().unwrap()),
                    binary_version: binary_version.to_owned(),
                    git_hash: git_hash.to_owned(),
                }
            })
            .collect::<Vec<_>>();

        let cluster_id = rand::random::<u64>();
        let leader = pd_infos.choose(&mut rand::thread_rng()).unwrap().clone();

        PDResponseGenerator {
            cluster_id,
            pd_infos,
            leader,
        }
    }

    pub fn members_resp(&self) -> String {
        serde_json::to_string_pretty(&self.members()).unwrap()
    }

    pub fn health_resp(&self) -> String {
        serde_json::to_string_pretty(&self.health()).unwrap()
    }

    pub fn members(&self) -> Members {
        Members {
            header: Header {
                cluster_id: self.cluster_id,
            },
            members: self.pd_infos.iter().map(Self::info_to_member).collect(),
            leader: Self::info_to_member(&self.leader),
            etcd_leader: Self::info_to_member(&self.leader),
        }
    }

    pub fn health(&self) -> Health {
        self.pd_infos.iter().map(Self::info_to_health).collect()
    }

    fn info_to_member(info: &PDInfo) -> MemberItem {
        MemberItem {
            name: info.name.clone(),
            member_id: info.member_id,
            peer_urls: vec![info.pd_url.peer_url.clone()],
            client_urls: vec![info.pd_url.client_url.clone()],
            deploy_path: info.deploy_path.clone(),
            binary_version: info.binary_version.clone(),
            git_hash: info.git_hash.clone(),
        }
    }

    fn info_to_health(info: &PDInfo) -> HealthItem {
        HealthItem {
            name: info.name.clone(),
            member_id: info.member_id,
            client_urls: vec![info.pd_url.client_url.clone()],
            health: true,
        }
    }
}
