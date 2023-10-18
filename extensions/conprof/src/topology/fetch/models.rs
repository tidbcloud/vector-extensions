use serde::{Deserialize, Serialize};

pub type HealthResponse = Vec<HealthItem>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HealthItem {
    pub member_id: u64,
    pub health: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MembersResponse {
    pub members: Vec<MemberItem>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MemberItem {
    pub member_id: u64,
    pub client_urls: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TopologyValue {
    pub status_port: u16,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StoresResponse {
    pub stores: Vec<StoreItem>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StoreItem {
    pub store: StoreInfo,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StoreInfo {
    pub address: String,
    pub status_address: String,
    pub state_name: String,
    #[serde(default)]
    pub labels: Vec<LabelItem>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LabelItem {
    pub key: String,
    pub value: String,
}
