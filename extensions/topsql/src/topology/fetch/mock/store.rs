#[derive(Clone, Debug)]
pub struct TiKVAddress {
    pub host: String,
    pub port: u16,
    pub status_port: u16,
}

#[derive(Clone, Debug)]
pub struct TiFlashAddress {
    pub host: String,
    pub tcp_port: u16,
    pub http_port: u16,
    pub flash_service_port: u16,
    pub flash_proxy_port: u16,
    pub flash_proxy_status_port: u16,
    pub metrics_port: u16,
}

pub struct StoresResponseGenerator {
    pub tikvs: Vec<TiKVAddress>,
    pub tiflashs: Vec<TiFlashAddress>,
}

impl StoresResponseGenerator {
    pub fn new(tikvs: Vec<TiKVAddress>, tiflashs: Vec<TiFlashAddress>) -> Self {
        StoresResponseGenerator { tikvs, tiflashs }
    }
}
