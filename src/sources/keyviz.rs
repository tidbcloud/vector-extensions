use std::{collections::HashSet, sync::Arc, time::Duration};

use chrono::Utc;
use rand::Rng;
use reqwest::{Certificate, Client, Identity};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use vector::{
    config::{GenerateConfig, SourceConfig, SourceContext},
    event::LogEvent,
    internal_events::StreamClosedError,
    tls::TlsSettings,
    SourceSender,
};
use vector_lib::{
    config::{DataType, LogNamespace, SourceOutput},
    configurable::configurable_component,
    internal_event::InternalEvent,
    source::Source,
    tls::TlsConfig,
};

use super::topsql::topology::{InstanceType, TopologyFetcher};

/// PLACEHOLDER
#[configurable_component(source("keyviz"))]
#[derive(Debug, Clone)]
pub struct KeyvizConfig {
    /// PLACEHOLDER
    pub pd_address: String,

    /// PLACEHOLDER
    pub tls: Option<TlsConfig>,
}

impl GenerateConfig for KeyvizConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            pd_address: "127.0.0.1:2379".to_owned(),
            tls: None,
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "keyviz")]
impl SourceConfig for KeyvizConfig {
    async fn build(&self, mut cx: SourceContext) -> vector::Result<Source> {
        self.validate_tls()?;
        let tls = self.tls.clone();
        let pd_address = if tls.is_some() {
            format!("https://{}", self.pd_address)
        } else {
            format!("http://{}", self.pd_address)
        };

        let mut builder = reqwest::Client::builder();
        if let Some(tls) = tls.clone() {
            let ca_file = tls.ca_file.clone().expect("tls ca file must be provided");
            let ca = match tokio::fs::read(ca_file).await {
                Ok(v) => v,
                Err(err) => {
                    error!(message = "Failed to read tls ca file", error = %err);
                    return Err(Box::new(err));
                }
            };
            let settings = TlsSettings::from_options(&Some(tls)).expect("invalid tls settings");
            let (crt, key) = settings.identity_pem().expect("invalid identity pem");
            builder = builder
                .add_root_certificate(Certificate::from_pem(&ca).expect("invalid ca"))
                .identity(Identity::from_pkcs8_pem(&crt, &key).expect("invalid crt & key"));
        }

        let client = match builder
            .timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(10))
            .build()
        {
            Ok(client) => client,
            Err(err) => {
                error!(message = "Failed to build reqwest client", %err);
                return Err(Box::new(err));
            }
        };

        let mut topo = TopologyFetcher::new(pd_address.clone(), tls.clone(), &cx.proxy).await?;
        let mut etcd = topo.etcd_client.clone();
        Ok(Box::pin(async move {
            tokio::time::sleep(Duration::from_secs(30)).await; // protect crash loop

            let tidb_instances = Arc::new(Mutex::new(Vec::new()));
            {
                let tidb_instances = tidb_instances.clone();
                let mut shutdown = cx.shutdown.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            _ = &mut shutdown => break,
                            _ = fetch_and_update_tidb_instances(&mut topo, tidb_instances.clone()) => {},
                        }
                        tokio::select! {
                            _ = &mut shutdown => break,
                            _ = tokio::time::sleep(Duration::from_secs(600)) => {},
                        }
                    }
                });
            }

            {
                let https = tls.is_some();
                let mut shutdown = cx.shutdown.clone();
                let mut client = client.clone();
                let mut out = cx.out.clone();
                tokio::spawn(async move {
                    let mut schema_version = -1;
                    loop {
                        tokio::select! {
                            _ = &mut shutdown => break,
                            _ = fetch_and_send_tidb_schema(
                                &mut client,
                                https,
                                &mut etcd,
                                &mut schema_version,
                                &mut out,
                                tidb_instances.clone(),
                            ) => {},
                        }
                        tokio::select! {
                            _ = &mut shutdown => break,
                            _ = tokio::time::sleep(Duration::from_secs(60)) => {},
                        }
                    }
                });
            }

            loop {
                let now = Utc::now();
                let filename = format!("{}.json", now.format("%Y%m%d%H%M").to_string());
                let mut ts = now.timestamp();
                ts -= ts % 60;
                let next_minute_ts = ts + 60;
                tokio::select! {
                    _ = &mut cx.shutdown => break,
                    _ = fetch_and_send_regions(
                        client.clone(),
                        &pd_address,
                        &mut cx.out,
                        filename,
                    ) => {},
                }
                let now = Utc::now().timestamp();
                if now < next_minute_ts {
                    tokio::select! {
                        _ = &mut cx.shutdown => break,
                        _ = tokio::time::sleep(Duration::from_secs((next_minute_ts - now + 1) as u64)) => {},
                    }
                }
            }
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

impl KeyvizConfig {
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
}

#[derive(Debug, Deserialize, Serialize)]
struct RegionsInfo {
    count: usize,
    regions: Vec<RegionInfo>,
}

#[derive(Debug, Deserialize, Serialize)]
struct RegionInfo {
    start_key: String,
    end_key: String,
    written_bytes: u64,
    read_bytes: u64,
    written_keys: u64,
    read_keys: u64,
}

async fn fetch_and_send_regions(
    client: Client,
    pd_address: &str,
    out: &mut SourceSender,
    filename: String,
) {
    match fetch_regions(client.clone(), pd_address).await {
        Ok(regions) => {
            let json = match serde_json::to_string(&regions) {
                Ok(v) => v,
                Err(err) => {
                    error!(message = "Failed to serialize regions json", %err);
                    return;
                }
            };
            let mut event = LogEvent::from_str_legacy(json);
            event.insert("filename", filename);
            if out.send_event(event).await.is_err() {
                StreamClosedError { count: 1 }.emit();
            }
        }
        Err(err) => {
            error!(message = "Failed to fetch regions", %err);
            return;
        }
    }
}

async fn fetch_regions(client: Client, pd_address: &str) -> reqwest::Result<RegionsInfo> {
    let mut all = RegionsInfo {
        count: 0,
        regions: vec![],
    };
    let mut start_key = Vec::new();
    loop {
        let mut regions =
            fetch_regions_part(client.clone(), pd_address, &start_key, &[], 51200).await?;
        // for region in &mut regions.regions {
        //     let start_bytes = match hex::decode(&region.start_key) {
        //         Ok(v) => v,
        //         Err(err) => {
        //             error!(message = "Failed to decode regions info start key", %err);
        //             continue;
        //         }
        //     };
        //     let end_bytes = match hex::decode(&region.end_key) {
        //         Ok(v) => v,
        //         Err(err) => {
        //             error!(message = "Failed to decode regions info end key", %err);
        //             continue;
        //         }
        //     };
        //     region.start_key = unsafe { String::from_utf8_unchecked(start_bytes) };
        //     region.end_key = unsafe { String::from_utf8_unchecked(end_bytes) };
        // }
        let last_key = regions.regions.last().map(|r| r.end_key.clone());
        all.regions.append(&mut regions.regions);
        all.count += regions.count;
        start_key = match last_key {
            None => break,
            Some(last_key) => {
                if last_key == "" {
                    break;
                }
                match hex::decode(&last_key) {
                    Err(_) => break,
                    Ok(last_key_bytes) => last_key_bytes,
                }
            }
        };
    }
    Ok(all)
}

async fn fetch_regions_part(
    client: Client,
    pd_address: &str,
    key: &[u8],
    end_key: &[u8],
    limit: i32,
) -> reqwest::Result<RegionsInfo> {
    let encoded_key = url::form_urlencoded::byte_serialize(key).collect::<String>();
    let encoded_end_key = url::form_urlencoded::byte_serialize(end_key).collect::<String>();
    client
        .get(format!("{}/pd/api/v1/regions/key", pd_address))
        .query(&[
            ("key", encoded_key),
            ("end_key", encoded_end_key),
            ("limit", limit.to_string()),
        ])
        .send()
        .await?
        .json()
        .await
}

#[derive(Debug, Deserialize, Serialize)]
struct DBTablesInfo {
    db: DBInfo,
    tables: Vec<TableInfo>,
}

#[derive(Debug, Deserialize, Serialize)]
struct DBInfo {
    id: i64,
    db_name: CIStr,
    state: u8,
}

#[derive(Debug, Deserialize, Serialize)]
struct TableInfo {
    id: i64,
    name: CIStr,
    index_info: Option<Vec<Option<IndexInfo>>>,
    partition: Option<PartitionInfo>,
}

#[derive(Debug, Deserialize, Serialize)]
struct IndexInfo {
    id: i64,
    idx_name: CIStr,
}

#[derive(Debug, Deserialize, Serialize)]
struct PartitionInfo {
    enable: bool,
    definitions: Option<Vec<Option<PartitionDefinition>>>,
}

#[derive(Debug, Deserialize, Serialize)]
struct PartitionDefinition {
    id: i64,
    name: CIStr,
}

#[derive(Debug, Deserialize, Serialize)]
struct CIStr {
    #[serde(rename = "O")]
    o: String,
    #[serde(rename = "L")]
    l: String,
}

async fn fetch_and_update_tidb_instances(
    topo: &mut TopologyFetcher,
    tidb_instances: Arc<Mutex<Vec<String>>>,
) {
    let mut components = HashSet::new();
    if let Err(err) = topo.get_up_components(&mut components).await {
        warn!(message = "Failed to fetch topology", %err);
        return;
    }
    let mut tidbs = vec![];
    for component in components {
        if component.instance_type == InstanceType::TiDB {
            tidbs.push(format!("{}:{}", component.host, component.secondary_port));
        }
    }
    *(tidb_instances.lock().await) = tidbs;
}

async fn fetch_and_send_tidb_schema(
    client: &mut Client,
    https: bool,
    etcd: &mut etcd_client::Client,
    schema_version: &mut i32,
    out: &mut SourceSender,
    tidb_instances: Arc<Mutex<Vec<String>>>,
) {
    let resp = match etcd.get("/tidb/ddl/global_schema_version", None).await {
        Ok(v) => v,
        Err(err) => {
            warn!(message = "Failed to fetch tidb schema version", %err);
            return;
        }
    };
    let kvs = resp.kvs();
    if kvs.len() != 1 {
        warn!(message = "Failed to fetch tidb schema version, invalid response");
        return;
    }
    let value = match String::from_utf8(kvs[0].value().to_owned()) {
        Ok(v) => v,
        Err(err) => {
            warn!(message = "Failed to parse tidb schema version", %err);
            return;
        }
    };
    let new_schema_version = match value.parse::<i32>() {
        Ok(v) => v,
        Err(err) => {
            warn!(message = "Failed to parse tidb schema version", %err);
            return;
        }
    };
    if new_schema_version == *schema_version {
        return;
    }
    let tidb_instance = {
        let tidb_instances = tidb_instances.lock().await;
        if tidb_instances.is_empty() {
            return;
        }
        let idx = rand::thread_rng().gen_range(0..tidb_instances.len());
        tidb_instances[idx].clone()
    };
    let dbinfos = match fetch_tidb_dbinfos(client, https, &tidb_instance).await {
        Ok(v) => v,
        Err(err) => {
            warn!(message = "Failed to fetch tidb db info", %err);
            return;
        }
    };
    let mut db_tables = Vec::with_capacity(dbinfos.len());
    let mut update_success = true;
    for dbinfo in dbinfos {
        if dbinfo.state == 0 {
            continue;
        }
        let tableinfos =
            match fetch_tidb_tableinfos(client, https, &tidb_instance, &dbinfo.db_name.o).await {
                Ok(v) => v,
                Err(err) => {
                    update_success = false;
                    warn!(message = "Failed to fetch tidb table info", %err);
                    continue;
                }
            };
        db_tables.push(DBTablesInfo {
            db: dbinfo,
            tables: tableinfos,
        })
    }
    if update_success {
        *schema_version = new_schema_version;
    }
    let json = match serde_json::to_string(&db_tables) {
        Ok(v) => v,
        Err(err) => {
            error!(message = "Failed to serialize tidb schema json", %err);
            return;
        }
    };
    let mut event = LogEvent::from_str_legacy(json);
    event.insert("filename", "tidb-schema");
    if out.send_event(event).await.is_err() {
        StreamClosedError { count: 1 }.emit();
    }
}

async fn fetch_tidb_dbinfos(
    client: &mut Client,
    https: bool,
    tidb_instance: &str,
) -> reqwest::Result<Vec<DBInfo>> {
    let schema = if tidb_instance.starts_with("http") {
        ""
    } else if https {
        "https://"
    } else {
        "http://"
    };
    client
        .get(format!("{}{}/schema", schema, tidb_instance))
        .send()
        .await?
        .json()
        .await
}

async fn fetch_tidb_tableinfos(
    client: &mut Client,
    https: bool,
    tidb_instance: &str,
    dbname: &str,
) -> reqwest::Result<Vec<TableInfo>> {
    let schema = if tidb_instance.starts_with("http") {
        ""
    } else if https {
        "https://"
    } else {
        "http://"
    };
    let dbname = url::form_urlencoded::byte_serialize(dbname.as_bytes()).collect::<String>();
    client
        .get(format!("{}{}/schema/{}", schema, tidb_instance, dbname))
        .send()
        .await?
        .json()
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_config() {
        vector::test_util::test_generate_config::<KeyvizConfig>();
    }
}
