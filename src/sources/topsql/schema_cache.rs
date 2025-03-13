use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tracing::{error, info};
use url::form_urlencoded;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DBInfo {
    #[serde(rename = "id")]
    pub id: i64,
    #[serde(rename = "db_name")]
    pub db_name: DBName,
    #[serde(rename = "state")]
    pub state: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DBName {
    #[serde(rename = "O")]
    pub o: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    #[serde(rename = "id")]
    pub id: i64,
    #[serde(rename = "name")]
    pub name: DBName,
    #[serde(rename = "indices")]
    pub indices: Vec<IndexInfo>,
    #[serde(rename = "partition")]
    pub partition: Option<PartitionInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexInfo {
    #[serde(rename = "id")]
    pub id: i64,
    #[serde(rename = "name")]
    pub name: DBName,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    #[serde(rename = "definitions")]
    pub definitions: Vec<PartitionDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionDefinition {
    #[serde(rename = "id")]
    pub id: i64,
    #[serde(rename = "name")]
    pub name: DBName,
}

#[derive(Debug, Clone)]
pub struct TableDetail {
    pub name: String,
    pub db: String,
    pub id: i64,
}

pub struct SchemaCache {
    cache: Arc<RwLock<HashMap<i64, TableDetail>>>,
    schema_version: Arc<AtomicI64>,
}

impl SchemaCache {
    pub fn new() -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            schema_version: Arc::new(AtomicI64::new(-1)),
        }
    }

    pub fn get(&self, table_id: i64) -> Option<TableDetail> {
        if let Ok(cache) = self.cache.read() {
            cache.get(&table_id).cloned()
        } else {
            None
        }
    }

    pub fn schema_version(&self) -> i64 {
        self.schema_version.load(Ordering::SeqCst)
    }

    pub async fn update(&self, client: &Client, tidb_instance: &str, https: bool) -> bool {
        let schema = if tidb_instance.starts_with("http") {
            ""
        } else if https {
            "https://"
        } else {
            "http://"
        };

        // Fetch all database info
        let db_infos: Vec<DBInfo> = match self
            .request_db(client, &format!("{}{}/schema", schema, tidb_instance))
            .await
        {
            Ok(infos) => infos,
            Err(err) => {
                error!(message = "Failed to fetch database info", %err);
                return false;
            }
        };

        let mut update_success = true;
        let mut new_cache = HashMap::new();

        // Fetch table info for each database
        for db in db_infos {
            if db.state == 0_i64 {
                // StateNone
                continue;
            }

            let encoded_name = form_urlencoded::Serializer::new(String::new())
                .append_pair("", &db.db_name.o)
                .finish();

            let table_infos: Vec<TableInfo> = match self
                .request_db(
                    client,
                    &format!(
                        "{}{}/schema/{}?id_name_only=true",
                        schema, tidb_instance, encoded_name
                    ),
                )
                .await
            {
                Ok(infos) => infos,
                Err(err) => {
                    error!(message = "Failed to fetch table info", db = %db.db_name.o, %err);
                    update_success = false;
                    continue;
                }
            };

            info!(message = "Updated table info", db = %db.db_name.o, table_count = table_infos.len());

            if table_infos.is_empty() {
                continue;
            }

            for table in table_infos {
                let detail = TableDetail {
                    name: table.name.o.clone(),
                    db: db.db_name.o.clone(),
                    id: table.id,
                };

                new_cache.insert(table.id, detail.clone());

                // Handle partitions
                if let Some(partition) = &table.partition {
                    for partition_def in &partition.definitions {
                        let partition_detail = TableDetail {
                            name: format!("{}/{}", table.name.o, partition_def.name.o),
                            db: db.db_name.o.clone(),
                            id: partition_def.id,
                        };
                        new_cache.insert(partition_def.id, partition_detail);
                    }
                }
            }
        }

        if update_success {
            if let Ok(mut cache) = self.cache.write() {
                *cache = new_cache;
                self.schema_version.fetch_add(1, Ordering::SeqCst);
            }
        }

        update_success
    }

    async fn request_db<T: for<'de> Deserialize<'de>>(
        &self,
        client: &Client,
        url: &str,
    ) -> Result<T, reqwest::Error> {
        client.get(url).send().await?.json().await
    }

    pub async fn update_schema_cache(
        &self,
        client: &Client,
        tidb_instance: &str,
        https: bool,
        etcd_client: &mut etcd_client::Client,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Get schema version from etcd
        let schema_version = {
            let ctx = tokio::time::timeout(
                Duration::from_secs(3),
                etcd_client.get("/tidb/ddl/global_schema_version", None),
            );

            let resp = match ctx.await {
                Ok(Ok(resp)) => resp,
                Ok(Err(err)) => {
                    tracing::error!("Failed to get schema version: {}", err);
                    return Err(Box::new(err));
                }
                Err(_) => {
                    tracing::error!("Timeout when getting schema version");
                    return Err("Timeout when getting schema version".into());
                }
            };

            if resp.kvs().is_empty() {
                return Ok(());
            }

            if resp.kvs().len() != 1 {
                tracing::warn!("Unexpected KV count when getting schema version");
                return Ok(());
            }

            match String::from_utf8(resp.kvs()[0].value().to_vec())
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
            {
                Some(version) => version,
                None => {
                    tracing::warn!("Failed to parse schema version");
                    return Ok(());
                }
            }
        };

        let current_version = self.schema_version();
        if schema_version == current_version {
            return Ok(());
        }

        tracing::info!(
            "Schema version changed: old={}, new={}",
            current_version,
            schema_version
        );

        // Create a temporary cache and update it
        let temp_cache = SchemaCache::new();
        if temp_cache.update(client, tidb_instance, https).await {
            // Only after successful update, acquire the write lock and update the version
            if let Ok(mut cache) = self.cache.write() {
                *cache = temp_cache.cache.read().unwrap().clone();
                self.schema_version.store(schema_version, Ordering::SeqCst);
            }
            Ok(())
        } else {
            Err("Failed to update schema cache".into())
        }
    }
}

pub struct SchemaManager {
    cache: Arc<SchemaCache>,
    client: Client,
    tidb_instance: String,
    https: bool,
    update_interval: Duration,
}

impl SchemaManager {
    pub fn new(tidb_instance: String, https: bool, update_interval: Duration) -> Self {
        Self {
            cache: Arc::new(SchemaCache::new()),
            client: Client::new(),
            tidb_instance,
            https,
            update_interval,
        }
    }

    pub fn get_cache(&self) -> Arc<SchemaCache> {
        self.cache.clone()
    }

    pub async fn run_update_loop_with_etcd(
        self,
        mut shutdown: watch::Receiver<()>,
        etcd_client: etcd_client::Client,
    ) {
        let etcd_client = Arc::new(tokio::sync::Mutex::new(etcd_client));

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    info!(message = "Schema manager is shutting down");
                    break;
                }
                _ = {
                    let cache = self.cache.clone();
                    let client = self.client.clone();
                    let tidb_instance = self.tidb_instance.clone();
                    let https = self.https;
                    let etcd = etcd_client.clone();

                    async move {
                        let mut etcd_lock = etcd.lock().await;
                        let _ = cache.update_schema_cache(
                            &client,
                            &tidb_instance,
                            https,
                            &mut *etcd_lock
                        ).await;
                    }
                } => {}
            }

            tokio::select! {
                _ = shutdown.changed() => {
                    info!(message = "Schema manager is shutting down");
                    break;
                }
                _ = tokio::time::sleep(self.update_interval) => {}
            }
        }
    }
}
