use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::utils::http::build_reqwest_client;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tracing::{error, info};
use vector::tls::TlsConfig;

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
    pub indices: Option<Vec<IndexInfo>>,
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

    // Calculate the memory usage of the schema cache
    pub fn memory_usage(&self) -> usize {
        if let Ok(cache) = self.cache.read() {
            // Size of HashMap overhead (rough estimate)
            let mut size = std::mem::size_of::<HashMap<i64, TableDetail>>();

            // Size of each entry
            for (_key, value) in cache.iter() {
                // Size of key (i64)
                size += std::mem::size_of::<i64>();

                // Size of TableDetail struct
                size += std::mem::size_of::<TableDetail>();

                // Size of String contents for name and db fields
                size += value.name.capacity();
                size += value.db.capacity();
            }

            size
        } else {
            0
        }
    }

    // Get the number of entries in the cache
    pub fn entry_count(&self) -> usize {
        if let Ok(cache) = self.cache.read() {
            cache.len()
        } else {
            0
        }
    }

    pub async fn update(
        &self,
        client: &Client,
        tidb_instance: &str,
        tls: &Option<TlsConfig>,
    ) -> bool {
        let schema = if tidb_instance.starts_with("http") {
            ""
        } else if tls.is_some() {
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

            let table_infos: Vec<TableInfo> = match self
                .request_db(
                    client,
                    &format!(
                        "{}{}/schema/{}?id_name_only=true",
                        schema, tidb_instance, &db.db_name.o
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

        // After successful update, acquire the write lock
        if let Ok(mut cache) = self.cache.write() {
            *cache = new_cache;
        }

        update_success
    }

    async fn request_db<T: for<'de> Deserialize<'de>>(
        &self,
        client: &Client,
        url: &str,
    ) -> Result<T, reqwest::Error> {
        let response = client.get(url).send().await?;

        // Check status first without consuming body
        if let Err(err) = response.error_for_status_ref() {
            // Status is non-success. Log body and return the error.
            let status = err.status().unwrap_or_default();
            let body_text = response
                .text() // Consume body for logging
                .await
                .unwrap_or_else(|_| "Failed to read body".to_string());
            error!(message = "Received non-success status code", %url, %status, body = %body_text);
            // Return the original error captured by error_for_status_ref
            return Err(err);
        }

        // Status is likely success, attempt to parse JSON directly.
        // This consumes the response body.
        match response.json::<T>().await {
            Ok(data) => {
                // Optional: Log successful data if needed (requires T: Debug)
                // tracing::debug!(message = "Successfully parsed response", %url);
                Ok(data)
            }
            Err(err) => {
                // .json() failed. Could be decode error or body reading error.
                // Log the reqwest::Error, which should contain details.
                error!(message = "Failed to process response body or decode JSON", %url, error = %err);
                // We cannot log the raw body here as .json() consumed it.
                Err(err)
            }
        }
    }

    pub async fn update_schema_cache(
        &self,
        client: &Client,
        tidb_instance: &str,
        tls: &Option<TlsConfig>,
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
        if temp_cache.update(client, tidb_instance, tls).await {
            // Only after successful update, acquire the write lock and update the version
            if let Ok(mut cache) = self.cache.write() {
                *cache = temp_cache.cache.read().unwrap().clone();
                self.schema_version.store(schema_version, Ordering::SeqCst);

                // Log memory usage after update
                let entries = self.entry_count();
                let memory = self.memory_usage();
                tracing::info!(
                    "Schema cache updated: entries={}, memory_usage={} bytes ({}KB)",
                    entries,
                    memory,
                    memory / 1024
                );
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
    tls: Option<TlsConfig>,
    update_interval: Duration,
}

impl SchemaManager {
    pub async fn new(
        tidb_instance: String,
        update_interval: Duration,
        tls: Option<TlsConfig>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // Use the standardized client builder
        let client = build_reqwest_client(tls.clone(), None, None).await?;

        Ok(Self {
            cache: Arc::new(SchemaCache::new()),
            client,
            tidb_instance,
            tls,
            update_interval,
        })
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
                    let tls = self.tls.clone();
                    let etcd = etcd_client.clone();

                    async move {
                        let mut etcd_lock = etcd.lock().await;
                        let _ = cache.update_schema_cache(
                            &client,
                            &tidb_instance,
                            &tls,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};

    // Create a comprehensive test cache with various table types
    fn create_complex_test_cache() -> SchemaCache {
        let mut cache_map = HashMap::new();

        // Add a regular table
        cache_map.insert(
            1,
            TableDetail {
                name: "regular_table".to_string(),
                db: "test_db".to_string(),
                id: 1,
            },
        );

        // Add a partitioned table
        cache_map.insert(
            2,
            TableDetail {
                name: "partitioned_table".to_string(),
                db: "test_db".to_string(),
                id: 2,
            },
        );

        // Add partitions
        cache_map.insert(
            21,
            TableDetail {
                name: "partitioned_table/p0".to_string(),
                db: "test_db".to_string(),
                id: 21,
            },
        );

        cache_map.insert(
            22,
            TableDetail {
                name: "partitioned_table/p1".to_string(),
                db: "test_db".to_string(),
                id: 22,
            },
        );

        // Add a temporary table
        cache_map.insert(
            3,
            TableDetail {
                name: "temp_table".to_string(),
                db: "temp_db".to_string(),
                id: 3,
            },
        );

        // Add a table with special characters in its name
        cache_map.insert(
            4,
            TableDetail {
                name: "special-table.name#123".to_string(),
                db: "special_db".to_string(),
                id: 4,
            },
        );

        // Add a table with a very long name
        cache_map.insert(5, TableDetail {
            name: "very_long_table_name_that_exceeds_normal_length_and_tests_memory_allocation_for_strings_in_the_cache_implementation".to_string(),
            db: "long_names_db".to_string(),
            id: 5,
        });

        SchemaCache {
            cache: Arc::new(RwLock::new(cache_map)),
            schema_version: Arc::new(AtomicI64::new(100)),
        }
    }

    #[test]
    fn test_get_with_complex_data() {
        let cache = create_complex_test_cache();

        // Test regular table
        let regular = cache.get(1);
        assert!(regular.is_some());
        let regular = regular.unwrap();
        assert_eq!(regular.name, "regular_table");
        assert_eq!(regular.db, "test_db");

        // Test partition table
        let partition = cache.get(21);
        assert!(partition.is_some());
        let partition = partition.unwrap();
        assert_eq!(partition.name, "partitioned_table/p0");
        assert_eq!(partition.db, "test_db");

        // Test table with special characters
        let special = cache.get(4);
        assert!(special.is_some());
        let special = special.unwrap();
        assert_eq!(special.name, "special-table.name#123");

        // Test table with long name
        let long = cache.get(5);
        assert!(long.is_some());

        // Test non-existent table
        let not_found = cache.get(999);
        assert!(not_found.is_none());
    }

    #[test]
    fn test_memory_usage_accuracy() {
        // Create an empty cache
        let empty_cache = SchemaCache::new();
        let empty_usage = empty_cache.memory_usage();

        // Create a cache with just one small table entry
        let mut single_item_map = HashMap::new();
        single_item_map.insert(
            1,
            TableDetail {
                name: "t".to_string(), // Short name
                db: "d".to_string(),   // Short database name
                id: 1,
            },
        );

        let single_item_cache = SchemaCache {
            cache: Arc::new(RwLock::new(single_item_map)),
            schema_version: Arc::new(AtomicI64::new(1)),
        };

        let single_usage = single_item_cache.memory_usage();

        // Create a cache with lots of data
        let complex_cache = create_complex_test_cache();
        let complex_usage = complex_cache.memory_usage();

        // Verify that memory usage increases with the number of entries
        assert!(empty_usage < single_usage);
        assert!(single_usage < complex_usage);

        // Create a large cache with 100 entries
        let mut large_map = HashMap::new();
        for i in 0..100 {
            large_map.insert(
                i,
                TableDetail {
                    name: format!("table_{}", i),
                    db: format!("db_{}", i / 10),
                    id: i,
                },
            );
        }

        let large_cache = SchemaCache {
            cache: Arc::new(RwLock::new(large_map)),
            schema_version: Arc::new(AtomicI64::new(1)),
        };

        let large_usage = large_cache.memory_usage();

        // Large cache should use more memory
        assert!(complex_usage < large_usage);

        // Test memory calculation logic - base size of empty HashMap plus number of entries * size per entry
        let estimated_base = std::mem::size_of::<HashMap<i64, TableDetail>>();
        let estimated_per_entry = std::mem::size_of::<i64>() + std::mem::size_of::<TableDetail>();

        // Minimum estimated memory usage for 100 entries
        let min_estimated = estimated_base + 100 * estimated_per_entry;

        // Actual usage should be greater than or equal to the minimum estimate (considering strings and other overhead)
        assert!(large_usage >= min_estimated);
    }

    // Test concurrent writing
    #[test]
    fn test_concurrent_write() {
        let cache = SchemaCache::new();
        let arc_cache = Arc::new(cache);

        // Create multiple threads to update different parts of the cache simultaneously
        let mut handles = vec![];
        for i in 0..5 {
            let cache_clone = arc_cache.clone();
            let handle = std::thread::spawn(move || {
                if let Ok(mut map) = cache_clone.cache.write() {
                    // Each thread adds 10 different entries
                    for j in 0..10 {
                        let id = i * 10 + j;
                        map.insert(
                            id,
                            TableDetail {
                                name: format!("table_{}", id),
                                db: format!("db_{}", i),
                                id,
                            },
                        );
                    }
                }
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify the result - should have 50 entries
        assert_eq!(arc_cache.entry_count(), 50);
    }

    // Simulate update method test without using external HTTP
    #[test]
    fn test_cache_update_simulation() {
        // Create a cache with initial data
        let schema_cache = SchemaCache::new();

        // Manually populate the cache
        {
            let mut cache_map = HashMap::new();
            cache_map.insert(
                1,
                TableDetail {
                    name: "table1".to_string(),
                    db: "db1".to_string(),
                    id: 1,
                },
            );

            if let Ok(mut cache) = schema_cache.cache.write() {
                *cache = cache_map;
            }
        }

        // Verify initial state
        assert_eq!(schema_cache.entry_count(), 1);
        let table1 = schema_cache.get(1);
        assert!(table1.is_some());
        assert_eq!(table1.unwrap().name, "table1");

        // Simulate an update with new data
        {
            let mut new_cache = HashMap::new();
            new_cache.insert(
                1,
                TableDetail {
                    name: "table1_updated".to_string(),
                    db: "db1".to_string(),
                    id: 1,
                },
            );
            new_cache.insert(
                2,
                TableDetail {
                    name: "table2".to_string(),
                    db: "db1".to_string(),
                    id: 2,
                },
            );

            if let Ok(mut cache) = schema_cache.cache.write() {
                *cache = new_cache;
            }
        }

        // Verify updated state
        assert_eq!(schema_cache.entry_count(), 2);
        let table1 = schema_cache.get(1);
        assert!(table1.is_some());
        assert_eq!(table1.unwrap().name, "table1_updated");

        let table2 = schema_cache.get(2);
        assert!(table2.is_some());
        assert_eq!(table2.unwrap().name, "table2");
    }
}
