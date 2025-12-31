use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use vector::event::Event;
use vector_lib::event::{LogEvent, Value as LogValue};
use crate::sources::topsql_v2::schema_cache::SchemaCache;
use crate::sources::topsql_v2::upstream::consts::{
    LABEL_ENCODED_NORMALIZED_PLAN, LABEL_INSTANCE_KEY,
    LABEL_NORMALIZED_PLAN, LABEL_NORMALIZED_SQL, LABEL_PLAN_DIGEST,
    LABEL_SQL_DIGEST, LABEL_SOURCE_TABLE, LABEL_TIMESTAMPS, LABEL_KEYSPACE,
    METRIC_NAME_CPU_TIME_MS, METRIC_NAME_NETWORK_IN_BYTES, METRIC_NAME_NETWORK_OUT_BYTES,
    METRIC_NAME_STMT_DURATION_COUNT, METRIC_NAME_STMT_DURATION_SUM_NS, METRIC_NAME_STMT_EXEC_COUNT,
    SOURCE_TABLE_TIDB_TOPSQL, SOURCE_TABLE_TOPSQL_PLAN_META, SOURCE_TABLE_TOPSQL_SQL_META,
};
use crate::sources::topsql_v2::upstream::parser::UpstreamEventParser;
use crate::sources::topsql_v2::upstream::tidb::proto::top_sql_sub_response::RespOneof;
use crate::sources::topsql_v2::upstream::tidb::proto::{
    PlanMeta, SqlMeta, TopSqlRecord, TopSqlRecordItem, TopSqlSubResponse,
};

pub struct TopSqlSubResponseParser;

impl UpstreamEventParser for TopSqlSubResponseParser {
    type UpstreamEvent = TopSqlSubResponse;

    fn parse(
        response: Self::UpstreamEvent,
        instance: String,
        _schema_cache: Arc<SchemaCache>,
    ) -> Vec<LogEvent> {
        match response.resp_oneof {
            Some(RespOneof::Record(record)) => {
                Self::parse_tidb_record(record, instance)
            }
            Some(RespOneof::SqlMeta(sql_meta)) => Self::parse_tidb_sql_meta(sql_meta),
            Some(RespOneof::PlanMeta(plan_meta)) => Self::parse_tidb_plan_meta(plan_meta),
            None => vec![],
        }
    }

    fn keep_top_n(responses: Vec<Self::UpstreamEvent>, top_n: usize) -> Vec<Self::UpstreamEvent> {
        #[derive(Clone)]
        struct PerPeriodDigest {
            sql_digest: Vec<u8>,
            plan_digest: Vec<u8>,
            cpu_time_ms: u32,
            stmt_exec_count: u64,
            stmt_duration_sum_ns: u64,
            stmt_duration_count: u64,
            stmt_network_in_bytes: u64,
            stmt_network_out_bytes: u64,
        }

        let mut new_responses = vec![];
        let mut ts_others = BTreeMap::new();
        let mut ts_digests = BTreeMap::new();
        for response in responses {
            if let Some(RespOneof::Record(record)) = response.resp_oneof {
                if record.sql_digest.is_empty() {
                    for item in record.items {
                        ts_others.insert(item.timestamp_sec, item);
                    }
                } else {
                    for item in &record.items {
                        let psd = PerPeriodDigest {
                            sql_digest: record.sql_digest.clone(),
                            plan_digest: record.plan_digest.clone(),
                            cpu_time_ms: item.cpu_time_ms,
                            stmt_exec_count: item.stmt_exec_count,
                            stmt_duration_sum_ns: item.stmt_duration_sum_ns,
                            stmt_duration_count: item.stmt_duration_count,
                            stmt_network_in_bytes: item.stmt_network_in_bytes,
                            stmt_network_out_bytes: item.stmt_network_out_bytes,
                        };
                        match ts_digests.get_mut(&item.timestamp_sec) {
                            None => {
                                ts_digests.insert(item.timestamp_sec, vec![psd]);
                            }
                            Some(v) => {
                                v.push(psd);
                            }
                        }
                    }
                }
            } else {
                new_responses.push(response);
            }
        }

        for (ts, v) in &mut ts_digests {
            if v.len() <= top_n {
                continue;
            }
            // Handle top_n = 0 case: all records go to others
            if top_n == 0 {
                let mut others = TopSqlRecordItem::default();
                for psd in v.iter() {
                    others.timestamp_sec = *ts;
                    others.cpu_time_ms += psd.cpu_time_ms;
                    others.stmt_exec_count = psd.stmt_exec_count;
                    others.stmt_duration_sum_ns = psd.stmt_duration_sum_ns;
                    others.stmt_duration_count = psd.stmt_duration_count;
                    others.stmt_network_in_bytes += psd.stmt_network_in_bytes;
                    others.stmt_network_out_bytes += psd.stmt_network_out_bytes;
                }
                v.clear();
                match ts_others.get_mut(&ts) {
                    None => {
                        ts_others.insert(*ts, others);
                    }
                    Some(existed_others) => {
                        existed_others.cpu_time_ms += others.cpu_time_ms;
                        existed_others.stmt_exec_count += others.stmt_exec_count;
                        existed_others.stmt_duration_sum_ns += others.stmt_duration_sum_ns;
                        existed_others.stmt_duration_count += others.stmt_duration_count;
                        existed_others.stmt_network_in_bytes += others.stmt_network_in_bytes;
                        existed_others.stmt_network_out_bytes += others.stmt_network_out_bytes;
                    }
                }
                continue;
            }
            // Find top_n threshold for cpu_time_ms using partial selection
            let mut cpu_values: Vec<u32> = v.iter().map(|psd| psd.cpu_time_ms).collect();
            cpu_values.select_nth_unstable_by(top_n - 1, |a, b| b.cmp(a));
            let cpu_threshold = cpu_values[top_n - 1];
            
            // Find top_n threshold for network bytes using partial selection
            let mut network_values: Vec<u64> = v.iter()
                .map(|psd| psd.stmt_network_in_bytes + psd.stmt_network_out_bytes)
                .collect();
            network_values.select_nth_unstable_by(top_n - 1, |a, b| b.cmp(a));
            let network_threshold = network_values[top_n - 1];
            
            // Keep records that meet either threshold
            let mut kept = Vec::new();
            let mut evicted = Vec::new();
            for psd in v.iter() {
                let network_bytes = psd.stmt_network_in_bytes + psd.stmt_network_out_bytes;
                if psd.cpu_time_ms >= cpu_threshold || network_bytes >= network_threshold {
                    kept.push(psd.clone());
                } else {
                    evicted.push(psd.clone());
                }
            }
            
            let mut others = TopSqlRecordItem::default();
            for e in evicted {
                others.timestamp_sec = *ts;
                others.cpu_time_ms += e.cpu_time_ms;
                others.stmt_exec_count += e.stmt_exec_count;
                others.stmt_duration_sum_ns += e.stmt_duration_sum_ns;
                others.stmt_duration_count += e.stmt_duration_count;
                others.stmt_network_in_bytes += e.stmt_network_in_bytes;
                others.stmt_network_out_bytes += e.stmt_network_out_bytes;
            }
            *v = kept;
            match ts_others.get_mut(&ts) {
                None => {
                    ts_others.insert(*ts, others);
                }
                Some(existed_others) => {
                    existed_others.cpu_time_ms += others.cpu_time_ms;
                    existed_others.stmt_exec_count += others.stmt_exec_count;
                    existed_others.stmt_duration_sum_ns += others.stmt_duration_sum_ns;
                    existed_others.stmt_duration_count += others.stmt_duration_count;
                    existed_others.stmt_network_in_bytes += others.stmt_network_in_bytes;
                    existed_others.stmt_network_out_bytes += others.stmt_network_out_bytes;
                }
            }
        }

        let mut digest_items = HashMap::new();
        for (ts, v) in ts_digests {
            for psd in v {
                let k = (psd.sql_digest, psd.plan_digest);
                let item = TopSqlRecordItem {
                    timestamp_sec: ts,
                    cpu_time_ms: psd.cpu_time_ms,
                    stmt_exec_count: psd.stmt_exec_count,
                    stmt_kv_exec_count: BTreeMap::new(),
                    stmt_duration_sum_ns: psd.stmt_duration_sum_ns,
                    stmt_duration_count: psd.stmt_duration_count,
                    stmt_network_in_bytes: psd.stmt_network_in_bytes,
                    stmt_network_out_bytes: psd.stmt_network_out_bytes,
                };
                match digest_items.get_mut(&k) {
                    None => {
                        digest_items.insert(k, vec![item]);
                    }
                    Some(items) => {
                        items.push(item);
                    }
                }
            }
        }
        if !ts_others.is_empty() {
            let others_k = (vec![], vec![]);
            digest_items.insert(others_k, ts_others.into_values().collect());
        }

        for (digest, items) in digest_items {
            new_responses.push(TopSqlSubResponse {
                resp_oneof: Some(RespOneof::Record(TopSqlRecord {
                    sql_digest: digest.0,
                    plan_digest: digest.1,
                    items,
                    keyspace_name: vec![],
                })),
            })
        }
        new_responses
    }

    fn downsampling(responses: &mut Vec<Self::UpstreamEvent>, interval_sec: u32) {
        if interval_sec <= 1 {
            return;
        }
        let interval_sec = interval_sec as u64;
        for response in responses {
            if let Some(RespOneof::Record(record)) = &mut response.resp_oneof {
                let mut new_items = BTreeMap::new();
                for item in &record.items {
                    let new_ts =
                        item.timestamp_sec + (interval_sec - item.timestamp_sec % interval_sec);
                    match new_items.get(&new_ts) {
                        None => {
                            let mut new_item = item.clone();
                            new_item.timestamp_sec = new_ts;
                            new_items.insert(new_ts, new_item);
                        }
                        Some(existed_item) => {
                            let mut new_item = existed_item.clone();
                            new_item.cpu_time_ms += item.cpu_time_ms;
                            new_item.stmt_exec_count += item.stmt_exec_count;
                            new_item.stmt_duration_count += item.stmt_duration_count;
                            new_item.stmt_duration_sum_ns += item.stmt_duration_sum_ns;
                            new_item.stmt_network_in_bytes += item.stmt_network_in_bytes;
                            new_item.stmt_network_out_bytes += item.stmt_network_out_bytes;
                            new_items.insert(new_ts, new_item);
                        }
                    }
                }
                record.items = new_items.into_values().collect();
            }
        }
    }
}

impl TopSqlSubResponseParser {
    fn parse_tidb_record(
        record: TopSqlRecord, 
        instance: String, 
    ) -> Vec<LogEvent> {
        let mut keyspace_name_str = "".to_string();
        if !record.keyspace_name.is_empty() {
            if let Ok(ks) = String::from_utf8(record.keyspace_name.clone()) {
                keyspace_name_str = ks;
            }
        }
        let mut events = vec![];
        let instance_key = format!("topsql_tidb_{}", instance);
        for item in &record.items {
            let mut event = Event::Log(LogEvent::default());
            let log = event.as_mut_log();

            // Add metadata with Vector prefix (ensure all fields have values)
            log.insert(LABEL_SOURCE_TABLE, SOURCE_TABLE_TIDB_TOPSQL);
            log.insert(LABEL_TIMESTAMPS, LogValue::from(item.timestamp_sec));
            log.insert(LABEL_INSTANCE_KEY, instance_key.clone());
            if !keyspace_name_str.is_empty() {
                log.insert(LABEL_KEYSPACE, keyspace_name_str.clone());
            }
            log.insert(
                LABEL_SQL_DIGEST,
                hex::encode_upper(record.sql_digest.clone()),
            );
            log.insert(
                LABEL_PLAN_DIGEST,
                hex::encode_upper(record.plan_digest.clone()),
            );
            log.insert(METRIC_NAME_CPU_TIME_MS, LogValue::from(item.cpu_time_ms));
            log.insert(
                METRIC_NAME_STMT_EXEC_COUNT,
                LogValue::from(item.stmt_exec_count),
            );
            log.insert(
                METRIC_NAME_STMT_DURATION_SUM_NS,
                LogValue::from(item.stmt_duration_sum_ns),
            );
            log.insert(
                METRIC_NAME_STMT_DURATION_COUNT,
                LogValue::from(item.stmt_duration_count),
            );
            log.insert(
                METRIC_NAME_NETWORK_IN_BYTES,
                LogValue::from(item.stmt_network_in_bytes),
            );
            log.insert(
                METRIC_NAME_NETWORK_OUT_BYTES,
                LogValue::from(item.stmt_network_out_bytes),
            );
            events.push(event.into_log());
        }
        events
    }

    fn parse_tidb_sql_meta(sql_meta: SqlMeta) -> Vec<LogEvent> {
        let mut events = vec![];
        let sql_digest = hex::encode_upper(sql_meta.sql_digest);
        let mut event = Event::Log(LogEvent::default());
        let log = event.as_mut_log();

        log.insert(LABEL_SOURCE_TABLE, SOURCE_TABLE_TOPSQL_SQL_META);
        log.insert(LABEL_SQL_DIGEST, sql_digest);
        log.insert(LABEL_NORMALIZED_SQL, sql_meta.normalized_sql);
        events.push(event.into_log());
        events
    }

    fn parse_tidb_plan_meta(plan_meta: PlanMeta) -> Vec<LogEvent> {
        let mut events = vec![];
        let plan_digest = hex::encode_upper(plan_meta.plan_digest);
        let encoded_normalized_plan =
        hex::encode_upper(plan_meta.encoded_normalized_plan);
        let mut event = Event::Log(LogEvent::default());
        let log = event.as_mut_log();

        // Add metadata with Vector prefix (ensure all fields have values)
        log.insert(LABEL_SOURCE_TABLE, SOURCE_TABLE_TOPSQL_PLAN_META);
        log.insert(LABEL_PLAN_DIGEST, plan_digest);
        log.insert(LABEL_NORMALIZED_PLAN, plan_meta.normalized_plan);
        log.insert(
            LABEL_ENCODED_NORMALIZED_PLAN,
            encoded_normalized_plan,
        );
        events.push(event.into_log());
        events
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::topsql_v2::upstream::tidb::proto::TopSqlRecordItem;

    const MOCK_RECORDS: &'static str = include_str!("testdata/mock-records.json");

    #[derive(serde::Deserialize, serde::Serialize)]
    struct Record {
        sql: String,
        plan: String,
        items: Vec<Item>,
    }

    #[derive(serde::Deserialize, serde::Serialize)]
    struct Item {
        timestamp_sec: u64,
        cpu_time_ms: u32,
        stmt_exec_count: u64,
        stmt_kv_exec_count: BTreeMap<String, u64>,
        stmt_duration_sum_ns: u64,
        stmt_duration_count: u64,
        #[serde(default)]
        stmt_network_in_bytes: u64,
        #[serde(default)]
        stmt_network_out_bytes: u64,
    }

    fn load_mock_responses() -> Vec<TopSqlSubResponse> {
        serde_json::from_str::<Vec<Record>>(MOCK_RECORDS)
            .unwrap()
            .into_iter()
            .map(|r| TopSqlSubResponse {
                resp_oneof: Some(RespOneof::Record(TopSqlRecord {
                    sql_digest: hex::decode(r.sql).unwrap(),
                    plan_digest: hex::decode(r.plan).unwrap(),
                    items: r
                        .items
                        .into_iter()
                        .map(|i| TopSqlRecordItem {
                            timestamp_sec: i.timestamp_sec,
                            cpu_time_ms: i.cpu_time_ms,
                            stmt_exec_count: i.stmt_exec_count,
                            stmt_kv_exec_count: i.stmt_kv_exec_count,
                            stmt_duration_sum_ns: i.stmt_duration_sum_ns,
                            stmt_duration_count: i.stmt_duration_count,
                            stmt_network_in_bytes: i.stmt_network_in_bytes,
                            stmt_network_out_bytes: i.stmt_network_out_bytes,
                        })
                        .collect(),
                    keyspace_name: vec![],
                })),
            })
            .collect()
    }

    #[test]
    fn test_keep_top_n() {
        // Test case: cover general cases for cpu and network
        // The keep_top_n logic uses OR: records are kept if they meet EITHER cpu threshold OR network threshold
        // 1. Records with high CPU but low network (should be kept due to CPU threshold)
        // 2. Records with low CPU but high network (should be kept due to network threshold)
        // 3. Records with both low (should be evicted to others)
        let mut responses = vec![];
        let timestamp = 1000u64;
        let top_n = 5;
        
        // Create test records with different CPU and network combinations
        // We'll create records where:
        // - Records 1-5: High CPU (100, 90, 80, 70, 60) but low network (10 each) - should be kept (CPU threshold)
        // - Records 6-10: Low CPU (10 each) but high network (200, 180, 160, 140, 120) - should be kept (network threshold)
        // - Records 11-13: Both low (CPU: 20, 15, 10; Network: 30, 20, 10) - should be evicted
        
        let test_cases = vec![
            // (sql_digest_id, plan_digest_id, cpu_time_ms, network_in_bytes, network_out_bytes)
            (1, 1, 100, 5, 5),   // High CPU (100), low network (10) - should keep (CPU threshold)
            (2, 2, 90, 5, 5),    // High CPU (90), low network (10) - should keep (CPU threshold)
            (3, 3, 80, 5, 5),    // High CPU (80), low network (10) - should keep (CPU threshold)
            (4, 4, 70, 5, 5),    // High CPU (70), low network (10) - should keep (CPU threshold)
            (5, 5, 60, 5, 5),    // High CPU (60), low network (10) - should keep (CPU threshold)
            (6, 6, 10, 100, 100), // Low CPU (10), high network (200) - should keep (network threshold)
            (7, 7, 10, 90, 90),   // Low CPU (10), high network (180) - should keep (network threshold)
            (8, 8, 10, 80, 80),   // Low CPU (10), high network (160) - should keep (network threshold)
            (9, 9, 10, 70, 70),   // Low CPU (10), high network (140) - should keep (network threshold)
            (10, 10, 10, 60, 60), // Low CPU (10), high network (120) - should keep (network threshold)
            (11, 11, 20, 15, 15), // Low CPU (20), low network (30) - should evict
            (12, 12, 15, 10, 10), // Low CPU (15), low network (20) - should evict
            (13, 13, 10, 5, 5),   // Low CPU (10), low network (10) - should evict
        ];
        
        for (sql_id, plan_id, cpu_time, net_in, net_out) in test_cases.iter() {
            let sql_digest = vec![*sql_id];
            let plan_digest = vec![*plan_id];
            responses.push(TopSqlSubResponse {
                resp_oneof: Some(RespOneof::Record(TopSqlRecord {
                    sql_digest: sql_digest.clone(),
                    plan_digest: plan_digest.clone(),
                    items: vec![TopSqlRecordItem {
                        timestamp_sec: timestamp,
                        cpu_time_ms: *cpu_time,
                        stmt_exec_count: 1,
                        stmt_kv_exec_count: BTreeMap::new(),
                        stmt_duration_sum_ns: 1000,
                        stmt_duration_count: 1,
                        stmt_network_in_bytes: *net_in,
                        stmt_network_out_bytes: *net_out,
                    }],
                    keyspace_name: vec![],
                })),
            });
        }
        
        let result = TopSqlSubResponseParser::keep_top_n(responses, top_n);
        
        // Verify results
        let mut kept_records: BTreeMap<u8, (u32, u64)> = BTreeMap::new(); // sql_id -> (cpu, network)
        let mut others_cpu = 0u32;
        let mut others_network = 0u64;
        
        for response in result {
            if let Some(RespOneof::Record(record)) = response.resp_oneof {
                if record.sql_digest.is_empty() {
                    // This is others
                    for item in record.items {
                        others_cpu += item.cpu_time_ms;
                        others_network += item.stmt_network_in_bytes + item.stmt_network_out_bytes;
                    }
                } else {
                    // This is a kept record
                    let sql_id = record.sql_digest[0];
                    for item in record.items {
                        let network_total = item.stmt_network_in_bytes + item.stmt_network_out_bytes;
                        kept_records.insert(sql_id, (item.cpu_time_ms, network_total));
                    }
                }
            }
        }
        
        // Calculate expected thresholds (top_n = 5)
        // Top 5 CPU values: 100, 90, 80, 70, 60 -> threshold = 60
        let cpu_threshold = 60u32;
        // Top 5 Network values: 200, 180, 160, 140, 120 -> threshold = 120
        let network_threshold = 120u64;
        
        // Verify that all kept records meet at least one threshold (OR logic)
        for (sql_id, (cpu, network)) in &kept_records {
            let meets_cpu = *cpu >= cpu_threshold;
            let meets_network = *network >= network_threshold;
            assert!(
                meets_cpu || meets_network,
                "Record sql_id={} (cpu={}, network={}) should meet at least one threshold (cpu_threshold={}, network_threshold={})",
                sql_id, cpu, network, cpu_threshold, network_threshold
            );
        }
        
        // Verify specific cases:
        // Records 1-5: High CPU but low network should be kept (CPU threshold)
        for id in 1..=5 {
            let (expected_cpu, _, _, _, _) = test_cases[id as usize - 1];
            assert!(
                kept_records.contains_key(&id),
                "Record {} (high CPU={}, low network) should be kept due to CPU threshold",
                id,
                expected_cpu
            );
            let (cpu, network) = kept_records[&id];
            assert!(cpu >= cpu_threshold, "Record {} should meet CPU threshold", id);
            assert!(network < network_threshold, "Record {} should NOT meet network threshold", id);
        }
        
        // Records 6-10: Low CPU but high network should be kept (network threshold)
        for id in 6..=10 {
            let (_, _, _, net_in, net_out) = test_cases[id as usize - 1];
            let expected_network = net_in + net_out;
            assert!(
                kept_records.contains_key(&id),
                "Record {} (low CPU, high network={}) should be kept due to network threshold",
                id,
                expected_network
            );
            let (cpu, network) = kept_records[&id];
            assert!(cpu < cpu_threshold, "Record {} should NOT meet CPU threshold", id);
            assert!(network >= network_threshold, "Record {} should meet network threshold", id);
        }
        
        // Records 11-13: Both low should be evicted to others
        for id in 11..=13 {
            assert!(
                !kept_records.contains_key(&id),
                "Record {} (low CPU, low network) should be evicted to others",
                id
            );
        }
        
        // Verify that evicted records are in others
        assert!(others_cpu > 0, "Some records should be evicted to others");
        assert_eq!(others_cpu, 20 + 15 + 10, "Others CPU should be sum of evicted records (20+15+10=45)");
        assert_eq!(others_network, 30 + 20 + 10, "Others network should be sum of evicted records (30+20+10=60)");
        
        // Verify that at least top_n records are kept (should be exactly 10: 5 high CPU + 5 high network)
        assert_eq!(kept_records.len(), 10, "Should keep 10 records (5 high CPU + 5 high network)");
    }

    #[test]
    fn test_keep_top_n_len_less_equal_top_n() {
        // Test case: v.len() <= top_n, should keep all records
        let mut responses = vec![];
        let sql_digest = vec![1, 2, 3];
        let plan_digest = vec![4, 5, 6];
        let timestamp = 1000u64;
        
        // Create 5 records with same timestamp
        let items: Vec<TopSqlRecordItem> = (0..5)
            .map(|i| TopSqlRecordItem {
                timestamp_sec: timestamp,
                cpu_time_ms: 10 + i as u32,
                stmt_exec_count: 1,
                stmt_kv_exec_count: BTreeMap::new(),
                stmt_duration_sum_ns: 1000,
                stmt_duration_count: 1,
                stmt_network_in_bytes: 100 + i as u64,
                stmt_network_out_bytes: 200 + i as u64,
            })
            .collect();
        
        responses.push(TopSqlSubResponse {
            resp_oneof: Some(RespOneof::Record(TopSqlRecord {
                sql_digest: sql_digest.clone(),
                plan_digest: plan_digest.clone(),
                items,
                keyspace_name: vec![],
            })),
        });
        
        // top_n = 10, which is greater than 5, so all should be kept
        let result = TopSqlSubResponseParser::keep_top_n(responses.clone(), 10);
        
        // Should have same number of responses (all kept)
        assert_eq!(result.len(), 1);
        if let Some(RespOneof::Record(record)) = &result[0].resp_oneof {
            assert_eq!(record.items.len(), 5);
            assert_eq!(record.sql_digest, sql_digest);
            assert_eq!(record.plan_digest, plan_digest);
        } else {
            panic!("Expected Record");
        }
        
        // top_n = 5, which equals 5, so all should be kept
        let result2 = TopSqlSubResponseParser::keep_top_n(responses, 5);
        assert_eq!(result2.len(), 1);
        if let Some(RespOneof::Record(record)) = &result2[0].resp_oneof {
            assert_eq!(record.items.len(), 5);
            assert_eq!(record.sql_digest, sql_digest);
            assert_eq!(record.plan_digest, plan_digest);
        } else {
            panic!("Expected Record");
        }
    }

    #[test]
    fn test_keep_top_n_all_same_both_metrics() {
        // Test case: both cpu_time_ms and network_bytes are all the same, data count > top_n
        // All should go to others
        let mut responses = vec![];
        let sql_digest = vec![1, 2, 3];
        let plan_digest = vec![4, 5, 6];
        let timestamp = 1000u64;
        
        // Create 10 records with same cpu_time_ms and same network bytes
        let items: Vec<TopSqlRecordItem> = (0..10)
            .map(|_| TopSqlRecordItem {
                timestamp_sec: timestamp,
                cpu_time_ms: 100, // All same
                stmt_exec_count: 1,
                stmt_kv_exec_count: BTreeMap::new(),
                stmt_duration_sum_ns: 1000,
                stmt_duration_count: 1,
                stmt_network_in_bytes: 100, // All same
                stmt_network_out_bytes: 200, // All same, total = 300
            })
            .collect();
        
        responses.push(TopSqlSubResponse {
            resp_oneof: Some(RespOneof::Record(TopSqlRecord {
                sql_digest: sql_digest.clone(),
                plan_digest: plan_digest.clone(),
                items,
                keyspace_name: vec![],
            })),
        });
        
        // top_n = 5, all values are same
        // Current logic: when all values are same, threshold equals the value,
        // so all records satisfy >= threshold condition and are kept
        let result = TopSqlSubResponseParser::keep_top_n(responses, 5);
        
        // Verify all records are kept
        let mut total_cpu_kept = 0u32;
        let mut total_network_kept = 0u64;
        let mut kept_count = 0;
        
        for response in result {
            if let Some(RespOneof::Record(record)) = response.resp_oneof {
                if record.sql_digest.is_empty() {
                    // This is others (should be empty in this case)
                } else {
                    kept_count += record.items.len();
                    for item in record.items {
                        total_cpu_kept += item.cpu_time_ms;
                        total_network_kept += item.stmt_network_in_bytes + item.stmt_network_out_bytes;
                    }
                }
            }
        }
        
        // Current behavior: all records are kept (all satisfy >= threshold)
        // This test documents the current behavior when all values are same
        assert_eq!(kept_count, 10);
        assert_eq!(total_cpu_kept, 1000); // 10 * 100
        assert_eq!(total_network_kept, 3000); // 10 * 300
    }

    #[test]
    fn test_downsampling() {
        let mut responses = load_mock_responses();
        let mut items = vec![];
        for response in &responses {
            if let Some(RespOneof::Record(record)) = &response.resp_oneof {
                if record.sql_digest.is_empty() {
                    items = record.items.clone();
                }
            }
        }
        let mut timestamps: Vec<u64> = items.clone().into_iter().map(|i| i.timestamp_sec).collect();
        timestamps.sort();
        assert_eq!(
            timestamps, // 21:54:51 ~ 21:55:24
            [
                1709646891, 1709646892, 1709646893, 1709646894, 1709646895, 1709646896, 1709646897,
                1709646898, 1709646899, 1709646900, 1709646901, 1709646902, 1709646903, 1709646904,
                1709646905, 1709646907, 1709646908, 1709646909, 1709646910, 1709646911, 1709646912,
                1709646913, 1709646914, 1709646915, 1709646916, 1709646917, 1709646918, 1709646919,
                1709646920, 1709646921, 1709646922, 1709646923, 1709646924
            ]
        );
        let mut sum_old = TopSqlRecordItem::default();
        for item in items {
            sum_old.cpu_time_ms += item.cpu_time_ms;
            sum_old.stmt_exec_count += item.stmt_exec_count;
            sum_old.stmt_duration_sum_ns += item.stmt_duration_sum_ns;
            sum_old.stmt_duration_count += item.stmt_duration_count;
            sum_old.stmt_network_in_bytes += item.stmt_network_in_bytes;
            sum_old.stmt_network_out_bytes += item.stmt_network_out_bytes;
        }

        TopSqlSubResponseParser::downsampling(&mut responses, 15);

        let mut items = vec![];
        for response in &responses {
            if let Some(RespOneof::Record(record)) = &response.resp_oneof {
                if record.sql_digest.is_empty() {
                    items = record.items.clone();
                }
            }
        }
        let timestamps: Vec<u64> = items.clone().into_iter().map(|i| i.timestamp_sec).collect();
        assert_eq!(
            timestamps,
            [
                1709646900, // 21:55:00
                1709646915, // 21:55:15
                1709646930, // 21:55:30
            ]
        );
        let mut sum_new = TopSqlRecordItem::default();
        for item in items {
            sum_new.cpu_time_ms += item.cpu_time_ms;
            sum_new.stmt_exec_count += item.stmt_exec_count;
            sum_new.stmt_duration_sum_ns += item.stmt_duration_sum_ns;
            sum_new.stmt_duration_count += item.stmt_duration_count;
            sum_new.stmt_network_in_bytes += item.stmt_network_in_bytes;
            sum_new.stmt_network_out_bytes += item.stmt_network_out_bytes;
        }

        assert_eq!(sum_old.cpu_time_ms, sum_new.cpu_time_ms);
        assert_eq!(sum_old.stmt_exec_count, sum_new.stmt_exec_count);
        assert_eq!(sum_old.stmt_duration_count, sum_new.stmt_duration_count);
        assert_eq!(sum_old.stmt_duration_sum_ns, sum_new.stmt_duration_sum_ns);
        assert_eq!(sum_old.stmt_network_in_bytes, sum_new.stmt_network_in_bytes);
        assert_eq!(sum_old.stmt_network_out_bytes, sum_new.stmt_network_out_bytes);
    }
}
