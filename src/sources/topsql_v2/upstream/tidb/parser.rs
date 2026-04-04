use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use chrono::Utc;
use vector::event::Event;
use vector_lib::event::{LogEvent, Value as LogValue};
use crate::sources::topsql_v2::schema_cache::SchemaCache;
use crate::sources::topsql_v2::upstream::consts::{
    LABEL_DATE, LABEL_ENCODED_NORMALIZED_PLAN, LABEL_INSTANCE_KEY,
    LABEL_NORMALIZED_PLAN, LABEL_NORMALIZED_SQL, LABEL_PLAN_DIGEST,
    LABEL_SQL_DIGEST, LABEL_SOURCE_TABLE, LABEL_TIMESTAMPS, LABEL_KEYSPACE, LABEL_USER,
    METRIC_NAME_CPU_TIME_MS, METRIC_NAME_NETWORK_IN_BYTES, METRIC_NAME_NETWORK_OUT_BYTES,
    METRIC_NAME_STMT_DURATION_COUNT, METRIC_NAME_STMT_DURATION_SUM_NS, METRIC_NAME_STMT_EXEC_COUNT,
    METRIC_NAME_TOTAL_RU, METRIC_NAME_EXEC_COUNT, METRIC_NAME_EXEC_DURATION,
    SOURCE_TABLE_TIDB_TOPSQL, SOURCE_TABLE_TOPSQL_PLAN_META, SOURCE_TABLE_TOPSQL_SQL_META, SOURCE_TABLE_TOPRU,
};
use crate::sources::topsql_v2::upstream::parser::{truncate_label_value, UpstreamEventParser};
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
            Some(RespOneof::RuRecord(ru_record)) => Self::parse_top_ru_record(ru_record),
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
        let mut keyspace_name = None;
        for response in responses {
            if let Some(RespOneof::Record(record)) = response.resp_oneof {
                // Save keyspace_name from the first record encountered
                if keyspace_name.is_none() {
                    keyspace_name = Some(record.keyspace_name.clone());
                }
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
            // Find top_n threshold for cpu_time_ms using partial selection
            let mut cpu_values: Vec<u32> = v.iter().map(|psd| psd.cpu_time_ms).collect();
            cpu_values.select_nth_unstable_by(top_n, |a, b| b.cmp(a));
            let cpu_threshold = cpu_values[top_n];
            
            // Find top_n threshold for network bytes using partial selection
            let mut network_values: Vec<u64> = v.iter()
                .map(|psd| psd.stmt_network_in_bytes + psd.stmt_network_out_bytes)
                .collect();
            network_values.select_nth_unstable_by(top_n, |a, b| b.cmp(a));
            let network_threshold = network_values[top_n];
            
            // Keep records that meet either threshold
            let mut kept = Vec::new();
            for psd in v.iter() {
                let network_bytes = psd.stmt_network_in_bytes + psd.stmt_network_out_bytes;
                if psd.cpu_time_ms > cpu_threshold || network_bytes > network_threshold {
                    kept.push(psd.clone());
                } else {
                    // Directly update ts_others for evicted records
                    let others = ts_others.entry(*ts).or_insert_with(|| {
                        let mut item = TopSqlRecordItem::default();
                        item.timestamp_sec = *ts;
                        item
                    });
                    others.cpu_time_ms += psd.cpu_time_ms;
                    others.stmt_exec_count += psd.stmt_exec_count;
                    others.stmt_duration_sum_ns += psd.stmt_duration_sum_ns;
                    others.stmt_duration_count += psd.stmt_duration_count;
                    others.stmt_network_in_bytes += psd.stmt_network_in_bytes;
                    others.stmt_network_out_bytes += psd.stmt_network_out_bytes;
                }
            }
            
            *v = kept;
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

        let keyspace_name = keyspace_name.unwrap_or_default();
        for (digest, items) in digest_items {
            new_responses.push(TopSqlSubResponse {
                resp_oneof: Some(RespOneof::Record(TopSqlRecord {
                    sql_digest: digest.0,
                    plan_digest: digest.1,
                    items,
                    keyspace_name: keyspace_name.clone(),
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
        let mut date = String::new();
        for item in &record.items {
            let mut event = Event::Log(LogEvent::default());
            let log = event.as_mut_log();

            // Add metadata with Vector prefix (ensure all fields have values)
            log.insert(LABEL_SOURCE_TABLE, SOURCE_TABLE_TIDB_TOPSQL);
            log.insert(LABEL_TIMESTAMPS, LogValue::from(item.timestamp_sec));
            if date.is_empty() {
                date = chrono::DateTime::from_timestamp(item.timestamp_sec as i64, 0)
                .map(|dt| dt.format("%Y-%m-%d").to_string())
                .unwrap_or_else(|| "1970-01-01".to_string());
            }
            log.insert(LABEL_DATE, LogValue::from(date.clone()));
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
        log.insert(
            LABEL_NORMALIZED_SQL,
            truncate_label_value(sql_meta.normalized_sql.clone()),
        );
        let now = Utc::now();
        log.insert(LABEL_TIMESTAMPS, LogValue::from(now.timestamp()));
        let date_str = now.format("%Y-%m-%d").to_string();
        log.insert(LABEL_DATE, LogValue::from(date_str));
        events.push(event.into_log());
        events
    }

    fn parse_tidb_plan_meta(plan_meta: PlanMeta) -> Vec<LogEvent> {
        let mut events = vec![];
        let plan_digest = hex::encode_upper(plan_meta.plan_digest);
        let encoded_normalized_plan = truncate_label_value(hex::encode_upper(
            plan_meta.encoded_normalized_plan,
        ));
        let mut event = Event::Log(LogEvent::default());
        let log = event.as_mut_log();

        // Add metadata with Vector prefix (ensure all fields have values)
        log.insert(LABEL_SOURCE_TABLE, SOURCE_TABLE_TOPSQL_PLAN_META);
        log.insert(LABEL_PLAN_DIGEST, plan_digest);
        log.insert(
            LABEL_NORMALIZED_PLAN,
            truncate_label_value(plan_meta.normalized_plan.clone()),
        );
        log.insert(
            LABEL_ENCODED_NORMALIZED_PLAN,
            encoded_normalized_plan,
        );
        let now = Utc::now();
        log.insert(LABEL_TIMESTAMPS, LogValue::from(now.timestamp()));
        let date_str = now.format("%Y-%m-%d").to_string();
        log.insert(LABEL_DATE, LogValue::from(date_str));
        events.push(event.into_log());
        events
    }

    fn parse_top_ru_record(record: crate::sources::topsql_v2::upstream::tidb::proto::TopRuRecord) -> Vec<LogEvent> {
        let mut events = vec![];
        let mut date = String::new();

        let mut keyspace_name_str = "".to_string();
        if !record.keyspace_name.is_empty() {
            if let Ok(ks) = String::from_utf8(record.keyspace_name.clone()) {
                keyspace_name_str = ks;
            }
        }

        for item in record.items {
            let mut event = Event::Log(LogEvent::default());
            let log = event.as_mut_log();

            log.insert(LABEL_SOURCE_TABLE, SOURCE_TABLE_TOPRU);
            log.insert(LABEL_TIMESTAMPS, LogValue::from(item.timestamp_sec));

            if date.is_empty() {
                date = chrono::DateTime::from_timestamp(item.timestamp_sec as i64, 0)
                    .map(|dt| dt.format("%Y-%m-%d").to_string())
                    .unwrap_or_else(|| "1970-01-01".to_string());
            }
            log.insert(LABEL_DATE, LogValue::from(date.clone()));

            if !keyspace_name_str.is_empty() {
                log.insert(LABEL_KEYSPACE, keyspace_name_str.clone());
            }
            log.insert(LABEL_USER, record.user.clone());
            log.insert(
                LABEL_SQL_DIGEST,
                hex::encode_upper(record.sql_digest.clone()),
            );
            log.insert(
                LABEL_PLAN_DIGEST,
                hex::encode_upper(record.plan_digest.clone()),
            );
            log.insert(METRIC_NAME_TOTAL_RU, LogValue::from(item.total_ru));
            log.insert(METRIC_NAME_EXEC_COUNT, LogValue::from(item.exec_count));
            log.insert(METRIC_NAME_EXEC_DURATION, LogValue::from(item.exec_duration));

            events.push(event.into_log());
        }
        events
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::topsql_v2::upstream::tidb::proto::{TopSqlRecordItem, TopRuRecord, TopRuRecordItem};

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
    fn test_keep_top_n_len_less_equal_top_n() {
        // Test case: v.len() <= top_n, should keep all records
        let mut responses = vec![];
        let sql_digest = vec![1, 2, 3];
        let plan_digest = vec![4, 5, 6];
        let timestamp = 1000u64;
        let test_keyspace_name = b"test_keyspace_2".to_vec();
        
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
                keyspace_name: test_keyspace_name.clone(),
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
            assert_eq!(record.keyspace_name, test_keyspace_name, "keyspace_name should be preserved");
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
            assert_eq!(record.keyspace_name, test_keyspace_name, "keyspace_name should be preserved");
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
        let test_keyspace_name = b"test_keyspace_3".to_vec();
        
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
                keyspace_name: test_keyspace_name.clone(),
            })),
        });
        
        // top_n = 5, all values are same
        // New logic: threshold equals the value (top_n-th largest, which is the same value),
        // so no records satisfy > threshold condition, all should go to others
        let result = TopSqlSubResponseParser::keep_top_n(responses, 5);
        
        // Verify all records go to others
        let mut total_cpu_kept = 0u32;
        let mut total_network_kept = 0u64;
        let mut kept_count = 0;
        let mut total_cpu_others = 0u32;
        let mut total_network_others = 0u64;
        
        for response in result {
            if let Some(RespOneof::Record(record)) = response.resp_oneof {
                // Verify keyspace_name is preserved
                assert_eq!(
                    record.keyspace_name,
                    test_keyspace_name,
                    "keyspace_name should be preserved in all records"
                );
                
                if record.sql_digest.is_empty() {
                    // This is others
                    for item in record.items {
                        total_cpu_others += item.cpu_time_ms;
                        total_network_others += item.stmt_network_in_bytes + item.stmt_network_out_bytes;
                    }
                } else {
                    kept_count += record.items.len();
                    for item in record.items {
                        total_cpu_kept += item.cpu_time_ms;
                        total_network_kept += item.stmt_network_in_bytes + item.stmt_network_out_bytes;
                    }
                }
            }
        }
        
        // New behavior: all records go to others (none satisfy > threshold when all values are same)
        assert_eq!(kept_count, 0);
        assert_eq!(total_cpu_kept, 0);
        assert_eq!(total_network_kept, 0);
        assert_eq!(total_cpu_others, 1000); // 10 * 100
        assert_eq!(total_network_others, 3000); // 10 * 300
    }

    #[test]
    fn test_keep_top_n() {
        // Test case: cover different timestamps with OR logic (CPU OR Network)
        // Each timestamp should be processed independently with top_n logic
        // Records are kept if they meet EITHER cpu threshold OR network threshold
        let mut responses = vec![];
        let top_n = 3;
        let test_keyspace_name = b"test_keyspace_timestamps".to_vec();
        
        // Timestamp 1000: 8 records mixing high CPU/low network, low CPU/high network, both high, both low
        // Expected: Keep records that meet either CPU threshold (>20) OR network threshold (>40)
        // Top 3 CPU: 100, 90, 80 -> threshold = 20 (4th largest)
        // Top 3 Network: 400, 350, 300 -> threshold = 40 (4th largest)
        let timestamp1 = 1000u64;
        let test_cases_ts1 = vec![
            // (sql_id, plan_id, cpu_time_ms, network_in_bytes, network_out_bytes, reason)
            (1, 1, 100, 10, 10),   // High CPU (100), low network (20) -> keep (CPU > 20)
            (2, 2, 90, 10, 10),   // High CPU (90), low network (20) -> keep (CPU > 20)
            (3, 3, 80, 10, 10),   // High CPU (80), low network (20) -> keep (CPU > 20)
            (4, 4, 10, 200, 200), // Low CPU (10), high network (400) -> keep (network > 40)
            (5, 5, 10, 175, 175), // Low CPU (10), high network (350) -> keep (network > 40)
            (6, 6, 10, 150, 150), // Low CPU (10), high network (300) -> keep (network > 40)
            (7, 7, 20, 20, 20),   // Low CPU (20), low network (40) -> evict (CPU == 20, network == 40)
            (8, 8, 15, 15, 15),   // Low CPU (15), low network (30) -> evict
        ];
        
        for (sql_id, plan_id, cpu_time, net_in, net_out) in test_cases_ts1.iter() {
            let sql_digest = vec![*sql_id];
            let plan_digest = vec![*plan_id];
            responses.push(TopSqlSubResponse {
                resp_oneof: Some(RespOneof::Record(TopSqlRecord {
                    sql_digest: sql_digest.clone(),
                    plan_digest: plan_digest.clone(),
                    items: vec![TopSqlRecordItem {
                        timestamp_sec: timestamp1,
                        cpu_time_ms: *cpu_time,
                        stmt_exec_count: 1,
                        stmt_kv_exec_count: BTreeMap::new(),
                        stmt_duration_sum_ns: 1000,
                        stmt_duration_count: 1,
                        stmt_network_in_bytes: *net_in,
                        stmt_network_out_bytes: *net_out,
                    }],
                    keyspace_name: test_keyspace_name.clone(),
                })),
            });
        }
        
        // Timestamp 2000: 7 records mixing different combinations
        // Expected: Keep records that meet either CPU threshold (>20) OR network threshold (>60)
        // Top 3 CPU: 100, 90, 70 -> threshold = 20 (4th largest)
        // Top 3 Network: 380, 360, 140 -> threshold = 60 (4th largest)
        let timestamp2 = 2000u64;
        let test_cases_ts2 = vec![
            (9, 9, 100, 10, 10),   // High CPU (100), low network (20) -> keep (CPU > 20)
            (10, 10, 90, 10, 10),  // High CPU (90), low network (20) -> keep (CPU > 20)
            (11, 11, 70, 10, 10),  // High CPU (70), low network (20) -> keep (CPU > 20)
            (12, 12, 10, 190, 190), // Low CPU (10), high network (380) -> keep (network > 60)
            (13, 13, 10, 180, 180), // Low CPU (10), high network (360) -> keep (network > 60)
            (14, 14, 10, 70, 70),   // Low CPU (10), high network (140) -> keep (network > 60)
            (15, 15, 20, 30, 30),   // Low CPU (20), low network (60) -> evict (CPU == 20, network == 60)
        ];
        
        for (sql_id, plan_id, cpu_time, net_in, net_out) in test_cases_ts2.iter() {
            let sql_digest = vec![*sql_id];
            let plan_digest = vec![*plan_id];
            responses.push(TopSqlSubResponse {
                resp_oneof: Some(RespOneof::Record(TopSqlRecord {
                    sql_digest: sql_digest.clone(),
                    plan_digest: plan_digest.clone(),
                    items: vec![TopSqlRecordItem {
                        timestamp_sec: timestamp2,
                        cpu_time_ms: *cpu_time,
                        stmt_exec_count: 1,
                        stmt_kv_exec_count: BTreeMap::new(),
                        stmt_duration_sum_ns: 1000,
                        stmt_duration_count: 1,
                        stmt_network_in_bytes: *net_in,
                        stmt_network_out_bytes: *net_out,
                    }],
                    keyspace_name: test_keyspace_name.clone(),
                })),
            });
        }
        
        // Timestamp 3000: 2 records (both should be kept since 2 <= top_n=3)
        let timestamp3 = 3000u64;
        let test_cases_ts3 = vec![
            (16, 16, 50, 50, 50),
            (17, 17, 40, 40, 40),
        ];
        
        for (sql_id, plan_id, cpu_time, net_in, net_out) in test_cases_ts3.iter() {
            let sql_digest = vec![*sql_id];
            let plan_digest = vec![*plan_id];
            responses.push(TopSqlSubResponse {
                resp_oneof: Some(RespOneof::Record(TopSqlRecord {
                    sql_digest: sql_digest.clone(),
                    plan_digest: plan_digest.clone(),
                    items: vec![TopSqlRecordItem {
                        timestamp_sec: timestamp3,
                        cpu_time_ms: *cpu_time,
                        stmt_exec_count: 1,
                        stmt_kv_exec_count: BTreeMap::new(),
                        stmt_duration_sum_ns: 1000,
                        stmt_duration_count: 1,
                        stmt_network_in_bytes: *net_in,
                        stmt_network_out_bytes: *net_out,
                    }],
                    keyspace_name: test_keyspace_name.clone(),
                })),
            });
        }
        
        let result = TopSqlSubResponseParser::keep_top_n(responses, top_n);
        
        // Group results by timestamp
        let mut results_by_timestamp: BTreeMap<u64, Vec<(u8, u32, u64)>> = BTreeMap::new(); // timestamp -> [(sql_id, cpu, network), ...]
        let mut others_by_timestamp: BTreeMap<u64, (u32, u64)> = BTreeMap::new(); // timestamp -> (cpu, network)
        
        for response in result {
            if let Some(RespOneof::Record(record)) = response.resp_oneof {
                // Verify keyspace_name is preserved
                assert_eq!(
                    record.keyspace_name,
                    test_keyspace_name,
                    "keyspace_name should be preserved in all records"
                );
                
                for item in record.items {
                    let timestamp = item.timestamp_sec;
                    let network_total = item.stmt_network_in_bytes + item.stmt_network_out_bytes;
                    
                    if record.sql_digest.is_empty() {
                        // This is others
                        let entry = others_by_timestamp.entry(timestamp).or_insert((0, 0));
                        entry.0 += item.cpu_time_ms;
                        entry.1 += network_total;
                    } else {
                        // This is a kept record
                        let sql_id = record.sql_digest[0];
                        results_by_timestamp
                            .entry(timestamp)
                            .or_insert_with(Vec::new)
                            .push((sql_id, item.cpu_time_ms, network_total));
                    }
                }
            }
        }
        
        // Verify timestamp 1000: should keep 6 records (3 high CPU + 3 high network), evict 2
        // CPU threshold = 20 (4th largest), keep records with CPU > 20
        // Network threshold = 40 (4th largest), keep records with network > 40
        let ts1_kept: Vec<u8> = results_by_timestamp
            .get(&timestamp1)
            .map(|records| records.iter().map(|r| r.0).collect())
            .unwrap_or_default();
        assert_eq!(ts1_kept.len(), 6, "Timestamp 1000 should keep 6 records (3 high CPU + 3 high network)");
        // High CPU records (1, 2, 3) should be kept
        assert!(ts1_kept.contains(&1), "Timestamp 1000 should keep sql_id 1 (high CPU)");
        assert!(ts1_kept.contains(&2), "Timestamp 1000 should keep sql_id 2 (high CPU)");
        assert!(ts1_kept.contains(&3), "Timestamp 1000 should keep sql_id 3 (high CPU)");
        // High network records (4, 5, 6) should be kept
        assert!(ts1_kept.contains(&4), "Timestamp 1000 should keep sql_id 4 (high network)");
        assert!(ts1_kept.contains(&5), "Timestamp 1000 should keep sql_id 5 (high network)");
        assert!(ts1_kept.contains(&6), "Timestamp 1000 should keep sql_id 6 (high network)");
        // Low both records (7, 8) should be evicted
        assert!(!ts1_kept.contains(&7), "Timestamp 1000 should NOT keep sql_id 7 (low both)");
        assert!(!ts1_kept.contains(&8), "Timestamp 1000 should NOT keep sql_id 8 (low both)");
        
        // Verify kept records meet at least one threshold
        if let Some(records) = results_by_timestamp.get(&timestamp1) {
            let cpu_threshold = 20u32;
            let network_threshold = 40u64;
            for (sql_id, cpu, network) in records {
                let meets_cpu = *cpu > cpu_threshold;
                let meets_network = *network > network_threshold;
                assert!(
                    meets_cpu || meets_network,
                    "Record sql_id={} (cpu={}, network={}) should meet at least one threshold (cpu_threshold={}, network_threshold={})",
                    sql_id, cpu, network, cpu_threshold, network_threshold
                );
            }
        }
        
        if let Some((others_cpu, others_network)) = others_by_timestamp.get(&timestamp1) {
            assert_eq!(*others_cpu, 20 + 15, "Timestamp 1000 others CPU should be 35 (20+15)");
            assert_eq!(*others_network, 40 + 30, "Timestamp 1000 others network should be 70 (40+30)");
        } else {
            panic!("Timestamp 1000 should have others records");
        }
        
        // Verify timestamp 2000: should keep 6 records (3 high CPU + 3 high network), evict 1
        // CPU threshold = 20 (4th largest), keep records with CPU > 20
        // Network threshold = 60 (4th largest), keep records with network > 60
        let ts2_kept: Vec<u8> = results_by_timestamp
            .get(&timestamp2)
            .map(|records| records.iter().map(|r| r.0).collect())
            .unwrap_or_default();
        assert_eq!(ts2_kept.len(), 6, "Timestamp 2000 should keep 6 records (3 high CPU + 3 high network)");
        // High CPU records (9, 10, 11) should be kept
        assert!(ts2_kept.contains(&9), "Timestamp 2000 should keep sql_id 9 (high CPU)");
        assert!(ts2_kept.contains(&10), "Timestamp 2000 should keep sql_id 10 (high CPU)");
        assert!(ts2_kept.contains(&11), "Timestamp 2000 should keep sql_id 11 (high CPU)");
        // High network records (12, 13, 14) should be kept
        assert!(ts2_kept.contains(&12), "Timestamp 2000 should keep sql_id 12 (high network)");
        assert!(ts2_kept.contains(&13), "Timestamp 2000 should keep sql_id 13 (high network)");
        assert!(ts2_kept.contains(&14), "Timestamp 2000 should keep sql_id 14 (high network)");
        // Low both record (15) should be evicted
        assert!(!ts2_kept.contains(&15), "Timestamp 2000 should NOT keep sql_id 15 (low both)");
        
        // Verify kept records meet at least one threshold
        if let Some(records) = results_by_timestamp.get(&timestamp2) {
            let cpu_threshold = 20u32;
            let network_threshold = 60u64;
            for (sql_id, cpu, network) in records {
                let meets_cpu = *cpu > cpu_threshold;
                let meets_network = *network > network_threshold;
                assert!(
                    meets_cpu || meets_network,
                    "Record sql_id={} (cpu={}, network={}) should meet at least one threshold (cpu_threshold={}, network_threshold={})",
                    sql_id, cpu, network, cpu_threshold, network_threshold
                );
            }
        }
        
        if let Some((others_cpu, others_network)) = others_by_timestamp.get(&timestamp2) {
            assert_eq!(*others_cpu, 20, "Timestamp 2000 others CPU should be 20");
            assert_eq!(*others_network, 60, "Timestamp 2000 others network should be 60 (30+30)");
        } else {
            panic!("Timestamp 2000 should have others records");
        }
        
        // Verify timestamp 3000: should keep all 2 records (2 <= top_n=3)
        let ts3_kept: Vec<u8> = results_by_timestamp
            .get(&timestamp3)
            .map(|records| records.iter().map(|r| r.0).collect())
            .unwrap_or_default();
        assert_eq!(ts3_kept.len(), 2, "Timestamp 3000 should keep all 2 records");
        assert!(ts3_kept.contains(&16), "Timestamp 3000 should keep sql_id 16");
        assert!(ts3_kept.contains(&17), "Timestamp 3000 should keep sql_id 17");
        
        // Timestamp 3000 should not have others since all records are kept
        assert!(!others_by_timestamp.contains_key(&timestamp3), "Timestamp 3000 should not have others");
        
        // Verify total counts
        let total_kept: usize = results_by_timestamp.values().map(|records| records.len()).sum();
        assert_eq!(total_kept, 14, "Total kept records should be 14 (6+6+2)");
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

    #[test]
    fn test_parse_top_ru_record() {
        let ru_record = TopRuRecord {
            keyspace_name: b"test_keyspace".to_vec(),
            user: "test_user".to_string(),
            sql_digest: b"sql_digest_123".to_vec(),
            plan_digest: b"plan_digest_456".to_vec(),
            items: vec![
                TopRuRecordItem {
                    timestamp_sec: 1709646900,
                    total_ru: 100.5,
                    exec_count: 10,
                    exec_duration: 50000000, // 50ms in nanoseconds
                },
                TopRuRecordItem {
                    timestamp_sec: 1709646960,
                    total_ru: 200.0,
                    exec_count: 20,
                    exec_duration: 100000000, // 100ms in nanoseconds
                },
            ],
        };

        let events = TopSqlSubResponseParser::parse_top_ru_record(ru_record);
        assert_eq!(events.len(), 2);

        // Check first event
        let event1 = &events[0];
        let log1 = event1;
        assert_eq!(log1.get(LABEL_SOURCE_TABLE), Some(&LogValue::from(SOURCE_TABLE_TOPRU)));
        assert_eq!(log1.get(LABEL_TIMESTAMPS), Some(&LogValue::from(1709646900)));
        assert_eq!(log1.get(LABEL_DATE), Some(&LogValue::from("2024-03-05")));
        assert_eq!(log1.get(LABEL_KEYSPACE), Some(&LogValue::from("test_keyspace")));
        assert_eq!(log1.get(LABEL_USER), Some(&LogValue::from("test_user")));
        assert_eq!(log1.get(LABEL_SQL_DIGEST), Some(&LogValue::from("73716C5F6469676573745F313233")));
        assert_eq!(log1.get(LABEL_PLAN_DIGEST), Some(&LogValue::from("706C616E5F6469676573745F343536")));
        assert_eq!(log1.get(METRIC_NAME_TOTAL_RU), Some(&LogValue::from(100.5)));
        assert_eq!(log1.get(METRIC_NAME_EXEC_COUNT), Some(&LogValue::from(10)));
        assert_eq!(log1.get(METRIC_NAME_EXEC_DURATION), Some(&LogValue::from(50000000)));

        // Check second event
        let event2 = &events[1];
        let log2 = event2;
        assert_eq!(log2.get(LABEL_SOURCE_TABLE), Some(&LogValue::from(SOURCE_TABLE_TOPRU)));
        assert_eq!(log2.get(LABEL_TIMESTAMPS), Some(&LogValue::from(1709646960)));
        assert_eq!(log2.get(LABEL_DATE), Some(&LogValue::from("2024-03-05")));
        assert_eq!(log2.get(METRIC_NAME_TOTAL_RU), Some(&LogValue::from(200.0)));
        assert_eq!(log2.get(METRIC_NAME_EXEC_COUNT), Some(&LogValue::from(20)));
        assert_eq!(log2.get(METRIC_NAME_EXEC_DURATION), Some(&LogValue::from(100000000)));
    }
}
