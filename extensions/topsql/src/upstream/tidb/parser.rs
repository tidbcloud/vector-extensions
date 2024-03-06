use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use chrono::Utc;
use vector::event::LogEvent;

use crate::upstream::consts::{
    INSTANCE_TYPE_TIDB, INSTANCE_TYPE_TIKV, LABEL_ENCODED_NORMALIZED_PLAN, LABEL_IS_INTERNAL_SQL,
    LABEL_NAME, LABEL_NORMALIZED_PLAN, LABEL_NORMALIZED_SQL, LABEL_PLAN_DIGEST, LABEL_SQL_DIGEST,
    METRIC_NAME_CPU_TIME_MS, METRIC_NAME_PLAN_META, METRIC_NAME_SQL_META,
    METRIC_NAME_STMT_DURATION_COUNT, METRIC_NAME_STMT_DURATION_SUM_NS, METRIC_NAME_STMT_EXEC_COUNT,
};
use crate::upstream::parser::{Buf, UpstreamEventParser};
use crate::upstream::tidb::proto::top_sql_sub_response::RespOneof;
use crate::upstream::tidb::proto::{PlanMeta, SqlMeta, TopSqlRecord, TopSqlSubResponse};
use crate::upstream::utils::make_metric_like_log_event;

pub struct TopSqlSubResponseParser;

impl UpstreamEventParser for TopSqlSubResponseParser {
    type UpstreamEvent = TopSqlSubResponse;

    fn parse(response: Self::UpstreamEvent, instance: String) -> Vec<LogEvent> {
        match response.resp_oneof {
            Some(RespOneof::Record(record)) => Self::parse_tidb_record(record, instance),
            Some(RespOneof::SqlMeta(sql_meta)) => Self::parse_tidb_sql_meta(sql_meta),
            Some(RespOneof::PlanMeta(plan_meta)) => Self::parse_tidb_plan_meta(plan_meta),
            None => vec![],
        }
    }

    fn keep_top_n(responses: Vec<Self::UpstreamEvent>, top_n: usize) -> Vec<Self::UpstreamEvent> {
        let mut cpu_time_map = HashMap::new();
        for response in &responses {
            if let Some(RespOneof::Record(record)) = &response.resp_oneof {
                if record.sql_digest.is_empty() {
                    continue; // others
                }
                let cpu_time: u32 = record.items.iter().map(|i| i.cpu_time_ms).sum();
                let k = (record.sql_digest.clone(), record.plan_digest.clone());
                let v = cpu_time_map.get(&k).unwrap_or(&0);
                cpu_time_map.insert(k, *v + cpu_time);
            }
        }
        let mut cpu_time_vec = cpu_time_map
            .into_iter()
            .collect::<Vec<((Vec<u8>, Vec<u8>), u32)>>();
        cpu_time_vec.sort_by(|a, b| b.1.cmp(&a.1));
        cpu_time_vec.truncate(top_n);
        let mut top_sql_plan = HashSet::new();
        for v in cpu_time_vec {
            top_sql_plan.insert(v.0);
        }

        let mut results = vec![];
        let mut records_others = vec![];
        for response in responses {
            match response.resp_oneof {
                Some(RespOneof::Record(record)) => {
                    if top_sql_plan
                        .contains(&(record.sql_digest.clone(), record.plan_digest.clone()))
                    {
                        results.push(TopSqlSubResponse {
                            resp_oneof: Some(RespOneof::Record(record)),
                        });
                    } else {
                        records_others.push(record);
                    }
                }
                _ => results.push(response),
            }
        }

        let mut others_ts_item = BTreeMap::new();
        for record in records_others {
            for item in record.items {
                match others_ts_item.get_mut(&item.timestamp_sec) {
                    None => {
                        others_ts_item.insert(item.timestamp_sec, item);
                    }
                    Some(i) => {
                        i.cpu_time_ms += item.cpu_time_ms;
                        i.stmt_exec_count += item.stmt_exec_count;
                        i.stmt_duration_sum_ns += item.stmt_duration_sum_ns;
                        i.stmt_duration_count += item.stmt_duration_count;
                        for (k, v) in item.stmt_kv_exec_count {
                            let iv = i.stmt_kv_exec_count.get(&k).unwrap_or(&0);
                            i.stmt_kv_exec_count.insert(k, *iv + v);
                        }
                    }
                }
            }
        }
        results.push(TopSqlSubResponse {
            resp_oneof: Some(RespOneof::Record(TopSqlRecord {
                sql_digest: vec![],
                plan_digest: vec![],
                items: others_ts_item.into_values().collect(),
            })),
        });

        results
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
                            for (k, v) in &item.stmt_kv_exec_count {
                                match new_item.stmt_kv_exec_count.get(k) {
                                    None => {
                                        new_item.stmt_kv_exec_count.insert(k.clone(), *v);
                                    }
                                    Some(existed_v) => {
                                        new_item
                                            .stmt_kv_exec_count
                                            .insert(k.clone(), *v + existed_v);
                                    }
                                }
                            }
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
    fn parse_tidb_record(record: TopSqlRecord, instance: String) -> Vec<LogEvent> {
        let mut logs = vec![];

        let mut buf = Buf::default();
        buf.instance(instance)
            .instance_type(INSTANCE_TYPE_TIDB)
            .sql_digest(hex::encode_upper(record.sql_digest))
            .plan_digest(hex::encode_upper(record.plan_digest));

        macro_rules! append {
            ($( ($label_name:expr, $item_name:tt), )* ) => {
                $(
                    buf.label_name($label_name)
                        .points(record.items.iter().filter_map(|item| {
                            if item.$item_name > 0 {
                                Some((item.timestamp_sec, item.$item_name as f64))
                            } else {
                                None
                            }
                        }));
                    if let Some(event) = buf.build_event() {
                        logs.push(event);
                    }
                )*
            };
        }
        append!(
            // cpu_time_ms
            (METRIC_NAME_CPU_TIME_MS, cpu_time_ms),
            // stmt_exec_count
            (METRIC_NAME_STMT_EXEC_COUNT, stmt_exec_count),
            // stmt_duration_sum_ns
            (METRIC_NAME_STMT_DURATION_SUM_NS, stmt_duration_sum_ns),
            // stmt_duration_count
            (METRIC_NAME_STMT_DURATION_COUNT, stmt_duration_count),
        );

        // stmt_kv_exec_count
        buf.label_name(METRIC_NAME_STMT_EXEC_COUNT)
            .instance_type(INSTANCE_TYPE_TIKV);

        let tikv_instances = record
            .items
            .iter()
            .flat_map(|item| item.stmt_kv_exec_count.keys())
            .collect::<BTreeSet<_>>();
        for tikv_instance in tikv_instances {
            buf.instance(tikv_instance)
                .points(record.items.iter().filter_map(|item| {
                    let count = item
                        .stmt_kv_exec_count
                        .get(tikv_instance)
                        .copied()
                        .unwrap_or_default();

                    if count > 0 {
                        Some((item.timestamp_sec, count as f64))
                    } else {
                        None
                    }
                }));
            if let Some(event) = buf.build_event() {
                logs.push(event);
            }
        }

        logs
    }

    fn parse_tidb_sql_meta(sql_meta: SqlMeta) -> Vec<LogEvent> {
        vec![make_metric_like_log_event(
            &[
                (LABEL_NAME, METRIC_NAME_SQL_META.to_owned()),
                (LABEL_SQL_DIGEST, hex::encode_upper(sql_meta.sql_digest)),
                (LABEL_NORMALIZED_SQL, sql_meta.normalized_sql),
                (LABEL_IS_INTERNAL_SQL, sql_meta.is_internal_sql.to_string()),
            ],
            &[Utc::now()],
            &[1.0],
        )]
    }

    fn parse_tidb_plan_meta(plan_meta: PlanMeta) -> Vec<LogEvent> {
        vec![make_metric_like_log_event(
            &[
                (LABEL_NAME, METRIC_NAME_PLAN_META.to_owned()),
                (LABEL_PLAN_DIGEST, hex::encode_upper(plan_meta.plan_digest)),
                (LABEL_NORMALIZED_PLAN, plan_meta.normalized_plan),
                (
                    LABEL_ENCODED_NORMALIZED_PLAN,
                    plan_meta.encoded_normalized_plan,
                ),
            ],
            &[Utc::now()],
            &[1.0],
        )]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upstream::tidb::proto::TopSqlRecordItem;

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
                        })
                        .collect(),
                })),
            })
            .collect()
    }

    #[test]
    fn test_keep_top_n() {
        let responses = load_mock_responses();
        let top_n = TopSqlSubResponseParser::keep_top_n(responses, 10);
        assert_eq!(top_n.len(), 11);
        let mut top_cpu_time = vec![];
        let mut others_cpu_time = 0;
        for response in top_n {
            if let Some(RespOneof::Record(record)) = response.resp_oneof {
                let cpu_time: u32 = record.items.iter().map(|i| i.cpu_time_ms).sum();
                if record.sql_digest.is_empty() {
                    others_cpu_time = cpu_time;
                } else {
                    top_cpu_time.push(cpu_time);
                }
            }
        }
        top_cpu_time.sort_by(|a, b| b.cmp(a));
        assert_eq!(top_cpu_time, [90, 60, 50, 50, 50, 40, 40, 40, 40, 40]);
        assert_eq!(others_cpu_time, 30590);
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
            for (k, v) in item.stmt_kv_exec_count {
                match sum_old.stmt_kv_exec_count.get(&k) {
                    None => {
                        sum_old.stmt_kv_exec_count.insert(k, v);
                    }
                    Some(sum_v) => {
                        sum_old.stmt_kv_exec_count.insert(k, *sum_v + v);
                    }
                }
            }
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
            for (k, v) in item.stmt_kv_exec_count {
                match sum_new.stmt_kv_exec_count.get(&k) {
                    None => {
                        sum_new.stmt_kv_exec_count.insert(k, v);
                    }
                    Some(sum_v) => {
                        sum_new.stmt_kv_exec_count.insert(k, *sum_v + v);
                    }
                }
            }
        }

        assert_eq!(sum_old.cpu_time_ms, sum_new.cpu_time_ms);
        assert_eq!(sum_old.stmt_exec_count, sum_new.stmt_exec_count);
        assert_eq!(sum_old.stmt_duration_count, sum_new.stmt_duration_count);
        assert_eq!(sum_old.stmt_duration_sum_ns, sum_new.stmt_duration_sum_ns);
        assert_eq!(sum_old.stmt_kv_exec_count, sum_new.stmt_kv_exec_count);
    }
}
