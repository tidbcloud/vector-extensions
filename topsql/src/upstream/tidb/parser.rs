use std::collections::BTreeSet;

use chrono::Utc;

use super::proto::{
    top_sql_sub_response::RespOneof, PlanMeta, SqlMeta, TopSqlRecord, TopSqlSubResponse,
};
use vector::event::LogEvent;

use crate::upstream::{
    consts::{
        INSTANCE_TYPE_TIDB, INSTANCE_TYPE_TIKV, LABEL_ENCODED_NORMALIZED_PLAN,
        LABEL_IS_INTERNAL_SQL, LABEL_NAME, LABEL_NORMALIZED_PLAN, LABEL_NORMALIZED_SQL,
        LABEL_PLAN_DIGEST, LABEL_SQL_DIGEST, METRIC_NAME_CPU_TIME_MS, METRIC_NAME_PLAN_META,
        METRIC_NAME_SQL_META, METRIC_NAME_STMT_DURATION_COUNT, METRIC_NAME_STMT_DURATION_SUM_NS,
        METRIC_NAME_STMT_EXEC_COUNT,
    },
    parser::{Buf, UpstreamEventParser},
    utils::make_metric_like_log_event,
};

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
}

impl TopSqlSubResponseParser {
    fn parse_tidb_record(record: TopSqlRecord, instance: String) -> Vec<LogEvent> {
        let mut logs = vec![];

        let mut buf = Buf::new();
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
