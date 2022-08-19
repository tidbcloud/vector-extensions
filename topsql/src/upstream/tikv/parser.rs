use prost::Message;

use super::proto::{resource_usage_record::RecordOneof, GroupTagRecord, ResourceUsageRecord};
use vector::event::LogEvent;

use crate::upstream::{
    consts::{
        INSTANCE_TYPE_TIKV, KV_TAG_LABEL_INDEX, KV_TAG_LABEL_ROW, KV_TAG_LABEL_UNKNOWN,
        METRIC_NAME_CPU_TIME_MS, METRIC_NAME_READ_KEYS, METRIC_NAME_WRITE_KEYS,
    },
    parser::{Buf, UpstreamEventParser},
    tidb::proto::ResourceGroupTag,
};

pub struct ResourceUsageRecordParser;

impl UpstreamEventParser for ResourceUsageRecordParser {
    type UpstreamEvent = ResourceUsageRecord;

    fn parse(response: Self::UpstreamEvent, instance: String) -> Vec<LogEvent> {
        match response.record_oneof {
            Some(RecordOneof::Record(record)) => Self::parse_tikv_record(record, instance),
            None => vec![],
        }
    }
}

impl ResourceUsageRecordParser {
    fn parse_tikv_record(record: GroupTagRecord, instance: String) -> Vec<LogEvent> {
        let decoded = Self::decode_tag(record.resource_group_tag.as_slice());
        if decoded.is_none() {
            return vec![];
        }

        let mut logs = vec![];

        let (sql_digest, plan_digest, tag_label) = decoded.unwrap();
        let mut buf = Buf::new();
        buf.instance(instance)
            .instance_type(INSTANCE_TYPE_TIKV)
            .sql_digest(sql_digest)
            .plan_digest(plan_digest)
            .tag_label(tag_label);

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
            // read_keys
            (METRIC_NAME_READ_KEYS, read_keys),
            // write_keys
            (METRIC_NAME_WRITE_KEYS, write_keys),
        );

        logs
    }

    fn decode_tag(tag: &[u8]) -> Option<(String, String, String)> {
        match ResourceGroupTag::decode(tag) {
            Ok(resource_tag) => {
                if resource_tag.sql_digest.is_none() {
                    warn!(message = "Unexpected resource tag without sql digest", tag = %hex::encode(tag));
                    None
                } else {
                    Some((
                        hex::encode_upper(resource_tag.sql_digest.unwrap()),
                        hex::encode_upper(resource_tag.plan_digest.unwrap_or_default()),
                        match resource_tag.label {
                            Some(1) => KV_TAG_LABEL_ROW.to_owned(),
                            Some(2) => KV_TAG_LABEL_INDEX.to_owned(),
                            _ => KV_TAG_LABEL_UNKNOWN.to_owned(),
                        },
                    ))
                }
            }
            Err(error) => {
                warn!(message = "Failed to decode resource tag", tag = %hex::encode(tag), %error);
                None
            }
        }
    }
}
