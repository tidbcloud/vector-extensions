use std::collections::{BTreeMap, HashMap, HashSet};

use prost::Message;
use vector::event::LogEvent;

use crate::upstream::consts::{
    INSTANCE_TYPE_TIKV, KV_TAG_LABEL_INDEX, KV_TAG_LABEL_ROW, KV_TAG_LABEL_UNKNOWN,
    METRIC_NAME_CPU_TIME_MS, METRIC_NAME_READ_KEYS, METRIC_NAME_WRITE_KEYS,
};
use crate::upstream::parser::{Buf, UpstreamEventParser};
use crate::upstream::tidb::proto::ResourceGroupTag;
use crate::upstream::tikv::proto::resource_usage_record::RecordOneof;
use crate::upstream::tikv::proto::{GroupTagRecord, ResourceUsageRecord};

pub struct ResourceUsageRecordParser;

impl UpstreamEventParser for ResourceUsageRecordParser {
    type UpstreamEvent = ResourceUsageRecord;

    fn parse(response: Self::UpstreamEvent, instance: String) -> Vec<LogEvent> {
        match response.record_oneof {
            Some(RecordOneof::Record(record)) => Self::parse_tikv_record(record, instance),
            None => vec![],
        }
    }

    fn keep_top_n(responses: Vec<Self::UpstreamEvent>, top_n: usize) -> Vec<Self::UpstreamEvent> {
        let mut cpu_time_map = HashMap::new();
        for response in &responses {
            if let Some(RecordOneof::Record(record)) = &response.record_oneof {
                let (sql_digest, _, _) = match Self::decode_tag(&record.resource_group_tag) {
                    Some(tag) => tag,
                    None => continue,
                };
                if sql_digest.is_empty() {
                    continue; // others
                }
                let cpu_time: u32 = record.items.iter().map(|i| i.cpu_time_ms).sum();
                let v = cpu_time_map.get(&record.resource_group_tag).unwrap_or(&0);
                cpu_time_map.insert(record.resource_group_tag.clone(), *v + cpu_time);
            }
        }
        let mut cpu_time_vec = cpu_time_map.into_iter().collect::<Vec<(Vec<u8>, u32)>>();
        cpu_time_vec.sort_by(|a, b| b.1.cmp(&a.1));
        cpu_time_vec.truncate(top_n);
        let mut top_tag = HashSet::new();
        for v in cpu_time_vec {
            top_tag.insert(v.0);
        }

        let mut results = vec![];
        let mut records_others = vec![];
        for response in responses {
            match response.record_oneof {
                Some(RecordOneof::Record(record)) => {
                    if top_tag.contains(&record.resource_group_tag) {
                        results.push(ResourceUsageRecord {
                            record_oneof: Some(RecordOneof::Record(record)),
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
                        i.read_keys += item.read_keys;
                        i.write_keys += item.write_keys;
                    }
                }
            }
        }
        results.push(ResourceUsageRecord {
            record_oneof: Some(RecordOneof::Record(GroupTagRecord {
                resource_group_tag: Self::encode_tag(vec![], vec![], None),
                items: others_ts_item.into_values().collect(),
            })),
        });

        results
    }

    // fn downsampling(responses: &mut Vec<Self::UpstreamEvent>, accuracy_sec: u32) {
    //     if accuracy_sec <= 1 {
    //         return;
    //     }
    // }
}

impl ResourceUsageRecordParser {
    fn parse_tikv_record(record: GroupTagRecord, instance: String) -> Vec<LogEvent> {
        let decoded = Self::decode_tag(record.resource_group_tag.as_slice());
        if decoded.is_none() {
            return vec![];
        }

        let mut logs = vec![];

        let (sql_digest, plan_digest, tag_label) = decoded.unwrap();
        let mut buf = Buf::default();
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

    fn encode_tag(sql_digest: Vec<u8>, plan_digest: Vec<u8>, label: Option<i32>) -> Vec<u8> {
        ResourceGroupTag::encode_to_vec(&ResourceGroupTag {
            sql_digest: Some(sql_digest),
            plan_digest: Some(plan_digest),
            label: label,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upstream::tikv::proto::GroupTagRecordItem;

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
        read_keys: u32,
        write_keys: u32,
    }

    #[test]
    fn test_keep_top_n() {
        let records: Vec<Record> = serde_json::from_str(MOCK_RECORDS).unwrap();
        let responses = records
            .into_iter()
            .map(|r| ResourceUsageRecord {
                record_oneof: Some(RecordOneof::Record(GroupTagRecord {
                    resource_group_tag: ResourceUsageRecordParser::encode_tag(
                        hex::decode(r.sql).unwrap(),
                        hex::decode(r.plan).unwrap(),
                        None,
                    ),
                    items: r
                        .items
                        .into_iter()
                        .map(|i| GroupTagRecordItem {
                            timestamp_sec: i.timestamp_sec,
                            cpu_time_ms: i.cpu_time_ms,
                            read_keys: i.read_keys,
                            write_keys: i.write_keys,
                        })
                        .collect(),
                })),
            })
            .collect();
        let top_n = ResourceUsageRecordParser::keep_top_n(responses, 10);
        assert_eq!(top_n.len(), 11);
        let mut top_cpu_time = vec![];
        let mut others_cpu_time = 0;
        for response in top_n {
            if let Some(RecordOneof::Record(record)) = response.record_oneof {
                let cpu_time: u32 = record.items.iter().map(|i| i.cpu_time_ms).sum();
                match ResourceUsageRecordParser::decode_tag(&record.resource_group_tag) {
                    None => others_cpu_time = cpu_time,
                    Some((sql_digest, _, _)) => {
                        if sql_digest.is_empty() {
                            others_cpu_time = cpu_time;
                        } else {
                            top_cpu_time.push(cpu_time);
                        }
                    }
                }
            }
        }
        top_cpu_time.sort_by(|a, b| b.cmp(a));
        assert_eq!(
            top_cpu_time,
            [723, 646, 621, 619, 574, 551, 549, 545, 544, 529]
        );
        assert_eq!(others_cpu_time, 65216);
    }
}
