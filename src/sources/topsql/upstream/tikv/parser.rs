use std::collections::{BTreeMap, HashMap};

use prost::Message;
use vector::event::LogEvent;

use crate::sources::topsql::upstream::consts::{
    INSTANCE_TYPE_TIKV, KV_TAG_LABEL_INDEX, KV_TAG_LABEL_ROW, KV_TAG_LABEL_UNKNOWN,
    METRIC_NAME_CPU_TIME_MS, METRIC_NAME_READ_KEYS, METRIC_NAME_WRITE_KEYS,
};
use crate::sources::topsql::upstream::parser::{Buf, UpstreamEventParser};
use crate::sources::topsql::upstream::tidb::proto::ResourceGroupTag;
use crate::sources::topsql::upstream::tikv::proto::resource_usage_record::RecordOneof;
use crate::sources::topsql::upstream::tikv::proto::{GroupTagRecord, GroupTagRecordItem, ResourceUsageRecord};

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
        struct PerSecondDigest {
            resource_group_tag: Vec<u8>,
            cpu_time_ms: u32,
            read_keys: u32,
            write_keys: u32,
        }

        let mut new_responses = vec![];
        let mut ts_others = BTreeMap::new();
        let mut ts_digests = BTreeMap::new();
        for response in responses {
            if let Some(RecordOneof::Record(record)) = response.record_oneof {
                let (sql_digest, _, _) = match Self::decode_tag(&record.resource_group_tag) {
                    Some(tag) => tag,
                    None => continue,
                };
                if sql_digest.is_empty() {
                    for item in record.items {
                        ts_others.insert(item.timestamp_sec, item);
                    }
                } else {
                    for item in &record.items {
                        let psd = PerSecondDigest {
                            resource_group_tag: record.resource_group_tag.clone(),
                            cpu_time_ms: item.cpu_time_ms,
                            read_keys: item.read_keys,
                            write_keys: item.write_keys,
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
            v.sort_by(|psd1, psd2| psd2.cpu_time_ms.cmp(&psd1.cpu_time_ms));
            let evicted = v.split_at(top_n).1;
            let mut others = GroupTagRecordItem::default();
            for e in evicted {
                others.timestamp_sec = *ts;
                others.cpu_time_ms += e.cpu_time_ms;
                others.read_keys += e.read_keys;
                others.write_keys += e.write_keys;
            }
            v.truncate(top_n);
            match ts_others.get_mut(&ts) {
                None => {
                    ts_others.insert(*ts, others);
                }
                Some(existed_others) => {
                    existed_others.cpu_time_ms += others.cpu_time_ms;
                    existed_others.read_keys += others.read_keys;
                    existed_others.write_keys += others.write_keys;
                }
            }
        }

        let mut digest_items = HashMap::new();
        for (ts, v) in ts_digests {
            for psd in v {
                let item = GroupTagRecordItem {
                    timestamp_sec: ts,
                    cpu_time_ms: psd.cpu_time_ms,
                    read_keys: psd.read_keys,
                    write_keys: psd.write_keys,
                };
                match digest_items.get_mut(&psd.resource_group_tag) {
                    None => {
                        digest_items.insert(psd.resource_group_tag, vec![item]);
                    }
                    Some(items) => {
                        items.push(item);
                    }
                }
            }
        }
        if !ts_others.is_empty() {
            let others_k = Self::encode_tag(vec![], vec![], None);
            digest_items.insert(others_k.clone(), ts_others.into_values().collect());
        }

        for (digest, items) in digest_items {
            new_responses.push(ResourceUsageRecord {
                record_oneof: Some(RecordOneof::Record(GroupTagRecord {
                    resource_group_tag: digest,
                    items: items,
                })),
            })
        }
        new_responses
    }

    // fn keep_top_n(responses: Vec<Self::UpstreamEvent>, top_n: usize) -> Vec<Self::UpstreamEvent> {
    //     let mut cpu_time_map = HashMap::new();
    //     for response in &responses {
    //         if let Some(RecordOneof::Record(record)) = &response.record_oneof {
    //             let (sql_digest, _, _) = match Self::decode_tag(&record.resource_group_tag) {
    //                 Some(tag) => tag,
    //                 None => continue,
    //             };
    //             if sql_digest.is_empty() {
    //                 continue; // others
    //             }
    //             let cpu_time: u32 = record.items.iter().map(|i| i.cpu_time_ms).sum();
    //             let v = cpu_time_map.get(&record.resource_group_tag).unwrap_or(&0);
    //             cpu_time_map.insert(record.resource_group_tag.clone(), v + cpu_time);
    //         }
    //     }
    //     let mut cpu_time_vec = cpu_time_map.into_iter().collect::<Vec<(Vec<u8>, u32)>>();
    //     cpu_time_vec.sort_by(|a, b| b.1.cmp(&a.1));
    //     cpu_time_vec.truncate(top_n);
    //     let mut top_tag = HashSet::new();
    //     for v in cpu_time_vec {
    //         top_tag.insert(v.0);
    //     }

    //     let mut results = vec![];
    //     let mut records_others = vec![];
    //     for response in responses {
    //         match response.record_oneof {
    //             Some(RecordOneof::Record(record)) => {
    //                 if top_tag.contains(&record.resource_group_tag) {
    //                     results.push(ResourceUsageRecord {
    //                         record_oneof: Some(RecordOneof::Record(record)),
    //                     });
    //                 } else {
    //                     records_others.push(record);
    //                 }
    //             }
    //             _ => results.push(response),
    //         }
    //     }

    //     let mut others_ts_item = BTreeMap::new();
    //     for record in records_others {
    //         for item in record.items {
    //             match others_ts_item.get_mut(&item.timestamp_sec) {
    //                 None => {
    //                     others_ts_item.insert(item.timestamp_sec, item);
    //                 }
    //                 Some(i) => {
    //                     i.cpu_time_ms += item.cpu_time_ms;
    //                     i.read_keys += item.read_keys;
    //                     i.write_keys += item.write_keys;
    //                 }
    //             }
    //         }
    //     }
    //     results.push(ResourceUsageRecord {
    //         record_oneof: Some(RecordOneof::Record(GroupTagRecord {
    //             resource_group_tag: Self::encode_tag(vec![], vec![], None),
    //             items: others_ts_item.into_values().collect(),
    //         })),
    //     });

    //     results
    // }

    fn downsampling(responses: &mut Vec<Self::UpstreamEvent>, interval_sec: u32) {
        if interval_sec <= 1 {
            return;
        }
        let interval_sec = interval_sec as u64;
        for response in responses {
            if let Some(RecordOneof::Record(record)) = &mut response.record_oneof {
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
                            new_item.read_keys += item.read_keys;
                            new_item.write_keys += item.write_keys;
                            new_items.insert(new_ts, new_item);
                        }
                    }
                }
                record.items = new_items.into_values().collect();
            }
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
    use crate::sources::topsql::upstream::tikv::proto::GroupTagRecordItem;

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

    fn load_mock_records() -> Vec<ResourceUsageRecord> {
        serde_json::from_str::<Vec<Record>>(MOCK_RECORDS)
            .unwrap()
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
            .collect()
    }

    #[test]
    fn test_keep_top_n() {
        let records = load_mock_records();
        let top_n = ResourceUsageRecordParser::keep_top_n(records, 10);
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

    #[test]
    fn test_downsampling() {
        let mut records = load_mock_records();
        let mut items = vec![];
        for record in &records {
            if let Some(RecordOneof::Record(record)) = &record.record_oneof {
                if ResourceUsageRecordParser::decode_tag(&record.resource_group_tag)
                    .unwrap()
                    .0
                    .is_empty()
                {
                    items = record.items.clone();
                }
            }
        }
        let mut timestamps: Vec<u64> = items.clone().into_iter().map(|i| i.timestamp_sec).collect();
        timestamps.sort();
        println!("{:?}", timestamps);
        assert_eq!(
            timestamps, // 00:03:31 ~ 00:03:59
            [
                1709654611, 1709654612, 1709654613, 1709654614, 1709654615, 1709654616, 1709654617,
                1709654618, 1709654619, 1709654620, 1709654621, 1709654622, 1709654623, 1709654624,
                1709654625, 1709654626, 1709654627, 1709654628, 1709654629, 1709654630, 1709654631,
                1709654632, 1709654633, 1709654634, 1709654635, 1709654636, 1709654637, 1709654638,
                1709654639
            ]
        );
        let mut sum_old = GroupTagRecordItem::default();
        for item in items {
            sum_old.cpu_time_ms += item.cpu_time_ms;
            sum_old.read_keys += item.read_keys;
            sum_old.write_keys += item.write_keys;
        }

        ResourceUsageRecordParser::downsampling(&mut records, 15);

        let mut items = vec![];
        for record in &records {
            if let Some(RecordOneof::Record(record)) = &record.record_oneof {
                if ResourceUsageRecordParser::decode_tag(&record.resource_group_tag)
                    .unwrap()
                    .0
                    .is_empty()
                {
                    items = record.items.clone();
                }
            }
        }
        let timestamps: Vec<u64> = items.clone().into_iter().map(|i| i.timestamp_sec).collect();
        assert_eq!(
            timestamps,
            [
                1709654625, // 00:03:45
                1709654640, // 00:04:00
            ]
        );
        let mut sum_new = GroupTagRecordItem::default();
        for item in items {
            sum_new.cpu_time_ms += item.cpu_time_ms;
            sum_new.read_keys += item.read_keys;
            sum_new.write_keys += item.write_keys;
        }

        assert_eq!(sum_old.cpu_time_ms, sum_new.cpu_time_ms);
        assert_eq!(sum_old.read_keys, sum_new.read_keys);
        assert_eq!(sum_old.write_keys, sum_new.write_keys);
    }
}
