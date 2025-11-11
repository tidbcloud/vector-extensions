use bytes::Bytes;
use chrono::{DateTime, Utc};
use ordered_float::NotNan;
use std::collections::BTreeMap;
use vector::event::{KeyString, Value};
use vector_lib::event::{Event, LogEvent, Metric, MetricKind, MetricTags, MetricValue};

use crate::sources::topsql::upstream::consts::{
    LABEL_INSTANCE, LABEL_INSTANCE_TYPE, LABEL_NAME, METRIC_NAME_INSTANCE,
};

pub fn make_metric_like_log_event(
    labels: &[(&'static str, String)],
    timestamps: &[DateTime<Utc>],
    values: &[f64],
) -> LogEvent {
    let mut labels_map = BTreeMap::<KeyString, Value>::new();
    for (k, v) in labels {
        const MAX_LABEL_VALUE_LEN: usize = 16384;
        let mut new_v = v.clone();
        // truncate label value if it's too long, the default limit is 16KB in vminsert.
        if new_v.len() > MAX_LABEL_VALUE_LEN {
            let mut idx = MAX_LABEL_VALUE_LEN;
            while idx != 0 && !new_v.is_char_boundary(idx) {
                idx -= 1;
            }
            new_v.truncate(idx);
        }
        labels_map.insert((*k).into(), Value::Bytes(Bytes::from(new_v)));
    }

    let timestamps_vec = timestamps
        .iter()
        .map(|t| Value::Timestamp(*t))
        .collect::<Vec<_>>();
    let values_vec = values
        .iter()
        .map(|v| Value::Float(NotNan::new(*v).unwrap()))
        .collect::<Vec<_>>();

    let mut log = BTreeMap::<KeyString, Value>::new();
    log.insert("labels".into(), Value::Object(labels_map));
    log.insert("timestamps".into(), Value::Array(timestamps_vec));
    log.insert("values".into(), Value::Array(values_vec));
    log.into()
}

pub fn instance_event(instance: String, instance_type: String) -> LogEvent {
    make_metric_like_log_event(
        &[
            (LABEL_NAME, METRIC_NAME_INSTANCE.to_owned()),
            (LABEL_INSTANCE, instance),
            (LABEL_INSTANCE_TYPE, instance_type),
        ],
        &[Utc::now()],
        &[1.0],
    )
}

pub fn instance_event_metric(instance: String, instance_type: String) -> Event {
    let mut tags = BTreeMap::new();
    tags.insert(LABEL_INSTANCE.to_owned(), instance);
    tags.insert(LABEL_INSTANCE_TYPE.to_owned(), instance_type);
    let metric = Metric::new(
        METRIC_NAME_INSTANCE,
        MetricKind::Absolute,
        MetricValue::Gauge { value: 1.0 },
    )
    .with_timestamp(Some(Utc::now()))
    .with_tags(Some(MetricTags::from(tags)));
    Event::Metric(metric)
}

pub fn instance_event_with_tags(
    instance: String,
    instance_type: String,
    cluster_id: String,
    vm_account_id: String,
    vm_project_id: String,
) -> Event {
    let mut tags = BTreeMap::new();
    tags.insert(LABEL_INSTANCE.to_owned(), instance);
    tags.insert(LABEL_INSTANCE_TYPE.to_owned(), instance_type);
    tags.insert("cluster_id".to_string(), cluster_id.clone());
    tags.insert("tidb_cluster_id".to_string(), cluster_id.clone());
    tags.insert("keyspace_name".to_string(), cluster_id.clone());
    tags.insert("vm_account_id".to_string(), vm_account_id.clone());
    tags.insert("vm_project_id".to_string(), vm_project_id.clone());
    let metric = Metric::new(
        METRIC_NAME_INSTANCE,
        MetricKind::Absolute,
        MetricValue::Gauge { value: 1.0 },
    )
    .with_timestamp(Some(Utc::now()))
    .with_tags(Some(MetricTags::from(tags)));
    Event::Metric(metric)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_make_metric_like_log_event_truncates_long_labels() {
        let long_value = "a".repeat(20000);
        let labels = vec![("long_label", long_value)];
        let timestamps = vec![Utc::now()];
        let values = vec![1.0];

        let log = make_metric_like_log_event(&labels, &timestamps, &values);

        if let Some(Value::Object(labels_map)) = log.get("labels") {
            if let Some(Value::Bytes(bytes)) = labels_map.get("long_label") {
                assert_eq!(bytes.len(), 16384);
                assert_eq!(bytes, &Bytes::from("a".repeat(16384)));
            }
        }
    }
}
