use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use ordered_float::NotNan;
use vector::event::{
    Event, KeyString, LogEvent, Metric, MetricKind, MetricTags, MetricValue, Value,
};

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
        labels_map.insert((*k).into(), Value::Bytes(Bytes::from(v.clone())));
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

pub fn instance_event(instance: String, instance_type: String) -> Event {
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
