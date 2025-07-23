use std::collections::BTreeMap;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use ordered_float::NotNan;
use vector::event::{KeyString, Value};
use vector_lib::event::LogEvent;





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




