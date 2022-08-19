use std::collections::BTreeMap;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use ordered_float::NotNan;

use super::consts::{LABEL_INSTANCE, LABEL_INSTANCE_TYPE, LABEL_NAME, METRIC_NAME_INSTANCE};
use vector::event::{EventMetadata, LogEvent, Value};

pub fn make_metric_like_log_event(
    labels: &[(&'static str, String)],
    timestamps: &[DateTime<Utc>],
    values: &[f64],
) -> LogEvent {
    let mut labels_map = BTreeMap::new();
    for (k, v) in labels {
        labels_map.insert(k.to_string(), Value::Bytes(Bytes::from(v.clone())));
    }

    let timestamps_vec = timestamps
        .iter()
        .map(|t| Value::Timestamp(*t))
        .collect::<Vec<_>>();
    let values_vec = values
        .iter()
        .map(|v| Value::Float(NotNan::new(*v).unwrap()))
        .collect::<Vec<_>>();

    let mut log = BTreeMap::new();
    log.insert("labels".to_owned(), Value::Object(labels_map));
    log.insert("timestamps".to_owned(), Value::Array(timestamps_vec));
    log.insert("values".to_owned(), Value::Array(values_vec));
    LogEvent::from_map(log, EventMetadata::default())
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
