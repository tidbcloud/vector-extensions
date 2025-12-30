use std::collections::BTreeMap;

use chrono::Utc;
use vector::event::{
    Event, Metric, MetricKind, MetricTags, MetricValue,
};

use crate::sources::topsql_v2::upstream::consts::{
    LABEL_INSTANCE, LABEL_INSTANCE_TYPE, METRIC_NAME_INSTANCE,
};

pub fn instance_event(
    instance: String,
    instance_type: String,
) -> Event {
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
