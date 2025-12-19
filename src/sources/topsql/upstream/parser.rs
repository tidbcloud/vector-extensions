use bytes::Bytes;
use chrono::{DateTime, Utc};
use ordered_float::NotNan;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use vector::event::{Event, Metric, MetricKind, MetricTags, MetricValue};
use vector_lib::event::{KeyString, LogEvent, Value};

use crate::common::features::is_nextgen_mode;

use crate::sources::topsql::schema_cache::SchemaCache;
use crate::sources::topsql::upstream::consts::{
    LABEL_DB_NAME, LABEL_INSTANCE, LABEL_INSTANCE_TYPE, LABEL_KEYSPACE_NAME, LABEL_NAME,
    LABEL_PLAN_DIGEST, LABEL_SHAREDPOOL_ID, LABEL_SQL_DIGEST, LABEL_TABLE_ID, LABEL_TABLE_NAME,
    LABEL_TAG_LABEL,
};
use crate::sources::topsql::upstream::consts::{LABEL_VM_ACCOUNT_ID, LABEL_VM_PROJECT_ID};

pub fn truncate_label_value(s: String) -> String {
    // Truncate label value if it's too long, the default limit is 16KB in vminsert.
    const MAX_LABEL_LEN: usize = 16384;
    if s.len() > MAX_LABEL_LEN {
        let mut idx = MAX_LABEL_LEN;
        while idx != 0 && !s.is_char_boundary(idx) {
            idx -= 1;
        }
        s[..idx].to_string()
    } else {
        s
    }
}

pub trait UpstreamEventParser {
    type UpstreamEvent;

    fn parse(
        event: Self::UpstreamEvent,
        instance: String,
        schema_cache: Arc<SchemaCache>,
        sharedpool_id: Option<String>,
        keyspace_to_vmtenants: HashMap<String, (String, String)>,
    ) -> Vec<Event>;

    fn keep_top_n(responses: Vec<Self::UpstreamEvent>, top_n: usize) -> Vec<Self::UpstreamEvent>;

    fn downsampling(responses: &mut Vec<Self::UpstreamEvent>, interval_sec: u32);
}

pub struct Buf {
    labels: Vec<(&'static str, String)>,
    timestamps: Vec<DateTime<Utc>>,
    values: Vec<f64>,
}

impl Default for Buf {
    fn default() -> Self {
        let labels = if is_nextgen_mode() {
            // Nextgen mode: 13 labels including keyspace and VM tenant labels
            vec![
                (LABEL_NAME, String::new()),
                (LABEL_INSTANCE, String::new()),
                (LABEL_INSTANCE_TYPE, String::new()),
                (LABEL_SQL_DIGEST, String::new()),
                (LABEL_PLAN_DIGEST, String::new()),
                (LABEL_TAG_LABEL, String::new()),
                (LABEL_DB_NAME, String::new()),
                (LABEL_TABLE_NAME, String::new()),
                (LABEL_TABLE_ID, String::new()),
                (LABEL_KEYSPACE_NAME, String::new()),
                (LABEL_VM_ACCOUNT_ID, String::new()),
                (LABEL_VM_PROJECT_ID, String::new()),
                (LABEL_SHAREDPOOL_ID, String::new()),
            ]
        } else {
            // Legacy mode: 9 basic labels only
            vec![
                (LABEL_NAME, String::new()),
                (LABEL_INSTANCE, String::new()),
                (LABEL_INSTANCE_TYPE, String::new()),
                (LABEL_SQL_DIGEST, String::new()),
                (LABEL_PLAN_DIGEST, String::new()),
                (LABEL_TAG_LABEL, String::new()),
                (LABEL_DB_NAME, String::new()),
                (LABEL_TABLE_NAME, String::new()),
                (LABEL_TABLE_ID, String::new()),
            ]
        };

        Self {
            labels,
            timestamps: vec![],
            values: vec![],
        }
    }
}

impl Buf {
    pub fn label_name(&mut self, label_name: impl Into<String>) -> &mut Self {
        self.labels[0].1 = label_name.into();
        self
    }

    pub fn instance(&mut self, instance: impl Into<String>) -> &mut Self {
        self.labels[1].1 = instance.into();
        self
    }

    pub fn instance_type(&mut self, instance_type: impl Into<String>) -> &mut Self {
        self.labels[2].1 = instance_type.into();
        self
    }

    pub fn sql_digest(&mut self, sql_digest: impl Into<String>) -> &mut Self {
        self.labels[3].1 = sql_digest.into();
        self
    }

    pub fn plan_digest(&mut self, plan_digest: impl Into<String>) -> &mut Self {
        self.labels[4].1 = plan_digest.into();
        self
    }

    pub fn tag_label(&mut self, tag_label: impl Into<String>) -> &mut Self {
        self.labels[5].1 = tag_label.into();
        self
    }

    pub fn db_name(&mut self, db_name: impl Into<String>) -> &mut Self {
        self.labels[6].1 = db_name.into();
        self
    }

    pub fn table_name(&mut self, table_name: impl Into<String>) -> &mut Self {
        self.labels[7].1 = table_name.into();
        self
    }

    pub fn table_id(&mut self, table_id: impl Into<String>) -> &mut Self {
        self.labels[8].1 = table_id.into();
        self
    }

    pub fn keyspace_name(&mut self, keyspace_name: impl Into<String>) -> &mut Self {
        // Only available in nextgen mode (index 9)
        if is_nextgen_mode() {
            self.labels[9].1 = keyspace_name.into();
        }
        self
    }

    pub fn vm_account_id(&mut self, vm_account_id: impl Into<String>) -> &mut Self {
        // Only available in nextgen mode (index 10)
        if is_nextgen_mode() {
            self.labels[10].1 = vm_account_id.into();
        }
        self
    }

    pub fn vm_project_id(&mut self, vm_project_id: impl Into<String>) -> &mut Self {
        // Only available in nextgen mode (index 11)
        if is_nextgen_mode() {
            self.labels[11].1 = vm_project_id.into();
        }
        self
    }

    pub fn sharedpool_id(&mut self, sharedpool_id: impl Into<String>) -> &mut Self {
        // Only available in nextgen mode (index 12)
        if is_nextgen_mode() {
            self.labels[12].1 = sharedpool_id.into();
        }
        self
    }

    pub fn points(&mut self, points: impl Iterator<Item = (u64, f64)>) -> &mut Self {
        for (timestamp_sec, value) in points {
            self.timestamps.push(
                DateTime::from_timestamp(timestamp_sec as i64, 0)
                    .expect("invalid or out-of-range datetime"),
            );
            self.values.push(value);
        }
        self
    }

    /// Build events based on runtime mode detection
    /// - In nextgen mode: generates individual Metric events (one per data point)
    /// - In legacy mode: generates single LogEvent with batched timestamps/values
    pub fn build_events(&mut self) -> Option<Vec<Event>> {
        if self.timestamps.is_empty() || self.values.is_empty() {
            return None;
        }

        let result = if is_nextgen_mode() {
            // Nextgen mode: generate individual Metric events
            let mut tags = BTreeMap::new();
            for (label, value) in &self.labels {
                tags.insert(label.to_string(), truncate_label_value(value.clone()));
            }

            let mut events = vec![];
            for (timestamp, value) in std::iter::zip(&self.timestamps, &self.values) {
                let metric = Metric::new(
                    self.labels[0].1.clone(),
                    MetricKind::Absolute,
                    MetricValue::Gauge {
                        value: value.clone(),
                    },
                )
                .with_timestamp(Some(timestamp.clone()))
                .with_tags(Some(MetricTags::from(tags.clone())));
                events.push(Event::Metric(metric));
            }
            Some(events)
        } else {
            // Legacy mode: generate single LogEvent with batched data
            Some(vec![Event::Log(make_metric_like_log_event(
                &self.labels,
                &self.timestamps,
                &self.values,
            ))])
        };

        self.timestamps.clear();
        self.values.clear();
        result
    }
}

/// Helper function for legacy LogEvent format (batched timestamps and values)
fn make_metric_like_log_event(
    labels: &[(&'static str, String)],
    timestamps: &[DateTime<Utc>],
    values: &[f64],
) -> LogEvent {
    let mut labels_map = BTreeMap::new();
    for (k, v) in labels {
        labels_map.insert(
            KeyString::from(*k),
            Value::Bytes(Bytes::from(truncate_label_value(v.clone()))),
        );
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
    log.insert(KeyString::from("labels"), Value::Object(labels_map));
    log.insert(KeyString::from("timestamps"), Value::Array(timestamps_vec));
    log.insert(KeyString::from("values"), Value::Array(values_vec));
    log.into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use vector::event::Event;

    #[test]
    fn test_legacy_mode_generates_log_event() {
        // In legacy mode (without nextgen feature), should generate LogEvent with batched data
        #[cfg(not(feature = "nextgen"))]
        {
            let events = Buf::default()
                .label_name("topsql_cpu_time_ms")
                .instance("db:10080")
                .instance_type("tidb")
                .sql_digest("DEAD")
                .plan_digest("BEEF")
                .points([(1661396787, 80.0), (1661396788, 443.0)].into_iter())
                .build_events()
                .unwrap();

            // Should generate 1 LogEvent with batched timestamps/values
            assert_eq!(events.len(), 1);

            let event = &events[0];
            assert!(matches!(event, Event::Log(_)));

            if let Event::Log(log_event) = event {
                // Check labels
                assert!(log_event.contains("labels"));

                // Check timestamps array
                assert!(log_event.contains("timestamps"));
                if let Some(Value::Array(timestamps)) = log_event.get("timestamps") {
                    assert_eq!(timestamps.len(), 2);
                }

                // Check values array
                assert!(log_event.contains("values"));
                if let Some(Value::Array(values)) = log_event.get("values") {
                    assert_eq!(values.len(), 2);
                }
            }
        }
    }

    #[test]
    fn test_nextgen_mode_generates_metric_events() {
        // In nextgen mode (with nextgen feature), should generate individual Metric events
        #[cfg(feature = "nextgen")]
        {
            let events = Buf::default()
                .label_name("topsql_cpu_time_ms")
                .instance("db:10080")
                .instance_type("tidb")
                .sql_digest("DEAD")
                .plan_digest("BEEF")
                .points([(1661396787, 80.0), (1661396788, 443.0)].into_iter())
                .build_events()
                .unwrap();

            // Should generate 2 Metric events (one per data point)
            assert_eq!(events.len(), 2);

            for event in &events {
                assert!(matches!(event, Event::Metric(_)));

                if let Event::Metric(metric) = event {
                    assert_eq!(metric.name(), "topsql_cpu_time_ms");
                    assert!(metric.timestamp().is_some());

                    // Check tags
                    if let Some(tags) = metric.tags() {
                        assert!(tags.contains_key("instance"));
                        assert!(tags.contains_key("instance_type"));
                        assert!(tags.contains_key("sql_digest"));
                        assert!(tags.contains_key("plan_digest"));
                    }
                }
            }
        }
    }

    #[test]
    fn test_label_count_by_mode() {
        let buf = Buf::default();

        #[cfg(not(feature = "nextgen"))]
        {
            // Legacy mode: 9 labels
            assert_eq!(buf.labels.len(), 9);
        }

        #[cfg(feature = "nextgen")]
        {
            // Nextgen mode: 13 labels
            assert_eq!(buf.labels.len(), 13);
        }
    }

    #[test]
    fn test_nextgen_only_setters_safe_in_legacy_mode() {
        // These setters should not panic in legacy mode (they become no-ops)
        #[cfg(not(feature = "nextgen"))]
        {
            let mut buf = Buf::default();
            buf.keyspace_name("test-keyspace")
                .vm_account_id("acc-123")
                .vm_project_id("proj-456")
                .sharedpool_id("pool-1");

            // Should not panic, calls are simply ignored
            assert_eq!(buf.labels.len(), 9);
        }
    }

    #[test]
    fn test_nextgen_only_setters_work_in_nextgen_mode() {
        // These setters should work in nextgen mode
        #[cfg(feature = "nextgen")]
        {
            let mut buf = Buf::default();
            buf.keyspace_name("test-keyspace")
                .vm_account_id("acc-123")
                .vm_project_id("proj-456")
                .sharedpool_id("pool-1");

            assert_eq!(buf.labels.len(), 13);
            assert_eq!(buf.labels[9].1, "test-keyspace");
            assert_eq!(buf.labels[10].1, "acc-123");
            assert_eq!(buf.labels[11].1, "proj-456");
            assert_eq!(buf.labels[12].1, "pool-1");
        }
    }
}
