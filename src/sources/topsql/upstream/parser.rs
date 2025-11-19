use chrono::{DateTime, Utc};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use vector::event::{Event, Metric, MetricKind, MetricTags, MetricValue};
use vector_lib::vrl::parser::ast::Op;

use crate::sources::topsql::schema_cache::SchemaCache;
use crate::sources::topsql::upstream::consts::{LABEL_VM_ACCOUNT_ID, LABEL_VM_PROJECT_ID};
use crate::sources::topsql::upstream::{
    consts::{
        LABEL_DB_NAME, LABEL_INSTANCE, LABEL_INSTANCE_TYPE, LABEL_KEYSPACE_NAME, LABEL_NAME,
        LABEL_PLAN_DIGEST, LABEL_SHAREDPOOL_ID, LABEL_SQL_DIGEST, LABEL_TABLE_ID, LABEL_TABLE_NAME,
        LABEL_TAG_LABEL,
    },
    utils::make_metric_like_log_event,
};

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
        Self {
            labels: vec![
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
            ],
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
        self.labels[9].1 = keyspace_name.into();
        self
    }

    pub fn vm_account_id(&mut self, vm_account_id: impl Into<String>) -> &mut Self {
        self.labels[10].1 = vm_account_id.into();
        self
    }

    pub fn vm_project_id(&mut self, vm_project_id: impl Into<String>) -> &mut Self {
        self.labels[11].1 = vm_project_id.into();
        self
    }

    pub fn sharedpool_id(&mut self, sharedpool_id: impl Into<String>) -> &mut Self {
        self.labels[12].1 = sharedpool_id.into();
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

    pub fn build_events(&mut self) -> Option<Vec<Event>> {
        let mut tags = BTreeMap::new();
        for (label, value) in &self.labels {
            tags.insert(label.to_string(), truncate_label_value(value.clone()));
        }

        let res = if self.timestamps.is_empty() || self.values.is_empty() {
            None
        } else {
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
            // Some(make_metric_like_log_event(
            //     &self.labels,
            //     &self.timestamps,
            //     &self.values,
            // ))
        };

        self.timestamps.clear();
        self.values.clear();
        res
    }
}
