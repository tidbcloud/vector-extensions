use chrono::{DateTime, NaiveDateTime, Utc};
use vector_core::event::LogEvent;

use crate::upstream::{
    consts::{
        LABEL_INSTANCE, LABEL_INSTANCE_TYPE, LABEL_NAME, LABEL_PLAN_DIGEST, LABEL_SQL_DIGEST,
        LABEL_TAG_LABEL,
    },
    utils::make_metric_like_log_event,
};

pub trait UpstreamEventParser {
    type UpstreamEvent;

    fn parse(response: Self::UpstreamEvent, instance: String) -> Vec<LogEvent>;
}

pub struct Buf {
    labels: Vec<(&'static str, String)>,
    timestamps: Vec<DateTime<Utc>>,
    values: Vec<f64>,
}

impl Buf {
    pub fn new() -> Self {
        Self {
            labels: vec![
                (LABEL_NAME, String::new()),
                (LABEL_INSTANCE, String::new()),
                (LABEL_INSTANCE_TYPE, String::new()),
                (LABEL_SQL_DIGEST, String::new()),
                (LABEL_PLAN_DIGEST, String::new()),
                (LABEL_TAG_LABEL, String::new()),
            ],
            timestamps: vec![],
            values: vec![],
        }
    }

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

    pub fn points(&mut self, points: impl Iterator<Item = (u64, f64)>) -> &mut Self {
        for (timestamp_sec, value) in points {
            self.timestamps.push(DateTime::<Utc>::from_utc(
                NaiveDateTime::from_timestamp(timestamp_sec as i64, 0),
                Utc,
            ));
            self.values.push(value);
        }
        self
    }

    pub fn build_event(&mut self) -> Option<LogEvent> {
        let res = if self.timestamps.is_empty() || self.values.is_empty() {
            None
        } else {
            Some(make_metric_like_log_event(
                &self.labels,
                &self.timestamps,
                &self.values,
            ))
        };

        self.timestamps.clear();
        self.values.clear();
        res
    }
}
