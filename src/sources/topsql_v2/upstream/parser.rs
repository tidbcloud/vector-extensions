use std::sync::Arc;
use vector_lib::event::LogEvent;

use crate::sources::topsql_v2::schema_cache::SchemaCache;

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
    ) -> Vec<LogEvent>;

    fn keep_top_n(responses: Vec<Self::UpstreamEvent>, top_n: usize) -> Vec<Self::UpstreamEvent>;

    fn downsampling(responses: &mut Vec<Self::UpstreamEvent>, interval_sec: u32);
}

