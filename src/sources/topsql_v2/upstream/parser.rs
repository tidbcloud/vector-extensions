use std::sync::Arc;
use vector_lib::event::LogEvent;

use crate::sources::topsql_v2::schema_cache::SchemaCache;
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

