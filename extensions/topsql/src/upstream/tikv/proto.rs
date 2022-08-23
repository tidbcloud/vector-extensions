#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::derive_partial_eq_without_eq)]

include!(concat!(env!("OUT_DIR"), "/resource_usage_agent.rs"));

use resource_usage_record::RecordOneof;
use vector_core::ByteSizeOf;

impl ByteSizeOf for ResourceUsageRecord {
    fn allocated_bytes(&self) -> usize {
        self.record_oneof.as_ref().map_or(0, ByteSizeOf::size_of)
    }
}

impl ByteSizeOf for RecordOneof {
    fn allocated_bytes(&self) -> usize {
        match self {
            RecordOneof::Record(record) => record.resource_group_tag.len() + record.items.size_of(),
        }
    }
}

impl ByteSizeOf for GroupTagRecordItem {
    fn allocated_bytes(&self) -> usize {
        0
    }
}
