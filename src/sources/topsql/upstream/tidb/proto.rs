#![allow(clippy::clone_on_ref_ptr)]
#![allow(non_snake_case)] // To avoid: Function `ScalarWrapper` should have snake_case name, e.g. `scalar_wrapper`

include!(concat!(env!("OUT_DIR"), "/tipb.rs"));

use top_sql_sub_response::RespOneof;
use vector_lib::ByteSizeOf;

impl ByteSizeOf for TopSqlSubResponse {
    fn allocated_bytes(&self) -> usize {
        self.resp_oneof.as_ref().map_or(0, ByteSizeOf::size_of)
    }
}

impl ByteSizeOf for RespOneof {
    fn allocated_bytes(&self) -> usize {
        match self {
            RespOneof::Record(record) => {
                record.items.size_of() + record.sql_digest.len() + record.plan_digest.len()
            }
            RespOneof::SqlMeta(sql_meta) => {
                sql_meta.sql_digest.len() + sql_meta.normalized_sql.len()
            }
            RespOneof::PlanMeta(plan_meta) => {
                plan_meta.plan_digest.len() + plan_meta.normalized_plan.len()
            }
        }
    }
}

impl ByteSizeOf for TopSqlRecordItem {
    fn allocated_bytes(&self) -> usize {
        self.stmt_kv_exec_count.size_of()
    }
}
