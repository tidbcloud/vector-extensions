pub const LABEL_DB_NAME: &str = "db";
pub const LABEL_TABLE_NAME: &str = "table";
pub const LABEL_TABLE_ID: &str = "table_id";
pub const LABEL_KEYSPACE: &str = "keyspace";
pub const LABEL_SQL_DIGEST: &str = "sql_digest";
pub const LABEL_PLAN_DIGEST: &str = "plan_digest";
pub const LABEL_TAG_LABEL: &str = "tag_label";
pub const LABEL_NORMALIZED_SQL: &str = "normalized_sql";
pub const LABEL_NORMALIZED_PLAN: &str = "normalized_plan";
pub const LABEL_ENCODED_NORMALIZED_PLAN: &str = "encoded_normalized_plan";
pub const LABEL_SOURCE_TABLE: &str = "source_table";
pub const LABEL_TIMESTAMPS: &str = "timestamps";
pub const LABEL_DATE: &str = "date";
pub const LABEL_INSTANCE_KEY: &str = "instance_key";
pub const LABEL_REGION_ID: &str = "region_id";

pub const METRIC_NAME_CPU_TIME_MS: &str = "topsql_cpu_time_ms";
pub const METRIC_NAME_READ_KEYS: &str = "topsql_read_keys";
pub const METRIC_NAME_WRITE_KEYS: &str = "topsql_write_keys";
pub const METRIC_NAME_NETWORK_IN_BYTES: &str = "topsql_network_in_bytes";
pub const METRIC_NAME_NETWORK_OUT_BYTES: &str = "topsql_network_out_bytes";
pub const METRIC_NAME_LOGICAL_READ_BYTES: &str = "topsql_logical_read_bytes";
pub const METRIC_NAME_LOGICAL_WRITE_BYTES: &str = "topsql_logical_write_bytes";
pub const METRIC_NAME_STMT_EXEC_COUNT: &str = "topsql_stmt_exec_count";
pub const METRIC_NAME_STMT_DURATION_SUM_NS: &str = "topsql_stmt_duration_sum_ns";
pub const METRIC_NAME_STMT_DURATION_COUNT: &str = "topsql_stmt_duration_count";

// TopRU related constants
pub const LABEL_USER: &str = "user";
pub const METRIC_NAME_TOTAL_RU: &str = "topru_total_ru";
pub const METRIC_NAME_EXEC_COUNT: &str = "topru_exec_count";
pub const METRIC_NAME_EXEC_DURATION: &str = "topru_exec_duration";

pub const KV_TAG_LABEL_ROW: &str = "row";
pub const KV_TAG_LABEL_INDEX: &str = "index";
pub const KV_TAG_LABEL_UNKNOWN: &str = "unknown";

// Log event field values
pub const SOURCE_TABLE_TIKV_TOPSQL: &str = "tikv_topsql";
pub const SOURCE_TABLE_TIKV_TOPREGION: &str = "tikv_topregion";
pub const SOURCE_TABLE_TIDB_TOPSQL: &str = "tidb_topsql";
pub const SOURCE_TABLE_TOPSQL_SQL_META: &str = "topsql_sql_meta";
pub const SOURCE_TABLE_TOPSQL_PLAN_META: &str = "topsql_plan_meta";
pub const SOURCE_TABLE_TOPRU: &str = "topsql_topru";
