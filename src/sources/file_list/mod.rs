use std::path::PathBuf;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use vector::config::{GenerateConfig, SourceConfig, SourceContext};
use vector_lib::{
    config::{DataType, LogNamespace, SourceOutput},
    configurable::configurable_component,
    source::Source,
};

use crate::sources::file_list::checkpoint::Checkpoint as FileListCheckpoint;
use crate::sources::file_list::controller::Controller;
use crate::sources::file_list::path_resolver::resolve_requests;

/// When to use per-line streaming vs whole-file read. `Auto` = stream only when file size > `stream_file_above_bytes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EmitPerLineMode {
    /// Whole file in one event (fast, higher memory for large files).
    #[default]
    Off,
    /// Always stream by line (bounded memory, slower).
    On,
    /// Stream only if file size > stream_file_above_bytes; otherwise whole file.
    Auto,
}

impl Serialize for EmitPerLineMode {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            EmitPerLineMode::Off => s.serialize_bool(false),
            EmitPerLineMode::On => s.serialize_bool(true),
            EmitPerLineMode::Auto => s.serialize_str("auto"),
        }
    }
}

fn default_emit_per_line_str() -> String {
    "false".to_string()
}

fn deserialize_emit_per_line_str<'de, D: serde::Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Raw {
        B(bool),
        S(String),
    }
    let raw = Raw::deserialize(d)?;
    Ok(match raw {
        Raw::B(true) => "true".to_string(),
        Raw::B(false) => "false".to_string(),
        Raw::S(s) if s.eq_ignore_ascii_case("auto") => "auto".to_string(),
        Raw::S(s) => {
            return Err(serde::de::Error::custom(format!(
                "emit_per_line must be true, false, or \"auto\", got \"{}\"",
                s
            )));
        }
    })
}

/// Parse config string to mode. Used when building Controller.
pub fn parse_emit_per_line(s: &str) -> EmitPerLineMode {
    match s.trim().to_lowercase().as_str() {
        "true" => EmitPerLineMode::On,
        "auto" => EmitPerLineMode::Auto,
        _ => EmitPerLineMode::Off,
    }
}

mod checkpoint;
mod controller;
mod file_lister;
mod line_parser;
mod object_store_builder;
mod path_resolver;

// Ensure the source is registered with typetag
#[allow(dead_code)]
fn _ensure_registered() {
    // The #[typetag::serde] attribute on the impl will register this source
}

/// Configuration for the file_list source.
/// Either use known data types (cluster_id + types + time) or explicit prefix/pattern.
#[configurable_component(source("file_list"))]
#[derive(Debug, Clone)]
pub struct FileListConfig {
    /// Cloud storage endpoint (e.g., s3://bucket, gs://bucket, az://account/container, oss://bucket)
    pub endpoint: String,

    /// Directory for checkpoint file (resume after OOM/restart). When set, completed prefixes are recorded so restart skips them. Default: /tmp/vector-tasks/file_list_checkpoint
    #[serde(default = "default_file_list_data_dir")]
    pub data_dir: PathBuf,

    /// Cloud provider: aws, gcp, azure, aliyun.
    #[serde(default = "default_cloud_provider")]
    pub cloud_provider: String,

    /// AWS region (e.g. us-west-2). Optional; when set, overrides AWS_REGION/AWS_DEFAULT_REGION for S3.
    pub region: Option<String>,

    /// Cluster ID. Required when `types` is set; paths are resolved in code per data type.
    pub cluster_id: Option<String>,
    /// Project ID. Required for slowlog, sql_statement, top_sql, conprof when using `types`.
    pub project_id: Option<String>,
    /// Optional org id for conprof path (default: project_id). Path: 0/{project_id}/{conprof_org_id}/{cluster_id}/profiles/
    pub conprof_org_id: Option<String>,

    /// Data types to list (paths are fixed in code). Values: raw_logs, slowlog, sql_statement, top_sql, conprof.
    pub types: Option<Vec<String>>,
    /// For raw_logs only: component subdirs under merged-logs/{YYYYMMDDHH}/ (e.g. tidb, loki, operator). Default when unset: ["tidb"].
    pub raw_log_components: Option<Vec<String>>,

    /// Explicit prefix (legacy / when types is not set). If set with pattern, used as single prefix list.
    pub prefix: Option<String>,
    /// Explicit pattern (legacy). Supports {YYYYMMDDHH}, *, ?.
    pub pattern: Option<String>,

    /// Start time for filtering (ISO 8601). Alias: start_time. Required for raw_logs when using types.
    #[serde(alias = "start_time")]
    pub time_range_start: Option<String>,
    /// End time for filtering (ISO 8601). Alias: end_time. Required for raw_logs when using types.
    #[serde(alias = "end_time")]
    pub time_range_end: Option<String>,

    /// Maximum number of keys to return per list.
    #[serde(default = "default_max_keys")]
    pub max_keys: usize,
    /// Poll interval in seconds (0 = one-time list).
    #[serde(default = "default_poll_interval_secs")]
    pub poll_interval_secs: u64,
    /// Whether to emit full file metadata.
    #[serde(default = "default_emit_metadata")]
    pub emit_metadata: bool,

    /// When true, download each listed file (FileList only), decompress if .gz, and emit content in event "message".
    /// Delta table / TopSQL list requests are unchanged (path only). Enables sync/aggregation in downstream sinks.
    #[serde(default)]
    pub emit_content: bool,

    /// With emit_content: true = always stream by line; false = whole file per event; "auto" = stream only when file size > stream_file_above_bytes (small files whole-file for speed).
    #[serde(default = "default_emit_per_line_str", deserialize_with = "deserialize_emit_per_line_str")]
    pub emit_per_line: String,

    /// When emit_per_line = "auto", files larger than this (bytes) use streaming; smaller use whole-file. Default 50 MiB.
    #[serde(default = "default_stream_file_above_bytes")]
    pub stream_file_above_bytes: usize,

    /// Optional list of regexes for per-line parsing. Each regex must use named capture groups `(?P<name>...)`; group names become event field names.
    /// Tried in order; first match wins; unmatched lines get line_type=raw. When non-empty, built-in (python/http) rules are not used.
    #[serde(default)]
    pub line_parse_regexes: Option<Vec<String>>,

    /// When emit_content is true, decompress gzip (.gz) before emitting. Ignored when emit_content is false.
    #[serde(default = "default_decompress_gzip")]
    pub decompress_gzip: bool,

    /// When using streaming (emit_content + emit_per_line), flush when buffered content reaches this many bytes. When unset or 0: flush after each 16 MiB read chunk (minimal memory). When set (e.g. 524288000 = 500 MiB): flush when batch reaches that size.
    #[serde(default)]
    pub max_content_buffer_bytes: Option<usize>,

    /// When using streaming (emit_content + emit_per_line), max number of files to process in parallel. Default 1 (sequential). Set to 2–8 to speed up when many small/medium files.
    #[serde(default = "default_stream_concurrency")]
    pub stream_concurrency: usize,

    /// When true (default), flush event batch after each file so sink gets one batch per file (e.g. ~15MB per object). When false, only flush when batch reaches max_content_buffer_bytes so sink can accumulate up to its batch.max_bytes (e.g. 50MB) and write larger objects.
    #[serde(default = "default_flush_after_each_file")]
    pub flush_after_each_file: bool,
}

fn default_cloud_provider() -> String {
    "aws".to_string()
}

fn default_file_list_data_dir() -> PathBuf {
    PathBuf::from("/tmp/vector-tasks/file_list_checkpoint")
}

fn default_max_keys() -> usize {
    1000
}

fn default_poll_interval_secs() -> u64 {
    0 // Default to one-time list
}

fn default_emit_metadata() -> bool {
    true
}

fn default_decompress_gzip() -> bool {
    true
}

fn default_stream_concurrency() -> usize {
    1
}

fn default_flush_after_each_file() -> bool {
    true
}

fn default_stream_file_above_bytes() -> usize {
    50 * 1024 * 1024 // 50 MiB
}

fn parse_data_type_kind(s: &str) -> Option<path_resolver::DataTypeKind> {
    match s.trim().to_lowercase().as_str() {
        "raw_logs" => Some(path_resolver::DataTypeKind::RawLogs),
        "slowlog" => Some(path_resolver::DataTypeKind::Slowlog),
        "sql_statement" => Some(path_resolver::DataTypeKind::SqlStatement),
        "top_sql" => Some(path_resolver::DataTypeKind::TopSql),
        "conprof" => Some(path_resolver::DataTypeKind::Conprof),
        _ => None,
    }
}

impl FileListConfig {
    /// When using explicit prefix (no types), return it.
    fn effective_prefix(&self) -> vector::Result<String> {
        self.prefix
            .as_ref()
            .filter(|p| !p.is_empty())
            .cloned()
            .ok_or_else(|| {
                "file_list: when 'types' is not set, 'prefix' must be set".into()
            })
    }
}

impl GenerateConfig for FileListConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            endpoint: "s3://my-bucket".to_string(),
            data_dir: default_file_list_data_dir(),
            cloud_provider: default_cloud_provider(),
            region: Some("us-west-2".to_string()),
            cluster_id: Some("10324983984131567830".to_string()),
            project_id: Some("1372813089209061633".to_string()),
            conprof_org_id: None,
            types: Some(vec!["raw_logs".to_string(), "conprof".to_string()]),
            raw_log_components: None,
            prefix: None,
            pattern: None,
            time_range_start: Some("2026-01-08T00:00:00Z".to_string()),
            time_range_end: Some("2026-01-08T23:59:59Z".to_string()),
            max_keys: default_max_keys(),
            poll_interval_secs: default_poll_interval_secs(),
            emit_metadata: default_emit_metadata(),
            emit_content: false,
            emit_per_line: "false".to_string(),
            stream_file_above_bytes: default_stream_file_above_bytes(),
            line_parse_regexes: None,
            decompress_gzip: default_decompress_gzip(),
            max_content_buffer_bytes: None,
            stream_concurrency: default_stream_concurrency(),
            flush_after_each_file: default_flush_after_each_file(),
        })
        .unwrap()
    }
}

/// Effective buffer cap: 0 = flush after each 16 MiB chunk (minimal memory); else flush when batch reaches this many bytes.
fn effective_max_content_buffer_bytes(config: &FileListConfig) -> usize {
    config.max_content_buffer_bytes.unwrap_or(0)
}

#[async_trait::async_trait]
#[typetag::serde(name = "file_list")]
impl SourceConfig for FileListConfig {
    async fn build(&self, cx: SourceContext) -> vector::Result<Source> {
        // Parse time range
        let time_range_start = self
            .time_range_start
            .as_ref()
            .map(|s| {
                DateTime::parse_from_rfc3339(s)
                    .map(|dt| dt.with_timezone(&Utc))
                    .map_err(|e| format!("Invalid time_range_start format: {}", e))
            })
            .transpose()?;

        let time_range_end = self
            .time_range_end
            .as_ref()
            .map(|s| {
                DateTime::parse_from_rfc3339(s)
                    .map(|dt| dt.with_timezone(&Utc))
                    .map_err(|e| format!("Invalid time_range_end format: {}", e))
            })
            .transpose()?;

        // Validate time range
        if let (Some(start), Some(end)) = (time_range_start, time_range_end) {
            if start > end {
                return Err("time_range_start must be before time_range_end".into());
            }
        }

        let poll_interval = if self.poll_interval_secs > 0 {
            Some(Duration::from_secs(self.poll_interval_secs))
        } else {
            None
        };

        let list_requests = if self.types.as_ref().map(|t| !t.is_empty()).unwrap_or(false) {
            let cluster_id = self
                .cluster_id
                .as_deref()
                .filter(|s| !s.is_empty())
                .ok_or("file_list: 'types' requires 'cluster_id'")?;
            let type_kinds: Vec<path_resolver::DataTypeKind> = self
                .types
                .as_ref()
                .unwrap()
                .iter()
                .filter_map(|s| parse_data_type_kind(s))
                .collect();
            if type_kinds.is_empty() {
                return Err("file_list: 'types' must contain at least one of: raw_logs, slowlog, sql_statement, top_sql, conprof".into());
            }
            let requests = resolve_requests(
                cluster_id,
                self.project_id.as_deref(),
                self.conprof_org_id.as_deref(),
                &type_kinds,
                time_range_start,
                time_range_end,
                self.raw_log_components.as_deref(),
            )?;
            Some(requests)
        } else {
            let prefix = self.effective_prefix()?;
            let custom_line_regexes = if matches!(parse_emit_per_line(&self.emit_per_line), EmitPerLineMode::On | EmitPerLineMode::Auto) {
                self.line_parse_regexes
                    .as_ref()
                    .filter(|v| !v.is_empty())
                    .map(|v| line_parser::compile_line_parse_regexes(v))
                    .transpose()?
            } else {
                None
            };
            let controller = Controller::new_legacy(
                self.endpoint.clone(),
                self.cloud_provider.clone(),
                self.region.clone(),
                prefix,
                self.pattern.clone(),
                time_range_start,
                time_range_end,
                self.max_keys,
                poll_interval,
                self.emit_metadata,
                self.emit_content,
                parse_emit_per_line(&self.emit_per_line),
                self.stream_file_above_bytes,
                custom_line_regexes,
                self.decompress_gzip,
                effective_max_content_buffer_bytes(self),
                self.stream_concurrency,
                self.flush_after_each_file,
                cx.out,
                cx.shutdown,
            )?;
            return Ok(Box::pin(async move {
                controller.run_legacy().await
            }));
        };

        let custom_line_regexes = if matches!(parse_emit_per_line(&self.emit_per_line), EmitPerLineMode::On | EmitPerLineMode::Auto) {
            self.line_parse_regexes
                .as_ref()
                .filter(|v| !v.is_empty())
                .map(|v| line_parser::compile_line_parse_regexes(v))
                .transpose()?
        } else {
            None
        };

        let checkpoint_path = FileListCheckpoint::get_path(&self.data_dir, &self.endpoint);
        let checkpoint = std::sync::Arc::new(tokio::sync::Mutex::new(FileListCheckpoint::load(
            &checkpoint_path,
        )?));

        let controller = Controller::new_with_requests(
            self.endpoint.clone(),
            self.cloud_provider.clone(),
            self.region.clone(),
            list_requests.unwrap(),
            time_range_start,
            time_range_end,
            self.max_keys,
            poll_interval,
            self.emit_metadata,
            self.emit_content,
            parse_emit_per_line(&self.emit_per_line),
            self.stream_file_above_bytes,
            custom_line_regexes,
            self.decompress_gzip,
            effective_max_content_buffer_bytes(self),
            self.stream_concurrency,
            self.flush_after_each_file,
            checkpoint_path,
            checkpoint,
            cx.out,
            cx.shutdown,
        )?;

        Ok(Box::pin(async move { controller.run().await }))
    }

    fn outputs(&self, _global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        vec![SourceOutput {
            port: None,
            ty: DataType::Log,
            schema_definition: None,
        }]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_config() {
        let config = FileListConfig::generate_config();
        assert!(config.is_table());
    }

    #[test]
    fn test_effective_prefix_with_prefix() {
        let config = FileListConfig {
            endpoint: "s3://bucket/path".to_string(),
            data_dir: default_file_list_data_dir(),
            cloud_provider: default_cloud_provider(),
            region: None,
            cluster_id: None,
            project_id: None,
            conprof_org_id: None,
            types: None,
            raw_log_components: None,
            prefix: Some("path/".to_string()),
            pattern: None,
            time_range_start: None,
            time_range_end: None,
            max_keys: default_max_keys(),
            poll_interval_secs: default_poll_interval_secs(),
            emit_metadata: default_emit_metadata(),
            emit_content: false,
            emit_per_line: "false".to_string(),
            stream_file_above_bytes: default_stream_file_above_bytes(),
            line_parse_regexes: None,
            decompress_gzip: default_decompress_gzip(),
            max_content_buffer_bytes: None,
            stream_concurrency: default_stream_concurrency(),
            flush_after_each_file: default_flush_after_each_file(),
        };
        assert_eq!(config.cloud_provider, "aws");
        assert_eq!(config.effective_prefix().unwrap(), "path/");
    }

    #[test]
    fn test_effective_prefix_requires_prefix_when_no_types() {
        let config = FileListConfig {
            endpoint: "s3://bucket".to_string(),
            data_dir: default_file_list_data_dir(),
            cloud_provider: "aws".to_string(),
            region: None,
            cluster_id: None,
            project_id: None,
            conprof_org_id: None,
            types: None,
            raw_log_components: None,
            prefix: None,
            pattern: None,
            time_range_start: None,
            time_range_end: None,
            max_keys: default_max_keys(),
            poll_interval_secs: default_poll_interval_secs(),
            emit_metadata: default_emit_metadata(),
            emit_content: false,
            emit_per_line: "false".to_string(),
            stream_file_above_bytes: default_stream_file_above_bytes(),
            line_parse_regexes: None,
            decompress_gzip: default_decompress_gzip(),
            max_content_buffer_bytes: None,
            stream_concurrency: default_stream_concurrency(),
            flush_after_each_file: default_flush_after_each_file(),
        };
        assert!(config.effective_prefix().is_err());
    }
}
