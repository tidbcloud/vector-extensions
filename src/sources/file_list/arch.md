# File List Source Architecture

## Overview

The `file_list` source lists and filters files (or Delta Lake table paths) from multi-cloud object storage. **Paths for known data types are fixed in code** so users only specify `cluster_id`, `types` (multi-select), and time range—no need to know where files are stored.

## Core Features

1. **Known data types (paths in code)**: `raw_logs`, `slowlog`, `sql_statement`, `top_sql`, `conprof`—each has a fixed path convention; user supplies cluster_id, types, and time.
2. **Multi-Cloud Support**: AWS S3, GCP Cloud Storage, Azure Blob Storage, Aliyun OSS via `object_store`.
3. **Time Range Filtering**: By modification time and (for raw_logs) by hourly partition.
4. **Delta Lake discovery**: For slowlog/sql_statement/top_sql, emits Delta table root paths (not individual files).
5. **Legacy mode**: Explicit `prefix` + `pattern` when `types` is not set.

## Data Types and Path Conventions (fixed in code)

| Type | Description | Path (bucket-relative) |
|------|-------------|------------------------|
| **raw_logs** | Gzip-compressed raw logs | `diagnosis/data/{cluster_id}/merged-logs/{YYYYMMDDHH}/tidb/*.log` |
| **slowlog** | Delta Lake slowlog table | `deltalake/{project_id}/{uuid}/slowlogs/` (discovered) |
| **sql_statement** | Delta Lake sqlstatement table | `deltalake/{project_id}/{uuid}/sqlstatement/` (discovered) |
| **top_sql** | Delta Lake TopSQL per instance | `deltalake/org={project_id}/cluster={cluster_id}/type=topsql_tidb/instance=*` |
| **conprof** | Pprof compressed files | `0/{project_id}/{conprof_org_id}/{cluster_id}/profiles/*.log.gz` |

Example URLs (for reference):

- Raw log: `.../diagnosis/data/10324983984131567830/merged-logs/2026010804/tidb/db-*-tidb-0.log`
- Slowlog: `.../deltalake/1372813089209061633/019aedbc-.../slowlogs/_delta_log/_last_checkpoint`
- TopSQL: `.../deltalake/org=1372813089209061633/cluster=10324983984131567830/type=topsql_tidb/instance=db.tidb-0/...`
- Conprof: `.../0/1372813089209061633/1372813089454544954/10324983984131567830/profiles/1767830400-pd-cpu-....log.gz`

## Architecture

### Component Structure

```
file_list/
├── mod.rs                    # Config, SourceConfig, and build
├── checkpoint.rs             # Checkpoint load/save (completed prefix keys for OOM/restart recovery)
├── path_resolver.rs          # DataTypeKind enum and path resolution (cluster_id + types + time → list requests)
├── controller.rs             # Runs list (legacy or by-request) and emits events
├── file_lister.rs            # list_files_at, list_delta_table_paths, list_topsql_instance_paths
└── object_store_builder.rs   # Multi-cloud ObjectStore builder
```

### Data Flow

**List-only mode** (`emit_content = false`, default):

```
Cloud Storage (S3/GCS/Azure/OSS)
    ↓
ObjectStore (object_store crate)
    ↓
FileLister (filter by time & pattern)
    ↓
FileMetadata Events (file_path, size, last_modified, ...)
    ↓
SourceSender → Downstream
```

**Content mode** (`emit_content = true`): For sync/aggregation; full copy pipeline runs inside Vector.

```
Cloud Storage (S3/GCS/Azure/OSS)
    ↓
ObjectStore list + get
    ↓
FileLister (filter) → per file: get bytes → optional gzip decompress
    ↓
LogEvent (file_path, message = file content, ...)
    ↓
SourceSender → e.g. official aws_s3 sink (encoding=text/json, batch.max_bytes for chunking)
```

## Implementation Details

### Multi-Cloud Support via `object_store`

The source uses the `object_store` crate as a unified abstraction layer for all cloud providers:

- **AWS S3**: Uses `AmazonS3Builder` from `object_store::aws`
- **GCP Cloud Storage**: Uses `GoogleCloudStorageBuilder` from `object_store::gcp`
- **Azure Blob Storage**: Uses `MicrosoftAzureBuilder` from `object_store::azure`
- **Aliyun OSS**: Uses `AmazonS3Builder` with custom endpoint (S3-compatible API)

**Advantages:**
- Single unified API for all providers
- Automatic credential chain support
- Consistent error handling
- Type-safe implementation

### Pattern Matching

The source supports glob-style patterns with special placeholders:

- `*`: Matches any sequence of characters
- `?`: Matches any single character
- `{YYYYMMDDHH}`: Matches exactly 10 digits (timestamp format)

**Pattern Compilation:**
- Patterns are compiled to regex at initialization
- Special regex characters are escaped
- Placeholders are replaced with regex patterns
- Full path matching (anchored with `^` and `$`)

**Example Patterns:**
- `{YYYYMMDDHH}/*.log` → Matches files like `2026010804/tidb-0.log`
- `profiles/*-cpu-*.log.gz` → Matches files like `profiles/1767830400-pd-cpu-instance.log.gz`
- `*.parquet` → Matches all `.parquet` files

### Time Range Filtering

Files are filtered by their `last_modified` timestamp:

- **Inclusive Start**: Files with `last_modified >= time_range_start` are included
- **Inclusive End**: Files with `last_modified <= time_range_end` are included
- **No Range**: If no time range is specified, all files matching the pattern are included

**Implementation:**
- Uses `object_store::ObjectMeta::last_modified` (SystemTime)
- Converts to `DateTime<Utc>` for comparison
- Filtering happens during file listing iteration

### File Metadata Events

Each matching file emits a Vector LogEvent.

**List-only** (`emit_content = false`). With `emit_metadata = true` (default):
```json
{
  "file_path": "diagnosis/data/.../merged-logs/2026010804/tidb/db-xxx-tidb-0.log",
  "file_size": 1048576,
  "last_modified": "2026-01-08T04:00:00Z",
  "bucket": "o11y-prod-shared-us-east-1",
  "full_path": "diagnosis/data/.../merged-logs/2026010804/tidb/db-xxx-tidb-0.log",
  "@timestamp": "2026-01-08T10:00:00Z"
}
```

**Content mode** (`emit_content = true`): In addition to the above, adds `message` (file content; .gz is decompressed first). Downstream can use the official **aws_s3** sink (`encoding.codec = "text"` or `"json"`, `batch.max_bytes`) to aggregate and write back to S3.

## Configuration

### Recommended: By data types (paths fixed in code)

User only specifies cluster_id, types (multi-select), and time range. Paths are resolved in the source.

```toml
[sources.file_list]
type = "file_list"
endpoint = "s3://o11y-prod-shared-us-east-1"
cloud_provider = "aws"
cluster_id = "10324983984131567830"
project_id = "1372813089209061633"
# conprof_org_id = "1372813089454544954"  # optional, default = project_id
types = ["raw_logs", "conprof"]
start_time = "2026-01-08T00:00:00Z"
end_time = "2026-01-08T23:59:59Z"
max_keys = 10000
emit_metadata = true
```

- **raw_logs** requires `start_time` and `end_time` (hourly partitions).
- **slowlog**, **sql_statement**, **top_sql**, **conprof** require `project_id`.

### Legacy: Explicit prefix + pattern

When `types` is not set, use explicit `prefix` and optional `pattern`.

```toml
[sources.file_list]
type = "file_list"
endpoint = "s3://my-bucket"
cloud_provider = "aws"
prefix = "path/to/files/"
pattern = "{YYYYMMDDHH}/*.log"
time_range_start = "2026-01-08T00:00:00Z"
time_range_end = "2026-01-08T23:59:59Z"
max_keys = 10000
poll_interval_secs = 0
emit_metadata = true
```

### Configuration Fields

- **`endpoint`** (required): Cloud storage endpoint (e.g. `s3://bucket-name`).

- **`cloud_provider`** (optional, default: "aws"): `aws`, `gcp`, `azure`, `aliyun`.

- **`region`** (optional, AWS only): AWS region (e.g. `us-west-2`). When set, overrides `AWS_REGION` / `AWS_DEFAULT_REGION` for S3. Omit to use environment.

- **`cluster_id`** (required when `types` is set): Cluster ID; paths are built from this and `project_id` per data type.

- **`project_id`** (required for slowlog, sql_statement, top_sql, conprof when using `types`).

- **`conprof_org_id`** (optional): For conprof path segment; default = `project_id`. Path: `0/{project_id}/{conprof_org_id}/{cluster_id}/profiles/`.

- **`types`** (optional): List of data types: `raw_logs`, `slowlog`, `sql_statement`, `top_sql`, `conprof`. When set, paths are resolved in code; user does not set prefix/pattern.

- **`prefix`** (optional, legacy): Used only when `types` is not set.

- **`pattern`** (optional, legacy): Glob pattern when using explicit prefix.

- **`time_range_start`** / **`start_time`**: Start time (ISO 8601). Required for raw_logs when using `types`.

- **`time_range_end`** / **`end_time`**: End time (ISO 8601). Required for raw_logs when using `types`.

- **`data_dir`** (optional, default: `/tmp/vector-tasks/file_list_checkpoint`): Directory for checkpoint file. When using **data types mode** (e.g. `types = ["raw_logs"]`), completed units (prefixes) are recorded here so that after OOM or restart the job resumes from the next unit instead of from the beginning. Checkpoint file name: `file_list_{endpoint_safe}.json`. Legacy (prefix/pattern) mode does not use checkpoint.

- **`max_keys`** (optional, default: 1000): Maximum number of files to return

- **`poll_interval_secs`** (optional, default: 0): Polling interval in seconds
  - `0` = one-time list (exit after first listing)
  - `> 0` = continuous polling mode

- **`emit_metadata`** (optional, default: true): Whether to emit full metadata

- **`emit_content`** (optional, default: false): When true, for each listed **file** (not Delta table paths), download from object store, optionally decompress .gz, and set event `message` to the content. Enables full sync/aggregation in Vector (e.g. file_list → content_to_s3).

- **`emit_per_line`** (optional, default: false): With `emit_content`, controls how file content is read. **`true`**: always stream by line (one event per line, parsed fields; bounded memory, slower). **`false`**: whole file in one event (fast, higher memory for large files). **`"auto"`**: stream only when file size > `stream_file_above_bytes`, otherwise whole file (small files fast, large files bounded memory). See [Line parsing rules](#line-parsing-rules-emit_per_line) below.

- **`stream_file_above_bytes`** (optional, default: 52428800 = 50 MiB): When `emit_per_line = "auto"`, files larger than this (bytes) use streaming; smaller files use whole-file read. Ignored when `emit_per_line` is `true` or `false`.

- **`line_parse_regexes`** (optional): List of regex strings for **custom** per-line parsing. When non-empty, **only** these regexes are used (built-in Python/HTTP rules are skipped). Each regex must contain at least one **named capture group** `(?P<name>...)`; capture names become event field names. Tried in order; first match wins; `line_type` is set to `custom`, and `message` is always the raw line. Unmatched lines get `line_type=raw`, `message` only. Example: `["^(?P<ts>\\d{4}-\\d{2}-\\d{2}) (?P<level>\\w+): (?P<msg>.*)$"]`.

- **`decompress_gzip`** (optional, default: true): When `emit_content` is true, decompress before emitting if either (1) path ends with `.gz` or `.log.gz`, or (2) content starts with gzip magic bytes (`1f 8b`), so misnamed or extension-less gzip data is still decompressed.

- **`max_content_buffer_bytes`** (optional): When using streaming (`emit_content` + `emit_per_line`), when to flush. **When unset or 0**: flush after each 16 MiB read chunk (minimal memory; output object size is entirely controlled by the sink’s `batch.max_bytes` / `timeout_secs`). When set (e.g. 524288000 = 500 MiB): flush when buffered content reaches that size. Content is streamed (object_store `into_stream` + async GzipDecoder).

- **`stream_concurrency`** (optional, default: 1): When using streaming (`emit_content` + `emit_per_line`), max number of files to process **in parallel**. 1 = sequential. Set to 2–8 to speed up when many small/medium files; a single batching task consumes events from a channel and flushes by `max_content_buffer_bytes` (if > 0) or after each chunk / at end of file.

- **`flush_after_each_file`** (optional, default: true): When true, the source also flushes after **each file**. When false, flushing is only by `max_content_buffer_bytes` (if set) or after each 16 MiB chunk (if unset/0), so the sink can accumulate up to its `batch.max_bytes` and produce larger objects.

- **`raw_log_components`** (optional, for raw_logs only): Component subdirs under `merged-logs/{YYYYMMDDHH}/` (e.g. `tidb`, `loki`, `operator`). **When not set = discover at runtime**: for each hour prefix we list with delimiter to get immediate subdir names (all components that actually exist in the bucket). Set explicitly to sync only a subset.

### Checkpoint (OOM / restart recovery)

When `data_dir` is set and the source runs in **data types mode** (e.g. `types = ["raw_logs"]`), progress is persisted to a JSON checkpoint file under `data_dir`. Each completed "unit" (one prefix for FileList/RawLogs, or one delta/topsql list request) is recorded. After an OOM kill or restart, the source loads the checkpoint and **skips** any unit whose key is already in `completed_keys`, then continues with the next. So the job does not start from the beginning. Checkpoint is saved after each unit is fully processed. On error, the checkpoint is marked `status: "error"` but completed keys are kept, so the next run still skips completed work. Legacy mode (single `prefix` + `pattern`) does not use checkpoint.

### Memory and process RSS (why RSS can exceed max_content_buffer_bytes)

When `max_content_buffer_bytes` is **unset or 0**, the source flushes after each 16 MiB read chunk, so source-side memory stays minimal (~16 MiB + decoder buffer per stream). When it is **set** (e.g. 500 MiB), it only caps the **source’s in-memory batch** before it is sent downstream. It does **not** cap total process memory. The process RSS can be several times larger because:

1. **Source → Sink pipeline**: After a flush, the batch is handed to Vector’s topology (channel + sink). Until the sink consumes it, that batch still lives in memory. So you can have: source batch (up to `max_content_buffer_bytes`) + one or more batches in the topology channel + the batch the sink is currently processing.
2. **Parallel stream readers**: With `stream_concurrency = 4`, each of the 4 streams uses a 16 MiB read chunk plus decoder buffers. That adds on the order of tens to ~100 MiB.
3. **Event overhead**: `content_bytes` in logs is the sum of line lengths (message). Each event also has metadata (e.g. `file_path`, `component`, `hour_partition`, `file_size`). Actual memory per event is often 1.1–1.3× the message size.
4. **Sink behavior**: The official `aws_s3` sink may hold a full batch in memory before writing to its buffer (disk or memory). So another ~`max_content_buffer_bytes` can be held in the sink when the source sends a 500 MiB batch.

**Example**: With `max_content_buffer_bytes = 524288000` (500 MiB), `stream_concurrency = 4`, and `flush_after_each_file = false`, you can easily see: 500 (source) + 500 (in topology / sink) + 500 (sink processing) + ~100 (stream readers) + overhead → **~1.5–3.5 GB** RSS. This is **not a leak**; it is multiple stages each holding a batch.

**To reduce memory**:

- Omit `max_content_buffer_bytes` (or set to 0): flush after each 16 MiB read chunk so source holds at most ~16 MiB + decoder buffer.
- Set `flush_after_each_file = true` for per-file batches (smaller, released sooner).
- Reduce `stream_concurrency` (e.g. 2) to cut reader buffers and parallel in-flight data.

### Line parsing rules (emit_per_line)

When `emit_per_line = true` or `"auto"` and the file is streamed:

- **If `line_parse_regexes` is set (non-empty)**: Only these regexes are used, in order; each must have **named captures** `(?P<name>...)` (capture names become field names). Match → `line_type=custom`; no match → `line_type=raw`, `message` only. Built-in Python/HTTP rules are not used.
- **If `line_parse_regexes` is not set**: The two built-in rules below are used.

| Rule | Example | Regex (brief) | Output fields |
|------|---------|----------------|---------------|
| **Python logging** | `2026-02-04 11:40:12,114 [slowlogconverter] [INFO] [Memory] message body` | `^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) \[([^\]]+)\] \[([^\]]+)\]\s*(?:\[([^\]]*)\]\s*)?(.*)$` | `line_type=python_logging`, `log_timestamp`, `logger`, `level`, `tag`, `message_body`, `message` (raw line) |
| **HTTP access** | `10.1.103.150 - - [04/Feb/2026 11:40:17] "GET /metrics HTTP/1.1" 200 -` | `^(\S+) - - \[([^\]]+)\] "(\S+) ([^"]*) (\S+)" (\d+) (\S*).*$` | `line_type=http_access`, `client_ip`, `request_date`, `method`, `path`, `protocol`, `status`, `response_size`, `message` (raw line) |
| **No match** | Any other line | — | `line_type=raw`, `message` (raw line) |

Every event has `message` (raw line). For custom regexes, use **JSON** output to keep all capture fields; for CSV, list column names in sink `encoding.csv.fields` (including custom names).

### Event fields (for sink key_prefix / template)

In aws_s3 and similar sinks, use `{{ field_name }}` in `key_prefix` to reference event fields. Fields available on file_list events:

**raw_logs file content events (one event per log line)**

| Field | Description | When present |
|-------|-------------|--------------|
| `component` | Component name, e.g. tidb / tikv / pd / tiflash / ticdc | raw_logs, parsed from merged-logs/{hour}/{component}/ |
| `hour_partition` | Hour partition, 10 digits e.g. 2026020411 | Same as above |
| `file_path` | Source file path in bucket | Always |
| `data_type` | Always `"file"` | Always |
| `message` | Raw line content | Always (when emit_content and per-line) |
| `line_type` | Line parse type: `raw` / `python_logging` / `http_access` / `custom` | When line parsing is used |
| `@timestamp` | Event time (RFC3339) | Always |
| `file_size` | File size in bytes | When `emit_metadata = true` |
| `last_modified` | File last modified time (RFC3339) | When `emit_metadata = true` |
| `bucket` | Bucket name | When `emit_metadata = true` |
| `full_path` | Full path (may match file_path) | When `emit_metadata = true` |

**Built-in line parse fields (by line_type)**

- `python_logging`: `log_timestamp`, `logger`, `level`, `tag`, `message_body`
- `http_access`: `client_ip`, `request_date`, `method`, `path`, `protocol`, `status`, `response_size`
- Custom `line_parse_regexes`: capture name `(?P<name>...)` becomes the field name

**Delta / TopSQL list events (path only, no content)**

| Field | Description |
|-------|-------------|
| `file_path` | Table or instance path |
| `data_type` | `"delta_table"` |
| `table_subdir` | Table subdir name (e.g. slowlog / topsql) |
| `@timestamp` | Event time |

**Legacy mode (prefix + pattern, not raw_logs)**

No `component` / `hour_partition`; only `file_path`, `data_type`, `@timestamp`, and optionally `file_size`, `last_modified`, `bucket`, `full_path` when `emit_metadata = true`.

## Usage Examples

### Example 1: Raw logs + Conprof (types-based, paths in code)

```toml
[sources.o11y_files]
type = "file_list"
endpoint = "s3://o11y-prod-shared-us-east-1"
cloud_provider = "aws"
cluster_id = "10324983984131567830"
project_id = "1372813089209061633"
conprof_org_id = "1372813089454544954"
types = ["raw_logs", "conprof"]
start_time = "2026-01-08T00:00:00Z"
end_time = "2026-01-08T23:59:59Z"
max_keys = 10000
```

### Example 2: Slowlog + TopSQL (Delta Lake table paths)

```toml
[sources.delta_tables]
type = "file_list"
endpoint = "s3://o11y-prod-shared-us-east-1"
cloud_provider = "aws"
cluster_id = "10324983984131567830"
project_id = "1372813089209061633"
types = ["slowlog", "top_sql"]
start_time = "2026-01-08T00:00:00Z"
end_time = "2026-01-08T23:59:59Z"
```

### Example 3: Sync logs (download + decompress + write to local mysql)

Full pipeline inside Vector: file_list fetches and decompresses, writes to local MySQL.

```toml
[api]
enabled = true
address = "127.0.0.1:0"

[sources.file_list]
type = "file_list"
endpoint = "s3://o11y-prod-shared-us-west-2-staging"
cloud_provider = "aws"
max_keys = 500
poll_interval_secs = 0
emit_metadata = true
emit_content = true
emit_per_line = true
decompress_gzip = true
line_parse_regexes = [ "level=(?P<level>\\S+)\\s+ts=(?P<log_timestamp>[^\\s]+)\\s+caller=(?P<logger>[^\\s]+)\\s+msg=\"(?P<message_body>[^\"]*)\"",]
region = "us-west-2"
cluster_id = "o11y"
types = [ "raw_logs",]
start_time = "2026-02-04T11:00:00Z"
end_time = "2026-02-04T11:15:00Z"
raw_log_components = [ "loki",]

[sinks.tidb_sink]
type = "tidb"
inputs = [ "file_list",]
connection_string = "mysql://root:root@localhost:3306/testdb"
table = "parsed_logs"
batch_size = 1000
max_connections = 10
connection_timeout = 30
```

### Example 4: Full pipeline (raw_logs with components → S3 by component/hour)

Full example: API enabled, file_list fetches and decompresses by component, aws_s3 uses `key_prefix` template `{{ component }}/{{ hour_partition }}/` for output.

```toml
[api]
enabled = true
address = "127.0.0.1:0"

[sources.file_list]
type = "file_list"
endpoint = "s3://o11y-prod-shared-us-west-2-staging"
cloud_provider = "aws"
max_keys = 10000
poll_interval_secs = 0
emit_metadata = true
emit_content = true
decompress_gzip = true
region = "us-west-2"
cluster_id = "o11y"
types = ["raw_logs"]
start_time = "2026-02-04T11:00:00Z"
end_time = "2026-02-04T13:59:59Z"
raw_log_components = ["loki", "operator", "o11ydiagnosis-deltalake"]

[sinks.to_s3]
type = "aws_s3"
inputs = ["file_list"]
bucket = "o11y-dev-shared-us-west-2"
key_prefix = "leotest/{{ component }}/{{ hour_partition }}/"
compression = "gzip"
region = "us-west-2"

[sinks.to_s3.encoding]
codec = "text"

[sinks.to_s3.batch]
max_bytes = 33554432
timeout_secs = 10
```

The demo sync-logs API uses **output_format** to control S3 write encoding (same as official aws_s3 encoding.codec): `text` (default), `json`, `csv`, `logfmt`, `raw_message`, `syslog`, `gelf`; `dest_bucket` and `dest_prefix` are always required. Formats that need extra schema (avro/cef/protobuf) are not supported; parquet is not supported by the official sink.

- **To keep maximum information** (e.g. multi-line/mixed logs like o11ydiagnosis-deltalake): use **json**. Each event has full `message` (raw log content) and metadata such as `file_path`, `component`, `hour_partition`, `file_size`, `last_modified`, `@timestamp` for downstream query and parsing.

### Example 5: Same file_list output as CSV to local file

Use the official **file** sink with `encoding.codec = "csv"` to write each file_list event as one CSV row; set column order via `encoding.csv.fields` (must match file_list event fields).

```toml
[api]
enabled = true
address = "127.0.0.1:0"

[sources.file_list]
type = "file_list"
endpoint = "s3://o11y-prod-shared-us-west-2-staging"
cloud_provider = "aws"
max_keys = 10000
poll_interval_secs = 0
emit_metadata = true
emit_content = true
decompress_gzip = true
region = "us-west-2"
cluster_id = "o11y"
types = ["raw_logs"]
start_time = "2026-02-04T11:00:00Z"
end_time = "2026-02-04T13:59:59Z"
raw_log_components = ["loki", "operator", "o11ydiagnosis-deltalake"]

[sinks.to_csv]
type = "file"
inputs = ["file_list"]
path = "/tmp/file_list-%Y-%m-%d.csv"

[sinks.to_csv.encoding]
codec = "csv"

# Column order matches file_list event fields; missing field outputs empty string
[sinks.to_csv.encoding.csv]
fields = ["file_path", "data_type", "hour_partition", "component", "file_size", "last_modified", "bucket", "full_path", "@timestamp", "message"]
```

Notes:

- **path**: Output file path; supports time template (e.g. `%Y-%m-%d`); multiple files are split by time/template.
- **encoding.csv.fields**: CSV column order; if an event is missing a field, that column is empty. `message` is file content (when `emit_content = true`) and can be large; omit `"message"` if you only need metadata.
- For list-only (no content), set `emit_content = false` and remove `"message"` from `fields`.

## Multi-Cloud Configuration

### AWS S3

```toml
endpoint = "s3://my-bucket"
cloud_provider = "aws"
```

**Credentials:**
- Environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`
- IAM Role (EC2/ECS/Lambda)
- AWS Profile
- Region: `AWS_REGION` environment variable

### GCP Cloud Storage

```toml
endpoint = "gs://my-bucket"
cloud_provider = "gcp"
```

**Credentials:**
- Service Account Key: `GOOGLE_APPLICATION_CREDENTIALS` environment variable
- Application Default Credentials (ADC)
- GCE/Cloud Run metadata service

### Azure Blob Storage

```toml
endpoint = "az://account-name/container-name"
cloud_provider = "azure"
```

**Credentials:**
- Environment variables: `AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_KEY`
- Connection String: `AZURE_STORAGE_CONNECTION_STRING`
- Managed Identity

### Aliyun OSS

```toml
endpoint = "oss://my-bucket"
cloud_provider = "aliyun"
```

**Credentials:**
- Environment variables:
  - `OSS_ENDPOINT`: OSS endpoint URL (required)
  - `OSS_ACCESS_KEY_ID` or `AWS_ACCESS_KEY_ID`
  - `OSS_ACCESS_KEY_SECRET` or `AWS_SECRET_ACCESS_KEY`

## Metrics

The source exposes the following Prometheus metrics:

- **`file_list_files_found_total`** (Counter): Total number of files found matching criteria

## Limitations and Notes

1. **Pattern Matching**: Currently uses regex-based pattern matching. Complex patterns may have performance implications for large file lists.

2. **Time Range**: Filtering by time range requires iterating through all files in the prefix, which may be slow for very large prefixes.

3. **Pagination**: The `max_keys` parameter limits results but doesn't provide continuation tokens. For very large result sets, consider using multiple requests with different prefixes.

4. **One-time vs Polling**: 
   - One-time mode (`poll_interval_secs = 0`): Lists files once and exits
   - Polling mode (`poll_interval_secs > 0`): Continuously polls for new files

5. **File content**: With `emit_content = true`, the source downloads each listed file (FileList only), optionally decompresses .gz, and sets event `message` to the content. Use with the **official aws_s3 sink** (`encoding.codec = "text"` or `"json"`, `batch.max_bytes`) to aggregate and write to S3. Delta table and TopSQL list requests still emit only paths.

6. **Streaming for large files**: When `emit_content` and `emit_per_line` are both true, the source uses **streaming** (object_store `into_stream()` + async GzipDecoder) so the full file is never loaded into memory. Events are sent (1) when buffered content reaches `max_content_buffer_bytes` (default 500 MiB) within a file, and (2) **after each file** so the batch is never carried across many files. That avoids both waiting for 500MB before the first write (e.g. 12×40MB files) and high memory (e.g. 900MB from batch + overhead). Single-file memory is bounded by roughly one file's size + 16 MiB read chunk + decoder buffers.

7. **Parallel file streaming**: When `stream_concurrency` > 1, multiple files are streamed in parallel (up to `stream_concurrency` at a time). Each file sends events to a shared channel; one batching task consumes and flushes by `max_content_buffer_bytes` or at end of file. This speeds up directories with many files without changing memory semantics.

## Future Enhancements

1. **Checkpoint Support**: Track which files have been processed to avoid duplicates in polling mode
2. **Parallel Listing**: Support for parallel file listing across multiple prefixes
3. **Advanced Pattern Matching**: Support for more complex patterns (regex, multiple placeholders)
4. **File Content Preview**: Option to read first N bytes of each file for inspection
5. **Incremental Listing**: Track last listing time and only return new/modified files
