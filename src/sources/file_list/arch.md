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

**Content mode** (`emit_content = true`): 用于同步/聚合场景，拷贝全流程在 Vector 内完成。

```
Cloud Storage (S3/GCS/Azure/OSS)
    ↓
ObjectStore list + get
    ↓
FileLister (filter) → per file: get bytes → optional gzip decompress
    ↓
LogEvent (file_path, message = file content, ...)
    ↓
SourceSender → e.g. 官方 aws_s3 sink（encoding=text/json，batch.max_bytes 分片）
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

**Content mode** (`emit_content = true`): 除上述字段外增加 `message`，为文件内容（若为 .gz 则先解压再填入）。下游用官方 **aws_s3** sink（`encoding.codec = "text"` 或 `"json"`，`batch.max_bytes`）即可按大小聚合写回 S3。

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

- **`max_keys`** (optional, default: 1000): Maximum number of files to return

- **`poll_interval_secs`** (optional, default: 0): Polling interval in seconds
  - `0` = one-time list (exit after first listing)
  - `> 0` = continuous polling mode

- **`emit_metadata`** (optional, default: true): Whether to emit full metadata

- **`emit_content`** (optional, default: false): When true, for each listed **file** (not Delta table paths), download from object store, optionally decompress .gz, and set event `message` to the content. Enables full sync/aggregation in Vector (e.g. file_list → content_to_s3).

- **`decompress_gzip`** (optional, default: true): When `emit_content` is true, decompress before emitting if either (1) path ends with `.gz` or `.log.gz`, or (2) content starts with gzip magic bytes (`1f 8b`), so misnamed or extension-less gzip data is still decompressed.

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

### Example 3: Sync logs (download + decompress + aggregate to S3)

全流程在 Vector 内完成：file_list 拉取并解压，官方 aws_s3 sink 按 batch 写回 S3。

```toml
[sources.file_list]
type = "file_list"
endpoint = "s3://source-bucket"
cloud_provider = "aws"
cluster_id = "10324983984131567830"
project_id = "1372813089209061633"
types = ["raw_logs"]
start_time = "2026-01-08T00:00:00Z"
end_time = "2026-01-08T23:59:59Z"
emit_content = true
decompress_gzip = true

[sinks.to_s3]
type = "aws_s3"
inputs = ["file_list"]
bucket = "dest-bucket"
key_prefix = "backup/logs/"
encoding = { codec = "text" }
batch = { max_bytes = 33554432 }
compression = "none"
```

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

## Future Enhancements

1. **Checkpoint Support**: Track which files have been processed to avoid duplicates in polling mode
2. **Parallel Listing**: Support for parallel file listing across multiple prefixes
3. **Advanced Pattern Matching**: Support for more complex patterns (regex, multiple placeholders)
4. **File Content Preview**: Option to read first N bytes of each file for inspection
5. **Incremental Listing**: Track last listing time and only return new/modified files
