# Delta Lake Watermark Source Architecture

## Overview

The `delta_lake_watermark` source is a custom Vector Source plugin designed to incrementally sync data from Delta Lake tables in multi-cloud environments (AWS S3, GCP Cloud Storage, Azure Blob Storage, Aliyun OSS). It supports fault recovery in Kubernetes environments through a Watermark-based checkpoint mechanism.

## Core Features

1. **Incremental Sync**: Incremental data synchronization based on timestamp and unique ID
2. **Fault Recovery**: Fault recovery through local checkpoint files
3. **Multi-Cloud Support**: Support for AWS, GCP, Azure, Aliyun cloud storage
4. **Acknowledgment Mechanism**: Support for end-to-end acknowledgment (At-least-once delivery)
5. **Metrics Exposure**: Expose Prometheus metrics for monitoring

## Architecture

### Component Structure

```
delta_lake_watermark/
├── mod.rs              # Configuration and SourceConfig implementation
├── controller.rs       # Main controller, handles query loop and event sending
├── checkpoint.rs       # Checkpoint management (read/write)
├── duckdb_query.rs     # DuckDB query executor
└── arch.md             # This document
```

### Data Flow

```
Delta Lake Table (S3/GCS/Azure/Aliyun)
    ↓
DuckDB Query Executor (delta_scan)
    ↓
RecordBatch (Arrow)
    ↓
Vector LogEvent
    ↓
SourceSender (with Ack)
    ↓
Downstream Sinks
```

### Key Components

#### 1. Checkpoint Management (`checkpoint.rs`)

Checkpoints are stored in JSON files under `data_dir`, containing:
- `last_watermark`: Last confirmed processed timestamp
- `last_processed_id`: Last processed unique ID (for handling records with same timestamp)
- `status`: Task status (running, finished, error)

**Checkpoint File Format**:
```json
{
  "last_watermark": "2026-02-09T12:00:00Z",
  "last_processed_id": "uuid-999",
  "status": "running"
}
```

#### 2. DuckDB Query Executor (`duckdb_query.rs`)

Uses DuckDB as the query engine, querying Delta Lake tables through the `delta_scan` function.

**Query Template**:

When `unique_id_column` is provided:
```sql
SELECT * FROM delta_scan('s3://bucket/path/to/delta_table')
WHERE (time > '{{last_watermark}}' OR (time = '{{last_watermark}}' AND unique_id > '{{last_processed_id}}'))
  AND ({{condition}})
ORDER BY time ASC, unique_id ASC
LIMIT {{batch_size}}
```

When `unique_id_column` is NOT provided:
```sql
SELECT * FROM delta_scan('s3://bucket/path/to/delta_table')
WHERE time >= '{{last_watermark}}'
  AND ({{condition}})
ORDER BY time ASC
LIMIT {{batch_size}}
```

**Note**: If no checkpoint exists, user should specify time range in `condition` (e.g., `condition = "time >= 1717632000 AND time <= 1718044799"`).

**Important Notes**:
- **With `unique_id_column`**: Uses OR condition to precisely skip already processed records, even when they share the same timestamp. This ensures no duplicates and no missed data.
- **Without `unique_id_column`**: Uses `>=` to include records with the same timestamp. This ensures data completeness but may cause duplicate processing of same-timestamp records after restart. Users should either:
  1. Ensure `order_by_column` (typically timestamp) is unique in the table, OR
  2. Provide `unique_id_column` for precise incremental sync

**Features**:
- Supports predicate pushdown
- Automatically handles Parquet file parsing
- Memory limit configuration (prevents OOM)

#### 3. Controller (`controller.rs`)

The main controller is responsible for:
1. Loading checkpoint
2. Building and executing queries
3. Converting data to Vector Events
4. Sending events and waiting for acknowledgment
5. Updating checkpoint
6. Updating Prometheus metrics

**Processing Flow**:
```
1. Load Checkpoint
2. Build SQL Query
3. Execute Query (DuckDB)
4. Convert to Events
5. Send Events (with Ack)
6. Update Checkpoint
7. Update Metrics
8. Repeat or Exit
```

## Configuration

### Basic Configuration

```toml
[sources.my_delta_source]
type = "delta_lake_watermark"
endpoint = "s3://my-bucket/path/to/delta_table"
cloud_provider = "aws"  # aws, gcp, azure, aliyun
data_dir = "/var/lib/vector/checkpoints/"
```

### Business Filtering

```toml
condition = "time >= 1717632000 AND time <= 1718044799 AND type = 'error' AND severity > 3"
order_by_column = "time"
unique_id_column = "unique_id"  # Optional but recommended
```

**Note**: All filtering including time ranges should be specified in `condition`. Examples:
- Time range: `condition = "time >= 1717632000 AND time <= 1718044799"`
- Business filter: `condition = "type = 'error' AND severity > 3"`
- Combined: `condition = "time >= 1717632000 AND time <= 1718044799 AND type = 'error' AND severity > 3"`

**Important**: 
- `order_by_column`: Column used for primary ordering and incremental sync (typically a timestamp column like `timestamp`, `created_at`, `event_time`, etc.)
- `unique_id_column`: **Highly recommended** for precise incremental sync. 
  - **Purpose**: Used for secondary sorting when multiple records share the same timestamp value
  - **Type**: Can be any column type (ID, UUID, string, integer, etc.). Examples: `id`, `uuid`, `request_id`, `record_id`, `event_id`
  - **Behavior**: When provided, enables precise incremental sync with no duplicates and no missed data. The source uses OR condition: `time > last_watermark OR (time = last_watermark AND unique_id > last_processed_id)`
  - **Without it**: The source uses `>=` for checkpoint recovery, which ensures no data is missed but may cause duplicate processing of same-timestamp records after restart

### Performance Configuration

```toml
batch_size = 10000
poll_interval_secs = 30
acknowledgements = true
duckdb_memory_limit = "2GB"  # Optional
duckdb_temp_directory = "/fast-ssd/duckdb_temp"  # Optional, enables disk spill; defaults to {data_dir}/duckdb_temp
duckdb_threads = 4  # Optional, reduce for lower memory (e.g. when ORDER BY + SELECT * over wide time range)
```

## Acknowledgment Mechanism

### At-least-once Delivery

The source supports end-to-end acknowledgment:

1. **Send Events**: Send events through `SourceSender::send_batch()`
2. **Wait for Acknowledgment**: Vector framework automatically handles acknowledgment (when `can_acknowledge()` returns `true`)
3. **Update Checkpoint**: Only update checkpoint after all events in the batch are acknowledged

### Checkpoint Update Strategy

- **Batch Acknowledgment**: Each batch is treated as an atomic operation
- **Last Record**: Checkpoint is updated to the timestamp and ID of the last record in the batch
- **Fault Recovery**: If the Pod crashes, restart from the last confirmed checkpoint

## Multi-Cloud Support

### AWS S3

```toml
endpoint = "s3://bucket/path/to/table"
cloud_provider = "aws"
```

Uses AWS default credential chain (environment variables, IAM roles, etc.).

### GCP Cloud Storage

```toml
endpoint = "gs://bucket/path/to/table"
cloud_provider = "gcp"
```

Uses GCP Application Default Credentials.

### Azure Blob Storage

```toml
endpoint = "az://account/container/path/to/table"
cloud_provider = "azure"
```

Uses Azure environment variables or Managed Identity.

### Aliyun OSS

```toml
endpoint = "oss://bucket/path/to/table"
cloud_provider = "aliyun"
```

Requires environment variables:
- `OSS_ENDPOINT`: OSS endpoint address
- `OSS_ACCESS_KEY_ID`: Access Key ID
- `OSS_ACCESS_KEY_SECRET`: Access Key Secret

DuckDB configuration:
- `s3_endpoint`: Set to OSS endpoint
- `s3_use_path_style`: `false`

## Metrics

The source exposes the following Prometheus metrics:

### `delta_sync_watermark_timestamp` (Gauge)

Current confirmed sync timestamp (Unix timestamp).

```
delta_sync_watermark_timestamp 1707480000.0
```

### `delta_sync_rows_processed_total` (Counter)

Total number of processed rows.

```
delta_sync_rows_processed_total 150000
```

### `delta_sync_is_finished` (Gauge)

Whether the task is finished (1 = finished, 0 = running).

```
delta_sync_is_finished 0.0
```

## Mission Modes

### One-off Task

When a time range is specified in `condition` and the query returns empty results, the task ends normally. Users should monitor task completion externally.

```toml
condition = "time >= 1717632000 AND time <= 1718044799"
poll_interval_secs = 30
```

### Streaming Task

Do not specify an end time in `condition`. When the query returns empty results, wait for `poll_interval_secs` before querying again.

```toml
condition = "time >= 1717632000"  # Only start time, no end time
poll_interval_secs = 30
```

## Fault Recovery

### Checkpoint Persistence

Checkpoint files are stored under `data_dir`, using persistent volumes (PV) to ensure data is not lost after Pod restart.

### Recovery Process

1. **On Startup**: Load checkpoint file
2. **If Exists**: Continue querying from `last_watermark` (incremental sync)
3. **If Not Exists**: User should specify time range in `condition` (e.g., `condition = "time >= 1717632000 AND time <= 1718044799"`)
4. **Query Execution**: Use timestamp and ID from checkpoint to build query conditions, combined with user-provided `condition`

### Data Consistency

- **At-least-once**: Ensures data is processed at least once (may be duplicated)
- **Ordering**: Ensures data is processed in time order through `ORDER BY`
- **Precise Recovery**: Handles records with same timestamp through unique ID

## Performance Optimization

### Memory Control (Critical for Wide Tables / Large Records)

When using `ORDER BY` + `SELECT *` over Delta Lake with wide time ranges, DuckDB may need to read and sort large amounts of data before applying `LIMIT`. This is especially true when:
- Records are large (e.g. 60KB+ per row with many columns)
- Delta Lake uses large compact files (e.g. 500MB each)
- The time range in `condition` spans many files

**Recommended configuration for high-memory scenarios:**

```toml
duckdb_memory_limit = "2GB"
duckdb_temp_directory = "/fast-ssd/duckdb_temp"  # Enables disk spill - use SSD
duckdb_threads = 4   # Reduce from default to lower parallel buffer usage
batch_size = 500     # Smaller batches reduce per-query memory
```

- **duckdb_memory_limit**: Hard cap on DuckDB memory. When exceeded, DuckDB spills to disk (if `duckdb_temp_directory` is set).
- **duckdb_temp_directory**: **Required for spill**. When unset, defaults to `{data_dir}/duckdb_temp`. Use fast storage (SSD/NVMe) for acceptable spill performance.
- **duckdb_threads**: Lower values (2–4) reduce parallel buffer memory; useful when memory is tight.
- **batch_size**: Smaller values (e.g. 500) reduce data volume per query; 1000 rows × 60KB ≈ 60MB per batch.

### Query Optimization

- **Index Utilization**: Delta Lake metadata helps DuckDB optimize queries
- **Columnar Storage**: Parquet format supports columnar scanning
- **Predicate Pushdown**: WHERE conditions filter at Parquet file level

## Schema Evolution

The source can handle Delta Lake schema changes:

1. **Dynamic Schema**: DuckDB automatically detects schema changes
2. **Field Mapping**: All fields are converted to JSON format
3. **Missing Fields**: Missing fields are filled with `null`

## Dependencies

- **duckdb**: DuckDB Rust bindings for querying Delta Lake
- **arrow**: Arrow data format support
- **chrono**: Timestamp handling
- **serde_json**: JSON serialization/deserialization
- **metrics**: Prometheus metrics exposure

## Usage Examples

### Basic Configuration

```toml
[sources.delta_sync]
type = "delta_lake_watermark"
endpoint = "s3://my-bucket/logs/delta_table"
cloud_provider = "aws"
data_dir = "/var/lib/vector/checkpoints/"
condition = "time >= 1717632000 AND time <= 1718044799"  # Time range in condition
order_by_column = "timestamp"
batch_size = 10000
acknowledgements = true
```

### With Filter Conditions and Unique ID

```toml
[sources.delta_sync]
type = "delta_lake_watermark"
endpoint = "s3://my-bucket/logs/delta_table"
cloud_provider = "aws"
data_dir = "/var/lib/vector/checkpoints/"
condition = "time >= 1717632000 AND time <= 1718044799 AND level = 'ERROR' AND status_code >= 500"
order_by_column = "timestamp"        # Primary sort: timestamp column
unique_id_column = "request_id"      # Secondary sort: can be ID, UUID, string, etc.
batch_size = 5000
poll_interval_secs = 60
acknowledgements = true
```

**Note**: `unique_id_column` can be any column type (ID, UUID, string, integer, etc.) that uniquely identifies records with the same timestamp. Common examples:
- `id` or `record_id` (integer or bigint)
- `uuid` or `event_id` (string/UUID)
- `request_id` or `transaction_id` (string)
- Any other column that provides uniqueness within the same timestamp

### Using with aws_s3 Sink (text / json / csv)

- **JSON codec**: The sink serializes the whole event, so all Delta columns appear. No extra transform needed.
- **CSV codec**: You must set `encoding.csv.fields` to the list of column names (same as your Delta table). Each event is one row.
- **Text codec**: The official aws_s3 sink with `codec = "text"` writes **only the `message` field** of each event. The delta_lake_watermark source emits one row per event with **column names as keys** (e.g. `id`, `name`, `time`); it does **not** set a `message` field unless your Delta table has a column named `message`. So with text codec alone, output is empty.

To get non-empty text output, add a **remap** transform that sets `message` from the event, then use that transform as the sink input. For example, to write each event as one JSON line (same idea as json codec but via the message field):

```toml
[transforms.delta_to_message]
type = "remap"
inputs = ["delta_lake_source"]
source = '''
.message = encode_json(.)
'''
```

Then in the sink, set `inputs = ["delta_to_message"]` instead of `inputs = ["delta_lake_source"]`. You can also set `.message` to a custom string (e.g. concatenate fields) instead of `encode_json(.)` if you need a different text format.

## Limitations and Notes

1. **DuckDB Extension**: Requires DuckDB's `delta` extension (or `delta_scan` function)
2. **Memory Usage**: Large queries may consume significant memory, need to properly configure `duckdb_memory_limit`
3. **Network Latency**: Cloud storage queries may be affected by network latency
4. **Schema Changes**: Frequent schema changes may affect performance
5. **unique_id_column Requirement**: 
   - **Highly Recommended**: Providing `unique_id_column` enables precise incremental sync, ensuring no duplicates and no missed data even when multiple records share the same timestamp
   - **Column Type**: `unique_id_column` can be any type (ID, UUID, string, integer, etc.). It's used for secondary sorting when records have the same timestamp. Examples: `id`, `uuid`, `request_id`, `record_id`, `event_id`
   - **Query Logic**: When provided, uses `time > last_watermark OR (time = last_watermark AND unique_id > last_processed_id)` to precisely skip already processed records
   - **Without unique_id_column**: The source uses `>=` for checkpoint recovery to ensure data completeness. This means:
     - ✅ **No data will be missed** (all records with same timestamp are included)
     - ⚠️ **May cause duplicate processing** of same-timestamp records after restart
     - 💡 **Best Practice**: Either ensure `order_by_column` is unique in your table, OR provide `unique_id_column` (any type) for precise incremental sync

## Future Improvements

1. **Parallel Queries**: Support parallel queries for multiple partitions
2. **Adaptive Batch Size**: Dynamically adjust batch size based on query performance
3. **Finer-grained Ack**: Support acknowledgment for individual records
4. **Compression Support**: Support Delta Lake compression formats
