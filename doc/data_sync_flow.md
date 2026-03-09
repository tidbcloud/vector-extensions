# Vector Data Synchronization Flow

## Overview

This document describes the data synchronization flow of the `delta_lake_watermark` source in Vector. The source enables incremental data synchronization from Delta Lake tables in multi-cloud environments with fault recovery capabilities.

## Why Vector?

Vector is chosen as the data synchronization platform for observability (o11y) data pipelines for several compelling reasons:

### 1. Rich Ecosystem of Sources and Sinks

Vector provides a comprehensive collection of built-in sources and sinks, making it easy to integrate with various data sources and destinations without custom development.

**Built-in Sources** (50+ available):
- **Log Sources**: `file`, `journald`, `syslog`, `docker`, `kubernetes_logs`, `aws_s3`, `gcp_pubsub`, `azure_blob`
- **Metrics Sources**: `prometheus`, `statsd`, `datadog_agent`, `influxdb_metrics`
- **Trace Sources**: `opentelemetry`, `datadog_agent`, `jaeger`
- **Database Sources**: `postgres`, `mysql`, `clickhouse`
- **Cloud Sources**: `aws_cloudwatch_logs`, `aws_kinesis`, `gcp_cloud_logging`, `azure_monitor_logs`
- **Custom Sources**: Extensible architecture allows custom sources like `delta_lake_watermark`, `topsql`, `conprof`

**Built-in Sinks** (60+ available):
- **Database Sinks**: `postgres`, `mysql`, `clickhouse`, `influxdb`, `databend`
- **Cloud Sinks**: `aws_s3`, `aws_cloudwatch_logs`, `aws_kinesis`, `gcp_cloud_logging`, `azure_blob`
- **Observability Sinks**: `prometheus`, `loki`, `elasticsearch`, `datadog_logs`, `datadog_metrics`, `splunk_hec`
- **Message Queue Sinks**: `kafka`, `pulsar`, `rabbitmq`, `nats`, `redis`
- **File Sinks**: `file`, `console`, `blackhole`
- **Custom Sinks**: Extensible architecture allows custom sinks like `tidb`, `vm_import`, `deltalake`

### 2. Powerful Transformation and Encoding Capabilities

Vector's transform system provides extensive data manipulation capabilities through VRL (Vector Remap Language) and built-in transforms, enabling flexible data format conversion for different observability data types.

**Built-in Transforms**:
- **Parsing**: `parse_json`, `parse_logfmt`, `parse_regex`, `parse_grok`, `parse_cef`, `parse_csv`
- **Filtering**: `filter`, `reduce`, `sample`
- **Field Operations**: `add_fields`, `remove_fields`, `rename_fields`, `coerce_types`
- **Data Enrichment**: `geoip`, `enrich_tables`, `tag_cardinality_limit`
- **Format Conversion**: `json`, `logfmt`, `cef`, `syslog`
- **Aggregation**: `aggregate`, `reduce`, `group_by`

**Encoding Support**:
- **Text Formats**: JSON, JSON Lines, Logfmt, CEF, Syslog, CSV
- **Binary Formats**: Protobuf, Avro, MessagePack
- **Compression**: Gzip, Zlib, Snappy, LZ4, Zstd
- **Serialization**: Native support for various serialization formats

### 3. Unified Pipeline for Observability Data

Vector excels at handling diverse observability data types through a unified pipeline architecture:

```mermaid
graph TB
    subgraph "Observability Data Sources"
        LOGS[Logs<br/>Application Logs<br/>System Logs<br/>Access Logs]
        METRICS[Metrics<br/>Prometheus<br/>StatsD<br/>Custom Metrics]
        SLOWLOG[Slowlog<br/>MySQL Slow Queries<br/>TiDB Slow Queries]
        SQLSTMT[SQL Statements<br/>Query Logs<br/>Statement History]
        TOPSQL[TopSQL<br/>TiDB TopSQL Data<br/>Performance Metrics]
    end
    
    subgraph "Vector Pipeline"
        SOURCE[Vector Sources<br/>delta_lake_watermark<br/>topsql<br/>conprof<br/>file<br/>prometheus]
        TRANS[Transforms<br/>VRL Remap<br/>Parse<br/>Filter<br/>Enrich]
        ENCODE[Encoders<br/>JSON<br/>Protobuf<br/>Custom Formats]
        SINK[Vector Sinks<br/>tidb<br/>deltalake<br/>vm_import<br/>elasticsearch<br/>prometheus]
    end
    
    subgraph "Destination Formats"
        DB[(Databases<br/>MySQL/TiDB<br/>PostgreSQL<br/>ClickHouse)]
        LAKE[Data Lakes<br/>Delta Lake<br/>S3/GCS/Azure]
        O11Y[Observability<br/>VictoriaMetrics<br/>Prometheus<br/>Loki<br/>Elasticsearch]
    end
    
    LOGS --> SOURCE
    METRICS --> SOURCE
    SLOWLOG --> SOURCE
    SQLSTMT --> SOURCE
    TOPSQL --> SOURCE
    
    SOURCE --> TRANS
    TRANS --> ENCODE
    ENCODE --> SINK
    
    SINK --> DB
    SINK --> LAKE
    SINK --> O11Y
    
    style LOGS fill:#e1f5ff
    style METRICS fill:#fff4e1
    style SLOWLOG fill:#e8f5e9
    style SQLSTMT fill:#f3e5f5
    style TOPSQL fill:#fce4ec
    style SOURCE fill:#fff4e1
    style SINK fill:#e8f5e9
```

**Observability Data Types Supported**:

1. **Logs** (Structured/Unstructured)
   - Application logs, system logs, access logs
   - Formats: JSON, Logfmt, Syslog, Plain Text
   - Sources: `file`, `journald`, `docker`, `kubernetes_logs`, `aws_s3`
   - Transforms: `parse_json`, `parse_logfmt`, `parse_regex`, `parse_grok`
   - Sinks: `elasticsearch`, `loki`, `datadog_logs`, `splunk_hec`, `file`

2. **Metrics** (Time-Series Data)
   - Prometheus metrics, StatsD metrics, custom metrics
   - Formats: Prometheus, StatsD, InfluxDB Line Protocol
   - Sources: `prometheus`, `statsd`, `datadog_agent`, `influxdb_metrics`
   - Transforms: `aggregate`, `reduce`, `sample`
   - Sinks: `prometheus`, `influxdb`, `datadog_metrics`, `vm_import`

3. **Slowlog** (Database Query Logs)
   - MySQL slow query logs, TiDB slow query logs
   - Formats: MySQL slowlog format, structured JSON
   - Sources: `delta_lake_watermark` (from Delta Lake), `file`, `mysql`
   - Transforms: `parse_regex`, `remap` (VRL), `add_fields`
   - Sinks: `tidb`, `mysql`, `postgres`, `deltalake`, `elasticsearch`

4. **SQL Statements** (Query History)
   - SQL query logs, statement history, query patterns
   - Formats: JSON, structured logs
   - Sources: `delta_lake_watermark`, `topsql`, `system_tables`, `mysql`
   - Transforms: `remap`, `filter`, `add_fields`, `coerce_types`
   - Sinks: `tidb`, `deltalake`, `clickhouse`, `elasticsearch`

5. **TopSQL** (Performance Data)
   - TiDB TopSQL data, query performance metrics
   - Formats: Protobuf, JSON
   - Sources: `topsql`, `topsql_v2` (custom sources)
   - Transforms: `remap`, `add_fields`, `coerce_types`
   - Sinks: `topsql_data_deltalake`, `topsql_meta_deltalake`, `vm_import`, `tidb`

### 4. Flexible Data Format Conversion

Vector's transform system enables seamless conversion between different data formats, making it ideal for observability data pipelines:

**Example: Converting Slowlog to Multiple Formats**

```mermaid
graph LR
    A[Delta Lake<br/>Slowlog Data] --> B[delta_lake_watermark<br/>Source]
    B --> C[remap Transform<br/>Format Conversion]
    C --> D1[MySQL Format<br/>for tidb Sink]
    C --> D2[JSON Format<br/>for elasticsearch]
    C --> D3[Prometheus Format<br/>for metrics]
    C --> D4[Delta Lake Format<br/>for deltalake Sink]
    
    style A fill:#e1f5ff
    style C fill:#fff4e1
    style D1 fill:#e8f5e9
    style D2 fill:#e8f5e9
    style D3 fill:#e8f5e9
    style D4 fill:#e8f5e9
```

**Configuration Example**:

```toml
# Source: Read slowlog from Delta Lake
[sources.slowlog_source]
type = "delta_lake_watermark"
endpoint = "s3://bucket/slowlogs/delta_table"
condition = "time >= 1717632000 AND time <= 1718044799"
order_by_column = "time"
unique_id_column = "id"

# Transform: Convert to different formats
[transforms.format_for_mysql]
type = "remap"
inputs = ["slowlog_source"]
source = """
  # Format as MySQL slowlog line
  .log_line = string!(.prev_stmt ?? "") + " | " + string!(.query_time ?? "")
  .log_timestamp = format_timestamp!(to_int!(.time) ?? 0, format: "%+")
"""

[transforms.format_for_elasticsearch]
type = "remap"
inputs = ["slowlog_source"]
source = """
  # Enrich with metadata
  .@timestamp = format_timestamp!(to_int!(.time) ?? 0, format: "%+")
  .source = "slowlog"
  .type = "database_query"
"""

# Sink: Write to MySQL
[sinks.mysql_sink]
type = "tidb"
inputs = ["format_for_mysql"]
connection_string = "mysql://user:pass@localhost:3306/db"
table = "slowlogs"

# Sink: Write to Elasticsearch
[sinks.elasticsearch_sink]
type = "elasticsearch"
inputs = ["format_for_elasticsearch"]
endpoint = "http://elasticsearch:9200"
index = "slowlogs-%Y-%m-%d"
```

### 5. Extensibility and Custom Components

Vector's plugin architecture allows easy extension with custom sources, transforms, and sinks:

**Custom Sources in This Project**:
- `delta_lake_watermark`: Incremental sync from Delta Lake tables
- `topsql`: TiDB TopSQL data collection
- `topsql_v2`: Enhanced TopSQL collection
- `conprof`: Continuous profiling data collection
- `system_tables`: System table data collection
- `keyviz`: Key visualization data collection

**Custom Sinks in This Project**:
- `tidb`: MySQL/TiDB database sink with dynamic schema
- `deltalake`: Delta Lake table writer
- `vm_import`: VictoriaMetrics import sink
- `topsql_data_deltalake`: TopSQL data to Delta Lake
- `topsql_meta_deltalake`: TopSQL metadata to Delta Lake
- `aws_s3_upload_file`: AWS S3 file upload
- `azure_blob_upload_file`: Azure Blob file upload
- `gcp_cloud_storage_upload_file`: GCP Cloud Storage upload

### 6. Production-Ready Features

Vector provides enterprise-grade features essential for production observability pipelines:

- **Reliability**: At-least-once delivery guarantees, checkpointing, fault recovery
- **Performance**: High-throughput processing, batching, backpressure handling
- **Observability**: Built-in metrics, health checks, structured logging
- **Security**: TLS/SSL support, authentication, encryption
- **Scalability**: Horizontal scaling, load balancing, distributed processing
- **Monitoring**: Prometheus metrics, health endpoints, status APIs

### 7. Unified Configuration and Management

All observability data pipelines can be managed through a single Vector configuration file, simplifying operations:

```toml
# Single configuration file for all o11y data types
[sources.logs]
type = "file"
include = ["/var/log/app/*.log"]

[sources.metrics]
type = "prometheus"
endpoint = "http://prometheus:9090"

[sources.slowlog]
type = "delta_lake_watermark"
endpoint = "s3://bucket/slowlogs"

[sources.topsql]
type = "topsql"
pd_endpoints = ["http://pd:2379"]

# Unified transforms
[transforms.enrich]
type = "remap"
inputs = ["logs", "metrics", "slowlog", "topsql"]
source = """
  .environment = "production"
  .region = "us-west-2"
"""

# Unified sinks
[sinks.elasticsearch]
type = "elasticsearch"
inputs = ["enrich"]
endpoint = "http://elasticsearch:9200"
```

### Summary: Why Vector for Observability Data?

```mermaid
mindmap
  root((Vector for O11y))
    Rich Ecosystem
     50+ Sources
     60+ Sinks
     Custom Components
    Data Format Conversion
     VRL Transforms
     Multiple Encoders
     Flexible Parsing
    Unified Pipeline
     Logs
     Metrics
     Slowlog
     SQL Statements
     TopSQL
    Production Ready
     Reliability
     Performance
     Observability
     Security
    Extensibility
     Custom Sources
     Custom Sinks
     Plugin Architecture
```

**Key Advantages**:
- ✅ **Single Platform**: Handle all observability data types in one system
- ✅ **Format Flexibility**: Convert between any data formats easily
- ✅ **Rich Ecosystem**: Leverage 100+ built-in components
- ✅ **Extensibility**: Add custom components for domain-specific needs
- ✅ **Production Ready**: Enterprise-grade reliability and performance
- ✅ **Unified Management**: Single configuration for all pipelines
- ✅ **Cost Effective**: Open-source, no vendor lock-in

Vector is the ideal choice for observability data synchronization because it provides a unified, extensible, and production-ready platform that can handle the diverse data types (logs, metrics, slowlog, SQL statements, TopSQL) while providing the flexibility to convert data to any required format for downstream systems.

## Architecture Diagram

```mermaid
graph TB
    subgraph "Delta Lake Storage"
        DL[Delta Lake Table<br/>S3/GCS/Azure/Aliyun]
    end
    
    subgraph "Vector Source"
        CP[Checkpoint Manager]
        DQ[DuckDB Query Executor]
        CTRL[Controller]
        SRC[delta_lake_watermark Source]
    end
    
    subgraph "Vector Pipeline"
        TRANS[Transforms<br/>Optional]
        SINK[Sinks<br/>MySQL/TiDB/etc]
    end
    
    subgraph "Monitoring"
        METRICS[Prometheus Metrics]
    end
    
    DL -->|Query via delta_scan| DQ
    DQ -->|RecordBatch| CTRL
    CTRL -->|Load/Save| CP
    CTRL -->|Events| TRANS
    TRANS -->|Events| SINK
    CTRL -->|Metrics| METRICS
    CP -.->|Persist State| FS[(Checkpoint Files)]
    
    style DL fill:#e1f5ff
    style SRC fill:#fff4e1
    style SINK fill:#e8f5e9
    style METRICS fill:#f3e5f5
```

## Data Synchronization Flow

### High-Level Flow

```mermaid
sequenceDiagram
    participant User
    participant Vector
    participant Source
    participant DuckDB
    participant DeltaLake
    participant Checkpoint
    participant Sink
    
    User->>Vector: Start Vector with config
    Vector->>Source: Initialize delta_lake_watermark source
    Source->>Checkpoint: Load checkpoint file
    Checkpoint-->>Source: Return checkpoint (or default)
    
    loop Batch Processing
        Source->>Source: Build SQL query with checkpoint
        Source->>DuckDB: Execute query
        DuckDB->>DeltaLake: Query via delta_scan
        DeltaLake-->>DuckDB: Return RecordBatch
        DuckDB-->>Source: Return RecordBatch
        Source->>Source: Convert to Vector Events
        Source->>Sink: Send events batch
        Sink-->>Source: Acknowledge (if enabled)
        Source->>Checkpoint: Update checkpoint
        Checkpoint->>Checkpoint: Save to disk
    end
```

### Detailed Processing Flow

```mermaid
flowchart TD
    Start([Start Vector]) --> Init[Initialize Source]
    Init --> LoadCP[Load Checkpoint]
    LoadCP --> CheckCP{Checkpoint<br/>Exists?}
    
    CheckCP -->|Yes| UseCP[Use last_watermark<br/>for incremental sync]
    CheckCP -->|No| UseCondition[Use condition<br/>for initial sync]
    
    UseCP --> BuildQuery[Build SQL Query]
    UseCondition --> BuildQuery
    
    BuildQuery --> ExecQuery[Execute Query via DuckDB]
    ExecQuery --> GetResults{Get Results}
    
    GetResults -->|Empty| Wait[Wait poll_interval_secs]
    Wait --> BuildQuery
    
    GetResults -->|Has Data| Convert[Convert to Events]
    Convert --> Send[Send Events to Sink]
    Send --> WaitAck{Wait for<br/>Acknowledgment?}
    
    WaitAck -->|Yes| Ack[Wait for Ack]
    WaitAck -->|No| UpdateCP
    Ack --> UpdateCP[Update Checkpoint]
    
    UpdateCP --> SaveCP[Save Checkpoint to Disk]
    SaveCP --> UpdateMetrics[Update Prometheus Metrics]
    UpdateMetrics --> CheckMore{More Data?}
    
    CheckMore -->|Yes| BuildQuery
    CheckMore -->|No| Wait
    
    style Start fill:#e8f5e9
    style UpdateCP fill:#fff4e1
    style SaveCP fill:#fff4e1
    style CheckMore fill:#e1f5ff
```

## Step-by-Step Process

### 1. Initialization Phase

```mermaid
graph LR
    A[Vector Starts] --> B[Load Config]
    B --> C[Create DuckDB Executor]
    C --> D[Initialize DuckDB Connection]
    D --> E[Load Delta Extension]
    E --> F[Configure Cloud Storage]
    F --> G[Load Checkpoint]
    G --> H{Checkpoint<br/>Found?}
    H -->|Yes| I[Use last_watermark]
    H -->|No| J[Start from condition]
    I --> K[Ready to Process]
    J --> K
    
    style A fill:#e8f5e9
    style K fill:#fff4e1
```

**Steps:**
1. Vector loads the configuration file
2. Creates `DuckDBQueryExecutor` with endpoint and cloud provider
3. Initializes DuckDB in-memory connection
4. Installs and loads Delta extension
5. Configures cloud storage credentials (AWS S3, GCP, Azure, Aliyun)
6. Loads checkpoint from `data_dir` (if exists)
7. If checkpoint exists, uses `last_watermark` for incremental sync
8. If no checkpoint, user should specify time range in `condition`

### 2. Query Building Phase

The source builds SQL queries based on checkpoint state and configuration:

**With Checkpoint and unique_id_column:**
```sql
SELECT * FROM delta_scan('s3://bucket/path/to/delta_table')
WHERE (time > '2026-01-15T12:00:00Z' 
       OR (time = '2026-01-15T12:00:00Z' AND unique_id > 'id-100'))
  AND (time >= 1717632000 AND time <= 1718044799 AND type = 'error')
ORDER BY time ASC, unique_id ASC
LIMIT 10000
```

**With Checkpoint but no unique_id_column:**
```sql
SELECT * FROM delta_scan('s3://bucket/path/to/delta_table')
WHERE time >= '2026-01-15T12:00:00Z'
  AND (time >= 1717632000 AND time <= 1718044799 AND type = 'error')
ORDER BY time ASC
LIMIT 10000
```

**Without Checkpoint (first run):**
```sql
SELECT * FROM delta_scan('s3://bucket/path/to/delta_table')
WHERE (time >= 1717632000 AND time <= 1718044799 AND type = 'error')
ORDER BY time ASC
LIMIT 10000
```

### 3. Query Execution Phase

```mermaid
sequenceDiagram
    participant Controller
    participant DuckDB
    participant DeltaLake
    participant Parquet
    
    Controller->>DuckDB: Prepare SQL Query
    DuckDB->>DuckDB: Parse Query
    DuckDB->>DeltaLake: Read Delta Log
    DeltaLake-->>DuckDB: Return Metadata
    DuckDB->>DuckDB: Apply Predicate Pushdown
    DuckDB->>Parquet: Scan Relevant Files
    Parquet-->>DuckDB: Return Data Chunks
    DuckDB->>DuckDB: Filter & Sort
    DuckDB->>DuckDB: Apply LIMIT
    DuckDB-->>Controller: Return RecordBatch
```

**Process:**
1. DuckDB parses the SQL query
2. Reads Delta Lake transaction log to identify relevant Parquet files
3. Applies predicate pushdown to filter at file level
4. Scans only relevant Parquet files (not all files)
5. Filters rows based on WHERE conditions
6. Sorts by `order_by_column` (and `unique_id_column` if provided)
7. Applies LIMIT to return batch
8. Returns Arrow `RecordBatch` to controller

### 4. Event Conversion Phase

```mermaid
graph LR
    A[RecordBatch] --> B[Extract Row]
    B --> C[Convert to JSON]
    C --> D[Convert to LogValue]
    D --> E[Create LogEvent]
    E --> F[Add to Batch]
    F --> G{More Rows?}
    G -->|Yes| B
    G -->|No| H[Event Batch Ready]
    
    style A fill:#e1f5ff
    style H fill:#e8f5e9
```

**Conversion Process:**
1. Iterate through each row in `RecordBatch`
2. Extract column values (handles String, i64, f64, bool, NULL)
3. Convert to `serde_json::Value`
4. Convert JSON values to Vector `LogValue`:
   - `Null` → `LogValue::Null`
   - `Boolean` → `LogValue::Boolean`
   - `Number (integer)` → `LogValue::Integer`
   - `Number (float)` → `LogValue::Float`
   - `String` → `LogValue::Bytes`
   - `Array` → `LogValue::Array`
   - `Object` → `LogValue::Object`
5. Create `LogEvent` with all fields
6. Extract `order_by_column` value as watermark
7. Extract `unique_id_column` value (if provided)
8. Add to event batch

### 5. Event Sending Phase

```mermaid
sequenceDiagram
    participant Controller
    participant SourceSender
    participant Sink
    participant Checkpoint
    
    Controller->>SourceSender: send_batch(events)
    SourceSender->>Sink: Forward events
    Sink->>Sink: Process events
    Sink-->>SourceSender: Acknowledge (if enabled)
    SourceSender-->>Controller: Batch acknowledged
    Controller->>Checkpoint: Update with last record
    Checkpoint->>Checkpoint: Save to disk
```

**Acknowledgment Flow:**
1. Controller sends event batch via `SourceSender::send_batch()`
2. Events flow through Vector pipeline (transforms → sinks)
3. If `acknowledgements = true`, Vector framework waits for sink acknowledgment
4. Only after all events in batch are acknowledged:
   - Controller updates checkpoint with last record's watermark and unique_id
   - Checkpoint is saved to disk
5. This ensures **at-least-once** delivery guarantee

### 6. Checkpoint Update Phase

```mermaid
stateDiagram-v2
    [*] --> Running: Start
    Running --> Processing: Load Checkpoint
    Processing --> Updating: Batch Acknowledged
    Updating --> Saved: Write to Disk
    Saved --> Processing: Next Batch
    Processing --> Running: Continue Loop
    Running --> Finished: Task Complete
    Running --> Error: Processing Error
    Error --> Running: Retry
    Finished --> [*]
```

**Checkpoint Update Logic:**
1. After batch acknowledgment, extract last record's:
   - `order_by_column` value → `last_watermark`
   - `unique_id_column` value (if provided) → `last_processed_id`
2. Update checkpoint in memory
3. Save checkpoint to disk atomically
4. Update Prometheus metrics:
   - `delta_sync_watermark_timestamp` (current watermark)
   - `delta_sync_rows_processed_total` (increment by batch size)

## Incremental Sync Mechanism

### With unique_id_column (Precise Sync)

```mermaid
graph TB
    subgraph "Query Logic"
        A[Last Watermark: T1<br/>Last ID: ID-100] --> B{New Record?}
        B -->|time > T1| C[Include Record]
        B -->|time = T1<br/>AND id > ID-100| C
        B -->|time = T1<br/>AND id <= ID-100| D[Skip Record]
        B -->|time < T1| D
    end
    
    style C fill:#e8f5e9
    style D fill:#ffebee
```

**Query Condition:**
```sql
WHERE (time > 'T1' OR (time = 'T1' AND unique_id > 'ID-100'))
```

**Benefits:**
- ✅ No duplicates
- ✅ No missed data
- ✅ Precise recovery even with same timestamp records

### Without unique_id_column (Data Completeness)

```mermaid
graph TB
    subgraph "Query Logic"
        A[Last Watermark: T1] --> B{New Record?}
        B -->|time >= T1| C[Include Record]
        B -->|time < T1| D[Skip Record]
    end
    
    style C fill:#fff4e1
    style D fill:#ffebee
```

**Query Condition:**
```sql
WHERE time >= 'T1'
```

**Trade-offs:**
- ✅ No missed data (includes all records with same timestamp)
- ⚠️ May cause duplicate processing of same-timestamp records after restart
- 💡 Best Practice: Ensure `order_by_column` is unique OR provide `unique_id_column`

## Fault Recovery Flow

### Normal Operation

```mermaid
timeline
    title Normal Sync Flow
    T1 : Query Batch 1
         : Process 10K rows
         : Update checkpoint
    T2 : Query Batch 2
         : Process 10K rows
         : Update checkpoint
    T3 : Query Batch 3
         : Process 10K rows
         : Update checkpoint
```

### Crash and Recovery

```mermaid
timeline
    title Fault Recovery Flow
    T1 : Query Batch 1
         : Process 10K rows
         : ✅ Checkpoint saved
    T2 : Query Batch 2
         : Process 10K rows
         : ✅ Checkpoint saved
    T3 : Query Batch 3
         : Process 5K rows
         : ❌ CRASH (checkpoint not saved)
    T4 : Restart Vector
         : Load checkpoint (T2)
         : Resume from Batch 3
         : Re-process 5K rows (duplicates OK)
```

**Recovery Process:**
1. **On Restart**: Load checkpoint file from `data_dir`
2. **If Checkpoint Exists**: 
   - Use `last_watermark` and `last_processed_id` (if available)
   - Build query to continue from last confirmed position
   - May re-process some records (at-least-once guarantee)
3. **If No Checkpoint**: 
   - User should specify time range in `condition`
   - Start from beginning of specified range

## Configuration Example

### Basic Configuration

```toml
[sources.delta_sync]
type = "delta_lake_watermark"
endpoint = "s3://my-bucket/logs/delta_table"
cloud_provider = "aws"
data_dir = "/var/lib/vector/checkpoints/"

# All filtering in condition (including time range)
condition = "time >= 1717632000 AND time <= 1718044799 AND type = 'error'"

# Ordering configuration
order_by_column = "time"              # Primary sort column
unique_id_column = "request_id"       # Secondary sort (recommended)

# Performance tuning
batch_size = 10000
poll_interval_secs = 30
duckdb_memory_limit = "2GB"

# Reliability
acknowledgements = true
```

### Complete Pipeline Example

```toml
[sources.delta_sync]
type = "delta_lake_watermark"
endpoint = "s3://my-bucket/logs/delta_table"
cloud_provider = "aws"
data_dir = "/var/lib/vector/checkpoints/"
condition = "time >= 1717632000 AND time <= 1718044799 AND level = 'ERROR'"
order_by_column = "timestamp"
unique_id_column = "event_id"
batch_size = 5000
poll_interval_secs = 60
acknowledgements = true
duckdb_memory_limit = "2GB"

[transforms.format_log]
type = "remap"
inputs = ["delta_sync"]
source = """
  .message = .message ?? ""
  .@timestamp = format_timestamp!(to_int!(.timestamp) ?? 0, format: "%+")
"""

[sinks.mysql_sink]
type = "tidb"
inputs = ["format_log"]
connection_string = "mysql://user:pass@localhost:3306/db"
table = "logs"
batch_size = 1000
```

## Monitoring and Metrics

### Prometheus Metrics

The source exposes the following metrics:

```mermaid
graph LR
    A[Source] --> B[delta_sync_watermark_timestamp<br/>Gauge]
    A --> C[delta_sync_rows_processed_total<br/>Counter]
    A --> D[delta_sync_is_finished<br/>Gauge]
    
    style B fill:#e1f5ff
    style C fill:#fff4e1
    style D fill:#e8f5e9
```

**Metrics Details:**

1. **`delta_sync_watermark_timestamp`** (Gauge)
   - Current confirmed sync timestamp (Unix timestamp)
   - Updated after each batch acknowledgment
   - Example: `1707480000.0`

2. **`delta_sync_rows_processed_total`** (Counter)
   - Total number of processed rows
   - Incremented by batch size after acknowledgment
   - Example: `150000`

3. **`delta_sync_is_finished`** (Gauge)
   - Task completion status
   - `1.0` = finished, `0.0` = running
   - Note: Currently always `0.0` (streaming mode)

### Monitoring Dashboard Example

```promql
# Current sync progress
delta_sync_watermark_timestamp

# Processing rate (rows per second)
rate(delta_sync_rows_processed_total[5m])

# Total processed
delta_sync_rows_processed_total

# Sync lag (if you have current time metric)
time() - delta_sync_watermark_timestamp
```

## Data Flow Diagram

### End-to-End Flow

```mermaid
graph TB
    subgraph "Source: Delta Lake"
        DL[Delta Lake Table<br/>Parquet Files]
    end
    
    subgraph "Vector Source"
        DQ[DuckDB Query Executor]
        CTRL[Controller]
        CP[Checkpoint]
    end
    
    subgraph "Vector Pipeline"
        TRANS[Transforms<br/>Optional Remap]
        SINK[TiDB Sink<br/>MySQL/TiDB]
    end
    
    subgraph "Destination"
        DB[(MySQL/TiDB<br/>Database)]
    end
    
    DL -->|delta_scan| DQ
    DQ -->|RecordBatch| CTRL
    CTRL <-->|Load/Save| CP
    CTRL -->|LogEvents| TRANS
    TRANS -->|LogEvents| SINK
    SINK -->|INSERT| DB
    SINK -.->|Ack| CTRL
    
    style DL fill:#e1f5ff
    style CTRL fill:#fff4e1
    style DB fill:#e8f5e9
```

## Query Execution Details

### Predicate Pushdown

```mermaid
graph LR
    A[SQL Query with<br/>WHERE conditions] --> B[DuckDB Parser]
    B --> C[Delta Lake<br/>Metadata]
    C --> D[Identify Relevant<br/>Parquet Files]
    D --> E[Scan Only<br/>Matching Files]
    E --> F[Filter Rows]
    F --> G[Return Results]
    
    style A fill:#e1f5ff
    style E fill:#fff4e1
    style G fill:#e8f5e9
```

**Benefits:**
- Only scans Parquet files that match WHERE conditions
- Reduces I/O and memory usage
- Faster query execution

### Batch Processing

```mermaid
graph TB
    A[Query Returns<br/>50K Rows] --> B[Process Batch 1<br/>10K rows]
    B --> C[Update Checkpoint]
    C --> D[Process Batch 2<br/>10K rows]
    D --> E[Update Checkpoint]
    E --> F[Process Batch 3<br/>10K rows]
    F --> G[Update Checkpoint]
    G --> H[Continue...]
    
    style C fill:#fff4e1
    style E fill:#fff4e1
    style G fill:#fff4e1
```

**Batch Processing Logic:**
1. Query returns up to `batch_size` rows per execution
2. Process entire batch as atomic unit
3. Update checkpoint only after batch acknowledgment
4. Next query continues from last checkpoint position
5. Repeat until no more data

## Error Handling

### Error Recovery Flow

```mermaid
stateDiagram-v2
    [*] --> Processing
    Processing --> Error: Query/Network Error
    Error --> MarkError: Log Error
    MarkError --> Wait: Wait & Retry
    Wait --> Processing: Retry Query
    Processing --> Success: Batch Processed
    Success --> UpdateCP: Acknowledge
    UpdateCP --> Processing: Next Batch
    Processing --> [*]: Shutdown
```

**Error Handling:**
1. **Query Execution Error**: Log error, mark checkpoint as error, continue processing
2. **Network Timeout**: DuckDB retries automatically (configurable)
3. **Checkpoint Write Error**: Log warning, continue (checkpoint may be stale)
4. **Event Send Error**: Retry via Vector framework

## Performance Optimization

### Memory Management

```mermaid
graph TB
    A[DuckDB Query] --> B{Memory Limit<br/>Set?}
    B -->|Yes| C[Limit Memory Usage]
    B -->|No| D[Use Default]
    C --> E[Prevent OOM]
    D --> E
    E --> F[Process Batch]
    
    style C fill:#fff4e1
    style E fill:#e8f5e9
```

**Memory Optimization:**
- Configure `duckdb_memory_limit` to prevent OOM
- Batch processing limits memory per batch
- Predicate pushdown reduces scanned data

### Query Optimization

```mermaid
graph LR
    A[User Condition] --> B[Predicate Pushdown]
    B --> C[File-Level Filtering]
    C --> D[Row-Level Filtering]
    D --> E[Sorting]
    E --> F[LIMIT]
    F --> G[Return Batch]
    
    style B fill:#fff4e1
    style C fill:#e8f5e9
```

## Best Practices

### 1. Always Provide unique_id_column

```toml
# ✅ Recommended
order_by_column = "timestamp"
unique_id_column = "event_id"  # or "id", "uuid", "request_id", etc.

# ⚠️ May cause duplicates
order_by_column = "timestamp"
# unique_id_column not provided
```

### 2. Specify Time Range in Condition

```toml
# ✅ For one-off tasks
condition = "time >= 1717632000 AND time <= 1718044799"

# ✅ For streaming tasks
condition = "time >= 1717632000"  # No end time
```

### 3. Use Persistent Volumes for Checkpoints

```yaml
# Kubernetes example
volumeMounts:
  - name: checkpoints
    mountPath: /var/lib/vector/checkpoints
volumes:
  - name: checkpoints
    persistentVolumeClaim:
      claimName: vector-checkpoints-pvc
```

### 4. Monitor Metrics

- Track `delta_sync_watermark_timestamp` to monitor progress
- Alert if `delta_sync_rows_processed_total` stops increasing
- Monitor checkpoint file updates

## Troubleshooting

### Common Issues

1. **No Data Synced**
   - Check `condition` includes correct time range
   - Verify checkpoint is not beyond data range
   - Check DuckDB can access Delta Lake table

2. **Duplicate Data**
   - Ensure `unique_id_column` is provided
   - Check checkpoint is being saved correctly
   - Verify `acknowledgements = true`

3. **Memory Issues**
   - Reduce `batch_size`
   - Set `duckdb_memory_limit`
   - Check Delta Lake table partition size

4. **Slow Performance**
   - Optimize `condition` for predicate pushdown
   - Increase `batch_size` (if memory allows)
   - Check network latency to cloud storage

## Summary

The `delta_lake_watermark` source provides:

- ✅ **Incremental Sync**: Efficiently syncs only new data
- ✅ **Fault Recovery**: Automatic recovery from checkpoints
- ✅ **At-least-once Delivery**: Guaranteed data delivery
- ✅ **Multi-Cloud Support**: Works with AWS, GCP, Azure, Aliyun
- ✅ **Monitoring**: Prometheus metrics for observability
- ✅ **Flexible Filtering**: All filtering via SQL `condition`

The source is designed for production use in Kubernetes environments with persistent volumes for checkpoint storage.
