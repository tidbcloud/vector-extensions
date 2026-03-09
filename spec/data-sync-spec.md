# Cluster Diagnostic Data Backup System Technical Specification

## 1. Overview

### 1.1 Background

This document defines the technical specification for a Vector-based cluster diagnostic data backup system. The system is primarily used to back up cluster diagnostic data (logs, slow query logs, SQL statements, metrics, etc.) for specified time periods. It supports user-defined filter rules to reduce transmission volume and speed up the backup process for important data.

### 1.2 Design Goals

- **Specificity**: Focused on cluster diagnostic data backup scenarios
- **Efficiency**: Supports filter rules to reduce unnecessary data transmission
- **Flexibility**: Supports multiple data formats and storage locations
- **Ease of Implementation**: Leverages Vector plugin ecosystem to minimize development effort
- **Guidance**: Provides clear, complete specifications to facilitate AI-assisted implementation

### 1.3 Core Principles

- Use Vector as the data collection, transformation, and transmission engine
- Leverage existing Vector plugins to minimize custom development
- Support precise time range specification
- Support user-defined filter rules
- Support multiple data source formats (compressed files, API, database, etc.)

## 2. Requirements Analysis

### 2.1 Core Scenarios

#### Scenario 1: Time-Range Diagnostic Data Backup (Primary Scenario)

**Requirements Description:**
Specify a cluster and time range, and back up all diagnostic data within that time range to target storage.

**Diagnostic Data Types:**
1. **Logs**: Application logs, system logs, etc.
2. **Slow Query Logs**: Database slow query records
3. **SQL Statements**: SQL execution records
4. **Metrics**: Performance metrics, monitoring metrics, etc.

**Time Range:**
- Support precise time range specification (start time + end time)
- Support timezone configuration
- Support relative time (e.g., last 24 hours)

#### Scenario 2: Filtered Backup (Secondary Scenario)

**Requirements Description:**
During backup, filter data according to user-specified rules, backing up only data that meets the conditions to reduce transmission volume and speed up backup.

**Filter Capabilities:**
- Keyword-based filtering
- Regular expression filtering
- Field value filtering
- Time range filtering (finer granularity)

### 2.2 Data Source Characteristics

#### 2.2.1 Data Format Diversity

Diagnostic data may be stored in multiple formats at different locations:

**Log Data:**
- **S3 Storage**: Log files stored on S3 in gzip-compressed format
- **Loki**: Logs also stored in Loki for querying
- **Parquet Statistics**: Background process generates parquet-format statistics hourly

**Slow Query Logs:**
- May be stored in database (e.g., TiDB's `information_schema.slow_query`)
- May be stored as files on S3
- May be provided via API

**SQL Statements:**
- Usually stored in database
- May be provided via monitoring system API
- May be recorded as logs

**Metrics Data:**
- Usually stored in time-series databases like Prometheus, VictoriaMetrics
- May be exported via API
- May be stored as files

#### 2.2.2 Storage Location Diversity

- **Object Storage**: S3, MinIO, Azure Blob, etc.
- **Time-Series Databases**: Prometheus, VictoriaMetrics, InfluxDB
- **Log Systems**: Loki, Elasticsearch
- **Relational Databases**: TiDB, MySQL, PostgreSQL
- **File Systems**: Local file system, NFS, etc.

### 2.3 Data Source Mapping Example

Using a TiDB cluster as an example, possible storage locations for diagnostic data:

```
Cluster: tidb-cluster-01
├── Logs
│   ├── S3: s3://logs-bucket/tidb-cluster-01/logs/2024/01/01/*.log.gz
│   ├── Loki: loki://loki-server:3100 (label: cluster=tidb-cluster-01)
│   └── Parquet: s3://stats-bucket/tidb-cluster-01/stats/hourly/*.parquet
├── Slow Query Logs
│   ├── Database: tidb://tidb-server:4000/information_schema.slow_query
│   └── S3: s3://logs-bucket/tidb-cluster-01/slowlogs/*.log
├── SQL Statements
│   ├── Database: tidb://tidb-server:4000/information_schema.statements_summary
│   └── API: http://tidb-server:10080/api/v1/statements
└── Metrics
    ├── Prometheus: http://prometheus:9090/api/v1/query_range
    └── VictoriaMetrics: http://vm:8428/api/v1/query_range
```

## 3. System Design

### 3.1 Overall Architecture (Kubernetes-based)

```
┌─────────────────────────────────────────────────────────────┐
│                  Management API (Management API)             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │ Task Mgmt    │  │ Task Sched   │  │ Status Mon   │     │
│  │ - Create     │  │ - Periodic   │  │ - Task State │     │
│  │ - Update     │  │ - One-time   │  │ - Exec Logs  │     │
│  │ - Delete     │  │ - Trigger    │  │ - Metrics    │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└─────────────────────────────────────────────────────────────┘
                            │
                            │ K8s API
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                        │
│                                                               │
│  ┌─────────────────────────────────────────────────────┐   │
│  │           Scheduled Task Vector Pod                  │   │
│  │  Pod: vector-scheduled                              │   │
│  │  ┌──────────────────────────────────────────────┐ │   │
│  │  │  Vector Container                             │ │   │
│  │  │  --config-dir=/vector/configs                  │ │   │
│  │  └──────────────────────────────────────────────┘ │   │
│  │  ┌──────────────────────────────────────────────┐ │   │
│  │  │  ConfigMap Mount                             │ │   │
│  │  │  /vector/configs/                             │ │   │
│  │  └──────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                               │
│  ConfigMaps (Scheduled task configs):                         │
│  ├── vector-task-scheduled-001 (task-001.toml)              │
│  ├── vector-task-scheduled-002 (task-002.toml)              │
│  └── vector-task-scheduled-003 (task-003.toml)              │
│                                                               │
│  ┌─────────────────────────────────────────────────────┐   │
│  │           One-time Task Vector Pods                  │   │
│  │                                                       │   │
│  │  Pod: vector-task-onetime-001                        │   │
│  │  ┌──────────────────────────────────────────────┐ │   │
│  │  │  Vector Container                             │ │   │
│  │  │  --config=/vector/config/vector.toml          │ │   │
│  │  └──────────────────────────────────────────────┘ │   │
│  │  ┌──────────────────────────────────────────────┐ │   │
│  │  │  ConfigMap Mount                             │ │   │
│  │  │  /vector/config/vector.toml                   │ │   │
│  │  └──────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                               │
│  ConfigMaps (One-time task configs):                          │
│  ├── vector-task-onetime-001 (vector.toml)                  │
│  ├── vector-task-onetime-002 (vector.toml)                  │
│  └── vector-task-onetime-003 (vector.toml)                  │
│                                                               │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
                    ┌──────────────┐
                    │ Data Sources/│
                    │ Targets      │
                    │ S3/Loki/DB   │
                    └──────────────┘
```

**Architecture Characteristics:**
- **Database-free**: All task configurations stored in K8s ConfigMaps
- **K8s Native**: Uses Pods and ConfigMaps to manage Vector instances
- **Status Query**: Obtains task status via K8s API Pod/Job status queries
- **Config Management**: Manages task configs via ConfigMaps with hot-reload support
- **Task Listing**: Lists ConfigMaps to get all tasks
- **Simplified Ops**: Leverages K8s native capabilities, no extra storage or management components

### 3.2 Component Description

#### 3.2.1 Management API

**Functions:**
- **Task Management**: Create, update, delete, and query backup tasks via K8s API
- **Task Scheduling**: Manage execution of scheduled and one-time tasks
- **Status Monitoring**: Monitor task status, collect logs and metrics via K8s API and Vector API
- **Config Management**: Manage task configs via ConfigMaps, no database required

**Core Features:**
- RESTful API interface
- Task type distinction (scheduled vs one-time)
- Manage Pods and ConfigMaps via K8s API
- Config stored in ConfigMaps with hot-reload support
- Task status from Pod status
- No database; all info from K8s resources

#### 3.2.2 Task Type Definitions

##### 3.2.2.1 Scheduled Tasks

**Characteristics:**
- Execute at fixed intervals (e.g., hourly, daily)
- All scheduled tasks share one Vector instance
- Config files stored in a unified directory; Vector monitors directory changes
- Config reloaded automatically after update; no Vector restart needed

**Config Example:**
```yaml
task:
  id: scheduled-backup-001
  name: "Daily Backup"
  type: "scheduled"  # Scheduled task
  schedule:
    type: "cron"  # or "interval"
    cron: "0 2 * * *"  # Run daily at 2:00 AM
    # or use interval: "24h"
  cluster: tidb-cluster-01
  data_types: ["logs", "metrics"]
  filters: { ... }
  target: { ... }
```

**K8s Deployment:**
- **Pod**: Single long-running Pod (`vector-scheduled`)
- **ConfigMap**: One ConfigMap per task (`vector-task-scheduled-{id}`)
- **Config Mount**: ConfigMap mounted to Pod's `/vector/configs/`
- **Auto Reload**: Vector watches config directory, loads new ConfigMaps and reloads modified configs
- **Status Query**: Get task run status via K8s API Pod status

##### 3.2.2.2 One-time Tasks

**Characteristics:**
- Execute once then terminate
- Each task starts its own Vector process
- Vector process exits after task completes
- Suitable for on-demand backup, ad-hoc backup

**Config Example:**
```yaml
task:
  id: onetime-backup-001
  name: "Ad-hoc Backup"
  type: "onetime"  # One-time task
  time_range:
    start: "2024-01-01T00:00:00Z"
    end: "2024-01-01T23:59:59Z"
  cluster: tidb-cluster-01
  data_types: ["logs", "slowlogs", "sqlstatements", "metrics"]
  filters: { ... }
  target: { ... }
```

**K8s Deployment:**
- **Pod**: One Pod per task (`vector-task-onetime-{id}`)
- **ConfigMap**: One ConfigMap per task (`vector-task-onetime-{id}`)
- **Config Mount**: ConfigMap mounted to Pod's `/vector/config/vector.toml`
- **Lifecycle**: Pod exits when task completes; management cleans up Pod and ConfigMap
- **Status Query**: Get task execution status via K8s API Pod status

#### 3.2.3 Vector Instance Management Strategy (K8s-based)

##### 3.2.3.1 Scheduled Task Vector Pod

**K8s Resources:**
- **Pod**: `vector-scheduled` (Deployment or StatefulSet)
- **ConfigMaps**: `vector-task-scheduled-{id}` (one per task)

**Pod Config Example:**
```yaml
apiVersion: v1
kind: Pod
metadata:
  name: vector-scheduled
  namespace: backup-system
spec:
  containers:
  - name: vector
    image: vector:latest
    command: ["vector"]
    args: ["--config-dir", "/vector/configs", "--watch-config"]
    volumeMounts:
    - name: configs
      mountPath: /vector/configs
      readOnly: true
  volumes:
  - name: configs
    projected:
      sources:
      # Dynamically mount all scheduled task ConfigMaps
      - configMap:
          name: vector-task-scheduled-001
      - configMap:
          name: vector-task-scheduled-002
      # ... more ConfigMaps
```

**ConfigMap Config Example:**
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: vector-task-scheduled-001
  namespace: backup-system
data:
  task-001.toml: |
    # Vector config content
    [sources.s3_logs]
    type = "aws_s3"
    # ...
```

**Management Flow:**
1. **Create Task**: Management creates ConfigMap; Pod auto-detects and loads
2. **Update Task**: Management updates ConfigMap; Vector auto-reloads config
3. **Delete Task**: Management deletes ConfigMap; Vector auto-removes task
4. **Status Query**: Query Pod status via K8s API

**Benefits:**
- **No Database**: Config stored in ConfigMap
- **Auto Reload**: Vector watches ConfigMap changes and auto-reloads
- **Resource Efficient**: Multiple tasks share one Pod
- **K8s Native**: Uses K8s config management

##### 3.2.3.2 One-time Task Vector Pod

**K8s Resources:**
- **Pod**: `vector-task-onetime-{id}` (Job or Pod)
- **ConfigMap**: `vector-task-onetime-{id}` (one per task)

**Pod Config Example:**
```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: vector-task-onetime-001
  namespace: backup-system
spec:
  ttlSecondsAfterFinished: 3600  # Auto-cleanup 1 hour after completion
  template:
    spec:
      containers:
      - name: vector
        image: vector:latest
        command: ["vector"]
        args: ["--config", "/vector/config/vector.toml"]
        volumeMounts:
        - name: config
          mountPath: /vector/config
          readOnly: true
      volumes:
      - name: config
        configMap:
          name: vector-task-onetime-001
      restartPolicy: Never  # No restart after task completion
```

**ConfigMap Config Example:**
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: vector-task-onetime-001
  namespace: backup-system
data:
  vector.toml: |
    # Vector config content
    [sources.s3_logs]
    type = "aws_s3"
    # ...
```

**Management Flow:**
1. **Create Task**: Management creates ConfigMap and Job
2. **Execute Task**: Job starts Pod to run task
3. **Monitor Status**: Query Job/Pod status via K8s API
4. **Cleanup**: After completion, Job's `ttlSecondsAfterFinished` auto-cleans, or management cleans manually

**Benefits:**
- **Good Isolation**: Each task has its own Pod
- **Auto Cleanup**: Uses Job TTL for auto cleanup
- **Clear Status**: Job status indicates execution status
- **K8s Native**: Uses K8s Job lifecycle

#### 3.2.4 Task Config Manager

**Functions:**
- Parse user-provided task config (YAML/JSON)
- Select Vector config generation strategy by task type
- Generate Vector TOML config files
- Manage config versions and change history

**Config Generation Strategy:**

**Scheduled Tasks:**
- Generate config to `/vector/configs/scheduled/`
- Filename format: `task-{id}.toml`
- Config includes task ID as identifier

**One-time Tasks:**
- Generate temp config to `/tmp/vector-tasks/`
- Filename format: `task-{id}-{timestamp}.toml`
- Auto-delete after completion

#### 3.2.5 Vector Execution Engine

**Responsibilities:**
- Execute data collection per config
- Apply filter rules
- Transform data format
- Write to target storage

**Key Features:**
- Uses Vector plugins (Source, Transform, Sink)
- Supports parallel processing of multiple sources
- Supports streaming and batch processing
- Supports checkpoint/resume

## 4. Data Source Definitions

### 4.1 Log Data Sources

#### 4.1.1 S3 Compressed Logs

**Characteristics:**
- File format: `.log.gz` (gzip)
- Storage: S3 bucket
- Naming: Often includes time info, e.g. `logs/2024/01/01/app-*.log.gz`

**Vector Config:**
```toml
[sources.s3_logs]
type = "aws_s3"
region = "us-west-2"
bucket = "logs-bucket"
key_prefix = "tidb-cluster-01/logs/"
compression = "gzip"
# Time filter: process only files in the specified time range
file_time_filter = { start = "2024-01-01T00:00:00Z", end = "2024-01-01T23:59:59Z" }
```

#### 4.1.2 Loki Logs

**Characteristics:**
- Query logs via Loki API
- Supports LogQL
- Supports label filtering

**Vector Config:**
```toml
[sources.loki_logs]
type = "loki"
endpoint = "http://loki-server:3100"
# Use LogQL to query logs for specified cluster and time range
query = '{cluster="tidb-cluster-01"}'
start_time = "2024-01-01T00:00:00Z"
end_time = "2024-01-01T23:59:59Z"
```

#### 4.1.3 Parquet Statistics Files

**Characteristics:**
- File format: `.parquet`
- Usually generated hourly
- Contains aggregated statistics

**Vector Config:**
```toml
[sources.parquet_stats]
type = "file"
include = ["s3://stats-bucket/tidb-cluster-01/stats/hourly/*.parquet"]
# Need to parse parquet format
[transforms.parse_parquet]
type = "parse_parquet"
inputs = ["parquet_stats"]
```

### 4.2 Slow Query Log Data Sources

#### 4.2.1 Database Table

**Characteristics:**
- Stored in system tables (e.g. `information_schema.slow_query`)
- Requires SQL query to fetch
- Supports time range filtering

**Vector Config:**
```toml
[sources.slow_query_db]
type = "sql"
connection_string = "mysql://user:pass@tidb-server:4000/information_schema"
query = """
  SELECT * FROM slow_query
  WHERE time >= ? AND time <= ?
"""
query_params = ["2024-01-01T00:00:00Z", "2024-01-01T23:59:59Z"]
interval = "1m"  # Poll interval
```

#### 4.2.2 S3 Files

**Characteristics:**
- Slow query logs stored as files on S3
- May be text or JSON

**Vector Config:**
```toml
[sources.slow_query_s3]
type = "aws_s3"
bucket = "logs-bucket"
key_prefix = "tidb-cluster-01/slowlogs/"
file_time_filter = { start = "2024-01-01T00:00:00Z", end = "2024-01-01T23:59:59Z" }
```

### 4.3 SQL Statement Data Sources

#### 4.3.1 Database Table

**Characteristics:**
- Stored in system tables (e.g. `information_schema.statements_summary`)
- Contains SQL execution statistics

**Vector Config:**
```toml
[sources.sql_statements_db]
type = "sql"
connection_string = "mysql://user:pass@tidb-server:4000/information_schema"
query = """
  SELECT * FROM statements_summary
  WHERE summary_begin_time >= ? AND summary_end_time <= ?
"""
query_params = ["2024-01-01T00:00:00Z", "2024-01-01T23:59:59Z"]
```

#### 4.3.2 API Interface

**Characteristics:**
- Fetch data via HTTP API
- Usually returns JSON

**Vector Config:**
```toml
[sources.sql_statements_api]
type = "http"
url = "http://tidb-server:10080/api/v1/statements"
method = "GET"
headers = { "Content-Type" = "application/json" }
# Query params include time range
query_params = {
  start_time = "2024-01-01T00:00:00Z",
  end_time = "2024-01-01T23:59:59Z"
}
```

### 4.4 Metrics Data Sources

#### 4.4.1 Prometheus

**Characteristics:**
- Export data via Prometheus Query API
- Supports PromQL
- Supports time range queries

**Vector Config:**
```toml
[sources.prometheus_metrics]
type = "prometheus"
endpoint = "http://prometheus:9090"
# Query metrics for specified cluster
query = 'up{cluster="tidb-cluster-01"}'
start_time = "2024-01-01T00:00:00Z"
end_time = "2024-01-01T23:59:59Z"
step = "30s"  # Sampling interval
```

#### 4.4.2 VictoriaMetrics

**Characteristics:**
- Prometheus API compatible
- Supports more efficient data export

**Vector Config:**
```toml
[sources.vm_metrics]
type = "prometheus"  # Use prometheus source, compatible with VM
endpoint = "http://vm:8428"
query = '{cluster="tidb-cluster-01"}'
start_time = "2024-01-01T00:00:00Z"
end_time = "2024-01-01T23:59:59Z"
```

## 5. Filter Rule Definitions

### 5.1 Filter Rule Types

#### 5.1.1 Keyword Filter

**Purpose:** Filter data by keyword match

**Config:**
```yaml
filter:
  type: keyword
  keywords:
    - "ERROR"
    - "WARN"
    - "critical"
  match_mode: "any"  # any: match any keyword, all: match all keywords
  case_sensitive: false
```

**Vector Implementation:**
```toml
[transforms.keyword_filter]
type = "filter"
inputs = ["source"]
condition = '''
  contains(.message, "ERROR") or
  contains(.message, "WARN") or
  contains(.message, "critical")
'''
```

#### 5.1.2 Regex Filter

**Purpose:** Complex pattern matching with regular expressions

**Config:**
```yaml
filter:
  type: regex
  pattern: ".*timeout.*|.*connection.*failed.*"
  field: "message"  # Field to match
```

**Vector Implementation:**
```toml
[transforms.regex_filter]
type = "filter"
inputs = ["source"]
condition = '.message =~ /timeout|connection.*failed/'
```

#### 5.1.3 Field Value Filter

**Purpose:** Filter by field value (numeric comparison, string match, etc.)

**Config:**
```yaml
filter:
  type: field
  field: "execution_time"
  operator: ">"  # >, <, >=, <=, ==, !=
  value: "1s"
```

**Vector Implementation:**
```toml
[transforms.field_filter]
type = "filter"
inputs = ["source"]
condition = '.execution_time > 1.0'
```

#### 5.1.4 Time Range Filter

**Purpose:** Finer-grained time filtering at data source or transform level

**Config:**
```yaml
filter:
  type: time_range
  field: "timestamp"
  start: "2024-01-01T10:00:00Z"
  end: "2024-01-01T12:00:00Z"
```

**Vector Implementation:**
```toml
[transforms.time_filter]
type = "filter"
inputs = ["source"]
condition = '''
  .timestamp >= "2024-01-01T10:00:00Z" and
  .timestamp <= "2024-01-01T12:00:00Z"
'''
```

### 5.2 Filter Rule Combination

Support combining multiple filters (AND/OR logic):

```yaml
filters:
  logs:
    enabled: true
    logic: "AND"  # AND: all rules must match, OR: any rule matches
    rules:
      - type: keyword
        keywords: ["ERROR", "WARN"]
      - type: regex
        pattern: ".*timeout.*"
```

## 6. Target Storage Definitions

### 6.1 S3 Storage

**Purpose:** Backup to S3 bucket

**Vector Config:**
```toml
[sinks.backup_s3]
type = "aws_s3"
inputs = ["filtered_data"]
bucket = "backup-bucket"
key_prefix = "backups/tidb-cluster-01/2024-01-01/"
# Organize files by data type
compression = "gzip"
encoding = { codec = "json" }
```

### 6.2 Local File System

**Purpose:** Backup to local file system

**Vector Config:**
```toml
[sinks.backup_file]
type = "file"
inputs = ["filtered_data"]
path = "/backup/tidb-cluster-01/2024-01-01/"
filename = "backup-%{data_type}-%{+YYYY-MM-dd-HH}.log"
compression = "gzip"
```

## 7. Vector Config Generation Specification

### 7.1 Config Generation Flow

```
User Config
  ↓
Parse Config
  ├─ Data source mapping (by cluster and data source config)
  ├─ Apply time range
  ├─ Convert filter rules
  └─ Target storage config
  ↓
Generate Vector TOML Config
  ↓
Execute Vector
```

### 7.2 Config Template Structure

```toml
# Vector config template
data_dir = "/var/lib/vector"

# Data source config (generated dynamically by source type)
[sources.<source_name>]
type = "<source_type>"
# ... source-specific config

# Transforms (decompress, parse, etc.)
[transforms.<transform_name>]
type = "<transform_type>"
inputs = ["<source_name>"]
# ... transform-specific config

# Filter rules (generated from user config)
[transforms.<filter_name>]
type = "filter"
inputs = ["<previous_transform>"]
condition = "<filter_condition>"

# Enrichment (add metadata)
[transforms.enrich]
type = "add_fields"
inputs = ["<filter_name>"]
fields.backup_id = "<backup_id>"
fields.cluster = "<cluster>"
fields.backup_time = "<timestamp>"

# Target storage
[sinks.<sink_name>]
type = "<sink_type>"
inputs = ["enrich"]
# ... sink-specific config
```

### 7.3 Config Generation Example

**Input Config:**
```yaml
backup_task:
  cluster: tidb-cluster-01
  time_range:
    start: "2024-01-01T00:00:00Z"
    end: "2024-01-01T23:59:59Z"
  data_types: ["logs"]
  filters:
    logs:
      enabled: true
      rules:
        - type: keyword
          keywords: ["ERROR", "WARN"]
  target:
    type: s3
    bucket: backup-bucket
    prefix: "backups/tidb-cluster-01/2024-01-01/"
```

**Generated Vector Config:**
```toml
# Vector data directory (for checkpoint)
data_dir = "/vector/data/checkpoints/backup-20240101-001"

# Enable API for monitoring and metrics collection
[api]
enabled = true
address = "127.0.0.1:8686"
graphql_enabled = false

# S3 log data source
[sources.s3_logs]
type = "aws_s3"
region = "us-west-2"
bucket = "logs-bucket"
key_prefix = "tidb-cluster-01/logs/"
compression = "gzip"
file_time_filter = {
  start = "2024-01-01T00:00:00Z",
  end = "2024-01-01T23:59:59Z"
}
# Vector records processed file positions to data_dir automatically

# Decompress
[transforms.decompress]
type = "decompress"
inputs = ["s3_logs"]
method = "gzip"

# Parse log format
[transforms.parse_logs]
type = "parse_grok"
inputs = ["decompress"]
pattern = "%{TIMESTAMP_ISO8601:timestamp} %{LOGLEVEL:level} %{GREEDYDATA:message}"

# Keyword filter
[transforms.keyword_filter]
type = "filter"
inputs = ["parse_logs"]
condition = 'contains(.message, "ERROR") or contains(.message, "WARN")'

# Add backup metadata
[transforms.enrich]
type = "add_fields"
inputs = ["keyword_filter"]
fields.backup_id = "backup-20240101-001"
fields.cluster = "tidb-cluster-01"
fields.backup_time = "2024-01-01T12:00:00Z"
fields.data_type = "logs"

# Write to backup S3
[sinks.backup_s3]
type = "aws_s3"
inputs = ["enrich"]
bucket = "backup-bucket"
key_prefix = "backups/tidb-cluster-01/2024-01-01/logs/"
compression = "gzip"
encoding = { codec = "json" }
```

## 8. Implementation Guide

### 8.1 Development Task Breakdown

#### Task 1: Config Parsing Module

**Functions:**
- Parse user-provided backup task config (YAML/JSON)
- Validate config completeness and correctness
- Convert config to internal data structures

**Implementation Notes:**
- Define config structs (Rust struct or Go struct)
- Use config parsing libraries (e.g., serde, viper)
- Implement config validation logic

#### Task 2: Data Source Mapping Module

**Functions:**
- Determine actual data source locations from cluster name and data source config
- Generate corresponding Vector Source config

**Implementation Notes:**
- Maintain data source config mapping table (cluster -> data source config)
- Select Source by data type (logs/slowlogs/sqlstatements/metrics)
- Apply time range filter to Source config

#### Task 3: Filter Rule Conversion Module

**Functions:**
- Convert user-defined filter rules to Vector Filter Transform config
- Support multiple filter rule types
- Support rule combination (AND/OR)

**Implementation Notes:**
- Implement conversion logic for each filter type
- Generate Vector VRL (Vector Remap Language) condition expressions
- Handle rule combination logic

#### Task 4: Vector Config Generation Module

**Functions:**
- Generate complete Vector TOML config from parsed config
- Assemble Source, Transform, Sink config

**Implementation Notes:**
- Use TOML generation libraries (e.g., toml, toml_edit)
- Follow Vector config specification
- Ensure config correctness and completeness

#### Task 5: Management API Module

**Functions:**
- Provide RESTful API
- Task CRUD (Create, Read, Update, Delete)
- Task execution control (Start, Stop, Pause, Resume)
- Task status query and monitoring

**API Design:**
```rust
// Task management API
POST   /api/v1/tasks              // Create task
GET    /api/v1/tasks              // List tasks
GET    /api/v1/tasks/{id}         // Get task detail
PUT    /api/v1/tasks/{id}         // Update task
DELETE /api/v1/tasks/{id}         // Delete task

// Task execution control
POST   /api/v1/tasks/{id}/start   // Start task
POST   /api/v1/tasks/{id}/stop    // Stop task
POST   /api/v1/tasks/{id}/pause   // Pause task
POST   /api/v1/tasks/{id}/resume  // Resume task

// Task status and monitoring
GET    /api/v1/tasks/{id}/status  // Get task status
GET    /api/v1/tasks/{id}/logs    // Get task logs
GET    /api/v1/tasks/{id}/metrics // Get task metrics
```

**Implementation Notes:**
- Use web framework (e.g., Actix-web, Rocket, Axum)
- Define task data structures (scheduled vs one-time)
- **No database**: Task config stored in K8s ConfigMap
- **Status from K8s**: Query Pod/Job status via K8s API
- Map K8s Pod/Job status to task status

#### Task 6: Task Scheduler Module

**Functions:**
- Manage scheduling of scheduled tasks
- Trigger execution of one-time tasks
- Handle task dependencies

**Implementation Notes:**
- Use scheduler libraries (e.g., cron, tokio-cron-scheduler)
- Scheduled tasks: register with scheduler, trigger by schedule
- One-time tasks: execute immediately or delayed
- Implement task queue management

#### Task 7: K8s Resource Management Module

**Functions:**
- Manage scheduled task Vector Pods via K8s API
- Manage one-time task Vector Pods via K8s API
- Manage ConfigMaps via K8s API
- Monitor Pod status
- Handle Pod failures and restarts

**Scheduled Task K8s Management:**
```rust
use k8s_openapi::api::core::v1::{ConfigMap, Pod};
use kube::{Api, Client};

// Create scheduled task ConfigMap
async fn create_scheduled_task_configmap(
    client: Client,
    task_id: &str,
    vector_config: &str,
) -> Result<()> {
    let configmaps: Api<ConfigMap> = Api::namespaced(client, "backup-system");
    let configmap = ConfigMap { /* ... */ };
    configmaps.create(&PostParams::default(), &configmap).await?;
    Ok(())
}

// Update scheduled task ConfigMap
async fn update_scheduled_task_configmap(/* ... */) -> Result<()> {
    // Vector Pod detects ConfigMap changes and reloads config automatically
    Ok(())
}

// Delete scheduled task ConfigMap
async fn delete_scheduled_task_configmap(/* ... */) -> Result<()> {
    // Vector Pod detects ConfigMap deletion and removes task automatically
    Ok(())
}

// Ensure scheduled Pod exists
async fn ensure_scheduled_pod_exists(client: Client) -> Result<()> { /* ... */ }
```

**One-time Task K8s Management:**
```rust
use k8s_openapi::api::batch::v1::Job;

// Create one-time task
async fn create_onetime_task(/* ... */) -> Result<()> {
    // 1. Create ConfigMap
    // 2. Create Job
    // 3. Start monitoring and progress collection
    Ok(())
}

// Build one-time task Job
fn build_onetime_job(task_id: &str) -> Job {
    // ttl_seconds_after_finished: 3600 (auto-cleanup 1 hour after completion)
    Job { /* ... */ }
}

// Monitor Job status
fn spawn_job_monitor(task_id: String) { /* ... */ }
```

**Implementation Notes:**
- Use K8s client libraries (e.g., kube-rs, client-go)
- Query Pod/Job status via K8s API
- Query task progress via Vector API
- Read and parse task config from ConfigMap
- No database; all info from K8s resources

#### Task 8: Task Status Query Module

**Functions:**
- Query Pod/Job status via K8s API
- Query task progress via Vector API
- Read task config from ConfigMap
- Aggregate task status info

### 8.2 Vector Plugin Usage Guide

#### 8.2.1 Source Plugins (Sources)

**S3 Source:**
- Plugin: `vector/sources-aws_s3`
- Docs: https://vector.dev/docs/reference/configuration/sources/aws_s3/
- Key config: bucket, key_prefix, compression, region

**Loki Source:**
- Plugin: `vector/sources-loki` (if exists) or HTTP Source
- Alternative: Use `http` source to call Loki API
- Key config: endpoint, query, headers

**Database Source:**
- Plugin: `vector/sources-sql` (if exists) or custom source
- Alternative: Use `http` source or custom source
- Key config: connection_string, query, interval

**Prometheus Source:**
- Plugin: `vector/sources-prometheus` (if exists)
- Alternative: Use `http` source to call Prometheus Query API
- Key config: endpoint, query, start_time, end_time

#### 8.2.2 Transform Plugins (Transforms)

**Decompress:**
- Plugin: `vector/transforms-decompress`
- Docs: https://vector.dev/docs/reference/configuration/transforms/decompress/
- Formats: gzip, zlib, snappy, lz4

**Parse:**
- Plugins: `parse_grok`, `parse_json`, `parse_regex`
- Docs: https://vector.dev/docs/reference/configuration/transforms/
- Choose parser by log format

**Filter:**
- Plugin: `vector/transforms-filter`
- Docs: https://vector.dev/docs/reference/configuration/transforms/filter/
- Use VRL condition expressions

**Field operations:**
- Plugins: `add_fields`, `remove_fields`, `rename_fields`
- For adding backup metadata

#### 8.2.3 Sink Plugins (Sinks)

**S3 Sink:**
- Plugin: `vector/sinks-aws_s3`
- Docs: https://vector.dev/docs/reference/configuration/sinks/aws_s3/
- Key config: bucket, key_prefix, compression, encoding

**File Sink:**
- Plugin: `vector/sinks-file`
- Docs: https://vector.dev/docs/reference/configuration/sinks/file/
- Key config: path, filename, compression

### 8.3 Suggested Code Structure

```
project/
├── src/
│   ├── api/                    # Management API module
│   │   ├── mod.rs
│   │   ├── handlers/
│   │   │   ├── tasks.rs
│   │   │   ├── clusters.rs
│   │   │   └── health.rs
│   │   ├── models/
│   │   │   ├── task.rs
│   │   │   └── response.rs
│   │   └── routes.rs
│   ├── config/
│   │   ├── mod.rs
│   │   ├── backup_task.rs
│   │   ├── data_source.rs
│   │   ├── filter.rs
│   │   ├── target.rs
│   │   └── task_type.rs
│   ├── scheduler/
│   │   ├── mod.rs
│   │   ├── cron_scheduler.rs
│   │   ├── task_queue.rs
│   │   └── trigger.rs
│   ├── vector_manager/
│   │   ├── mod.rs
│   │   ├── scheduled.rs
│   │   ├── onetime.rs
│   │   ├── process_manager.rs
│   │   └── config_manager.rs
│   ├── mapper/
│   │   ├── mod.rs
│   │   ├── source_mapper.rs
│   │   └── cluster_config.rs
│   ├── filter/
│   │   ├── mod.rs
│   │   ├── keyword_filter.rs
│   │   ├── regex_filter.rs
│   │   ├── field_filter.rs
│   │   └── vrl_generator.rs
│   ├── vector/
│   │   ├── mod.rs
│   │   ├── config_generator.rs
│   │   ├── source_builder.rs
│   │   ├── transform_builder.rs
│   │   └── sink_builder.rs
│   ├── k8s/
│   │   ├── mod.rs
│   │   ├── client.rs
│   │   ├── configmap.rs
│   │   ├── pod.rs
│   │   ├── job.rs
│   │   └── status.rs
│   ├── monitor/
│   │   ├── mod.rs
│   │   ├── task_monitor.rs
│   │   └── metrics.rs
│   └── main.rs
├── config/
│   ├── cluster_config.yaml
│   └── backup_task.yaml
└── tests/
    ├── unit/
    └── integration/
```

### 8.4 Key Implementation Details

#### 8.4.1 Time Range Handling

- Use ISO 8601 format: `2024-01-01T00:00:00Z`
- Support timezone conversion
- Apply time filter at source level when supported
- Apply secondary time filter at transform level for precision

#### 8.4.2 Filter Rule Implementation

- Keyword: VRL `contains()` function
- Regex: VRL regex `=~`
- Field: VRL comparison operators
- Combination: VRL `and`/`or`

#### 8.4.3 Error Handling

- Source connection failure: Retry, log error
- Parse failure: Skip bad data, log warning
- Write failure: Retry, dead letter queue
- Task timeout: Set timeout, terminate on exceed

#### 8.4.4 Performance Optimization

- Parallel multi-source processing
- Batch I/O
- Compress data in transit
- Stream large files

#### 8.4.5 Task Reliability (Checkpoint, Monitoring, Completion)

- **Checkpoint**: Use Vector data_dir and/or custom checkpoint for resume
- **Pod/Job monitoring**: Monitor via K8s API; restart from checkpoint on failure
- **Completion**: Use source completion state, Vector exit code, and target verification

## 9. Config Examples

### 9.1 Scheduled Task Config

```yaml
task:
  id: scheduled-backup-001
  name: "Daily Cluster Backup"
  type: "scheduled"
  enabled: true

  schedule:
    type: "cron"
    cron: "0 2 * * *"  # Daily at 2:00 AM
    timezone: "UTC"

  cluster: tidb-cluster-01

  time_range:
    type: "relative"
    offset: "-24h"  # Past 24 hours

  data_types:
    - logs
    - metrics

  filters:
    logs:
      enabled: true
      rules:
        - type: keyword
          keywords: ["ERROR", "WARN"]

  target:
    type: s3
    bucket: backup-bucket
    prefix: "backups/tidb-cluster-01/daily/"
    compression: "gzip"

  options:
    timeout: "2h"
    retry:
      max_attempts: 3
```

### 9.2 One-time Task Config

```yaml
task:
  id: onetime-backup-001
  name: "Ad-hoc Backup for Incident"
  type: "onetime"
  enabled: true

  time_range:
    type: "absolute"
    start: "2024-01-01T00:00:00Z"
    end: "2024-01-01T23:59:59Z"
    timezone: "UTC"

  cluster: tidb-cluster-01

  data_types:
    - logs
    - slowlogs
    - sqlstatements
    - metrics

  filters:
    logs:
      enabled: true
      logic: "OR"
      rules:
        - type: keyword
          keywords: ["ERROR", "WARN", "critical"]
        - type: regex
          pattern: ".*timeout.*"

    slowlogs:
      enabled: true

    sqlstatements:
      enabled: true
      rules:
        - type: field
          field: "execution_time"
          operator: ">"
          value: "1s"

  target:
    type: s3
    bucket: backup-bucket
    prefix: "backups/tidb-cluster-01/incident-20240101/"
    compression: "gzip"

  options:
    timeout: "4h"
    retry:
      max_attempts: 3
      backoff: "exponential"
```

### 9.3 Full Backup Task Config (Generic Format)

```yaml
backup_task:
  id: backup-20240101-001
  cluster: tidb-cluster-01
  time_range:
    start: "2024-01-01T00:00:00Z"
    end: "2024-01-01T23:59:59Z"
    timezone: "UTC"

  data_types:
    - logs
    - slowlogs
    - sqlstatements
    - metrics

  filters:
    logs:
      enabled: true
      logic: "OR"
      rules:
        - type: keyword
          keywords: ["ERROR", "WARN", "critical"]
          case_sensitive: false
        - type: regex
          pattern: ".*timeout.*"
          field: "message"

    slowlogs:
      enabled: false

    sqlstatements:
      enabled: true
      logic: "AND"
      rules:
        - type: field
          field: "execution_time"
          operator: ">"
          value: "1s"
        - type: keyword
          keywords: ["SELECT", "UPDATE", "DELETE"]
          field: "sql_text"

    metrics:
      enabled: false

  target:
    type: s3
    bucket: backup-bucket
    prefix: "backups/tidb-cluster-01/2024-01-01/"
    compression: "gzip"
    encryption: true

  options:
    parallel_sources: true
    batch_size: 1000
    timeout: "2h"
    retry:
      max_attempts: 3
      backoff: "exponential"
```

### 9.4 Cluster Data Source Config

```yaml
clusters:
  tidb-cluster-01:
    logs:
      s3:
        bucket: "logs-bucket"
        region: "us-west-2"
        prefix: "tidb-cluster-01/logs/"
        compression: "gzip"
      loki:
        endpoint: "http://loki-server:3100"
        query_template: '{cluster="tidb-cluster-01"}'
      parquet:
        bucket: "stats-bucket"
        prefix: "tidb-cluster-01/stats/hourly/"

    slowlogs:
      database:
        connection_string: "mysql://user:pass@tidb-server:4000/information_schema"
        table: "slow_query"
        time_field: "time"
      s3:
        bucket: "logs-bucket"
        prefix: "tidb-cluster-01/slowlogs/"

    sqlstatements:
      database:
        connection_string: "mysql://user:pass@tidb-server:4000/information_schema"
        table: "statements_summary"
        time_field: "summary_begin_time"
      api:
        endpoint: "http://tidb-server:10080/api/v1/statements"

    metrics:
      prometheus:
        endpoint: "http://prometheus:9090"
        query_template: '{cluster="tidb-cluster-01"}'
      victoriametrics:
        endpoint: "http://vm:8428"
        query_template: '{cluster="tidb-cluster-01"}'
```

### 9.5 Management Config

```yaml
management:
  api:
    host: "0.0.0.0"
    port: 8080
    enable_cors: true

  kubernetes:
    namespace: "backup-system"

  vector:
    image: "vector:latest"
    scheduled_pod_name: "vector-scheduled"
    onetime_job:
      ttl_seconds_after_finished: 3600

  scheduler:
    cron:
      enabled: true
      timezone: "UTC"
    queue:
      max_concurrent_tasks: 10
      task_timeout: "4h"

  monitoring:
    enabled: true
    metrics_port: 9090
    log_level: "info"
  # No database config; all task info in K8s ConfigMaps
```

### 9.6 API Request Examples

**Create scheduled task:**
```bash
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Daily Backup",
    "type": "scheduled",
    "schedule": {
      "type": "cron",
      "cron": "0 2 * * *"
    },
    "cluster": "tidb-cluster-01",
    "time_range": {
      "type": "relative",
      "offset": "-24h"
    },
    "data_types": ["logs", "metrics"],
    "target": {
      "type": "s3",
      "bucket": "backup-bucket",
      "prefix": "backups/tidb-cluster-01/daily/"
    }
  }'
```

**Create one-time task:**
```bash
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Ad-hoc Backup",
    "type": "onetime",
    "time_range": {
      "type": "absolute",
      "start": "2024-01-01T00:00:00Z",
      "end": "2024-01-01T23:59:59Z"
    },
    "cluster": "tidb-cluster-01",
    "data_types": ["logs", "slowlogs", "sqlstatements", "metrics"],
    "target": {
      "type": "s3",
      "bucket": "backup-bucket",
      "prefix": "backups/tidb-cluster-01/incident-20240101/"
    }
  }'
```

**Query task status:**
```bash
curl http://localhost:8080/api/v1/tasks/scheduled-backup-001/status
```

**Stop task:**
```bash
curl -X POST http://localhost:8080/api/v1/tasks/scheduled-backup-001/stop
```

## 10. Testing and Validation

### 10.1 Unit Tests

- Config parsing
- Filter rule conversion
- Vector config generation

### 10.2 Integration Tests

- End-to-end backup flow
- Multi-source backup
- Filter behavior
- Error handling

### 10.3 Performance Tests

- Large data backup
- Concurrent backups
- Filter performance

## 11. Appendix

### 11.1 Vector Resources

- Vector docs: https://vector.dev/docs/
- Vector config reference: https://vector.dev/docs/reference/configuration/
- VRL reference: https://vector.dev/docs/reference/vrl/

### 11.2 Data Format References

- ISO 8601: https://en.wikipedia.org/wiki/ISO_8601
- Parquet: https://parquet.apache.org/
- Prometheus format: https://prometheus.io/docs/instrumenting/exposition_formats/

### 11.3 Glossary

- **Cluster**: A TiDB cluster instance
- **Diagnostic Data**: Logs, slow queries, SQL statements, metrics, etc.
- **Filter**: Rules to select data for backup
- **Source**: Vector data source plugin
- **Transform**: Vector transform plugin
- **Sink**: Vector sink plugin
- **VRL**: Vector Remap Language, Vector's expression language
