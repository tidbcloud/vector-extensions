# Vector Extensions Demo - Architecture Documentation

## System Architecture

### Overall Architecture

```
┌─────────────┐
│   Client    │
│  (curl/API) │
└──────┬──────┘
       │ HTTP REST API
       ↓
┌─────────────────────────────────────┐
│      Flask API Server (app.py)      │
│  ┌──────────────────────────────┐  │
│  │  Task Management              │  │
│  │  - Create/Query/Delete Tasks  │  │
│  └──────────────────────────────┘  │
│  ┌──────────────────────────────┐  │
│  │  Data Preprocessing           │  │
│  │  - S3 Parquet → JSON Lines    │  │
│  │  - Time Range Filtering       │  │
│  └──────────────────────────────┘  │
│  ┌──────────────────────────────┐  │
│  │  Vector Config Generation     │  │
│  │  - Generate TOML Config      │  │
│  └──────────────────────────────┘  │
│  ┌──────────────────────────────┐  │
│  │  Process Management           │  │
│  │  - Start Vector Process       │  │
│  │  - Monitor Process Status     │  │
│  └──────────────────────────────┘  │
└──────┬──────────────────────────────┘
       │
       ├─────────────────┬─────────────────┐
       ↓                 ↓                 ↓
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│  S3 (Parquet)│  │  Vector      │  │  MySQL       │
│              │  │  Process     │  │              │
│  - Read      │  │  - Process   │  │  - Import    │
│  - Filter    │  │  - Transform │  │  - Store     │
│  - Convert   │  │  - Output    │  │              │
└──────────────┘  └──────┬───────┘  └──────────────┘
                         │
                         ↓
                  ┌──────────────┐
                  │ File Output  │
                  │ (JSON Lines) │
                  └──────┬───────┘
                         │
                         ↓
                  ┌──────────────┐
                  │ Background   │
                  │ Thread       │
                  │ - Monitor    │
                  │ - Import     │
                  └──────────────┘
```

## Core Components

### 1. Flask API Server (`app.py`)

**Responsibilities**:
- Provide RESTful API interfaces
- Task lifecycle management
- Data preprocessing
- Vector configuration generation
- Process management

**Main Functions**:
- `POST /api/v1/tasks` - Create task
- `GET /api/v1/tasks` - List all tasks
- `GET /api/v1/tasks/{id}` - Query task status
- `DELETE /api/v1/tasks/{id}` - Delete task
- `GET /health` - Health check

### 2. Data Preprocessing Module

**Functions**:
- Read Parquet files from S3
- Time range filtering (file level + row level)
- Data format conversion (structured → text)
- Output JSON Lines format

**Implementation** (`preprocess_parquet_to_jsonl`):
```python
1. List S3 Parquet files
2. Filter files by time range (based on date=YYYYMMDD in path)
3. Read Parquet files
4. Filter by timestamp in row data (time field)
5. Convert to slowlog text format
6. Write to JSON Lines file
```

### 3. Vector Configuration Generation

**Functions**:
- Automatically generate Vector TOML configuration
- Configure data source (file source)
- Configure transforms (parse_json, filter)
- Configure output (file sink)

**Configuration Structure**:
```toml
[sources.jsonl_source]
type = "file"
include = ["/path/to/input.jsonl"]

[transforms.parse_json]
type = "remap"
inputs = ["jsonl_source"]
source = "parsed = parse_json!(string!(.message))"

[sinks.file_sink]
type = "file"
inputs = ["parse_json"]
path = "/tmp/vector-output/{task_id}/output.jsonl"
```

### 4. Vector Process Management

**Functions**:
- Start Vector process
- Monitor process status
- Automatic fallback (if Vector is unavailable)

**Implementation**:
- Auto-detect Vector binary (`target/debug/vector` or `target/release/vector`)
- If Vector is available → use Vector processing mode
- If Vector is unavailable → automatically switch to direct import mode

### 5. MySQL Import Module

**Functions**:
- Real-time monitoring of Vector output files
- Parse JSON Lines line by line
- Batch write to MySQL

**Implementation** (`import_to_mysql`):
```python
1. Monitor output directory
2. Detect new files
3. Read JSON Lines line by line
4. Batch insert to MySQL (batch_size=100)
5. Log progress
```

## Data Flow

### Complete Flow

```
1. API Request
   ↓
2. Data Preprocessing
   - S3 Parquet → JSON Lines
   - Time range filtering
   ↓
3. Vector Configuration Generation
   - Generate TOML configuration
   ↓
4. Vector Process Start (if available)
   - Read JSON Lines
   - Parse and filter
   - Output to file
   ↓
5. Background Thread Monitoring
   - Monitor output files
   - Batch import to MySQL
```

### Time Range Filtering

**File-Level Filtering**:
- Based on `date=YYYYMMDD` in S3 path
- Example: `deltalake/slowlogs/date=20250606/part-xxx.parquet`

**Row-Level Filtering**:
- Based on `time` field in Parquet data
- Supports `start_time` and `end_time` parameters

### Data Format Conversion

**Input**: Parquet structured data
```json
{
  "time": 1749204000.0,
  "db": "db1",
  "user": "u1",
  "host": "h1",
  "query_time": "0.1",
  "result_rows": 0,
  "prev_stmt": "d3"
}
```

**Output**: Slowlog text format
```
# Time: 1749204000.0 | DB: db1 | User: u1@h1 | Query_time: 0.1 | Rows: 0 | SQL: d3
```

## Technology Stack

### Backend
- **Python 3.8+**
- **Flask** - Web framework
- **boto3** - AWS SDK
- **pyarrow** - Parquet file processing
- **pymysql** - MySQL client

### Data Processing
- **Vector** - Data pipeline tool
- **Parquet** - Columnar storage format
- **JSON Lines** - Text format

### Storage
- **Amazon S3** - Data source
- **MySQL** - Data destination

## Design Decisions

### 1. Why Use Python for Preprocessing?

- Parquet file processing requires complex library support
- Vector's Parquet source may not support complex time filtering
- Python provides better flexibility and debugging capabilities

### 2. Why Use Files as Intermediate Format?

- Vector doesn't have a native MySQL sink
- File format is convenient for debugging and monitoring
- Supports real-time streaming processing

### 3. Why Support Automatic Fallback?

- Improves system availability
- Can still work when Vector is unavailable
- Convenient for development and testing

## Performance Considerations

### Batch Processing
- MySQL import uses batch insert (batch_size=100)
- Reduces database connection overhead

### Concurrent Processing
- Each task is an independent process
- Background thread for asynchronous import

### Resource Management
- Vector processes are automatically cleaned up
- Temporary files are automatically cleaned up

## Scalability

### Horizontal Scaling
- API server can be deployed with multiple instances
- Each task is processed independently

### Vertical Scaling
- Can increase batch size
- Can increase concurrent task count

## Security

### AWS Credentials
- Passed via environment variables
- Not hardcoded in code

### MySQL Connection
- Connection string passed via API
- Supports SSL connection (if configured)

## Monitoring and Logging

### Logging
- Flask application logs
- Vector process logs
- MySQL import logs

### Status Query
- Task status API
- Process PID tracking

## Future Improvements

1. **Task Progress Query** - Get detailed progress via Vector API
2. **Task Pause/Resume** - Support task control
3. **Error Retry Mechanism** - Automatically retry failed tasks
4. **K8s Deployment** - Use Pods and ConfigMaps
5. **Metrics Collection** - Prometheus metrics
6. **Log Aggregation** - Centralized log management
