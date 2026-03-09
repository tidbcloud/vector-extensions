# Vector Extensions Demo - AI Agent Guide

This document provides guidance for AI agents on system implementation and development.

## System Overview

This is a Vector-based data synchronization system demo that demonstrates how to control Vector via API to perform slowlog backup tasks from S3 to MySQL.

## Core Features

1. **API Server** - Flask RESTful API providing task management interfaces
2. **Data Preprocessing** - Read Parquet files from S3, convert to JSON Lines
3. **Vector Integration** - Automatically generate Vector configuration, start Vector process
4. **MySQL Import** - Real-time monitoring and import data to MySQL

## Project Structure

```
demo/
├── app.py                    # Flask API server main program
├── requirements.txt          # Python dependencies
├── scripts/                  # Scripts directory
│   ├── 01_setup.sh          # Initialize environment
│   ├── 02_start.sh          # Start server
│   ├── 03_test.sh           # End-to-end test
│   └── 04_test_api.sh       # API test
├── config/                   # Configuration files directory
│   ├── create_mysql_table.sql
│   ├── test_request.json
│   └── example_request.json
└── tests/                    # Test scripts directory
    ├── run_full_test.py
    ├── direct_import.py
    └── ...
```

## Key Code Modules

### 1. Data Preprocessing (`preprocess_parquet_to_jsonl`)

**Location**: `app.py`

**Functions**:
- Read Parquet files from S3
- Filter by time range (file level + row level)
- Convert to slowlog text format
- Output JSON Lines

**Key Logic**:
```python
# File-level filtering (based on date=YYYYMMDD in path)
if 'date=' in key:
    date_str = key.split('date=')[1].split('/')[0]
    file_date = datetime.strptime(date_str, '%Y%m%d')
    # Filter logic...

# Row-level filtering (based on time field in data)
if 'time' in df.columns:
    start_ts = datetime.fromisoformat(start_time).timestamp()
    df = df[df['time'] >= start_ts]
```

### 2. Vector Configuration Generation (`generate_vector_config`)

**Location**: `app.py`

**Functions**:
- Generate Vector TOML configuration
- Configure data source, transforms, output

**Configuration Structure**:
```python
config = {
    "sources": {
        "jsonl_source": {
            "type": "file",
            "include": [jsonl_file],
            "read_from": "beginning"
        }
    },
    "transforms": {
        "parse_json": {
            "type": "remap",
            "inputs": ["jsonl_source"],
            "source": "parsed = parse_json!(string!(.message))"
        }
    },
    "sinks": {
        "file_sink": {
            "type": "file",
            "inputs": ["parse_json"],
            "path": f"/tmp/vector-output/{task_id}/output.jsonl"
        }
    }
}
```

### 3. Vector Process Management (`start_vector_process`)

**Location**: `app.py`

**Functions**:
- Start Vector process
- Monitor process status
- Automatic fallback (if Vector is unavailable)

**Vector Detection Logic**:
```python
def find_vector_binary():
    # 1. Check environment variable VECTOR_BINARY
    # 2. Check project target/debug/vector
    # 3. Check project target/release/vector
    # 4. Check system PATH
    # 5. Default return "vector"
```

### 4. MySQL Import (`import_to_mysql`)

**Location**: `app.py`

**Functions**:
- Real-time monitoring of Vector output files
- Parse JSON Lines line by line
- Batch write to MySQL

**Implementation**:
```python
# Monitor output directory
for file_path in output_dir.glob("*.jsonl"):
    with open(file_path, 'r') as f:
        for line in f:
            data = json.loads(line)
            batch.append((data['message'], data['timestamp'], task_id))
            
            if len(batch) >= batch_size:
                cursor.executemany(sql, batch)
                conn.commit()
```

## API Interfaces

### Create Task

**Endpoint**: `POST /api/v1/tasks`

**Request Body**:
```json
{
  "s3_bucket": "o11y-dev-shared-us-west-2",
  "s3_prefix": "deltalake/slowlogs/",
  "s3_region": "us-west-2",
  "start_time": "2025-06-06T00:00:00Z",
  "end_time": "2025-06-10T23:59:59Z",
  "mysql_connection": "mysql://root:root@localhost:3306/testdb",
  "mysql_table": "slowlogs",
  "filter_keywords": []
}
```

**Processing Flow**:
1. Validate request parameters
2. Generate task ID
3. Data preprocessing (`preprocess_parquet_to_jsonl`)
4. Generate Vector configuration (`generate_vector_config`)
5. Start Vector process or direct import (`start_vector_process` or `start_direct_import`)
6. Return task information

### Query Task Status

**Endpoint**: `GET /api/v1/tasks/{task_id}`

**Response**:
```json
{
  "task_id": "...",
  "status": "running",
  "pid": 12345,
  "created_at": "2024-01-01T10:00:00",
  "updated_at": "2024-01-01T10:00:00",
  "config": {...}
}
```

## Data Flow

```
API Request
  ↓
preprocess_parquet_to_jsonl()
  - S3 Parquet → JSON Lines
  - Time range filtering
  ↓
generate_vector_config()
  - Generate TOML configuration
  ↓
start_vector_process() or start_direct_import()
  - Start Vector or direct import
  ↓
import_to_mysql() (background thread)
  - Monitor files
  - Batch import to MySQL
```

## Environment Variables

- `VECTOR_BINARY`: Vector binary path (default: auto-detect)
- `CONFIG_DIR`: Vector configuration file directory (default: `/tmp/vector-tasks`)
- `AWS_ACCESS_KEY_ID`: AWS access key
- `AWS_SECRET_ACCESS_KEY`: AWS secret key
- `AWS_SESSION_TOKEN`: AWS session token
- `AWS_REGION`: AWS region

## Test Scripts

### 01_setup.sh
- Create MySQL database and tables
- Configure AWS credentials (prompt)

### 02_start.sh
- Check Python dependencies
- Check MySQL connection
- Auto-detect Vector binary
- Start Flask server

### 03_test.sh
- Health check
- Create backup task
- Query task status
- Check MySQL data

## Common Issues

### Vector Not Found

**Symptom**: System automatically falls back to direct import mode

**Cause**: Vector binary not in expected location

**Solution**: 
- Ensure Vector is built (`cargo build --release`)
- Or set `VECTOR_BINARY` environment variable

### S3 Access Failed

**Symptom**: `botocore.exceptions.NoCredentialsError`

**Cause**: AWS credentials not configured

**Solution**: 
- Set environment variables `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
- Or configure `~/.aws/credentials`

### MySQL Connection Failed

**Symptom**: `pymysql.err.OperationalError`

**Cause**: MySQL not running or table not created

**Solution**: 
- Run `01_setup.sh` to create table
- Check MySQL connection string

## Development Guide

### Adding New Features

1. **New API Endpoint**: Add route in `app.py`
2. **New Data Processing**: Add new preprocessing function
3. **New Vector Configuration**: Modify `generate_vector_config`

### Debugging

1. **View Logs**: Server logs output to console
2. **Check Vector Config**: `/tmp/vector-tasks/{task_id}.toml`
3. **Check Output Files**: `/tmp/vector-output/{task_id}/`
4. **Test Scripts**: Use scripts in `tests/` directory

### Testing

1. **Unit Tests**: Test individual functions
2. **Integration Tests**: Use `03_test.sh`
3. **End-to-End Tests**: Complete flow testing

## Extension Directions

1. **Task Progress Query** - Via Vector API
2. **Task Pause/Resume** - Process control
3. **Error Retry** - Automatic retry mechanism
4. **K8s Deployment** - Pods and ConfigMaps
5. **Metrics Collection** - Prometheus integration
6. **Log Aggregation** - Centralized logging

## Related Documentation

- User Guide: [readme.md](./readme.md)
- Architecture Documentation: [arch.md](./arch.md)
