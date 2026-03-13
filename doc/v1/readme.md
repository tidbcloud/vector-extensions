# Vector Extensions Demo - User Guide

## Overview

This is a Vector-based data synchronization system demo that demonstrates how to control Vector via API to perform slowlog backup tasks from S3 to MySQL.

## Quick Start

### Prerequisites

1. **Python 3.8+**
2. **Vector Binary** - Built vector image or binary (located at `target/debug/vector` or `target/release/vector`)
3. **MySQL** - Local MySQL instance (Docker or local installation)
4. **AWS Credentials** - For accessing S3 (via environment variables or `~/.aws/credentials`)

### Three-Step Setup

```bash
cd demo

# 1. Initialize environment (create MySQL tables, configure AWS credentials)
./scripts/01_setup.sh

# 2. Start API server
./scripts/02_start.sh

# 3. Run tests in another terminal
./scripts/03_test.sh
```

## Detailed Steps

### Step 1: Initialize Environment

Run the `scripts/01_setup.sh` script:

```bash
./scripts/01_setup.sh
```

This script will:
- Create MySQL database and tables
- Prompt for AWS credentials configuration

**Configure AWS Credentials** (if not configured):

```bash
export AWS_ACCESS_KEY_ID="your-access-key-id"
export AWS_SECRET_ACCESS_KEY="your-secret-access-key"
export AWS_SESSION_TOKEN="your-session-token"  # If using temporary credentials
export AWS_REGION="us-west-2"
```

### Step 2: Start Server

Run the `scripts/02_start.sh` script:

```bash
./scripts/02_start.sh
```

This script will:
- Check and install Python dependencies
- Check MySQL connection
- Auto-detect Vector binary
- Start Flask API server (`http://0.0.0.0:8080`)

### Step 3: Test

Run the `scripts/03_test.sh` script in another terminal:

```bash
./scripts/03_test.sh
```

This script will:
- Health check
- Create backup task
- Query task status
- Check MySQL data

## API Usage

### Create Backup Task

```bash
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d @config/test_request.json
```

**Request Parameters** (`config/test_request.json`):

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

**Parameter Description**:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| s3_bucket | string | Yes | S3 bucket name |
| s3_prefix | string | Yes | S3 path prefix |
| s3_region | string | No | S3 region (default: us-west-2) |
| start_time | string | No | Start time (ISO 8601 format) |
| end_time | string | No | End time (ISO 8601 format) |
| mysql_connection | string | Yes | MySQL connection string |
| mysql_table | string | Yes | MySQL table name |
| filter_keywords | array | No | Keyword filter list |

**Response**:

```json
{
  "task_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "running",
  "message": "Task created and started with PID: 12345",
  "pid": 12345
}
```

### Query Task Status

```bash
curl http://localhost:8080/api/v1/tasks/{task_id}
```

### List All Tasks

```bash
curl http://localhost:8080/api/v1/tasks
```

### Delete Task

```bash
curl -X DELETE http://localhost:8080/api/v1/tasks/{task_id}
```

## Data Flow

```
API Request (with time range)
    ↓
Python Preprocessing:
  - Read Parquet files from S3
  - Filter by time range (file level + row level)
  - Convert to JSON Lines
    ↓
Vector Processing (if available):
  - Read JSON Lines
  - Parse JSON
  - Filter (optional)
  - Write to file
    ↓
Python Background Thread:
  - Monitor Vector output files
  - Read line by line
  - Batch write to MySQL
```

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
│   ├── create_mysql_table.sql  # MySQL table creation script
│   ├── test_request.json       # Test request example
│   └── example_request.json    # Request example
└── tests/                    # Test scripts directory
    ├── run_full_test.py
    ├── direct_import.py
    └── ...
```

## Configuration

### Environment Variables

- `VECTOR_BINARY`: Vector binary path (default: auto-detect `target/debug/vector` or `target/release/vector`)
- `CONFIG_DIR`: Vector configuration file directory (default: `/tmp/vector-tasks`)
- `AWS_ACCESS_KEY_ID`: AWS access key
- `AWS_SECRET_ACCESS_KEY`: AWS secret key
- `AWS_SESSION_TOKEN`: AWS session token (if using temporary credentials)
- `AWS_REGION`: AWS region (default: us-west-2)

### MySQL Table Structure

Table structure is defined in `config/create_mysql_table.sql`:

```sql
CREATE TABLE IF NOT EXISTS slowlogs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    log_line TEXT NOT NULL,
    log_timestamp DATETIME,
    task_id VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_task_id (task_id),
    INDEX idx_timestamp (log_timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

## Troubleshooting

### Vector Process Not Started

- Check `VECTOR_BINARY` environment variable
- Check Vector configuration file: `/tmp/vector-tasks/{task_id}.toml`
- View Vector process logs

### MySQL Import Failed

- Check MySQL connection string format
- Confirm table is created (run `01_setup.sh`)
- View Python console error messages

### S3 Read Failed

- **Check AWS Credentials Configuration**:
  - Confirm environment variables are set: `echo $AWS_ACCESS_KEY_ID`
  - Or check credentials file: `cat ~/.aws/credentials`
  - Ensure credentials are set **before** starting the server
- **Verify S3 Access**:
  ```bash
  aws s3 ls s3://your-bucket-name/your-prefix/
  ```
- **Check Permissions**: Ensure credentials have `s3:GetObject` and `s3:ListBucket` permissions

## Notes

1. **Vector Binary**: The system automatically detects Vector binary in the project (`target/debug/vector` or `target/release/vector`). If not found, it will automatically fall back to direct import mode
2. **MySQL Table**: Table must be created in advance (run `01_setup.sh`)
3. **S3 Permissions**: AWS credentials are required to access S3
4. **File Monitoring**: Background thread monitors Vector output files in real-time and imports to MySQL
5. **Time Range Filtering**: Supports file-level (based on `date=YYYYMMDD` in path) and row-level (based on `time` field in data) filtering

## More Information

- Architecture Documentation: [arch.md](./arch.md)
- AI Agent Guide: [agent.md](./agent.md)
