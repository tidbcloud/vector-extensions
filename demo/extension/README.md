# Vector Extension Demo - Python Scripts

This directory contains Python scripts that demonstrate Vector extension functionality.
These scripts are executed by Vector's `exec` source and will be converted to proper
Rust-based Vector plugins in the future.

## Directory Structure

```
extension/
├── sources/          # Data source scripts (executed by Vector exec source)
├── transforms/       # Data transformation scripts (if needed)
├── sinks/            # Data sink scripts (if needed)
└── README.md         # This file
```

## Sources

### `sources/parquet_s3_processor.py`

Processes Parquet files from S3 and outputs JSON Lines to stdout.

**Usage:**
- Executed by Vector's `exec` source
- Reads configuration from environment variables:
  - `S3_BUCKET`: S3 bucket name
  - `S3_PREFIX`: S3 prefix/path
  - `S3_REGION`: AWS region (default: us-west-2)
  - `START_TIME`: ISO 8601 start time (optional)
  - `END_TIME`: ISO 8601 end time (optional)
- AWS credentials are inherited from Vector process environment

**Output:**
- JSON Lines to stdout, one event per line
- Each event contains:
  - `message`: Slowlog text format
  - `timestamp`: ISO 8601 timestamp
  - `source`: S3 key of the source file

**Future:**
- This will be converted to a Rust-based Vector source plugin
- The plugin will handle S3 authentication, file listing, and Parquet parsing natively

## Transforms

(To be added as needed)

## Sinks

### `sinks/mysql_writer.py`

Writes JSON Lines from stdin to MySQL database.

**Usage:**
- Executed by Vector's `exec` sink
- Reads configuration from environment variables:
  - `MYSQL_HOST`: MySQL host (default: localhost)
  - `MYSQL_PORT`: MySQL port (default: 3306)
  - `MYSQL_USER`: MySQL user (default: root)
  - `MYSQL_PASSWORD`: MySQL password
  - `MYSQL_DATABASE`: MySQL database name (default: testdb)
  - `MYSQL_TABLE`: MySQL table name (default: slowlogs)
  - `TASK_ID`: Task identifier

**Input:**
- JSON Lines from stdin (sent by Vector exec sink)
- Each line is a JSON event with `message`, `timestamp`, etc.

**Output:**
- Writes to MySQL table in batches (100 rows per batch)
- Progress messages to stderr

**Future:**
- This will be converted to a Rust-based Vector sink plugin
- The plugin will handle MySQL connections, batching, and error handling natively

## Migration Path

These Python scripts serve as prototypes for future Rust-based Vector plugins:

1. **Current**: Python scripts executed by Vector `exec` source
2. **Next**: Rust-based Vector plugins in `src/sources/`, `src/transforms/`, `src/sinks/`
3. **Benefits**:
   - Better performance
   - Native Vector integration
   - Type safety
   - No subprocess overhead
