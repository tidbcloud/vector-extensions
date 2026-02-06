# TiDB Sink Architecture

## Overview

The TiDB sink is a Vector sink component that writes log events to MySQL/TiDB databases. It uses the `sqlx` library with MySQL support to connect to TiDB or MySQL databases and insert events as rows.

## Purpose

The TiDB sink allows Vector to write log events directly to MySQL/TiDB databases, making it suitable for:
- Storing logs in a relational database for querying and analysis
- Integrating with TiDB clusters for observability data storage
- Backing up diagnostic data to MySQL-compatible databases

## Architecture

### Components

1. **TiDBConfig** (`mod.rs`): Configuration structure for the sink
   - Connection string (MySQL format)
   - Table name
   - Connection pool settings
   - Batch size configuration

2. **TiDBSink** (`sink.rs`): Main sink implementation
   - Manages MySQL connection pool
   - Processes events in batches
   - Inserts events into the specified table

### Data Flow

```
Vector Events (Event stream)
    ↓
TiDBSink::run()
    ↓
Batch events (batch_size)
    ↓
TiDBSink::insert_batch()
    ↓
Extract fields from LogEvent
    ↓
SQL INSERT statement
    ↓
MySQL/TiDB Database
```

## Configuration

### Required Fields

- `connection_string`: MySQL connection string (e.g., `mysql://user:password@host:port/database`)
- `table`: Target table name

### Optional Fields

- `max_connections`: Maximum connections in pool (default: 10)
- `connection_timeout`: Connection timeout in seconds (default: 30)
- `batch_size`: Batch size for inserts (default: 1000)
- `tls`: TLS configuration
- `acknowledgements`: Acknowledgments configuration

### Example Configuration

```toml
[sinks.tidb_sink]
type = "tidb"
inputs = ["source_name"]
connection_string = "mysql://root:password@localhost:4000/testdb"
table = "slowlogs"
batch_size = 1000
max_connections = 10
```

## Implementation Details

### Table Schema

The sink expects a table with the following columns:
- `log_line` (TEXT/VARCHAR): The log message content
- `log_timestamp` (DATETIME/TIMESTAMP): The event timestamp
- `task_id` (VARCHAR): Optional task identifier

### Field Extraction

The sink extracts fields from log events in the following order:
1. `message` or `log` field for `log_line`
2. `timestamp` or `time` field for `log_timestamp`
3. `task_id` field for `task_id`
4. Falls back to event metadata timestamp if no timestamp field found

### Batch Processing

- Events are collected into batches of `batch_size`
- Batches are inserted using prepared statements
- Errors in one batch don't stop processing of other batches

## Dependencies

- `sqlx`: MySQL database driver (with `mysql` and `runtime-tokio-rustls` features)
- `vector`: Vector core library
- `vector_lib`: Vector library utilities
- `futures_util`: Async stream utilities
- `tracing`: Logging

## Error Handling

- Connection errors are logged and returned
- Insert errors are logged but processing continues
- Healthcheck failures return appropriate errors

## Performance Considerations

- Uses connection pooling for efficient database access
- Batch inserts reduce database round trips
- Configurable batch size allows tuning for throughput vs latency

## Future Improvements

1. **Custom Schema Mapping**: Allow configuration of field-to-column mappings
2. **Schema Evolution**: Handle table schema changes gracefully
3. **Transaction Support**: Option to use transactions for batch inserts
4. **Retry Logic**: Automatic retry for transient failures
5. **Metrics**: Add metrics for insert rates, errors, and latency

## Testing

The sink includes:
- Configuration generation test
- Healthcheck functionality
- Error handling for various failure scenarios

## References

- Vector PostgreSQL Sink: https://github.com/vectordotdev/vector/tree/master/src/sinks/postgres
- sqlx Documentation: https://docs.rs/sqlx/
- TiDB Documentation: https://docs.pingcap.com/tidb/stable
