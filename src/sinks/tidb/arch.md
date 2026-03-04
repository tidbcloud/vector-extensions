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
- `auto_create_table`: When true (default), create the table automatically from the first batch if it doesn't exist
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

### Auto-Create Table

When `auto_create_table` is true (default) and the target table does not exist:

1. On first batch, the sink creates the table using `CREATE TABLE` from the first event's field structure
2. Column types are inferred from Vector `Value` types (Integer→BIGINT, Float→DOUBLE, Bytes→TEXT/VARCHAR, etc.)
3. If events contain `_schema_metadata` with `mysql_type` (e.g. from deltalake/topsql sinks), those types are used for better accuracy
4. An `id` column is added as `BIGINT AUTO_INCREMENT PRIMARY KEY`
5. After creation, the schema is loaded and inserts proceed normally

Set `auto_create_table = false` to require the table to exist beforehand (original behavior).

### Dynamic Schema Discovery

When the table exists, the sink queries the target table schema on initialization using `SHOW COLUMNS FROM table`. This allows the sink to:
- Discover all available columns dynamically
- Adapt to different table structures without code changes
- Handle nullable/non-nullable columns appropriately
- Skip auto-increment columns (like `id`) and auto-generated columns (like `created_at`)

### Field Mapping

The sink uses **automatic field matching** to map event fields to table columns:

1. **Exact Match**: First tries to find an event field with the exact same name as the column
2. **Case-Insensitive Match**: If no exact match, searches all event fields case-insensitively
3. **No Hard-coded Mappings**: The sink does not use hard-coded field name mappings, making it truly generic

### Field Extraction

For each column in the table schema:
- The sink attempts to find a matching event field using the matching strategy above
- If a match is found, the value is extracted and converted to the appropriate format
- If no match is found:
  - For nullable columns: The value is set to NULL
  - For non-nullable columns: A default value is used based on the column type:
    - Integer types → `0`
    - Float types → `0.0`
    - DATETIME/TIMESTAMP → Current timestamp
    - Other types → Empty string

### Type Conversion

The sink automatically handles type conversions:
- **Timestamp Conversion**: Automatically detects DATETIME/TIMESTAMP columns and converts ISO 8601 timestamps (e.g., `2025-06-06T18:00:00`) to MySQL DATETIME format (`2025-06-06 18:00:00`)
- **Value Serialization**: Complex types (objects, arrays) are serialized as JSON strings
- **String Handling**: All values are converted to strings for SQL binding

### Batch Processing

- Events are collected into batches of `batch_size`
- For each batch, a dynamic INSERT statement is generated based on the table schema
- Only columns that exist in the table schema are included in the INSERT statement
- Batches are inserted using prepared statements with proper type binding
- Errors in one batch don't stop processing of other batches

### Dynamic SQL Generation

The sink generates INSERT statements dynamically:
- Queries table schema on initialization
- Builds INSERT statement with only the columns that exist in the table
- Automatically skips auto-increment and auto-generated columns
- Handles NULL values appropriately based on column nullability

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

1. **Custom Field Mapping**: Allow configuration of field-to-column mappings (e.g., `message` → `log_line`)
2. **Schema Evolution**: Handle table schema changes gracefully (re-query schema on errors)
3. **Transaction Support**: Option to use transactions for batch inserts
4. **Retry Logic**: Automatic retry for transient failures
5. **Metrics**: Add metrics for insert rates, errors, and latency
6. **Type-aware Binding**: Use proper SQL types instead of string binding for better performance
7. **Batch Optimization**: Use multi-row INSERT statements for better performance

## Testing

The sink includes:
- Configuration generation test
- Healthcheck functionality
- Error handling for various failure scenarios

## References

- Vector PostgreSQL Sink: https://github.com/vectordotdev/vector/tree/master/src/sinks/postgres
- sqlx Documentation: https://docs.rs/sqlx/
- TiDB Documentation: https://docs.pingcap.com/tidb/stable
