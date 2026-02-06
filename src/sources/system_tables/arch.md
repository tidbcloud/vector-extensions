# System Tables Source - Architecture Documentation

## Overview

The System Tables source collects data from TiDB system tables, providing insights into database operations, SQL execution, and system metrics.

## Purpose

- Collect data from TiDB system tables
- Monitor SQL execution statistics
- Track coprocessor operations
- Provide system-level observability

## Architecture

### Component Structure

```
System Tables Source
├── Controller          # Main orchestration logic
├── Data Collector      # Data collection logic
├── Collector Factory   # Factory for collectors
└── Collectors          # Specific collectors
    ├── SQL Collector  # SQL execution data
    └── Coprocessor Collector # Coprocessor data
```

### Data Flow

```
TiDB System Tables
    ↓ (SQL Queries)
Data Collector
    ↓ (Transform)
Controller
    ↓ (Vector Event)
Vector Pipeline
```

## Configuration

### SystemTablesConfig

```rust
pub struct SystemTablesConfig {
    // Configuration for system table collection
    // Connection details, query intervals, etc.
}
```

## Collectors

### SQL Collector

- Collects SQL execution statistics
- Queries system tables like `information_schema.statements_summary`
- Tracks query performance metrics

### Coprocessor Collector

- Collects coprocessor operation data
- Monitors TiKV coprocessor statistics
- Tracks data processing metrics

## Data Collection Process

1. **Connection**: Connect to TiDB instance
2. **Query Execution**: Execute queries against system tables
3. **Data Transformation**: Transform query results to events
4. **Event Emission**: Emit Vector events
5. **Scheduling**: Schedule periodic collection

## Dependencies

- **vector**: Vector core library
- **sqlx**: SQL database client
- **tokio**: Async runtime

## Error Handling

- **Connection Errors**: Retry with backoff
- **Query Errors**: Log and continue
- **Data Errors**: Skip invalid rows

## Performance Considerations

- **Query Optimization**: Optimize system table queries
- **Batch Collection**: Collect data in batches
- **Connection Pooling**: Reuse database connections
