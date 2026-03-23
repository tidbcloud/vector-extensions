# TopSQL Data Delta Lake Sink - Architecture Documentation

## Overview

The TopSQL Data Delta Lake sink writes TopSQL execution data to Delta Lake format, providing structured storage for SQL performance analysis.

## Purpose

- Write TopSQL execution data to Delta Lake
- Support SQL performance analysis
- Enable historical data queries
- Integrate with data lake architectures

## Architecture

### Component Structure

```
TopSQL Data Delta Lake Sink
└── Processor          # TopSQL-specific Delta Lake processing
```

### Data Flow

```
TopSQL Events
    ↓
Processor
    ↓ (Convert & Write)
Delta Lake (via deltalake_writer)
    ↓
Cloud Storage (S3)
```

## Configuration

Similar to Delta Lake sink but optimized for TopSQL data:

```rust
pub struct TopSQLDataDeltaLakeConfig {
    // Delta Lake configuration
    // TopSQL-specific options
}
```

## Data Processing

1. **Event Reception**: Receive TopSQL events
2. **Data Transformation**: Transform TopSQL data format
3. **Schema Management**: Handle TopSQL schema
4. **Delta Lake Writing**: Write using deltalake_writer
5. **Partitioning**: Partition by time/SQL digest

## TopSQL-Specific Features

- **SQL Digest Grouping**: Group by SQL digest
- **Time Partitioning**: Partition by execution time
- **Schema Optimization**: Optimized schema for TopSQL data
- **Keyspace-based Routing**: Optional PD keyspace lookup can prepend `org=<id>/cluster=<id>` path segments before the table layout, which is especially useful for `topru` data written to shared S3 prefixes
- **TopRU Path Layout**: `topsql_topru` is written under `type=topsql/component=topru/instance=default` so shared prefixes can keep a stable `type=topsql` partition while separating the TopRU payload by component

## Dependencies

- **deltalake_writer**: Shared Delta Lake writing utilities
- **deltalake**: Delta Lake Rust crate

## Related Components

- **deltalake**: General Delta Lake sink
- **topsql_meta_deltalake**: TopSQL metadata sink
- **topsql source**: TopSQL data source
