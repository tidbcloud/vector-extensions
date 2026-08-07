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
- **Detailed TiKV I/O**: Stores logical reads, logical writes, and
  `topsql_rocksdb_block_read_count`; historical rows without the block-read column remain
  compatible through nullable schema evolution
- **Keyspace-based Routing**: When `enable_keyspace_cluster_mapping = true`, `base_path` must already contain `org=xxx/cluster=xxx` template segments; the sink resolves keyspace via PD and replaces those template values with the routed `org` / `cluster`
- **Component-based Path Layout**: TopSQL data is partitioned by `component=<tidb|tikv|topru>` and `instance=<id>`

## Dependencies

- **deltalake_writer**: Shared Delta Lake writing utilities
- **deltalake**: Delta Lake Rust crate

## Related Components

- **deltalake**: General Delta Lake sink
- **topsql_meta_deltalake**: TopSQL metadata sink
- **topsql source**: TopSQL data source
