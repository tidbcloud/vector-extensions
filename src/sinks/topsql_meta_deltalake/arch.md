# TopSQL Meta Delta Lake Sink - Architecture Documentation

## Overview

The TopSQL Meta Delta Lake sink writes TopSQL metadata (SQL schemas, query plans, etc.) to Delta Lake format, providing structured storage for SQL metadata analysis.

## Purpose

- Write TopSQL metadata to Delta Lake
- Support SQL schema analysis
- Enable metadata queries
- Integrate with data lake architectures

## Architecture

### Component Structure

```
TopSQL Meta Delta Lake Sink
└── Processor          # TopSQL metadata-specific Delta Lake processing
```

### Data Flow

```
TopSQL Metadata Events
    ↓
Processor
    ↓ (Convert & Write)
Delta Lake (via deltalake_writer)
    ↓
Cloud Storage (S3)
```

## Configuration

Similar to Delta Lake sink but optimized for TopSQL metadata:

```rust
pub struct TopSQLMetaDeltaLakeConfig {
    // Delta Lake configuration
    // TopSQL metadata-specific options
}
```

## Data Processing

1. **Event Reception**: Receive TopSQL metadata events
2. **Metadata Transformation**: Transform metadata format
3. **Schema Management**: Handle metadata schema
4. **Delta Lake Writing**: Write using deltalake_writer
5. **Partitioning**: Partition by metadata type

## TopSQL Metadata Features

- **Schema Storage**: Store SQL schemas
- **Query Plan Storage**: Store query execution plans
- **Metadata Versioning**: Track metadata changes over time

## Dependencies

- **deltalake_writer**: Shared Delta Lake writing utilities
- **deltalake**: Delta Lake Rust crate

## Related Components

- **deltalake**: General Delta Lake sink
- **topsql_data_deltalake**: TopSQL data sink
- **topsql source**: TopSQL data source
