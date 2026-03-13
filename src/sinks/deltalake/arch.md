# Delta Lake Sink - Architecture Documentation

## Overview

The Delta Lake sink writes Vector events to Delta Lake format, which provides ACID transactions, time travel, and schema evolution for data lakes. It supports writing to cloud storage backends like S3.

## Purpose

- Write Vector events to Delta Lake format
- Support ACID transactions for data consistency
- Enable schema evolution
- Support time travel queries
- Integrate with data lake architectures

## Architecture

### Component Structure

```
Delta Lake Sink
├── Processor          # Main processing logic
└── Delta Lake Writer  # Delta Lake operations (from common/)
    ├── Converter      # Data conversion
    ├── Delta Ops      # Delta Lake operations
    ├── Schema         # Schema management
    └── Types          # Type definitions
```

### Data Flow

```
Vector Events
    ↓
Delta Lake Processor
    ↓ (Convert to Arrow)
Delta Lake Writer
    ↓ (Write to Delta Lake)
Cloud Storage (S3)
```

## Configuration

### DeltaLakeConfig

```rust
pub struct DeltaLakeConfig {
    pub base_path: String,
    pub batch_size: usize,
    pub timeout_secs: u64,
    pub delta_table_config: DeltaTableConfig,
    pub write_config: WriteConfig,
    // AWS S3 configuration
    pub region: Option<RegionOrEndpoint>,
    pub auth: Option<AwsAuthentication>,
    // ... more fields
}
```

### Key Configuration Options

- **base_path**: Base path for Delta Lake tables
- **batch_size**: Number of records per batch
- **timeout_secs**: Write timeout in seconds
- **delta_table_config**: Delta table specific configuration
- **write_config**: Write operation configuration

## Data Processing

1. **Event Reception**: Receive Vector events from pipeline
2. **Batch Accumulation**: Accumulate events into batches
3. **Schema Detection**: Detect or use existing schema
4. **Arrow Conversion**: Convert events to Apache Arrow format
5. **Parquet Writing**: Write to Parquet files
6. **Delta Operations**: Create Delta Lake transaction logs
7. **Cloud Upload**: Upload to cloud storage (S3)

## Delta Lake Operations

### Transaction Log

- Maintains ACID properties
- Records all changes to the table
- Enables time travel queries

### Schema Evolution

- Automatically handles schema changes
- Merges new fields with existing schema
- Validates schema compatibility

### Partitioning

- Supports partitioning by fields
- Optimizes query performance
- Reduces data scanning

## Dependencies

- **deltalake**: Delta Lake Rust implementation
- **arrow**: Apache Arrow for columnar data
- **parquet**: Parquet file format support
- **aws-sdk-s3**: AWS S3 SDK for storage
- **datafusion**: Data processing engine

## Error Handling

- **Write Failures**: Retry with exponential backoff
- **Schema Conflicts**: Handle schema evolution gracefully
- **Storage Errors**: Retry S3 operations
- **Transaction Failures**: Rollback and retry

## Performance Considerations

- **Batch Writing**: Write in configurable batch sizes
- **Parallel Writes**: Support parallel partition writes
- **Compression**: Parquet compression for storage efficiency
- **Caching**: Cache schema and metadata

## Use Cases

- Data lake ingestion
- ETL pipelines
- Historical data storage
- Analytics workloads

## Related Components

- **deltalake_writer**: Shared Delta Lake writing utilities
- **topsql_data_deltalake**: TopSQL-specific Delta Lake sink
- **topsql_meta_deltalake**: TopSQL metadata Delta Lake sink
