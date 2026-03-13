# Delta Lake Writer - Architecture Documentation

## Overview

The Delta Lake Writer is a common utility module that provides Delta Lake writing capabilities for multiple sinks. It handles Delta Lake operations, schema management, and data conversion.

## Purpose

- Provide reusable Delta Lake writing functionality
- Handle Delta Lake transaction operations
- Manage schema evolution
- Convert data to Delta Lake format

## Architecture

### Component Structure

```
Delta Lake Writer
├── Converter          # Data conversion utilities
├── Delta Ops          # Delta Lake operations
├── Schema             # Schema management
└── Types              # Type definitions
```

### Key Components

#### Converter

- Converts Vector events to Arrow format
- Handles type conversions
- Manages field mappings

#### Delta Ops

- Creates Delta Lake transaction logs
- Handles ACID operations
- Manages table metadata

#### Schema

- Detects and manages schemas
- Handles schema evolution
- Validates schema compatibility

#### Types

- Type definitions for Delta Lake operations
- Configuration types
- Error types

## Usage

Used by multiple sinks:

- **deltalake**: General Delta Lake sink
- **topsql_data_deltalake**: TopSQL data sink
- **topsql_meta_deltalake**: TopSQL metadata sink

## Data Conversion

### Vector Event → Arrow

- Maps Vector event fields to Arrow columns
- Handles nested structures
- Preserves data types

### Arrow → Parquet

- Converts Arrow batches to Parquet files
- Applies compression
- Writes to temporary storage

### Parquet → Delta Lake

- Creates Delta Lake transaction log entries
- Updates table metadata
- Commits transactions

## Schema Management

### Schema Detection

- Automatically detects schema from first batch
- Handles missing fields
- Validates data types

### Schema Evolution

- Adds new fields automatically
- Handles field type changes
- Validates compatibility

## Delta Lake Operations

### Transaction Log

- Creates transaction log entries
- Records file additions/deletions
- Maintains ACID properties

### Metadata Management

- Updates table metadata
- Tracks schema versions
- Manages partition information

## Error Handling

- **Conversion Errors**: Log and skip invalid events
- **Schema Errors**: Handle schema evolution gracefully
- **Transaction Errors**: Rollback and retry
- **Storage Errors**: Retry with backoff

## Performance Considerations

- **Batch Processing**: Process events in batches
- **Parallel Writes**: Support parallel partition writes
- **Caching**: Cache schemas and metadata
- **Compression**: Efficient Parquet compression

## Configuration

### WriteConfig

```rust
pub struct WriteConfig {
    pub mode: WriteMode,
    pub partition_by: Vec<String>,
    // ... more fields
}
```

### DeltaTableConfig

```rust
pub struct DeltaTableConfig {
    pub table_path: String,
    pub storage_options: HashMap<String, String>,
    // ... more fields
}
```

## Dependencies

- **deltalake**: Delta Lake Rust crate
- **arrow**: Apache Arrow
- **parquet**: Parquet file format
