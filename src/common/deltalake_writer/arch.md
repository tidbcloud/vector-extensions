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

- Support for S3 (s3://) and Azure Blob (az://) storage backends

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

## Storage Backends
- **S3**: S3-compatible storage (AWS S3, Aliyun OSS, etc.)
- **Azure Blob**: Azure Blob Storage with managed identity support

- **Local**: Local filesystem

## Azure Blob Storage Configuration
The Azure Blob Storage backend supports multiple authentication methods:

### Environment Variables for Authentication
- **AZURE_CLIENT_ID**: Managed identity client ID (required for user-assigned managed identity)
- **AZURE_TENANT_ID**: Azure tenant ID (optional)
- **AZURE_STORAGE_ACCOUNT**: Storage account name

### Alternative Authentication Methods
1. **Managed Identity** (recommended for AKS/pods):
   ```bash
   export AZURE_CLIENT_ID="your-managed-identity-client-id"
   export AZURE_STORAGE_ACCOUNT="your-storage-account"
   ```

2. **Access Key**:
   ```bash
   export AZURE_STORAGE_ACCOUNT="your-storage-account"
   export AZURE_STORAGE_KEY="your-storage-key"
   ```

3. **SAS Token**:
   ```bash
   export AZURE_STORAGE_ACCOUNT="your-storage-account"
   export AZURE_SAS_TOKEN="your-sas-token"
   ```

4. **Connection String**:
   ```bash
   export AZURE_STORAGE_CONNECTION_STRING="your-connection-string"
   ```

### Vector Configuration Example
```toml
[sinks.deltalake]
type = "deltalake"
inputs = ["your_source"]
base_path = "az://container-name/path/to/delta-tables"
batch_size = 1000
timeout_secs = 30
```

## Storage Backend Authentication

### S3 Authentication
Uses AWS SDK credential chain via `deltalake_s3.rs` module.

### Azure Blob Authentication
Support for multiple authentication methods:
- **Managed Identity** (recommended): Set `AZURE_CLIENT_ID` environment variable
- **Access Key**: Set `AZURE_STORAGE_ACCOUNT` and `AZURE_STORAGE_KEY`
- **SAS Token**: Set `AZURE_STORAGE_ACCOUNT` and `AZURE_SAS_KEY`
- **Connection String**: Set `AZURE_STORAGE_CONNECTION_STRING`

Environment variables for Azure Managed Identity:
- `AZURE_CLIENT_ID`: Managed identity client ID (required)
- `AZURE_TENANT_ID`: Azure tenant ID (optional)
- `AZURE_STORAGE_ACCOUNT`: Storage account name
