# TiDB Coprocessor Interface Documentation

This document describes the Coprocessor interface implementation used by the System Tables Source for efficient data collection from TiDB clusters.

## Overview

The Coprocessor interface provides a high-performance alternative to SQL-based data collection by directly communicating with TiDB's coprocessor layer using gRPC protocol. This approach bypasses the SQL layer and allows for more efficient data retrieval, especially for system tables and monitoring data.

## Architecture

```
┌─────────────────┐    gRPC     ┌──────────────────┐
│ Vector Source   │ ──────────► │ TiDB Coprocessor │
│ (Collector)     │             │ gRPC Endpoint    │
└─────────────────┘             └──────────────────┘
         │                               │
         │ HTTP (Fallback)               │
         ▼                               ▼
┌─────────────────┐             ┌──────────────────┐
│ TiDB HTTP API   │             │ TiKV Storage     │
│ (Status Port)   │             │ Layer            │
└─────────────────┘             └──────────────────┘
```

## Protocol Definitions

### Core Request/Response Types

#### CoprocessorRequest

The main request structure for communicating with TiDB's coprocessor.

```rust
#[derive(Clone, PartialEq, Message)]
pub struct CoprocessorRequest {
    #[prost(int64, tag = "1")]
    pub tp: i64,                    // Request type (103 for DAG)
    
    #[prost(bytes = "vec", tag = "2")]
    pub data: Vec<u8>,              // Serialized DAG request
    
    #[prost(message, repeated, tag = "3")]
    pub ranges: Vec<KeyRange>,      // Key ranges to scan
    
    #[prost(message, optional, tag = "4")]
    pub context: Option<Context>,   // Request context
    
    #[prost(uint64, tag = "5")]
    pub start_ts: u64,              // Transaction start timestamp
}
```

#### DAGRequest

The DAG (Directed Acyclic Graph) request defines the execution plan for data retrieval.

```rust
#[derive(Clone, PartialEq, Message)]
pub struct DAGRequest {
    #[prost(string, tag = "1")]
    pub time_zone_name: String,     // Time zone name (e.g., "UTC")
    
    #[prost(int64, tag = "2")]
    pub time_zone_offset: i64,      // Time zone offset in seconds
    
    #[prost(uint64, tag = "3")]
    pub flags: u64,                 // Execution flags
    
    #[prost(int32, tag = "4")]
    pub encode_type: i32,           // Data encoding type
    
    #[prost(message, optional, tag = "5")]
    pub user: Option<UserIdentity>, // User information
    
    #[prost(message, repeated, tag = "6")]
    pub executors: Vec<Executor>,   // Execution operators
    
    #[prost(uint32, repeated, tag = "7")]
    pub output_offsets: Vec<u32>,   // Output column offsets
    
    #[prost(bool, optional, tag = "8")]
    pub collect_execution_summaries: Option<bool>, // Collect performance stats
}
```

#### CoprocessorResponse

The response structure containing query results.

```rust
#[derive(Clone, PartialEq, Message)]
pub struct CoprocessorResponse {
    #[prost(bytes = "vec", tag = "1")]
    pub data: Vec<u8>,              // Serialized response data
    
    #[prost(string, tag = "2")]
    pub other_error: String,        // Error message if any
}
```

### Supporting Types

#### Context

Request execution context containing region and peer information.

```rust
#[derive(Clone, PartialEq, Message)]
pub struct Context {
    #[prost(uint64, tag = "1")]
    pub region_id: u64,             // TiKV region ID
    
    #[prost(message, optional, tag = "2")]
    pub region_epoch: Option<RegionEpoch>, // Region version
    
    #[prost(message, optional, tag = "3")]
    pub peer: Option<Peer>,         // Peer information
    
    #[prost(message, optional, tag = "4")]
    pub source_stmt: Option<SourceStmt>, // Source statement info
}
```

#### KeyRange

Defines the key range for data scanning.

```rust
#[derive(Clone, PartialEq, Message)]
pub struct KeyRange {
    #[prost(bytes = "vec", tag = "1")]
    pub start: Vec<u8>,             // Start key (inclusive)
    
    #[prost(bytes = "vec", tag = "2")]
    pub end: Vec<u8>,               // End key (exclusive)
}
```

#### TableScan Executor

Defines table scanning parameters.

```rust
#[derive(Clone, PartialEq, Message)]
pub struct TableScan {
    #[prost(int64, tag = "1")]
    pub table_id: i64,              // Physical table ID
    
    #[prost(message, repeated, tag = "2")]
    pub columns: Vec<ColumnInfo>,   // Columns to scan
    
    #[prost(bool, tag = "3")]
    pub desc: bool,                 // Scan in descending order
}
```

## Endpoint Configuration

### gRPC Endpoint

The coprocessor communicates with TiDB via gRPC on a dedicated port:

```
Endpoint: http://{tidb_host}:{tidb_port + 6080}
Default:  http://127.0.0.1:10080 (for TiDB on port 4000)
```

### HTTP Fallback Endpoint

For schema discovery and fallback operations:

```
Schema API: http://{tidb_host}:{tidb_port + 6080}/schema/{database}/{table}
Example:    http://127.0.0.1:10080/schema/information_schema/CLUSTER_INFO
```

## Connection Management

### Initialization Process

1. **Create gRPC Channel**
   ```rust
   let endpoint = Endpoint::from_shared(grpc_endpoint)
       .timeout(Duration::from_secs(10))
       .connect_timeout(Duration::from_secs(5));
   
   let channel = endpoint.connect().await?;
   ```

2. **Establish Connection**
   - Connect to TiDB's coprocessor gRPC service
   - Configure timeouts and retry policies
   - Validate connection health

3. **Schema Discovery**
   - Fetch table schema via HTTP API
   - Extract table ID and column information
   - Build column type mappings

### Error Handling

The implementation includes robust error handling:

```rust
pub enum CollectionError {
    ConfigurationError(String),    // Configuration issues
    ConnectionError(String),       // gRPC connection failures
    NetworkError(String),          // HTTP API failures
    ParseError(String),            // Data parsing errors
    ProtocolError(String),         // Protocol-level errors
}
```

## Request Building Process

### 1. Schema Resolution

```rust
async fn get_table_schema_via_http(
    &self,
    table: &TableConfig,
) -> Result<TableSchema, CollectionError>
```

- Fetches table schema from TiDB's HTTP status API
- Extracts table ID and column definitions
- Creates column type mappings for data conversion

### 2. DAG Request Construction

```rust
async fn build_coprocessor_request(
    &self,
    table_schema: &TableSchema,
) -> Result<CoprocessorRequest, CollectionError>
```

- Builds DAG execution plan
- Configures table scan executor
- Sets up column projections and filters

### 3. Key Range Calculation

```rust
ranges: vec![KeyRange {
    start: vec![0x74, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01],
    end: vec![0x74, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02],
}]
```

- Calculates appropriate key ranges for table data
- Handles table ID encoding in TiKV key format
- Supports range partitioning for large tables

## Supported System Tables

The coprocessor collector is optimized for these table types:

### Cluster-Level Tables
- `CLUSTER_INFO` - Cluster topology information
- `CLUSTER_CONFIG` - Configuration settings across cluster
- `CLUSTER_HARDWARE` - Hardware information
- `CLUSTER_LOAD` - System load metrics
- `CLUSTER_SYSTEMINFO` - Operating system information

### Performance Tables
- `CLUSTER_STATEMENTS_SUMMARY` - SQL statement statistics
- `CLUSTER_STATEMENTS_SUMMARY_HISTORY` - Historical statement data
- `CLUSTER_SLOW_QUERY` - Slow query logs
- `CLUSTER_PROCESSLIST` - Active connections

### Monitoring Tables
- `CLUSTER_TIDB_TRX` - Transaction information
- `CLUSTER_DEADLOCKS` - Deadlock detection results
- `CLUSTER_MEMORY_USAGE` - Memory usage statistics

## Performance Characteristics

### Advantages over SQL Collection

1. **Lower Overhead**: Bypasses SQL parsing and optimization
2. **Batch Processing**: Efficient bulk data retrieval
3. **Streaming**: Supports streaming large result sets
4. **Direct Access**: No intermediate SQL layer processing

### Performance Metrics

The collector tracks these performance metrics:

- **Collection Duration**: Time taken for data retrieval
- **Row Count**: Number of records collected
- **Network Round Trips**: gRPC call statistics
- **Fallback Usage**: When HTTP fallback is used

## Configuration Example

```toml
[sources.tidb_system_tables]
type = "system_tables"

# Database connection for fallback HTTP operations
database_host = "127.0.0.1"
database_port = 4000

# PD configuration for topology discovery
pd_address = "127.0.0.1:2379"

# Use coprocessor collection method
collection_method = "coprocessor"

# Tables optimized for coprocessor collection
[[sources.tidb_system_tables.tables]]
source_schema = "information_schema"
source_table = "CLUSTER_STATEMENTS_SUMMARY"
dest_table = "statements_summary"
collection_interval = "short"
enabled = true
```

## Error Handling and Fallback

### Automatic Fallback

When coprocessor requests fail, the system automatically falls back to HTTP API collection:

1. **Schema Fetch Failure**: Falls back to basic schema inference
2. **gRPC Connection Issues**: Uses HTTP API for data collection
3. **Protocol Errors**: Retries with simplified requests

### Monitoring and Logging

The collector provides detailed logging:

```rust
info!("Creating gRPC connection to: {}", grpc_endpoint);
warn!("Failed to get schema for table {}: {}. Using fallback.", table_name, error);
debug!("Coprocessor request built successfully for table: {}", table_name);
```

## Security Considerations

### Authentication

- Currently uses connection-level authentication
- Supports TLS connections for encrypted communication
- Future versions will support user-based authentication

### Authorization

- Inherits TiDB's permission model
- Requires appropriate privileges for system table access
- Respects cluster security policies

## Future Enhancements

### Planned Features

1. **Full gRPC Implementation**: Complete coprocessor protocol support
2. **Advanced Filtering**: Server-side filtering and aggregation
3. **Batch Optimization**: Intelligent batching for multiple tables
4. **Metrics Collection**: Enhanced performance monitoring
5. **Connection Pooling**: Efficient connection management

### API Extensions

1. **Custom Executors**: Support for custom data processing
2. **Streaming Responses**: Large dataset streaming support
3. **Compression**: Data compression for network efficiency
4. **Caching**: Response caching for frequently accessed data

## Troubleshooting

### Common Issues

1. **Connection Timeout**: Increase timeout values in configuration
2. **Schema Not Found**: Verify table exists and permissions are correct
3. **Protocol Mismatch**: Ensure compatible TiDB version
4. **gRPC Port Blocked**: Check firewall settings for port 10080

### Debug Mode

Enable debug logging for detailed troubleshooting:

```toml
[sources.tidb_system_tables]
log_level = "debug"
```

This will provide detailed information about:
- gRPC connection establishment
- Request/response processing
- Error conditions and fallback usage
- Performance metrics and timing