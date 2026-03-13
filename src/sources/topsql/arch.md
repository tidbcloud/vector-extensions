# TopSQL Source - Architecture Documentation

## Overview

The TopSQL source collects SQL execution data from TiDB and TiKV clusters. It connects to cluster components via gRPC to fetch TopSQL statistics, which include SQL execution metrics, query plans, and performance data.

## Purpose

- Collect TopSQL execution data from TiDB/TiKV clusters
- Support real-time and historical SQL performance monitoring
- Provide data for SQL optimization and troubleshooting

## Architecture

### Component Structure

```
TopSQL Source
├── Controller          # Main orchestration logic
├── Schema Cache        # Caches SQL schema information
├── Upstream            # Communication with TiDB/TiKV
│   ├── TiDB Client    # TiDB gRPC client
│   ├── TiKV Client    # TiKV gRPC client
│   └── Parser         # Protocol buffer parsing
└── Shutdown            # Graceful shutdown handling
```

### Data Flow

```
TiDB/TiKV Cluster
    ↓ (gRPC)
TopSQL Upstream
    ↓ (Parse & Transform)
Controller
    ↓ (Vector Event)
Vector Pipeline
```

### Key Components

#### Controller

- Manages the overall source lifecycle
- Coordinates data collection from multiple cluster components
- Handles topology discovery and connection management
- Manages retry logic and error handling

#### Schema Cache

- Caches SQL schema information to reduce redundant queries
- Improves performance by avoiding repeated schema lookups
- Handles schema updates and invalidation

#### Upstream

- **TiDB Client**: Connects to TiDB servers via gRPC
- **TiKV Client**: Connects to TiKV servers via gRPC
- **Parser**: Parses protocol buffer messages from cluster components

## Configuration

### TopSQLConfig

```rust
pub struct TopSQLConfig {
    pub sharedpool_id: Option<String>,
    pub tidb_group: Option<String>,
    pub label_k8s_instance: Option<String>,
    pub keyspace_to_vmtenants: Option<String>,
    pub pd_address: Option<String>,
    pub tls: Option<TlsConfig>,
    pub init_retry_delay_seconds: f64,
    pub topology_fetch_interval_seconds: f64,
    // ... more fields
}
```

### Key Configuration Options

- **pd_address**: PD (Placement Driver) address for topology discovery
- **tls**: TLS configuration for secure connections
- **topology_fetch_interval_seconds**: How often to refresh cluster topology
- **init_retry_delay_seconds**: Delay between initialization retries

## Data Collection Process

1. **Topology Discovery**: Fetch cluster topology from PD
2. **Connection Establishment**: Connect to TiDB/TiKV components
3. **Schema Caching**: Cache SQL schema information
4. **Data Collection**: Continuously collect TopSQL data via gRPC
5. **Event Generation**: Convert collected data to Vector events
6. **Error Handling**: Retry on failures, handle disconnections

## Dependencies

- **vector**: Vector core library
- **tonic**: gRPC framework
- **prost**: Protocol buffer support
- **etcd-client**: For PD connectivity (via topology module)

## Error Handling

- **Connection Failures**: Automatic retry with exponential backoff
- **Topology Changes**: Automatic reconnection to new components
- **Schema Errors**: Schema cache invalidation and refresh
- **gRPC Errors**: Error propagation with context

## Performance Considerations

- **Schema Caching**: Reduces redundant schema queries
- **Batch Collection**: Collects data in batches for efficiency
- **Connection Pooling**: Reuses connections where possible
- **Async Operations**: Non-blocking async I/O

## Testing

- Unit tests for individual components
- Integration tests with mocked TiDB/TiKV
- End-to-end tests with real cluster (optional)

## Related Components

- **topsql_v2**: Next-generation version with improved features
- **topology**: Shared topology fetching utilities
- **deltalake_writer**: For writing TopSQL data to Delta Lake
