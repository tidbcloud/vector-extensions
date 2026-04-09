# Topology - Architecture Documentation

## Overview

The Topology module provides utilities for fetching and managing TiDB cluster topology information. It discovers cluster components (PD, TiDB, TiKV, TiFlash) and provides topology data to sources and sinks.

## Purpose

- Fetch TiDB cluster topology from PD
- Discover cluster components
- Provide topology information to components
- Handle topology changes

## Architecture

### Component Structure

```
Topology
└── Fetch              # Topology fetching logic
    ├── PD             # PD client
    ├── TiDB           # TiDB topology
    ├── TiKV           # TiKV topology
    ├── TiKV Nextgen   # Next-gen TiKV topology
    ├── TiDB Nextgen   # Next-gen TiDB topology
    ├── Store          # Store topology
    └── Utils           # Utility functions
```

### Data Flow

```
PD (Placement Driver)
    ↓ (gRPC/HTTP)
Topology Fetcher
    ↓ (Parse & Transform)
Topology Data
    ↓
Components (Sources/Sinks)
```

## Key Components

### PD Client

- Connects to PD server
- Fetches cluster metadata
- Discovers component locations

### Component Discovery

- **TiDB**: Discovers TiDB server instances
- **TiKV**: Discovers TiKV store instances
- **TiFlash**: Discovers TiFlash instances
- **Store**: Discovers store information
- **Manager-based TiDB discovery**: In legacy mode, TiDB instances can be fetched from manager `/api/tidb/get_active_tidb`

### Next-Gen Support

- **TiDB Nextgen**: Support for next-gen TiDB features
- **TiKV Nextgen**: Support for next-gen TiKV features

## Configuration

### TopologyFetcher

```rust
pub struct TopologyFetcher {
    pd_address: String,
    tls: Option<TlsConfig>,
    // ... more fields
}
```

Legacy manager-based TiDB discovery also supports keyspace-aware sharding:

- `manager_server_address`: fetch active TiDB instances from manager instead of etcd
- `tidb_namespace`: namespace list sent to `/api/tidb/get_active_tidb?namespace=...`; required when `manager_server_address` is set
- `VECTOR_STS_REPLICA_COUNT`: total number of Vector StatefulSet replicas
- `VECTOR_STS_ID`: current Vector StatefulSet ordinal

When both shard envs are set, the manager response must include `keyspace_name`. The topology fetcher hashes `keyspace_name`, applies modulo `VECTOR_STS_REPLICA_COUNT`, and only keeps entries whose shard matches `VECTOR_STS_ID`. Entries without `keyspace_name` are skipped in this mode.

## Topology Data

### Component Information

- Component type (PD, TiDB, TiKV, TiFlash)
- Component address
- Component status
- Component labels

### Cluster Information

- Cluster ID
- Cluster version
- Component distribution

## Dependencies

- **etcd-client**: For PD connectivity
- **tonic**: gRPC client
- **reqwest**: HTTP client

## Error Handling

- **Connection Failures**: Retry with backoff
- **Topology Changes**: Handle dynamic topology updates
- **Parse Errors**: Handle invalid topology data

## Performance Considerations

- **Caching**: Cache topology data
- **Polling Interval**: Configurable refresh interval
- **Parallel Fetching**: Fetch from multiple PD instances

## Usage

Used by multiple sources:

- **topsql**: For discovering TiDB/TiKV instances
- **conprof**: For discovering components to profile
- **system_tables**: For discovering TiDB instances
