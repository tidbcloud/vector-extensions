# TopSQL v2 Source - Architecture Documentation

## Overview

TopSQL v2 is the next-generation version of the TopSQL source with improved features, better performance, and enhanced capabilities for collecting SQL execution data from TiDB and TiKV clusters.

## Purpose

- Enhanced TopSQL data collection with improved reliability
- Better support for large-scale clusters
- Improved error handling and recovery
- Support for next-generation TiDB features

## Architecture

### Component Structure

```
TopSQL v2 Source
├── Controller          # Main orchestration logic
├── Schema Cache        # Enhanced schema caching
├── Upstream            # Next-gen communication layer
│   ├── TiDB Client    # Enhanced TiDB gRPC client
│   ├── TiKV Client    # Enhanced TiKV gRPC client
│   ├── TLS Proxy      # TLS proxy support
│   └── Parser         # Improved protocol parsing
└── Shutdown            # Graceful shutdown handling
```

### Key Improvements over v1

1. **Enhanced Topology Support**: Better handling of cluster topology changes
2. **Improved Error Recovery**: More robust error handling and recovery
3. **Better Performance**: Optimized data collection and processing
4. **Next-gen Features**: Support for new TiDB/TiKV features
5. **Manager-based TiDB Discovery**: In legacy mode, active TiDB instances can be discovered from a manager service via `manager_server_address` and `tidb_namespace`

## Configuration

Similar to TopSQL v1 but with additional options for next-generation features:

```rust
pub struct TopSQLV2Config {
    // Similar to TopSQLConfig
    // Additional next-gen specific options
}
```

Legacy mode discovery options:

- `pd_address`: used for PD/store discovery and schema management
- `manager_server_address`: optional manager endpoint used to fetch active TiDB instances
- `tidb_namespace`: manager namespace list used when calling `/api/tidb/get_active_tidb`
- `enable_tikv_topsql`: whether to collect `tikv_topsql` and `tikv_topregion`; defaults to `true`
- `topru`: TiDB TopRU subscription options (`enable`, `report_interval_seconds`, `item_interval_seconds`)

## Data Flow

Same as TopSQL v1 but with improved reliability and performance.

TiDB subscriptions emit four log-event families:

- `tidb_topsql`: execution metrics keyed by SQL/plan digest
- `topsql_topru`: RU metrics keyed by SQL/plan digest and user
- `topsql_sql_meta`: normalized SQL text
- `topsql_plan_meta`: normalized / encoded plan text

For TiDB-originated events, `keyspace` is propagated when the upstream payload includes `keyspace_name`, including `topsql_sql_meta` and `topsql_plan_meta`.

TiKV subscriptions emit `tikv_topsql` and `tikv_topregion` events. In addition to CPU,
key counts, network bytes, and logical read/write bytes, these events include
`topsql_rocksdb_block_read_count`. The value is the foreground request's RocksDB block-read
count and must not be interpreted as device-level read IOPS.

Per-second Top N filtering keeps the union of records selected independently by CPU, combined
network traffic, logical reads, logical writes, and RocksDB block reads. Metrics from evicted
records and upstream `others` records are merged without dropping any dimension, and the same
fields are preserved during downsampling.

## Dependencies

- Same as TopSQL v1
- Additional support for next-generation TiDB features

## Differences from v1

- **Better Topology Handling**: More robust topology change detection
- **Enhanced TLS Support**: Improved TLS proxy capabilities
- **Performance Optimizations**: Faster data collection and processing
- **Future-Proof**: Designed for upcoming TiDB features

## Migration from v1

- Configuration is largely compatible
- Improved performance and reliability
- Better error messages and diagnostics
