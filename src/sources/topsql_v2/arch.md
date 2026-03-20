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

## Configuration

Similar to TopSQL v1 but with additional options for next-generation features:

```rust
pub struct TopSQLV2Config {
    // Similar to TopSQLConfig
    // Additional next-gen specific options
}
```

Notable source-specific options:

- `manager_server_address` + `tidb_namespace`: discover active TiDB instances from manager.
- `enable_tikv_topsql`: when `false`, only TiDB TopSQL is collected; TiKV TopSQL subscriptions are skipped.

## Data Flow

Same as TopSQL v1 but with improved reliability and performance.

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
