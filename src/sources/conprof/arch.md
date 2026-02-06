# Conprof Source - Architecture Documentation

## Overview

The Conprof (Continuous Profiling) source collects continuous profiling data from TiDB cluster components including PD, TiDB, TiKV, and TiFlash. It enables performance profiling and analysis of cluster components.

## Purpose

- Collect continuous profiling data from cluster components
- Support CPU and memory profiling
- Enable performance analysis and optimization
- Provide profiling data for troubleshooting

## Architecture

### Component Structure

```
Conprof Source
├── Controller          # Main orchestration logic
├── Topology            # Cluster topology management
│   └── Fetch          # Topology fetching from PD
├── Upstream            # Communication with components
├── Tools               # Profiling tools (jeprof, etc.)
└── Shutdown            # Graceful shutdown handling
```

### Data Flow

```
TiDB Cluster Components (PD/TiDB/TiKV/TiFlash)
    ↓ (HTTP/gRPC)
Conprof Upstream
    ↓ (Parse & Transform)
Controller
    ↓ (Vector Event)
Vector Pipeline
```

## Configuration

### ConprofConfig

```rust
pub struct ConprofConfig {
    pub pd_address: String,
    pub tls: Option<TlsConfig>,
    pub topology_fetch_interval_seconds: f64,
    pub components_profile_types: ComponentsProfileTypes,
}
```

### ComponentsProfileTypes

Configures profiling types for each component:

```rust
pub struct ComponentsProfileTypes {
    pub pd: ProfileTypes,
    pub tidb: ProfileTypes,
    pub tikv: ProfileTypes,
    pub tiflash: ProfileTypes,
}
```

### Profile Types

- **CPU**: CPU profiling
- **Memory**: Memory profiling
- **Heap**: Heap profiling
- **Goroutine**: Goroutine profiling

## Data Collection Process

1. **Topology Discovery**: Fetch cluster topology from PD
2. **Component Discovery**: Identify PD, TiDB, TiKV, TiFlash instances
3. **Profile Collection**: Collect profiling data from each component
4. **Data Processing**: Process and transform profiling data
5. **Event Generation**: Convert to Vector events

## Dependencies

- **vector**: Vector core library
- **reqwest**: HTTP client for profiling endpoints
- **tonic**: gRPC for some component communication
- **jeprof**: Profiling data processing tools

## Error Handling

- **Component Failures**: Skip failed components, continue with others
- **Topology Changes**: Automatic re-discovery of components
- **Profile Collection Errors**: Retry with exponential backoff

## Performance Considerations

- **Parallel Collection**: Collect from multiple components in parallel
- **Sampling**: Configurable profiling sampling rates
- **Data Compression**: Compress profiling data before transmission

## Use Cases

- Performance bottleneck identification
- Memory leak detection
- CPU usage analysis
- Component health monitoring
