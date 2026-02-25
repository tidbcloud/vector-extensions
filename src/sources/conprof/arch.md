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
    pub topology_mode: TopologyMode,       // "pd" | "k8s", default "pd"
    pub topology_k8s: Option<TopologyK8sConfig>,  // required when topology_mode = "k8s"
    pub topology_fetch_interval_seconds: f64,
    pub components_profile_types: ComponentsProfileTypes,
    pub jeprof_fetch_mode: JeprofFetchMode,  // "perl" (default) | "rust", for jeheap fetch only
}
```

### Topology mode (quick rollback)

- **`topology_mode = "pd"`** (default): Discover instances via PD API and etcd (TiDB/TiProxy from etcd, TiKV/TiFlash from PD stores). Requires `pd_address` and optional `tls`.
- **`topology_mode = "k8s"`**: Discover instances via Kubernetes pod labels. Use when PD/etcd is unavailable or for quick rollback. Requires `topology_k8s`; `pd_address` is not used for topology in this mode.

When `topology_mode = "k8s"`, which components to collect and which profile config to use are **fully configurable** via `topology_k8s.component_label_to_instance_type`: keys = component label values to collect (any name), values = instance type for profile lookup (`pd`, `tidb`, `tikv`, `tiflash`, `tiproxy`, `lightning`, `tikv_worker`, `coprocessor_worker`).

```toml
[sources.conprof]
type = "conprof"
pd_address = "db-pd:2379"
topology_mode = "k8s"
topology_k8s.component_label_key = "pingcap.com/component"
# topology_k8s.namespace = "mynamespace"   # optional

# Which components to collect and which profile to use (key = label value, value = instance_type)
[topology_k8s.component_label_to_instance_type]
"pd" = "pd"
"tidb" = "tidb"
"worker-tidb" = "tidb"
"tikv" = "tikv"
"tikv-worker" = "tikv_worker"
"coprocessor-worker" = "coprocessor_worker"
"write-tiflash" = "tiflash"
"tiproxy" = "tiproxy"
# Any other label name is allowed; value must be one of the instance types above.
```

- Only pods whose component label value is a **key** in this map are collected.
- **Port for pprof/metrics**: For each pod, the conprof port is taken from the pod annotation `prometheus.io/port` when present (e.g. TiDB Operator sets this to `19000` for coprocessor-worker); otherwise a default port per instance type is used (e.g. 20180 for TiKV/tikv-worker/coprocessor-worker).
- The **value** selects which profile config to use (`components_profile_types.tidb`, `.tikv_worker`, etc.). Separate config for `tikv`, `tikv_worker`, `coprocessor_worker` lets you enable/disable or tune profiles per component.

### ComponentsProfileTypes

Configures which profile types to collect per component. There is no separate "enable TiKV heap" flag; use `components_profile_types.tikv.heap` (and the same pattern for other components). Adding or changing profile types for any component is done via config only.

```rust
pub struct ComponentsProfileTypes {
    pub pd: ProfileTypes,
    pub tidb: ProfileTypes,
    pub tikv: ProfileTypes,
    pub tiflash: ProfileTypes,
    pub tiproxy: ProfileTypes,
    pub lightning: ProfileTypes,
    pub tikv_worker: ProfileTypes,      // K8s e.g. "tikv-worker"
    pub coprocessor_worker: ProfileTypes, // K8s e.g. "coprocessor-worker"
}
```

### Profile Types

- **cpu**: CPU profiling
- **heap**: Collect heap via HTTP (pprof).
- **jeheap**: TiKV only. Collect heap via jeprof (jemalloc). Fetch mode: `jeprof_fetch_mode` = `perl` (default) or `rust`. Both produce the same output (symbol header + raw heap) for offline `jeprof --text`; **rust** does not require Perl/curl. See `doc/conprof-jeprof-fetch-modes.md`.
- **mutex**: Mutex profiling
- **goroutine**: Goroutine profiling

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
