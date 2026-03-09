# Vector metrics: performance and utilization

## What is `vector_utilization`?

**`vector_utilization`** is a **per-component** gauge (0–1 in normal cases). It means:

- **Fraction of time** that component (e.g. a sink like `to_s3`) is **busy processing** vs **idle waiting** for events.
- Implemented as an EWMA, updated about every 5 seconds.
- **Not** system CPU or memory: it’s “how much this component is busy,” not “how much CPU/memory Vector uses.”

So:

- **High utilization** → that component is busy most of the time.
- **Low utilization** → that component is often waiting for data.

Note: there are known issues where this metric can get stuck or show odd values (e.g. negative) in some topologies; treat it as indicative, not always exact.

---

## What performance-related metrics does Vector expose?

From your `/metrics` (Prometheus exporter), Vector exposes things like:

| Metric | Type | Meaning |
|--------|------|--------|
| `vector_utilization` | gauge | Per-component busy ratio (see above). |
| `vector_uptime_seconds` | gauge | Process uptime in seconds. |
| `vector_build_info` | gauge | Build/version info (labels: version, arch, etc.). |
| `vector_buffer_byte_size` | gauge | Current buffer size in bytes (per buffer). |
| `vector_buffer_events` | gauge | Current number of events in buffer. |
| `vector_*_duration_*` | histogram | Various latencies (e.g. buffer send, adaptive concurrency). |
| `vector_adaptive_concurrency_*` | histogram | Concurrency/backpressure for sinks. |

So: **throughput, buffers, latencies, and component utilization** — yes. **Process CPU and memory** — **no**, not from Vector’s own `/metrics`.

---

## CPU and memory (process/container)

Vector’s `internal_metrics` source does **not** expose process CPU or memory on the Prometheus exporter by default. To get **CPU and memory** for the Vector process/container you typically use:

1. **Kubernetes / cAdvisor (recommended for pods)**  
   - `container_cpu_usage_seconds_total`  
   - `container_memory_working_set_bytes` (or `container_memory_usage_bytes`)  
   - Filter by pod/container (e.g. your Vector pod name and container name).

2. **Node exporter (host-level)**  
   - `process_cpu_seconds_total`, `process_resident_memory_bytes` for the PID, if you scrape the host and have process metrics.

3. **Kubernetes resource metrics API**  
   - If your cluster exposes it, you can use the “resource” metrics (CPU/memory per pod/container) in Grafana (e.g. “Kubernetes / Compute resources / Pod” or similar dashboards).

So: **CPU/memory** → use cluster/container/host metrics (cAdvisor, node_exporter, or k8s metrics API). **Component busy-ness and pipeline health** → use Vector’s own metrics (`vector_utilization`, buffers, throughput, errors).
