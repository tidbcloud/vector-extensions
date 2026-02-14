# Design: Why Vector for Observability Data Sync

This document explains the rationale for building observability and log synchronization on **Vector**: why it was chosen, how it affects cost and stability, how we achieve at-least-once delivery, and how to approach monitoring and alerting.

---

## 1. Why Vector

### 1.1 Unified pipeline in a single process

Vector runs **sources → transforms → sinks** in one process. For our use cases (raw logs from S3, Delta Lake tables, sync to S3 or MySQL/TiDB), we avoid:

- **Multiple hand-written services** (e.g. a custom “lister” service, a separate “uploader” service, another for DB writes), each with its own deployment, monitoring, and failure modes.
- **Ad-hoc scripts** that do list → download → parse → write with no standard semantics for backpressure, batching, or retries.

We get a **single config-driven pipeline**: e.g. `file_list` (source) → optional per-line parsing → `aws_s3` or `tidb` (sink). One binary, one config, one place to tune timeouts and batch sizes.

### 1.2 Extensibility without forking the engine

Vector is designed for **custom components** via the same interfaces as built-in ones. We can:

- Add a **file_list** source that lists and reads from object storage (S3/GCS/Azure) with type-based path resolution and optional per-line parsing.
- Add a **tidb** sink that writes log events to MySQL/TiDB with schema-aware column mapping.
- Keep using **official** sinks (e.g. `aws_s3`) and transforms where they fit.

We stay on upstream Vector (e.g. v0.49) and plug in our logic instead of maintaining a full fork. Upgrades and security fixes from the Vector project still apply.

### 1.3 Built-in semantics we rely on

- **Backpressure**: Vector’s internal channels apply backpressure so a slow sink doesn’t unboundedly buffer events.
- **Batching**: Sinks like `aws_s3` and our tidb sink batch events (e.g. by `batch_size` or `max_bytes`), reducing round-trips and improving throughput.
- **Encoding**: Standard codecs (text, json, csv, logfmt, etc.) are built in; we only need to emit structured events from our source.
- **Healthchecks**: Vector runs healthchecks on sources and sinks at startup, so misconfiguration (e.g. wrong DB table or missing credentials) fails fast.

These reduce the amount of custom plumbing we have to build and maintain.

---

## 2. Cost

### 2.1 Operational cost

- **Single process**: One Vector process per pipeline (or per “task” in the demo) instead of multiple services. Fewer moving parts means less operational overhead (deploy, monitor, debug).
- **No extra queue layer for simple flows**: For sync jobs (e.g. file_list → S3 or file_list → MySQL), we don’t require Kafka/SQS/etc. Data flows source → sink inside Vector. Queues become necessary only if we need durable buffering or fan-out across many consumers.
- **Resource usage**: Vector is Rust-based and can be tuned via `batch_size`, `max_bytes`, and timeouts. We can cap memory and CPU by limiting concurrency and batch sizes in config.

### 2.2 Storage and transfer cost

- **Source-side filtering**: The file_list source filters by time range and prefix before downloading. We only read objects that match (e.g. hourly partitions for raw_logs), avoiding unnecessary GETs and transfer.
- **Compression**: When writing to S3 we use gzip (e.g. in the aws_s3 sink), reducing storage and transfer cost.
- **Incremental sync where applicable**: For Delta Lake–backed flows, the delta_lake_watermark source uses checkpoints so we only process new data on subsequent runs, reducing repeated reads and writes.

Cost control is therefore largely a matter of configuration (time range, max_keys, batch size, compression) rather than re-architecting the pipeline.

---

## 3. Stability

### 3.1 Failure containment

- **Process boundary**: Each sync run is a Vector process. If it crashes or is killed (e.g. timeout), the host process manager (or the demo API) can restart or report failure without bringing down other workloads.
- **No shared in-process state across tasks**: Different tasks (e.g. different task_ids in the demo) use different config files and, where applicable, different checkpoint directories. One bad task doesn’t corrupt another.

### 3.2 Config-driven behavior

- Pipelines are defined in TOML. Changing timeouts, batch sizes, or sink options doesn’t require code changes. This makes it easier to tune for stability (e.g. increase `timeout_secs` for large syncs) and to replicate behavior across environments.

### 3.3 Observability of the pipeline

- Vector emits structured logs and metrics. We can log to stdout/stderr and capture them (e.g. in the demo we write to `vector_log_path`). Failures (e.g. “Failed to insert event”, “Table doesn’t exist”) are visible in those logs for quick diagnosis.

### 3.4 Sink and source robustness

- **tidb sink**: Uses a connection pool, retries on transient DB errors (depending on implementation), and validates table schema at startup so missing or wrong tables fail early.
- **file_list source**: Uses the object_store crate for S3/GCS/Azure with standard credential and retry behavior. List and get operations can be tuned (e.g. timeouts) via config.

Stability is improved by failing fast on misconfiguration, containing failures to a single process/task, and making failures visible in logs.

---

## 4. Data Guarantee: At Least Once

We need to ensure that data is **not lost** when we sync from object storage or Delta Lake to S3 or MySQL/TiDB: each record should be delivered **at least once** (duplicates are acceptable and can be handled by idempotent writes or deduplication).

### 4.1 Where we need at-least-once

- **Delta Lake → downstream (e.g. TiDB)**: The delta_lake_watermark source reads from a Delta table (e.g. in S3) and writes to a sink. If we advance the checkpoint only after the sink has accepted the data, we avoid “read and checkpointed but not written” and thus avoid silent loss.
- **Raw logs (file_list) → S3 or MySQL**: Here the “source of truth” is the object store. If a run fails mid-way, we can re-run the same time range and prefix; the sink (S3 or DB) may see some duplicates but we don’t lose data if we design for idempotency or re-sync from a known range.

### 4.2 How we achieve it

**Acknowledgements**

- Vector supports **acknowledgements**: a sink can acknowledge events only after they have been durably written. The delta_lake_watermark source is designed to work with this: it can update its checkpoint only after the downstream has acked the batch. That way we don’t advance the checkpoint for data that never reached the sink.
- In our demo and docs we enable acknowledgements where applicable (e.g. for the delta_lake_watermark → tidb pipeline) so that checkpoint advancement is tied to successful sink delivery.

**Checkpointing (Delta Lake path)**

- The delta_lake_watermark source persists a **checkpoint** (e.g. last watermark and last processed id) on disk. On restart, it resumes from that checkpoint. Combined with acknowledgements, we get:
  - **No double-advance**: We don’t move the checkpoint past a record until the sink has accepted it.
  - **Resume after crash**: After a failure, we re-run from the last checkpoint instead of from the beginning, and we don’t re-checkpoint data that wasn’t acked.

So for the Delta Lake–based sync path, at-least-once is achieved by **checkpoint + acknowledgements**.

**Re-runnable sync (file_list path)**

- For file_list-driven sync (raw logs to S3 or MySQL), the source lists objects and emits events in a deterministic way (same cluster_id, types, time range → same list). If a run fails:
  - We do **not** persist a checkpoint in the current file_list implementation for content sync; the run is “one-shot” for that time range.
  - To avoid loss, we **re-run the same time range**. That may produce duplicates in the sink (same file or same log lines written again). So we get at-least-once by **re-running**; idempotency or deduplication (e.g. by primary key or file path + offset) is left to the sink or downstream (e.g. overwrite by key, or “insert ignore” / upsert in DB).

So for the file_list path, at-least-once is achieved by **re-runnable jobs and idempotent or deduplicating sinks**, not by an in-process checkpoint.

### 4.3 Summary

| Path | Mechanism for at-least-once |
|------|-----------------------------|
| Delta Lake → TiDB (or other sink) | Checkpoint + acknowledgements: advance checkpoint only after sink acks. |
| file_list (raw logs) → S3 / MySQL | Re-run same time range on failure; design sink for idempotency or deduplication. |

In both cases the goal is **no silent data loss**: every record that we intend to sync is delivered at least once to the sink, with Vector’s backpressure and batching helping avoid overload and partial writes where applicable.

---

## 5. Monitoring and alerting

To keep sync pipelines reliable we need to **observe** their behaviour and **alert** when something is wrong. This section describes what to monitor and how to turn that into alerts.

### 5.1 What to monitor

**Process and task outcome**

- **Vector process exit code**: A non-zero exit (or timeout/kill) means the run failed. The orchestrator (e.g. demo API or a job runner) should treat this as a failure and optionally retry or notify.
- **Task status**: In the demo we store per-task status (e.g. `completed` vs failed) and `vector_log_path`. A monitoring system can poll the API or a DB to see “last run failed” or “no successful run in the last N hours” for a given pipeline.

**Logs**

- **Vector stdout/stderr**: We capture these to a file (e.g. `vector_log_path`). They contain:
  - Startup: config load, healthcheck pass/fail (e.g. “Table doesn’t exist”, “Failed to connect”).
  - Runtime: source progress (e.g. “Found N files”), sink errors (e.g. “Failed to insert event”), and backpressure/throughput hints.
- **Orchestrator logs**: The demo or job runner may log task start/end, timeout, and the chosen `vector_log_path` for later inspection.

**Optional: Vector metrics**

- Vector can expose **Prometheus metrics** (e.g. via its API or a dedicated metrics sink). Useful metrics include:
  - Events received/sent per source/sink, and errors/drops.
  - Buffer sizes and processing latency.
- If you run Vector under a process manager or in Kubernetes, you can also monitor **resource usage** (CPU, memory) and alert on sustained high usage or OOM.

### 5.2 How to get signals

| Signal | How to get it | Use for |
|--------|----------------|--------|
| Run failed | Vector exit code ≠ 0 or timeout | Alert: “Sync task X failed.” |
| Run succeeded | Exit code 0, task status `completed` | Dashboards, “last success” time. |
| Why it failed | Tail or ship `vector_log_path` to a log store, search for ERROR | On-call diagnosis, post-mortem. |
| Throughput / health | Vector Prometheus metrics (if enabled) | Capacity and backpressure alerts. |
| Orchestrator health | Demo API liveness, task list, or job queue depth | Alert if orchestrator is down or backlog grows. |

So: **exit code + task status** for “did it work?”, **logs** for “why not?”, and **metrics** (optional) for “how much and how healthy?”.

### 5.3 Alerting strategy

- **Critical**: Sync task failed (non-zero exit or timeout). Someone should be notified so they can re-run, fix config (e.g. table name, credentials), or fix the sink (e.g. DB full).
- **Warning**: No successful run for a given pipeline in the last N hours (e.g. cron didn’t fire or all runs failed). Reduces silent gaps in data.
- **Optional**: High error rate or drop rate in Vector metrics, or sustained high CPU/memory, to catch degradation before total failure.

We do **not** implement the alerting channel ourselves (e.g. PagerDuty, Slack). Instead we assume:

- The **orchestrator** (demo API, Kubernetes Job, or cron wrapper) observes exit code and/or task status and reports to your existing monitoring system (e.g. Prometheus + Alertmanager, Datadog, CloudWatch).
- **Logs** are shipped (e.g. Fluentd, CloudWatch Logs, or a file collector) so that “Vector run failed” alerts can be correlated with “Failed to insert event” or “Table doesn’t exist” in the same run.

So monitoring and alerting are **integration points**: we expose outcome (exit code, status, logs, optional metrics), and you plug them into your existing monitoring and alerting stack to get at-least-once behaviour and timely reaction to failures.

### 5.4 Real-time logs and Vector as a separate container

**Why logs only appeared after the task finished (fixed)**

- Previously the demo ran Vector with `subprocess.run(..., capture_output=True)`, so stdout/stderr were buffered in memory and written to the log file only when the process exited. That’s why you only saw logs after the task finished.
- **Change**: The demo now runs Vector with stdout/stderr **directly connected to the log file** (no capture). Vector writes to the file as it runs, so you can **tail the log file while the task is running** and see progress immediately, e.g. `tail -f /tmp/vector-tasks/<task_id>_sync_logs.log`.

**When Vector runs as an independent image/container**

- **Logs**: In a container, Vector should write to **stdout/stderr** (not to a file inside the container). Then the container runtime captures logs and you can use:
  - **Docker**: `docker logs -f <container_id>` to stream logs in real time.
  - **Kubernetes**: `kubectl logs -f <pod> -c <vector_container>`.
  - Your log aggregator (Fluentd, CloudWatch Logs, etc.) can collect from the runtime so logs are available even after the container exits.
- In the container image, run Vector **without** redirecting to a file: e.g. `vector --config /etc/vector/vector.toml` so that all Vector output goes to stdout/stderr. If the demo or another process used to write to a file, in container mode the “orchestrator” should not start Vector with a file redirect; instead, the container’s main process is Vector and the runtime handles logs.

- **Task progress**: Vector exposes an **API** when `api.enabled = true` in config (the demo sets `address = "127.0.0.1:0"`, i.e. a random port on localhost). To see progress when Vector runs in its own container:
  1. **Fix the API port and expose it**: e.g. set `address = "0.0.0.0:8686"` in the Vector config and expose port 8686 in the container. Then from the host or another service you can call Vector’s API (e.g. `GET /api/v1/metrics` or the topology/health endpoints) to get metrics such as events received/sent per component.
  2. **Metrics**: Vector’s API can expose internal metrics (e.g. `vector_*`). You can poll `http://<vector_container>:8686/api/v1/metrics` (or the port you chose) to get counters like `vector_events_processed_total` by component, so you can show “files listed”, “events sent to sink”, etc.
  3. **Or rely on logs**: The file_list source logs lines like “Found N files”, “listed file file_path=...”. By streaming container logs (e.g. `docker logs -f`) you see progress as it happens; no API needed if log streaming is enough.

Summary: **Real-time logs** = no capture, write to file (demo) or stdout (container); **progress** = stream those logs and/or expose Vector’s API port and poll metrics.

### 5.5 Vector as a standalone Pod (no demo): how to get task progress and running state

When Vector runs as an **independent Pod** (e.g. Kubernetes Job or Deployment), there is **no demo API**. You cannot call something like “GET /tasks/&lt;id&gt;/progress”. Task progress and running state must come **from Vector itself** in one of two ways.

**1. Logs (always available)**

- Vector writes to **stdout/stderr**. The container runtime captures this.
- **Stream logs in real time**:
  - Kubernetes: `kubectl logs -f <pod> -c <vector_container>`
  - Docker: `docker logs -f <container_id>`
- **What to look for (file_list source)**:
  - `Listing files with prefix: ... merged-logs/2026021312/loki/` → which hour/component is being listed.
  - `Found N files matching criteria` → one such line per (hour, component) partition; counting these gives “partitions completed”.
  - `listed file file_path=...` → each file in that partition (noisy).
  - Sink errors: `Failed to insert event`, etc.
- So **progress** = count of “Found … files matching criteria” in the log. If you know total partitions (e.g. from time range and `raw_log_components`: 31 hours × 3 components = 93), then progress ≈ (that count) / 93. You can do this parsing in a sidecar, a log pipeline, or by hand when tailing.

**2. Vector API (metrics + health)**

- With **no demo**, the only way to get “running state” and throughput in a machine-readable way is Vector’s **built-in API**.
- In the Vector config used in the Pod, enable the API and **bind to a fixed port** so you can expose it from the Pod and poll it from outside:

```toml
[api]
enabled = true
address = "0.0.0.0:8686"
```

- In the Pod spec, expose port 8686 and (if needed) a service so you can reach the Pod.
- **Endpoints you can use**:
  - **Health / liveness**: e.g. `GET http://<pod-ip>:8686/health` or the root/API path (see Vector docs for exact path). Use this for “is Vector still running?” and for Kubernetes liveness/readiness if you want.
  - **Metrics**: `GET http://<pod-ip>:8686/api/v1/metrics` (or the URL your Vector version exposes). Returns Prometheus-style metrics such as:
    - `vector_events_processed_total` (by component_id: file_list, tidb_sink, etc.) → “events out of source” / “events into sink”; you can derive “events processed so far” and, if you know total events (e.g. from total files × avg lines), a rough ETA.
- **From outside the cluster** (e.g. your laptop), use port-forward then curl:

```bash
kubectl port-forward pod/<vector-pod> 8686:8686
curl -s http://127.0.0.1:8686/api/v1/metrics
```

- So **running state** = “does the API respond?”; **progress** = “events_processed_total for file_list (and optionally for the sink)” from the metrics endpoint. You can build a small dashboard or script that polls this and, if you know total work from the job spec (time range + components), computes progress % and ETA.

**Summary (no demo)**

| What you need   | How (Vector standalone Pod) |
|-----------------|-----------------------------|
| Real-time logs  | `kubectl logs -f <pod>` (or `docker logs -f`) |
| “Is it still running?” | Pod not Completed; or poll Vector API health |
| Progress (human) | Count “Found … files matching criteria” in logs; compare to total partitions (hours × components) |
| Progress (machine) | Enable `api.enabled = true`, `address = "0.0.0.0:8686"`, expose 8686, poll `/api/v1/metrics` for `vector_events_processed_total` |
| ETA             | From metrics: (total_events - events_processed) / rate; or from logs: (total_partitions - done_partitions) × (elapsed / done_partitions) |

---

## References

- Vector documentation: [vector.dev/docs](https://vector.dev/docs/)
- Project: `AGENTS.md`, `src/sources/file_list/arch.md`, `src/sinks/tidb/arch.md`
- Demo (checkpoint + acknowledgements): `demo/app.py` (delta_lake_watermark flow)
- Delta Lake watermark source: `src/sources/delta_lake_watermark/` (checkpoint, acknowledgements)
