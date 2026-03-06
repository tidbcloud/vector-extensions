# Demo - AI Agent Guide

This document defines development and maintenance rules for the Demo directory, for AI agents and developers.

## Core Principle: Demo Contains No Business Logic

**Demo must not contain any business logic code.**

- Demo responsibilities are limited to:
  - Generating Vector config (TOML)
  - Managing Vector process (start, monitor, stop)
  - Providing task/config REST API (create task, query status, etc.)
- All data-related logic (filtering, transformation, path parsing, time range, etc.) must be implemented in **Vector extensions**, not in Demo Python/scripts.

### Directory Filtering: Done by file_list source (paths fixed in code)

Directory/path filtering should not be hardcoded in Demo or assembled by Demo. **Path rules are fixed in file_list source by data type**; users do not need to know where files live.

When file_list supports "by data type" config, **users only specify**:

| Parameter | Description |
|-----------|-------------|
| `cluster_id` | Cluster ID (required) |
| `project_id` | Project ID (required for slowlog / sql_statement / top_sql / conprof) |
| `types` | Data types: `raw_logs`, `slowlog`, `sql_statement`, `top_sql`, `conprof` |
| `start_time` | Time range start (ISO 8601, required for raw_logs) |
| `end_time` | Time range end (ISO 8601, required for raw_logs) |

Type-to-path mapping is **fixed in file_list source**, e.g.:

- **raw_logs**: gzip raw logs → `diagnosis/data/{cluster_id}/merged-logs/{YYYYMMDDHH}/tidb/*.log`
- **slowlog**: Delta Lake table → `deltalake/{project_id}/{uuid}/slowlogs/`
- **sql_statement**: Delta Lake table → `deltalake/{project_id}/{uuid}/sqlstatement/`
- **top_sql**: per-instance Delta Lake → `deltalake/org={project_id}/cluster={cluster_id}/type=topsql_tidb/instance=*`
- **conprof**: pprof compressed files → `0/{project_id}/{conprof_org_id}/{cluster_id}/profiles/*.log.gz`

Demo passes `cluster_id`, `project_id` (if needed), `types`, `start_time`, `end_time` to file_list when generating Vector config; **path resolution and assembly are inside file_list source**.

### Sync/Copy: Full flow in Vector

Log sync (e.g. sync-logs) must **not** use boto3 etc. in Demo. Correct approach:

- **file_list**: `emit_content = true`, `decompress_gzip = true`; source fetches files, decompresses, puts content in event `message`.
- Downstream uses **official aws_s3 sink**: `encoding.codec = "text"` or `"json"`, `batch.max_bytes` controls object size, `key_prefix` for target prefix.
- Demo only: generates above Vector config, starts Vector, returns task status; **does not parse file_list output or perform any copy logic**.

## Demo Directory Structure

```
demo/
├── app.py              # API service, Vector config generation, process management
├── agents.md           # This file
├── config/             # Example/test configs
├── extension/          # Extension scripts (prefer migrating to Vector plugins)
├── scripts/            # Setup, start, test scripts
└── tests/              # Test scripts
```

## Related Docs

- Project overview and components: [AGENTS.md](../AGENTS.md)
- Demo architecture and API: [doc/v1/agent.md](../doc/v1/agent.md)
- file_list source architecture: [src/sources/file_list/arch.md](../src/sources/file_list/arch.md)
