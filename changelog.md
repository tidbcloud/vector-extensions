# Changelog

This document records issues and resolutions during sync-logs / file_list / S3 partitioning development for maintenance and troubleshooting.

---

## I. sync-logs Full Flow in Vector

**Issue**: Demo used boto3 to copy objects from source to dest bucket; logic in Python overlapped with Vector and could not reuse Vector's encoding, batch, compression.

**Resolution**:

- **file_list source**: Fetches object list, downloads content on demand, decompresses gzip by path or content, puts file content in event `message`.
- **Official aws_s3 sink**: Batch aggregation, `max_bytes` sharding, encoding (text/json), compression (gzip) upload.
- Demo only generates Vector config and starts Vector; no S3 copy logic.

**Files**: `demo/app.py` (`generate_sync_logs_vector_config`, `sync_logs`), file_list `emit_content`, `decompress_gzip`.

---

## II. Use Official aws_s3 Sink, Not Custom "Path Upload" Sink

**Issue**: Maintain a custom "write content to S3" sink (e.g. content_to_s3)?

**Resolution**: Use **official aws_s3 sink**; existing features suffice:

- `encoding`: Use `message` field, choose `text` or `json`.
- `batch`: Use `max_bytes` for object size.
- `compression`: Set `gzip` for storage and bandwidth savings.

No need for a custom "read local file/content and upload" sink; reduces maintenance and aligns with upstream Vector.

**Files**: `demo/app.py` sink config `type = "aws_s3"`, with `encoding`, `batch`, `compression`.

---

## III. Detect gzip by Content (Not Extension Only)

**Issue**: Some objects have no `.gz` suffix but content is gzip; path-only check skips decompression, causing downstream garbage/binary.

**Resolution**:

- In file_list after fetch: besides path suffix, add **content magic** check: if first two bytes are `1f 8b` (gzip magic), treat as gzip.
- When `decompress_gzip = true`, apply both "path suffix" and "magic" checks.

**Files**: `src/sources/file_list/file_lister.rs` (or related download/decompress logic) gzip detection.

---

## IV. raw_logs Without Components: How to Get "All Components"

**Issue**: raw_logs organized by "hour + component" (e.g. `merged-logs/2026020411/tidb/`, `.../operator/`). When user omits `raw_log_components`, expect automatic discovery of all components for that hour.

**Resolution**:

- Add **RawLogsDiscover** request: pass hour-level prefix only (e.g. `merged-logs/2026020411/`); file_list **lists next-level subdir names** under that prefix as component list.
- Use storage **list_with_delimiter** (or equivalent) under `hour_prefix` to list subdirs, get component names; then for each `(hour_prefix, component)` issue FileList and emit events.
- If user **explicitly passes** `raw_log_components`, use original flow per (hour, component) FileList, no Discover.

**Files**: `path_resolver.rs` (`ListRequest::RawLogsDiscover`), `file_lister.rs` (`list_subdir_names`), controller handling of RawLogsDiscover.

---

## V. Multi-Component Logs by "Component + Time", Readable Paths

**Issue**: Multiple components (e.g. tidb, operator) mixed in one stream to S3; path/filename does not indicate component or time; hard to debug or manage.

**Resolution**:

1. **Events with partition fields**: file_list writes **`component`** and **`hour_partition`** (10-digit hour, e.g. `2026020411`) on each raw_logs event.
   - **FileList branch**: `parse_raw_logs_prefix(prefix)` parses `(hour_partition, component)` from path; if found, write to event.
   - **RawLogsDiscover branch**: `hour_prefix` and subdir name `comp` known; last segment of `hour_prefix` → `hour_partition`, `comp` → `component`.
2. **S3 path by partition**: Use official aws_s3 sink **key_prefix template**, e.g. `key_prefix = "your_prefix/{{ component }}/{{ hour_partition }}/"`. Sink batches by rendered key; same prefix writes under same path; filenames still from sink rules. Path then shows "which component, which hour".

**Files**: `controller.rs` (write `component` / `hour_partition`), `path_resolver.rs` raw_logs path convention, `demo/app.py` aws_s3 `key_prefix`.

---

## VI. Must We Implement Custom "Partitioned S3" Sink?

**Issue**: Assumed official aws_s3 cannot use event fields (e.g. component, hour_partition) for dynamic path; considered custom **s3_content_partitioned** sink with per-(component, hour_partition) buffers and fixed paths (e.g. `part-NNNNN.log.gz`).

**Resolution**: Official **aws_s3 key_prefix supports templates** ([Vector Template syntax](https://vector.dev/docs/reference/configuration/template-syntax/)):

- Use **`{{ field_name }}`** for event fields, e.g. `{{ component }}`, `{{ hour_partition }}`.
- Sink groups events by **rendered key_prefix**; same prefix writes to same batch and path.
- So just configure `key_prefix = "dest_prefix/{{ component }}/{{ hour_partition }}/"` for component+hour partitioning; **no** custom partition sink needed.

**Conclusion**: sync-logs uses official aws_s3 + key_prefix template; custom **s3_content_partitioned** remains in repo for "fixed part numbering" or different sharding strategies.

**Files**: `demo/app.py` (switch to `aws_s3` + template key_prefix), `src/sinks/s3_content_partitioned/` (kept but not default).

---

## VII. Summary Table

| Issue | Resolution |
|-------|------------|
| sync-logs logic in demo, overlaps Vector | Full flow in Vector: file_list fetch+decompress, aws_s3 aggregate+shard+compress |
| Maintain custom "write content to S3" sink? | No; use official aws_s3 (encoding / batch / compression) |
| No .gz suffix but content is gzip | Detect by content magic 1f 8b and decompress |
| raw_logs without components → "all components" | RawLogsDiscover + list_subdir_names discover by hour |
| Multi-component logs mixed, paths unreadable | Events with component / hour_partition; sink partitions by path |
| Can official sink partition by event fields? | Yes; key_prefix `{{ component }}/{{ hour_partition }}/`; no custom sink |

---

*Document updated with features; if implementation differs from above, follow code and arch docs.*
