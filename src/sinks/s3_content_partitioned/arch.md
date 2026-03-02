# s3_content_partitioned architecture

## Purpose

Write log events that have `component` and `hour_partition` to S3 by partition, so object paths reflect **component** and **hour partition** for lookup and governance. Typical upstream is the file_list source (raw_logs mode emits these fields).

## Overview

- **Input**: Log events with `message`, `component`, `hour_partition`.
- **Buffering**: Buffer by key `(component, hour_partition)`; upload one object when a key’s buffer reaches `max_file_bytes`.
- **Output path**: `{key_prefix}/{component}/{hour_partition}/part-NNNNN.log` or `.log.gz`.

## Configuration

| Option | Description |
|--------|-------------|
| bucket | S3 bucket name |
| key_prefix | Object key prefix, e.g. `loki` or `logs/raw` |
| region | AWS region or endpoint (optional) |
| max_file_bytes | Upload when a partition buffer reaches this many bytes; default 64MiB |
| compression_gzip | Whether to gzip uploads; default true |

## Data flow

1. Read `component`, `hour_partition`, `message` from each event; drop event if any is missing.
2. Append `message` (with newline if needed) to the buffer for that `(component, hour_partition)`.
3. When buffer length ≥ `max_file_bytes`, upload the first `max_file_bytes` bytes; object key is  
   `{key_prefix}/{component}/{hour_partition}/part-{part_index:05}.log[.gz]`, with part_index incrementing from 0.
4. At stream end, upload remaining buffer for each partition.

## Dependencies

- AWS SDK S3 (same as Vector’s existing S3 support)
- Upstream must provide `component` and `hour_partition` (e.g. file_list raw_logs discovery/list)

## Difference from aws_s3

- The official `aws_s3` sink builds keys from time-based rules and **cannot** partition by event fields (e.g. component, hour_partition).
- This sink is designed for S3 writes by component + hour partition; paths are `{component}/{hour_partition}/part-*.log[.gz]` for component- and time-based organization.
