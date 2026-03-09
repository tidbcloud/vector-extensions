# S3 Direct Sync Image (No Vector Required)

For **raw_logs backup without format conversion**: parses `start_time`, `end_time`, `raw_log_components` and the fixed part of sink's `key_prefix` from Vector config, then runs `aws s3 sync` per **minimal directory (per hour × per component)** for progress visibility.

## Path Rules (Same as file_list)

- Source: `s3://{bucket}/diagnosis/data/{cluster_id}/merged-logs/{YYYYMMDDHH}/{component}/`
- Dest: `s3://{bucket}/{key_prefix_fixed_part}/{component}/{YYYYMMDDHH}/`
- `key_prefix` uses only the part before the first `{{`, e.g. `leotest6/{{ component }}/{{ hour_partition }}/` → fixed part is `leotest6`, data is copied under `leotest6/{component}/{YYYYMMDDHH}/`.

## Build

```bash
cd demo
docker build -f Dockerfile.s3-sync -t s3-sync-from-config:latest .
```

**When running on Kubernetes (x86_64 nodes)**: If building on Mac M1/M2 (arm64), image arch will not match the cluster and you may get `exec format error`. Build and push for amd64:

```bash
docker build --platform linux/amd64 -f Dockerfile.s3-sync -t s3-sync-from-config:latest .
```

## Run

Mount a config file (TOML or `vector.toml` inside YAML ConfigMap) and set AWS credentials:

```bash
docker run --rm \
  -v $(pwd)/vector-config.yaml:/config/vector.toml:ro \
  -e AWS_ACCESS_KEY_ID=... \
  -e AWS_SECRET_ACCESS_KEY=... \
  s3-sync-from-config:latest
```

The script parses:

- `[sources.file_list]`: `endpoint`, `cluster_id`, `start_time`, `end_time`, `raw_log_components`, `types` (only `raw_logs` supported)
- `[sinks.to_s3]`: `bucket`, `key_prefix` (fixed prefix only), `region`

Then runs sync per (hour, component) and prints `[current/total] sync YYYYMMDDHH / component` as progress.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CONFIG_FILE` | `/config/vector.toml` | Config file path |
| `SYNC_EXTRA_ARGS` | (empty) | Extra args for each `aws s3 sync`, e.g. `--dryrun`, `--delete` |
| `AWS_EXTRA_ARGS` | (empty) | Global args passed to `aws` |

Dry run (no S3 writes):

```bash
docker run --rm ... -e SYNC_EXTRA_ARGS="--dryrun" s3-sync-from-config:latest
```

## Differences from Vector

- No format conversion, no Vector pipeline; S3→S3 copy only.
- Runs `aws s3 sync` per minimal folder (per hour × per component) for visibility and debugging.
- Only needs AWS CLI + script; lighter resource usage.
