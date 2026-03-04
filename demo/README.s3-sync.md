# S3 直连同步镜像（无需 Vector）

面向仅需**原样备份 raw_logs、不做格式转换**的场景：从 Vector 配置中解析 `start_time`、`end_time`、`raw_log_components` 和 sink 的 `key_prefix` 固定部分，按 **最小目录（每小时 × 每个 component）** 逐个执行 `aws s3 sync`，便于看进度。

## 路径规则（与 file_list 一致）

- 源：`s3://{bucket}/diagnosis/data/{cluster_id}/merged-logs/{YYYYMMDDHH}/{component}/`
- 目标：`s3://{bucket}/{key_prefix固定部分}/{component}/{YYYYMMDDHH}/`
- `key_prefix` 只取第一个 `{{` 之前的部分，例如 `leotest6/{{ component }}/{{ hour_partition }}/` → 固定部分为 `leotest6`，数据拷贝到 `leotest6/{component}/{YYYYMMDDHH}/` 下。

## 构建

```bash
cd demo
docker build -f Dockerfile.s3-sync -t s3-sync-from-config:latest .
```

**在 Kubernetes（x86_64 节点）上跑时**：若在 Mac M1/M2（arm64）上构建，镜像架构会与集群不一致，容器内会报 `exec format error`。需指定目标平台为 amd64 再构建并推送：

```bash
docker build --platform linux/amd64 -f Dockerfile.s3-sync -t s3-sync-from-config:latest .
```

## 运行

挂载包含 vector 配置的文件（支持纯 TOML 或 YAML ConfigMap 中的 `vector.toml`），并配置 AWS 凭证：

```bash
docker run --rm \
  -v $(pwd)/vector-config.yaml:/config/vector.toml:ro \
  -e AWS_ACCESS_KEY_ID=... \
  -e AWS_SECRET_ACCESS_KEY=... \
  s3-sync-from-config:latest
```

脚本会解析：

- `[sources.file_list]`：`endpoint`、`cluster_id`、`start_time`、`end_time`、`raw_log_components`、`types`（仅支持 `raw_logs`）
- `[sinks.to_s3]`：`bucket`、`key_prefix`（只取固定前缀）、`region`

然后按 (hour, component) 逐个执行 sync，并打印 `[当前/总数] sync YYYYMMDDHH / component` 作为进度。

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `CONFIG_FILE` | `/config/vector.toml` | 配置文件路径 |
| `SYNC_EXTRA_ARGS` | 空 | 传给每次 `aws s3 sync` 的额外参数，如 `--dryrun`、`--delete` |
| `AWS_EXTRA_ARGS` | 空 | 传给 `aws` 的全局参数 |

试跑（不写 S3）：

```bash
docker run --rm ... -e SYNC_EXTRA_ARGS="--dryrun" s3-sync-from-config:latest
```

## 与 Vector 的差异

- 不做格式转换、不经过 Vector 管道，仅做 S3→S3 原样拷贝。
- 按最小文件夹（每小时 × 每个 component）多次执行 `aws s3 sync`，便于观察进度和排查。
- 仅需 AWS CLI + 脚本，资源占用更小。
