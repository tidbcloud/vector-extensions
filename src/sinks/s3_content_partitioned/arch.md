# s3_content_partitioned 架构说明

## 目的

将带有 `component` 与 `hour_partition` 的日志事件按分区写入 S3，使路径能直接反映**组件**和**小时分区**，便于按组件、时间查找与治理。典型上游为 file_list source（raw_logs 模式会下发上述字段）。

## 架构概览

- **输入**：Log 事件，需包含 `message`、`component`、`hour_partition`。
- **缓冲**：按 `(component, hour_partition)` 分 key 缓冲，每个 key 达到 `max_file_bytes` 时上传一个对象。
- **输出路径**：`{key_prefix}/{component}/{hour_partition}/part-NNNNN.log` 或 `.log.gz`。

## 配置

| 配置项 | 说明 |
|--------|------|
| bucket | S3 bucket 名称 |
| key_prefix | 对象 key 前缀，例如 `loki` 或 `logs/raw` |
| region | AWS region 或 endpoint（可选） |
| max_file_bytes | 每个分区缓冲达到该字节数时触发一次上传，默认 64MiB |
| compression_gzip | 是否对上传内容做 gzip 压缩，默认 true |

## 数据流

1. 从事件中读取 `component`、`hour_partition`、`message`；缺字段则丢弃该事件。
2. 将 `message`（必要时加换行）追加到对应 `(component, hour_partition)` 的缓冲。
3. 当缓冲长度 ≥ `max_file_bytes` 时，取前 `max_file_bytes` 字节上传，对象 key 为  
   `{key_prefix}/{component}/{hour_partition}/part-{part_index:05}.log[.gz]`，part_index 从 0 递增。
4. 流结束时将各分区剩余缓冲依次上传。

## 依赖

- AWS SDK S3（与 vector 现有 s3 能力一致）
- 上游需提供 `component`、`hour_partition`（如 file_list 的 raw_logs 发现/列表）

## 与 aws_s3 的区别

- 官方 `aws_s3` sink 的 key 由时间等固定规则生成，**不能**按事件字段（如 component、hour_partition）动态分区。
- 本 sink 专为“按组件 + 小时分区”写 S3 设计，路径即 `{component}/{hour_partition}/part-*.log[.gz]`，便于按组件、时间区分日志。
