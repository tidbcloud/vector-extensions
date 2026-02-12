# Changelog

本文档记录 sync-logs / file_list / S3 分区相关功能开发过程中遇到的问题及解决方式，便于后续维护与排查。

---

## 一、sync-logs 全流程在 Vector 内完成

**问题**：原先 demo 侧用 boto3 从源 bucket 拷贝对象到目标 bucket，业务逻辑写在 Python 里，与 Vector 职责重叠，且难以复用 Vector 的 encoding、batch、compression 等能力。

**解决**：

- 由 **file_list source** 负责：拉取对象列表、按需下载内容、按路径或内容解压 gzip，将文件内容放入事件的 `message` 字段。
- 由 **官方 aws_s3 sink** 负责：按 batch 聚合、按 `max_bytes` 分片、encoding（text/json）、compression（gzip）上传。
- Demo 只生成 Vector 配置并启动 Vector，不再包含任何 S3 拷贝业务逻辑。

**涉及**：`demo/app.py`（`generate_sync_logs_vector_config`、`sync_logs`）、file_list 的 `emit_content`、`decompress_gzip`。

---

## 二、使用官方 aws_s3 sink 而非自研“按路径上传”sink

**问题**：是否需要维护自定义的“content 写 S3”类 sink（如曾考虑的 content_to_s3）？

**解决**：采用**官方 aws_s3 sink**，通过其已有能力即可满足需求：

- `encoding`：使用 `message` 字段，选 `text` 或 `json`。
- `batch`：用 `max_bytes` 控制每个对象大小。
- `compression`：设为 `gzip` 节省存储与带宽。

无需再维护一套“读本地文件/内容再上传”的自定义 sink，减少维护成本并与上游 Vector 行为一致。

**涉及**：`demo/app.py` 中 sink 配置为 `type = "aws_s3"`，并配置 `encoding`、`batch`、`compression`。

---

## 三、按内容识别 gzip（不只看扩展名）

**问题**：部分对象未带 `.gz` 后缀但内容实为 gzip，仅按路径后缀判断会不解压，导致下游拿到乱码或二进制。

**解决**：

- 在 file_list 拉取到内容后，除按路径是否以 `.gz` 结尾决定是否解压外，增加**按内容魔数**判断：若前两字节为 `1f 8b`（gzip magic），则按 gzip 解压。
- 配置项 `decompress_gzip = true` 时，同时应用“路径后缀”与“魔数”两种判断。

**涉及**：`src/sources/file_list/file_lister.rs`（或相关下载/解压逻辑）中的 gzip 检测与解压。

---

## 四、raw_logs 不传组件时如何得到“全部组件”

**问题**：raw_logs 按“小时 + 组件”组织目录（如 `merged-logs/2026020411/tidb/`、`.../operator/`）。用户不传 `raw_log_components` 时期望自动发现该小时下所有组件，而不是写死或报错。

**解决**：

- 引入 **RawLogsDiscover** 请求：只传小时级 prefix（如 `merged-logs/2026020411/`），由 file_list 在该 prefix 下**列出下一级子目录名**作为组件列表。
- 使用存储的 **list_with_delimiter**（或等价“按 delimiter 列前缀”）在 `hour_prefix` 下列出子目录，得到组件名；再对每个 `(hour_prefix, component)` 发 FileList 列文件并下发事件。
- 若用户**显式传入** `raw_log_components`，则按原有方式对每个 (小时, 组件) 发 FileList，不再先 Discover。

**涉及**：`src/sources/file_list/path_resolver.rs`（`ListRequest::RawLogsDiscover`、未传 `raw_log_components` 时只发 RawLogsDiscover）、`file_lister.rs`（`list_subdir_names`）、controller 对 RawLogsDiscover 的处理。

---

## 五、多组件日志要按“组件 + 时间”分开写，路径可读

**问题**：多个组件（如 tidb、operator）的日志若混在同一流里写 S3，无法从**路径/文件名**直接看出是哪个组件、哪段时间，不利于按组件与时间排查和管理。

**解决**：

1. **事件带分区字段**：file_list 在发出每条与 raw_logs 相关的事件时，写入 **`component`** 与 **`hour_partition`**（10 位小时，如 `2026020411`）。
   - **FileList 分支**：用 `parse_raw_logs_prefix(prefix)` 从路径中解析出 `(hour_partition, component)`，若解析到则写入事件。
   - **RawLogsDiscover 分支**：已知 `hour_prefix` 与子目录名 `comp`，将 `hour_prefix` 最后一段作为 `hour_partition`，`comp` 作为 `component` 写入事件。
2. **S3 路径按分区**：使用官方 aws_s3 sink 的 **key_prefix 模板**，将路径设为按组件和小时分区，例如：
   - `key_prefix = "your_prefix/{{ component }}/{{ hour_partition }}/"`
   - 官方 sink 会按渲染后的 key 分批，同一 `(component, hour_partition)` 写入同一前缀下，文件名仍由 sink 的时间/UUID 等规则生成。这样从路径即可看出“哪个组件、哪一小时”。

**涉及**：`src/sources/file_list/controller.rs`（两处写入 `component` / `hour_partition`）、`path_resolver.rs` 的 raw_logs 路径约定、`demo/app.py` 中 aws_s3 的 `key_prefix` 配置。

---

## 六、是否必须自研“按分区写 S3”的 sink

**问题**：曾认为官方 aws_s3 无法按事件字段（如 component、hour_partition）动态决定路径，因此考虑自研 **s3_content_partitioned** 类 sink，按 `(component, hour_partition)` 分 buffer 并写入固定格式路径（如 `part-NNNNN.log.gz`）。

**解决**：官方 **aws_s3 的 key_prefix 支持模板语法**（[Vector Template syntax](https://vector.dev/docs/reference/configuration/template-syntax/)）：

- 可使用 **`{{ field_name }}`** 引用事件字段，例如 `{{ component }}`、`{{ hour_partition }}`。
- Sink 会按**渲染后的 key_prefix** 对事件分组，同一前缀的写入同一批、同一路径下。
- 因此只需配置：  
  `key_prefix = "dest_prefix/{{ component }}/{{ hour_partition }}/"`  
  即可实现“按组件 + 小时”分区，**无需**自定义分区 sink。

**结论**：sync-logs 场景改用官方 aws_s3 + key_prefix 模板即可；自研的 **s3_content_partitioned** 仍保留在代码库中，若有“固定 part 编号”或与官方不同的分片策略需求时可选用。

**涉及**：`demo/app.py`（改回 `aws_s3` + 模板 key_prefix）、`src/sinks/s3_content_partitioned/`（保留但非默认）。

---

## 七、小结表

| 问题 | 解决 |
|------|------|
| sync-logs 业务逻辑在 demo 里、与 Vector 重叠 | 全流程在 Vector 内：file_list 拉取+解压，aws_s3 聚合+分片+压缩 |
| 是否维护自定义“写内容到 S3”的 sink | 不维护，用官方 aws_s3（encoding / batch / compression） |
| 无 .gz 后缀但内容为 gzip 的对象 | 按内容魔数 1f 8b 判断并解压 |
| raw_logs 不传组件时要“全部组件” | RawLogsDiscover + list_subdir_names 按小时发现组件 |
| 多组件日志混在一起、路径不可读 | 事件带 component / hour_partition，sink 按路径分区 |
| 官方 sink 能否按事件字段分区 | 能，key_prefix 用 `{{ component }}/{{ hour_partition }}/` 即可，无需自研分区 sink |

---

*文档随功能迭代更新，若实现与上述描述不一致，以代码与 arch 文档为准。*
