# Demo - AI Agent 指南

本文档为 Demo 目录的开发与维护规范，供 AI Agent 与开发者遵循。

## 核心原则：Demo 不包含业务逻辑

**Demo 中不得包含任何业务逻辑代码。**

- Demo 的职责仅限于：
  - 生成 Vector 配置（TOML）
  - 管理 Vector 进程（启动、监控、停止）
  - 提供任务/配置的 REST API（创建任务、查询状态等）
- 所有与数据本身相关的逻辑（过滤、转换、目录解析、时间范围等）必须由 **Vector 扩展** 完成，而不是在 Demo 的 Python/脚本中实现。

### 目录过滤：由 file_list source 完成（路径在代码中固定）

目录/路径过滤不应在 Demo 中写死或由 Demo 拼路径。**路径规则在 file_list source 内部按数据类型写死**，用户不需要知道文件具体存在哪。

file_list source 支持「按数据类型」配置时，**用户只需指定**：

| 参数名 | 说明 |
|--------|------|
| `cluster_id` | 集群 ID（必填） |
| `project_id` | 项目 ID（slowlog / sql_statement / top_sql / conprof 时需要） |
| `types` | 数据类型，可多选：`raw_logs`、`slowlog`、`sql_statement`、`top_sql`、`conprof` |
| `start_time` | 时间范围起点（ISO 8601，raw_logs 必填） |
| `end_time` | 时间范围终点（ISO 8601，raw_logs 必填） |

各类型与路径的对应关系在 **file_list 源码中固定**，例如：

- **raw_logs**：gz 压缩的原始日志 → `diagnosis/data/{cluster_id}/merged-logs/{YYYYMMDDHH}/tidb/*.log`
- **slowlog**：Delta Lake 表 → `deltalake/{project_id}/{uuid}/slowlogs/`
- **sql_statement**：Delta Lake 表 → `deltalake/{project_id}/{uuid}/sqlstatement/`
- **top_sql**：按 instance 的 Delta Lake → `deltalake/org={project_id}/cluster={cluster_id}/type=topsql_tidb/instance=*`
- **conprof**：pprof 压缩文件 → `0/{project_id}/{conprof_org_id}/{cluster_id}/profiles/*.log.gz`

Demo 只需在生成 Vector 配置时，将 `cluster_id`、`project_id`（按需）、`types`、`start_time`、`end_time` 透传给 file_list；**路径识别与拼装均在 file_list source 内部实现**。

### 同步/拷贝：全流程在 Vector 内完成

同步日志（如 sync-logs）**不得**在 Demo 中用 boto3 等做拷贝。正确做法：

- **file_list** 配置 `emit_content = true`、`decompress_gzip = true`，由 source 拉取文件、解压，事件中带 `message`（文件内容）。
- 下游使用 **官方 aws_s3 sink**：`encoding.codec = "text"` 或 `"json"`（只写 message），`batch.max_bytes` 控制每对象大小，`key_prefix` 为目标前缀。
- Demo 仅：生成上述 Vector 配置、启动 Vector、返回任务状态；**不解析 file_list 输出、不执行任何拷贝逻辑**。

## Demo 目录结构

```
demo/
├── app.py              # 仅：API 服务、生成 Vector 配置、进程管理
├── agents.md           # 本文件
├── config/             # 示例/测试用配置文件
├── extension/          # 扩展脚本（若仍需要，应尽量迁移为 Vector 插件）
├── scripts/            # 环境准备、启动、测试脚本
└── tests/              # 测试脚本
```

## 相关文档

- 项目总览与组件说明：[AGENTS.md](../AGENTS.md)
- Demo 架构与 API 说明：[doc/v1/agent.md](../doc/v1/agent.md)
- file_list source 架构：[src/sources/file_list/arch.md](../src/sources/file_list/arch.md)
