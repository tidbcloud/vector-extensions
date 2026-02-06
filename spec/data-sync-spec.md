# 集群诊断数据备份系统技术规范

## 1. 概述

### 1.1 背景

本文档定义了基于 Vector 的集群诊断数据备份系统的技术规范。该系统主要用于按指定时间段备份集群的诊断数据（日志、慢查询日志、SQL 语句、指标等），支持用户自定义过滤规则以减少传输量，加快重要数据的备份过程。

### 1.2 设计目标

- **专用性**: 专注于集群诊断数据的备份场景
- **高效性**: 支持过滤规则，减少不必要的数据传输
- **灵活性**: 支持多种数据格式和存储位置
- **易实现**: 充分利用 Vector 插件生态，减少开发工作量
- **可指导**: 提供清晰、完整的规范，便于 AI 辅助实现

### 1.3 核心原则

- 使用 Vector 作为数据采集、转换和传输引擎
- 充分利用 Vector 现有插件，减少自定义开发
- 支持时间段精确指定
- 支持用户自定义过滤规则
- 支持多种数据源格式（压缩文件、API、数据库等）

## 2. 需求分析

### 2.1 核心场景

#### 场景 1: 时间段诊断数据备份（首要场景）

**需求描述:**
指定一个集群（cluster）和时间段，将该时间段内的所有诊断数据备份到目标存储。

**诊断数据类型:**
1. **日志 (Logs)**: 应用日志、系统日志等
2. **慢查询日志 (Slow Logs)**: 数据库慢查询记录
3. **SQL 语句 (SQL Statements)**: SQL 执行记录
4. **指标 (Metrics)**: 性能指标、监控指标等

**时间范围:**
- 支持精确的时间段指定（开始时间 + 结束时间）
- 支持时区配置
- 支持相对时间（如最近 24 小时）

#### 场景 2: 过滤式备份（次要场景）

**需求描述:**
在备份过程中，根据用户指定的过滤规则对数据进行过滤，只备份符合条件的数据，以减少传输量和加快备份速度。

**过滤能力:**
- 基于关键字过滤
- 基于正则表达式过滤
- 基于字段值过滤
- 基于时间范围过滤（更细粒度）

### 2.2 数据源特点

#### 2.2.1 数据格式多样性

诊断数据可能以多种格式存储在不同位置：

**日志数据:**
- **S3 存储**: 日志文件以 gzip 压缩格式存储在 S3 上
- **Loki**: 日志同时存储在 Loki 中，便于查询
- **Parquet 统计**: 后台程序每小时生成 parquet 格式的统计信息

**慢查询日志:**
- 可能存储在数据库中（如 TiDB 的 `information_schema.slow_query`）
- 可能以文件形式存储在 S3
- 可能通过 API 接口提供

**SQL 语句:**
- 通常存储在数据库中
- 可能通过监控系统 API 提供
- 可能以日志形式记录

**指标数据:**
- 通常存储在 Prometheus、VictoriaMetrics 等时序数据库
- 可能通过 API 导出
- 可能以文件形式存储

#### 2.2.2 存储位置多样性

- **对象存储**: S3、MinIO、Azure Blob 等
- **时序数据库**: Prometheus、VictoriaMetrics、InfluxDB
- **日志系统**: Loki、Elasticsearch
- **关系数据库**: TiDB、MySQL、PostgreSQL
- **文件系统**: 本地文件系统、NFS 等

### 2.3 数据源映射示例

以 TiDB 集群为例，诊断数据可能的存储位置：

```
集群: tidb-cluster-01
├── 日志
│   ├── S3: s3://logs-bucket/tidb-cluster-01/logs/2024/01/01/*.log.gz
│   ├── Loki: loki://loki-server:3100 (label: cluster=tidb-cluster-01)
│   └── Parquet: s3://stats-bucket/tidb-cluster-01/stats/hourly/*.parquet
├── 慢查询日志
│   ├── 数据库: tidb://tidb-server:4000/information_schema.slow_query
│   └── S3: s3://logs-bucket/tidb-cluster-01/slowlogs/*.log
├── SQL 语句
│   ├── 数据库: tidb://tidb-server:4000/information_schema.statements_summary
│   └── API: http://tidb-server:10080/api/v1/statements
└── 指标
    ├── Prometheus: http://prometheus:9090/api/v1/query_range
    └── VictoriaMetrics: http://vm:8428/api/v1/query_range
```

## 3. 系统设计

### 3.1 整体架构（基于 Kubernetes）

```
┌─────────────────────────────────────────────────────────────┐
│                      管理端 (Management API)                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │ 任务管理     │  │ 任务调度     │  │ 状态监控     │     │
│  │ - 创建任务   │  │ - 周期性任务 │  │ - 任务状态   │     │
│  │ - 更新任务   │  │ - 一次性任务 │  │ - 执行日志   │     │
│  │ - 删除任务   │  │ - 任务触发   │  │ - 指标统计   │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└─────────────────────────────────────────────────────────────┘
                            │
                            │ K8s API
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    Kubernetes 集群                            │
│                                                               │
│  ┌─────────────────────────────────────────────────────┐   │
│  │           周期性任务 Vector Pod                      │   │
│  │  Pod: vector-scheduled                              │   │
│  │  ┌──────────────────────────────────────────────┐ │   │
│  │  │  Vector 容器                                  │ │   │
│  │  │  --config-dir=/vector/configs                  │ │   │
│  │  └──────────────────────────────────────────────┘ │   │
│  │  ┌──────────────────────────────────────────────┐ │   │
│  │  │  ConfigMap 挂载                               │ │   │
│  │  │  /vector/configs/                             │ │   │
│  │  └──────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                               │
│  ConfigMaps (周期性任务配置):                                 │
│  ├── vector-task-scheduled-001 (task-001.toml)              │
│  ├── vector-task-scheduled-002 (task-002.toml)             │
│  └── vector-task-scheduled-003 (task-003.toml)              │
│                                                               │
│  ┌─────────────────────────────────────────────────────┐   │
│  │           一次性任务 Vector Pods                     │   │
│  │                                                       │   │
│  │  Pod: vector-task-onetime-001                        │   │
│  │  ┌──────────────────────────────────────────────┐ │   │
│  │  │  Vector 容器                                  │ │   │
│  │  │  --config=/vector/config/vector.toml         │ │   │
│  │  └──────────────────────────────────────────────┘ │   │
│  │  ┌──────────────────────────────────────────────┐ │   │
│  │  │  ConfigMap 挂载                               │ │   │
│  │  │  /vector/config/vector.toml                   │ │   │
│  │  └──────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                               │
│  ConfigMaps (一次性任务配置):                                 │
│  ├── vector-task-onetime-001 (vector.toml)                  │
│  ├── vector-task-onetime-002 (vector.toml)                  │
│  └── vector-task-onetime-003 (vector.toml)                  │
│                                                               │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
                    ┌──────────────┐
                    │  数据源/目标  │
                    │  S3/Loki/DB  │
                    └──────────────┘
```

**架构特点:**
- **无数据库**: 所有任务配置存储在 K8s ConfigMap 中
- **K8s 原生**: 使用 Pod 和 ConfigMap 管理 Vector 实例
- **状态查询**: 通过 K8s API 查询 Pod/Job 状态获取任务状态
- **配置管理**: 通过 ConfigMap 管理任务配置，支持热更新
- **任务查询**: 通过列出 ConfigMap 获取所有任务列表
- **简化运维**: 利用 K8s 的原生能力，无需额外存储和管理组件

### 3.2 组件说明

#### 3.2.1 管理端 (Management API)

**功能:**
- **任务管理**: 通过 K8s API 创建、更新、删除、查询备份任务
- **任务调度**: 管理周期性任务和一次性任务的执行
- **状态监控**: 通过 K8s API 和 Vector API 监控任务状态、收集日志和指标
- **配置管理**: 通过 ConfigMap 管理任务配置，无需数据库

**核心特性:**
- RESTful API 接口
- 任务类型区分（周期性 vs 一次性）
- 通过 K8s API 管理 Pod 和 ConfigMap
- 配置存储在 ConfigMap 中，支持热更新
- 任务状态从 Pod 状态获取
- 无需数据库，所有信息从 K8s 资源获取

#### 3.2.2 任务类型定义

##### 3.2.2.1 周期性任务 (Scheduled Tasks)

**特点:**
- 按固定时间间隔重复执行（如每小时、每天）
- 所有周期性任务共享一个 Vector 实例
- 配置文件存储在统一目录下，Vector 自动监控目录变化
- 配置更新后自动重载，无需重启 Vector

**配置示例:**
```yaml
task:
  id: scheduled-backup-001
  name: "Daily Backup"
  type: "scheduled"  # 周期性任务
  schedule:
    type: "cron"  # 或 "interval"
    cron: "0 2 * * *"  # 每天凌晨 2 点执行
    # 或使用 interval: "24h"
  cluster: tidb-cluster-01
  data_types: ["logs", "metrics"]
  filters: { ... }
  target: { ... }
```

**K8s 部署方式:**
- **Pod**: 单个长期运行的 Pod (`vector-scheduled`)
- **ConfigMap**: 每个任务一个 ConfigMap (`vector-task-scheduled-{id}`)
- **配置挂载**: ConfigMap 挂载到 Pod 的 `/vector/configs/` 目录
- **自动重载**: Vector 监控配置目录，自动加载新 ConfigMap 和重载修改的配置
- **状态查询**: 通过 K8s API 查询 Pod 状态获取任务运行状态

##### 3.2.2.2 一次性任务 (One-time Tasks)

**特点:**
- 执行一次后自动结束
- 每个任务启动独立的 Vector 进程
- 任务完成后 Vector 进程自动退出
- 适合按需备份、临时备份场景

**配置示例:**
```yaml
task:
  id: onetime-backup-001
  name: "Ad-hoc Backup"
  type: "onetime"  # 一次性任务
  time_range:
    start: "2024-01-01T00:00:00Z"
    end: "2024-01-01T23:59:59Z"
  cluster: tidb-cluster-01
  data_types: ["logs", "slowlogs", "sqlstatements", "metrics"]
  filters: { ... }
  target: { ... }
```

**K8s 部署方式:**
- **Pod**: 每个任务一个独立的 Pod (`vector-task-onetime-{id}`)
- **ConfigMap**: 每个任务一个 ConfigMap (`vector-task-onetime-{id}`)
- **配置挂载**: ConfigMap 挂载到 Pod 的 `/vector/config/vector.toml`
- **生命周期**: 任务完成后 Pod 自动退出，管理端清理 Pod 和 ConfigMap
- **状态查询**: 通过 K8s API 查询 Pod 状态获取任务执行状态

#### 3.2.3 Vector 实例管理策略（基于 K8s）

##### 3.2.3.1 周期性任务 Vector Pod

**K8s 资源:**
- **Pod**: `vector-scheduled` (Deployment 或 StatefulSet)
- **ConfigMaps**: `vector-task-scheduled-{id}` (每个任务一个)

**Pod 配置示例:**
```yaml
apiVersion: v1
kind: Pod
metadata:
  name: vector-scheduled
  namespace: backup-system
spec:
  containers:
  - name: vector
    image: vector:latest
    command: ["vector"]
    args: ["--config-dir", "/vector/configs", "--watch-config"]
    volumeMounts:
    - name: configs
      mountPath: /vector/configs
      readOnly: true
  volumes:
  - name: configs
    projected:
      sources:
      # 动态挂载所有周期性任务的 ConfigMap
      - configMap:
          name: vector-task-scheduled-001
      - configMap:
          name: vector-task-scheduled-002
      # ... 更多 ConfigMap
```

**ConfigMap 配置示例:**
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: vector-task-scheduled-001
  namespace: backup-system
data:
  task-001.toml: |
    # Vector 配置内容
    [sources.s3_logs]
    type = "aws_s3"
    # ...
```

**管理流程:**
1. **创建任务**: 管理端创建 ConfigMap，Pod 自动检测并加载
2. **更新任务**: 管理端更新 ConfigMap，Vector 自动重载配置
3. **删除任务**: 管理端删除 ConfigMap，Vector 自动移除任务
4. **状态查询**: 通过 K8s API 查询 Pod 状态

**优势:**
- **无数据库**: 配置存储在 ConfigMap 中
- **自动重载**: Vector 监控 ConfigMap 变化，自动重载
- **资源高效**: 多个任务共享一个 Pod
- **K8s 原生**: 利用 K8s 的配置管理能力

##### 3.2.3.2 一次性任务 Vector Pod

**K8s 资源:**
- **Pod**: `vector-task-onetime-{id}` (Job 或 Pod)
- **ConfigMap**: `vector-task-onetime-{id}` (每个任务一个)

**Pod 配置示例:**
```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: vector-task-onetime-001
  namespace: backup-system
spec:
  ttlSecondsAfterFinished: 3600  # 完成后 1 小时自动清理
  template:
    spec:
      containers:
      - name: vector
        image: vector:latest
        command: ["vector"]
        args: ["--config", "/vector/config/vector.toml"]
        volumeMounts:
        - name: config
          mountPath: /vector/config
          readOnly: true
      volumes:
      - name: config
        configMap:
          name: vector-task-onetime-001
      restartPolicy: Never  # 任务完成后不重启
```

**ConfigMap 配置示例:**
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: vector-task-onetime-001
  namespace: backup-system
data:
  vector.toml: |
    # Vector 配置内容
    [sources.s3_logs]
    type = "aws_s3"
    # ...
```

**管理流程:**
1. **创建任务**: 管理端创建 ConfigMap 和 Job
2. **执行任务**: Job 启动 Pod 执行任务
3. **监控状态**: 通过 K8s API 查询 Job/Pod 状态
4. **清理资源**: 任务完成后，Job 的 `ttlSecondsAfterFinished` 自动清理，或管理端手动清理

**优势:**
- **隔离性好**: 每个任务独立 Pod，互不影响
- **自动清理**: 使用 Job 的 TTL 机制自动清理
- **状态清晰**: 通过 Job 状态明确任务执行状态
- **K8s 原生**: 利用 K8s Job 的生命周期管理

#### 3.2.4 任务配置管理器

**功能:**
- 解析用户提供的任务配置（YAML/JSON）
- 根据任务类型选择 Vector 配置生成策略
- 生成 Vector TOML 配置文件
- 管理配置版本和变更历史

**配置生成策略:**

**周期性任务:**
- 生成配置文件到 `/vector/configs/scheduled/` 目录
- 文件名格式: `task-{id}.toml`
- 配置中包含任务 ID 作为标识

**一次性任务:**
- 生成临时配置文件到 `/tmp/vector-tasks/` 目录
- 文件名格式: `task-{id}-{timestamp}.toml`
- 任务完成后自动删除

#### 3.2.5 Vector 执行引擎

**职责:**
- 根据配置执行数据采集
- 应用过滤规则
- 转换数据格式
- 写入目标存储

**关键特性:**
- 使用 Vector 现有插件（Source、Transform、Sink）
- 支持并行处理多个数据源
- 支持流式处理和批处理
- 支持断点续传（checkpoint）

## 4. 数据源定义

### 4.1 日志数据源

#### 4.1.1 S3 压缩日志

**特点:**
- 文件格式: `.log.gz` (gzip 压缩)
- 存储位置: S3 存储桶
- 命名规则: 通常包含时间信息，如 `logs/2024/01/01/app-*.log.gz`

**Vector 配置:**
```toml
[sources.s3_logs]
type = "aws_s3"
region = "us-west-2"
bucket = "logs-bucket"
key_prefix = "tidb-cluster-01/logs/"
compression = "gzip"
# 时间过滤：只处理指定时间段内的文件
file_time_filter = { start = "2024-01-01T00:00:00Z", end = "2024-01-01T23:59:59Z" }
```

#### 4.1.2 Loki 日志

**特点:**
- 通过 Loki API 查询日志
- 支持 LogQL 查询语言
- 支持标签过滤

**Vector 配置:**
```toml
[sources.loki_logs]
type = "loki"
endpoint = "http://loki-server:3100"
# 使用 LogQL 查询指定集群和时间段的日志
query = '{cluster="tidb-cluster-01"}'
start_time = "2024-01-01T00:00:00Z"
end_time = "2024-01-01T23:59:59Z"
```

#### 4.1.3 Parquet 统计文件

**特点:**
- 文件格式: `.parquet`
- 通常按小时生成
- 包含聚合统计信息

**Vector 配置:**
```toml
[sources.parquet_stats]
type = "file"
include = ["s3://stats-bucket/tidb-cluster-01/stats/hourly/*.parquet"]
# 需要解析 parquet 格式
[transforms.parse_parquet]
type = "parse_parquet"
inputs = ["parquet_stats"]
```

### 4.2 慢查询日志数据源

#### 4.2.1 数据库表

**特点:**
- 存储在数据库系统表中（如 `information_schema.slow_query`）
- 需要 SQL 查询获取数据
- 支持时间范围过滤

**Vector 配置:**
```toml
[sources.slow_query_db]
type = "sql"
connection_string = "mysql://user:pass@tidb-server:4000/information_schema"
query = """
  SELECT * FROM slow_query 
  WHERE time >= ? AND time <= ?
"""
query_params = ["2024-01-01T00:00:00Z", "2024-01-01T23:59:59Z"]
interval = "1m"  # 轮询间隔
```

#### 4.2.2 S3 文件

**特点:**
- 慢查询日志以文件形式存储在 S3
- 可能是文本格式或 JSON 格式

**Vector 配置:**
```toml
[sources.slow_query_s3]
type = "aws_s3"
bucket = "logs-bucket"
key_prefix = "tidb-cluster-01/slowlogs/"
file_time_filter = { start = "2024-01-01T00:00:00Z", end = "2024-01-01T23:59:59Z" }
```

### 4.3 SQL 语句数据源

#### 4.3.1 数据库表

**特点:**
- 存储在系统表中（如 `information_schema.statements_summary`）
- 包含 SQL 执行统计信息

**Vector 配置:**
```toml
[sources.sql_statements_db]
type = "sql"
connection_string = "mysql://user:pass@tidb-server:4000/information_schema"
query = """
  SELECT * FROM statements_summary 
  WHERE summary_begin_time >= ? AND summary_end_time <= ?
"""
query_params = ["2024-01-01T00:00:00Z", "2024-01-01T23:59:59Z"]
```

#### 4.3.2 API 接口

**特点:**
- 通过 HTTP API 获取数据
- 通常返回 JSON 格式

**Vector 配置:**
```toml
[sources.sql_statements_api]
type = "http"
url = "http://tidb-server:10080/api/v1/statements"
method = "GET"
headers = { "Content-Type" = "application/json" }
# 查询参数中包含时间范围
query_params = { 
  start_time = "2024-01-01T00:00:00Z",
  end_time = "2024-01-01T23:59:59Z"
}
```

### 4.4 指标数据源

#### 4.4.1 Prometheus

**特点:**
- 通过 Prometheus Query API 导出数据
- 支持 PromQL 查询
- 支持时间范围查询

**Vector 配置:**
```toml
[sources.prometheus_metrics]
type = "prometheus"
endpoint = "http://prometheus:9090"
# 查询指定集群的指标
query = 'up{cluster="tidb-cluster-01"}'
start_time = "2024-01-01T00:00:00Z"
end_time = "2024-01-01T23:59:59Z"
step = "30s"  # 采样间隔
```

#### 4.4.2 VictoriaMetrics

**特点:**
- 兼容 Prometheus API
- 支持更高效的数据导出

**Vector 配置:**
```toml
[sources.vm_metrics]
type = "prometheus"  # 使用 prometheus source，兼容 VM
endpoint = "http://vm:8428"
query = '{cluster="tidb-cluster-01"}'
start_time = "2024-01-01T00:00:00Z"
end_time = "2024-01-01T23:59:59Z"
```

## 5. 过滤规则定义

### 5.1 过滤规则类型

#### 5.1.1 关键字过滤

**用途:** 基于关键字匹配过滤数据

**配置:**
```yaml
filter:
  type: keyword
  keywords:
    - "ERROR"
    - "WARN"
    - "critical"
  match_mode: "any"  # any: 匹配任意关键字, all: 匹配所有关键字
  case_sensitive: false
```

**Vector 实现:**
```toml
[transforms.keyword_filter]
type = "filter"
inputs = ["source"]
condition = '''
  contains(.message, "ERROR") or 
  contains(.message, "WARN") or 
  contains(.message, "critical")
'''
```

#### 5.1.2 正则表达式过滤

**用途:** 使用正则表达式进行复杂模式匹配

**配置:**
```yaml
filter:
  type: regex
  pattern: ".*timeout.*|.*connection.*failed.*"
  field: "message"  # 指定要匹配的字段
```

**Vector 实现:**
```toml
[transforms.regex_filter]
type = "filter"
inputs = ["source"]
condition = '.message =~ /timeout|connection.*failed/'
```

#### 5.1.3 字段值过滤

**用途:** 基于字段值进行过滤（数值比较、字符串匹配等）

**配置:**
```yaml
filter:
  type: field
  field: "execution_time"
  operator: ">"  # >, <, >=, <=, ==, !=
  value: "1s"
```

**Vector 实现:**
```toml
[transforms.field_filter]
type = "filter"
inputs = ["source"]
condition = '.execution_time > 1.0'
```

#### 5.1.4 时间范围过滤

**用途:** 在数据源级别或转换级别进行更细粒度的时间过滤

**配置:**
```yaml
filter:
  type: time_range
  field: "timestamp"
  start: "2024-01-01T10:00:00Z"
  end: "2024-01-01T12:00:00Z"
```

**Vector 实现:**
```toml
[transforms.time_filter]
type = "filter"
inputs = ["source"]
condition = '''
  .timestamp >= "2024-01-01T10:00:00Z" and 
  .timestamp <= "2024-01-01T12:00:00Z"
'''
```

### 5.2 过滤规则组合

支持多个过滤规则的组合（AND/OR 逻辑）：

```yaml
filters:
  logs:
    enabled: true
    logic: "AND"  # AND: 所有规则都满足, OR: 任意规则满足
    rules:
      - type: keyword
        keywords: ["ERROR", "WARN"]
      - type: regex
        pattern: ".*timeout.*"
```

## 6. 目标存储定义

### 6.1 S3 存储

**用途:** 备份到 S3 存储桶

**Vector 配置:**
```toml
[sinks.backup_s3]
type = "aws_s3"
inputs = ["filtered_data"]
bucket = "backup-bucket"
key_prefix = "backups/tidb-cluster-01/2024-01-01/"
# 按数据类型组织文件
compression = "gzip"
encoding = { codec = "json" }
```

### 6.2 本地文件系统

**用途:** 备份到本地文件系统

**Vector 配置:**
```toml
[sinks.backup_file]
type = "file"
inputs = ["filtered_data"]
path = "/backup/tidb-cluster-01/2024-01-01/"
filename = "backup-%{data_type}-%{+YYYY-MM-dd-HH}.log"
compression = "gzip"
```

## 7. Vector 配置生成规范

### 7.1 配置生成流程

```
用户配置
  ↓
解析配置
  ├─ 数据源映射 (根据 cluster 和数据源配置)
  ├─ 时间范围应用
  ├─ 过滤规则转换
  └─ 目标存储配置
  ↓
生成 Vector TOML 配置
  ↓
执行 Vector
```

### 7.2 配置模板结构

```toml
# Vector 配置模板
data_dir = "/var/lib/vector"

# 数据源配置（根据数据源类型动态生成）
[sources.<source_name>]
type = "<source_type>"
# ... source 特定配置

# 数据转换（解压缩、解析等）
[transforms.<transform_name>]
type = "<transform_type>"
inputs = ["<source_name>"]
# ... transform 特定配置

# 过滤规则（根据用户配置生成）
[transforms.<filter_name>]
type = "filter"
inputs = ["<previous_transform>"]
condition = "<filter_condition>"

# 数据丰富（添加元数据）
[transforms.enrich]
type = "add_fields"
inputs = ["<filter_name>"]
fields.backup_id = "<backup_id>"
fields.cluster = "<cluster>"
fields.backup_time = "<timestamp>"

# 目标存储
[sinks.<sink_name>]
type = "<sink_type>"
inputs = ["enrich"]
# ... sink 特定配置
```

### 7.3 配置生成示例

**输入配置:**
```yaml
backup_task:
  cluster: tidb-cluster-01
  time_range:
    start: "2024-01-01T00:00:00Z"
    end: "2024-01-01T23:59:59Z"
  data_types: ["logs"]
  filters:
    logs:
      enabled: true
      rules:
        - type: keyword
          keywords: ["ERROR", "WARN"]
  target:
    type: s3
    bucket: backup-bucket
    prefix: "backups/tidb-cluster-01/2024-01-01/"
```

**生成的 Vector 配置:**
```toml
# Vector 数据目录（用于 checkpoint）
data_dir = "/vector/data/checkpoints/backup-20240101-001"

# 启用 API 用于监控和指标收集
[api]
enabled = true
address = "127.0.0.1:8686"
graphql_enabled = false

# S3 日志数据源
[sources.s3_logs]
type = "aws_s3"
region = "us-west-2"
bucket = "logs-bucket"
key_prefix = "tidb-cluster-01/logs/"
compression = "gzip"
file_time_filter = { 
  start = "2024-01-01T00:00:00Z", 
  end = "2024-01-01T23:59:59Z" 
}
# Vector 会自动记录已处理的文件位置到 data_dir

# 解压缩
[transforms.decompress]
type = "decompress"
inputs = ["s3_logs"]
method = "gzip"

# 解析日志格式
[transforms.parse_logs]
type = "parse_grok"
inputs = ["decompress"]
pattern = "%{TIMESTAMP_ISO8601:timestamp} %{LOGLEVEL:level} %{GREEDYDATA:message}"

# 关键字过滤
[transforms.keyword_filter]
type = "filter"
inputs = ["parse_logs"]
condition = 'contains(.message, "ERROR") or contains(.message, "WARN")'

# 添加备份元数据
[transforms.enrich]
type = "add_fields"
inputs = ["keyword_filter"]
fields.backup_id = "backup-20240101-001"
fields.cluster = "tidb-cluster-01"
fields.backup_time = "2024-01-01T12:00:00Z"
fields.data_type = "logs"

# 写入备份 S3
[sinks.backup_s3]
type = "aws_s3"
inputs = ["enrich"]
bucket = "backup-bucket"
key_prefix = "backups/tidb-cluster-01/2024-01-01/logs/"
compression = "gzip"
encoding = { codec = "json" }
```

## 8. 实现指导

### 8.1 开发任务分解

#### 任务 1: 配置解析模块

**功能:**
- 解析用户提供的备份任务配置（YAML/JSON）
- 验证配置的完整性和正确性
- 将配置转换为内部数据结构

**实现要点:**
- 定义配置结构体（Rust struct 或 Go struct）
- 使用配置解析库（如 serde、viper）
- 实现配置验证逻辑

#### 任务 2: 数据源映射模块

**功能:**
- 根据集群名称和数据源配置，确定实际的数据源位置
- 生成对应的 Vector Source 配置

**实现要点:**
- 维护数据源配置映射表（集群 -> 数据源配置）
- 根据数据类型（logs/slowlogs/sqlstatements/metrics）选择对应的 Source
- 应用时间范围过滤到 Source 配置

#### 任务 3: 过滤规则转换模块

**功能:**
- 将用户定义的过滤规则转换为 Vector Filter Transform 配置
- 支持多种过滤规则类型
- 支持规则组合（AND/OR）

**实现要点:**
- 实现每种过滤规则类型的转换逻辑
- 生成 Vector VRL (Vector Remap Language) 条件表达式
- 处理规则组合逻辑

#### 任务 4: Vector 配置生成模块

**功能:**
- 根据解析的配置，生成完整的 Vector TOML 配置文件
- 组装 Source、Transform、Sink 配置

**实现要点:**
- 使用 TOML 生成库（如 toml、toml_edit）
- 按照 Vector 配置规范生成配置
- 确保配置的正确性和完整性

#### 任务 5: 管理端 API 模块

**功能:**
- 提供 RESTful API 接口
- 任务 CRUD 操作（创建、读取、更新、删除）
- 任务执行控制（启动、停止、暂停、恢复）
- 任务状态查询和监控

**API 设计:**

```rust
// 任务管理 API
POST   /api/v1/tasks              // 创建任务
GET    /api/v1/tasks              // 获取任务列表
GET    /api/v1/tasks/{id}         // 获取任务详情
PUT    /api/v1/tasks/{id}         // 更新任务
DELETE /api/v1/tasks/{id}         // 删除任务

// 任务执行控制
POST   /api/v1/tasks/{id}/start   // 启动任务
POST   /api/v1/tasks/{id}/stop    // 停止任务
POST   /api/v1/tasks/{id}/pause   // 暂停任务
POST   /api/v1/tasks/{id}/resume  // 恢复任务

// 任务状态和监控
GET    /api/v1/tasks/{id}/status  // 获取任务状态
GET    /api/v1/tasks/{id}/logs    // 获取任务日志
GET    /api/v1/tasks/{id}/metrics // 获取任务指标
```

**实现要点:**
- 使用 Web 框架（如 Actix-web、Rocket、Axum）
- 定义任务数据结构（区分周期性任务和一次性任务）
- **无需数据库**: 任务配置存储在 K8s ConfigMap 中
- **状态从 K8s 获取**: 通过 K8s API 查询 Pod/Job 状态
- 实现任务状态映射（K8s Pod/Job 状态 -> 任务状态）

#### 任务 6: 任务调度模块

**功能:**
- 管理周期性任务的调度
- 触发一次性任务的执行
- 处理任务依赖关系

**实现要点:**
- 使用调度库（如 cron、tokio-cron-scheduler）
- 周期性任务：注册到调度器，按计划触发
- 一次性任务：立即执行或延迟执行
- 实现任务队列管理

#### 任务 7: K8s 资源管理模块

**功能:**
- 通过 K8s API 管理周期性任务的 Vector Pod
- 通过 K8s API 管理一次性任务的 Vector Pod
- 通过 K8s API 管理 ConfigMap
- 监控 Pod 状态
- 处理 Pod 异常和重启

**周期性任务 K8s 管理:**

```rust
use k8s_openapi::api::core::v1::{ConfigMap, Pod};
use kube::{Api, Client};

// 创建周期性任务 ConfigMap
async fn create_scheduled_task_configmap(
    client: Client,
    task_id: &str,
    vector_config: &str,
) -> Result<()> {
    let configmaps: Api<ConfigMap> = Api::namespaced(client, "backup-system");
    
    let configmap = ConfigMap {
        metadata: ObjectMeta {
            name: Some(format!("vector-task-scheduled-{}", task_id)),
            namespace: Some("backup-system".to_string()),
            ..Default::default()
        },
        data: Some({
            let mut map = BTreeMap::new();
            map.insert(format!("task-{}.toml", task_id), vector_config.to_string());
            map
        }),
        ..Default::default()
    };
    
    configmaps.create(&PostParams::default(), &configmap).await?;
    Ok(())
}

// 更新周期性任务 ConfigMap
async fn update_scheduled_task_configmap(
    client: Client,
    task_id: &str,
    vector_config: &str,
) -> Result<()> {
    let configmaps: Api<ConfigMap> = Api::namespaced(client, "backup-system");
    let name = format!("vector-task-scheduled-{}", task_id);
    
    // 获取现有 ConfigMap
    let mut configmap = configmaps.get(&name).await?;
    
    // 更新配置
    if let Some(data) = &mut configmap.data {
        data.insert(format!("task-{}.toml", task_id), vector_config.to_string());
    }
    
    // 更新 ConfigMap
    configmaps.replace(&name, &PostParams::default(), &configmap).await?;
    
    // Vector Pod 会自动检测到 ConfigMap 变化并重载配置
    Ok(())
}

// 删除周期性任务 ConfigMap
async fn delete_scheduled_task_configmap(
    client: Client,
    task_id: &str,
) -> Result<()> {
    let configmaps: Api<ConfigMap> = Api::namespaced(client, "backup-system");
    let name = format!("vector-task-scheduled-{}", task_id);
    
    configmaps.delete(&name, &DeleteParams::default()).await?;
    
    // Vector Pod 会自动检测到 ConfigMap 删除并移除任务
    Ok(())
}

// 确保周期性任务 Pod 存在
async fn ensure_scheduled_pod_exists(client: Client) -> Result<()> {
    let pods: Api<Pod> = Api::namespaced(client, "backup-system");
    
    // 检查 Pod 是否存在
    match pods.get("vector-scheduled").await {
        Ok(_) => Ok(()),  // Pod 已存在
        Err(kube::Error::Api(ResponseError { code: 404, .. })) => {
            // Pod 不存在，创建它
            create_scheduled_pod(client).await
        }
        Err(e) => Err(e.into()),
    }
}

// 创建周期性任务 Pod
async fn create_scheduled_pod(client: Client) -> Result<()> {
    let pods: Api<Pod> = Api::namespaced(client, "backup-system");
    
    // 获取所有周期性任务 ConfigMap
    let configmaps: Api<ConfigMap> = Api::namespaced(client, "backup-system");
    let configmap_list = configmaps.list(&ListParams::default().labels("type=scheduled")).await?;
    
    // 构建 Pod 配置，挂载所有 ConfigMap
    let pod = build_scheduled_pod(configmap_list.items);
    
    pods.create(&PostParams::default(), &pod).await?;
    Ok(())
}
```

**一次性任务 K8s 管理:**

```rust
use k8s_openapi::api::batch::v1::Job;

// 创建一次性任务
async fn create_onetime_task(
    client: Client,
    task_id: &str,
    vector_config: &str,
    checkpoint: Option<TaskCheckpoint>,
) -> Result<()> {
    // 如果有 checkpoint，更新配置以从断点继续
    let mut final_config = vector_config.to_string();
    if let Some(cp) = checkpoint {
        final_config = apply_checkpoint_to_config(&final_config, &cp)?;
    }
    
    // 1. 创建 ConfigMap
    let configmaps: Api<ConfigMap> = Api::namespaced(client.clone(), "backup-system");
    let configmap = ConfigMap {
        metadata: ObjectMeta {
            name: Some(format!("vector-task-onetime-{}", task_id)),
            namespace: Some("backup-system".to_string()),
            ..Default::default()
        },
        data: Some({
            let mut map = BTreeMap::new();
            map.insert("vector.toml".to_string(), final_config);
            map
        }),
        ..Default::default()
    };
    configmaps.create(&PostParams::default(), &configmap).await?;
    
    // 2. 创建 Job
    let jobs: Api<Job> = Api::namespaced(client, "backup-system");
    let job = build_onetime_job(task_id);
    jobs.create(&PostParams::default(), &job).await?;
    
    // 3. 启动监控和进度收集
    spawn_job_monitor(task_id);
    spawn_progress_collector(task_id);
    
    Ok(())
}

// 构建一次性任务 Job
fn build_onetime_job(task_id: &str) -> Job {
    Job {
        metadata: ObjectMeta {
            name: Some(format!("vector-task-onetime-{}", task_id)),
            namespace: Some("backup-system".to_string()),
            ..Default::default()
        },
        spec: Some(JobSpec {
            ttl_seconds_after_finished: Some(3600),  // 完成后 1 小时自动清理
            template: PodTemplateSpec {
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "vector".to_string(),
                        image: Some("vector:latest".to_string()),
                        command: Some(vec!["vector".to_string()]),
                        args: Some(vec!["--config".to_string(), "/vector/config/vector.toml".to_string()]),
                        volume_mounts: Some(vec![VolumeMount {
                            name: "config".to_string(),
                            mount_path: "/vector/config".to_string(),
                            read_only: Some(true),
                            ..Default::default()
                        }]),
                        ..Default::default()
                    }],
                    volumes: Some(vec![Volume {
                        name: "config".to_string(),
                        config_map: Some(ConfigMapVolumeSource {
                            name: Some(format!("vector-task-onetime-{}", task_id)),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }]),
                    restart_policy: Some("Never".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        }),
        ..Default::default()
    }
}

// 监控 Job 状态
fn spawn_job_monitor(task_id: String) {
    tokio::spawn(async move {
        let client = Client::try_default().await.unwrap();
        let jobs: Api<Job> = Api::namespaced(client, "backup-system");
        let job_name = format!("vector-task-onetime-{}", task_id);
        
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        
        loop {
            interval.tick().await;
            
            // 查询 Job 状态
            match jobs.get(&job_name).await {
                Ok(job) => {
                    if let Some(status) = &job.status {
                        // 检查 Job 是否完成
                        if let Some(completion_time) = &status.completion_time {
                            // Job 完成
                            let succeeded = status.succeeded.unwrap_or(0) > 0;
                            let failed = status.failed.unwrap_or(0) > 0;
                            
                            if succeeded {
                                update_task_status(&task_id, TaskStatus::Completed).await;
                            } else if failed {
                                update_task_status(&task_id, TaskStatus::Failed).await;
                            }
                            break;
                        }
                        
                        // 检查是否有失败的 Pod
                        if status.failed.unwrap_or(0) > 0 {
                            // 检查是否需要重启（基于 checkpoint）
                            let checkpoint = load_checkpoint(&task_id).await;
                            if let Some(cp) = checkpoint {
                                // 从 checkpoint 重启
                                if let Err(e) = restart_onetime_task(&task_id, Some(cp)).await {
                                    log::error!("Failed to restart task {}: {}", task_id, e);
                                    update_task_status(&task_id, TaskStatus::Failed).await;
                                    break;
                                }
                            }
                        }
                    }
                }
                Err(kube::Error::Api(ResponseError { code: 404, .. })) => {
                    // Job 不存在（可能已被清理）
                    update_task_status(&task_id, TaskStatus::Completed).await;
                    break;
                }
                Err(e) => {
                    log::error!("Error monitoring job {}: {}", job_name, e);
                }
            }
        }
    });
}

// 监控一次性任务进程
fn spawn_monitor_task(task_id: String, pid: u32, config_file: String) {
    tokio::spawn(async move {
        let mut health_check_interval = tokio::time::interval(Duration::from_secs(10));
        
        loop {
            health_check_interval.tick().await;
            
            // 检查进程是否还在运行
            if !is_process_running(pid) {
                // 进程退出，检查退出原因
                let exit_code = get_process_exit_code(pid).await;
                
                // 加载 checkpoint 检查任务是否完成
                let checkpoint = load_checkpoint(&task_id).await;
                let is_completed = is_task_completed(&task_id, &checkpoint).await;
                
                if is_completed {
                    // 任务完成
                    update_task_status(&task_id, TaskStatus::Completed).await;
                    cleanup_task_resources(&task_id, &config_file).await;
                    break;
                } else if exit_code == Some(0) {
                    // 正常退出但任务未完成（可能配置问题）
                    log::error!("Vector exited normally but task not completed: {}", task_id);
                    update_task_status(&task_id, TaskStatus::Failed).await;
                    cleanup_task_resources(&task_id, &config_file).await;
                    break;
                } else {
                    // 异常退出，尝试重启
                    log::warn!("Vector process exited unexpectedly for task {}, attempting restart", task_id);
                    
                    if let Err(e) = restart_vector_task(&task_id, checkpoint).await {
                        log::error!("Failed to restart task {}: {}", task_id, e);
                        update_task_status(&task_id, TaskStatus::Failed).await;
                        cleanup_task_resources(&task_id, &config_file).await;
                        break;
                    }
                    // 重启成功，继续监控新进程
                    break;
                }
            }
            
            // 健康检查
            if !check_vector_health(pid).await {
                log::warn!("Vector health check failed for task {}", task_id);
                // 可以选择重启或标记为不健康
            }
        }
    });
}

// 进度收集器
fn spawn_progress_collector(task_id: String) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        
        loop {
            interval.tick().await;
            
            // 收集进度
            if let Ok(progress) = collect_task_progress(&task_id).await {
                // 保存进度
                let _ = save_task_progress(&task_id, &progress).await;
                
                // 如果任务完成，退出
                if progress.status == TaskStatus::Completed 
                    || progress.status == TaskStatus::Failed {
                    break;
                }
            }
        }
    });
}
```

**实现要点:**
- 使用进程管理库（如 tokio::process）
- 维护进程映射表（task_id -> process）
- 实现进程健康检查
- 实现进程重启机制（周期性任务）
- 实现进程清理机制（一次性任务）

#### 任务 8: 任务状态查询模块

**功能:**
- 通过 K8s API 查询 Pod/Job 状态
- 通过 Vector API 查询任务进度
- 从 ConfigMap 读取任务配置
- 聚合任务状态信息

**状态查询实现:**
```rust
// 查询任务状态（从 K8s 和 Vector API）
async fn get_task_status(
    client: Client,
    task_id: &str,
    task_type: TaskType,
) -> Result<TaskStatusResponse> {
    match task_type {
        TaskType::Scheduled => {
            // 查询周期性任务 Pod 状态
            let pods: Api<Pod> = Api::namespaced(client.clone(), "backup-system");
            let pod = pods.get("vector-scheduled").await?;
            
            // 从 Pod 状态获取信息
            let pod_status = pod.status.as_ref();
            let phase = pod_status.and_then(|s| s.phase.as_ref()).cloned();
            
            // 从 ConfigMap 读取任务配置
            let configmaps: Api<ConfigMap> = Api::namespaced(client, "backup-system");
            let configmap_name = format!("vector-task-scheduled-{}", task_id);
            let configmap = configmaps.get(&configmap_name).await?;
            
            // 从 Vector API 获取进度（如果 Pod 运行中）
            let progress = if phase == Some("Running".to_string()) {
                get_vector_progress(task_id).await.ok()
            } else {
                None
            };
            
            Ok(TaskStatusResponse {
                task_id: task_id.to_string(),
                status: map_pod_phase_to_task_status(&phase),
                pod_phase: phase,
                progress,
                // ...
            })
        }
        TaskType::Onetime => {
            // 查询一次性任务 Job 状态
            let jobs: Api<Job> = Api::namespaced(client.clone(), "backup-system");
            let job_name = format!("vector-task-onetime-{}", task_id);
            let job = jobs.get(&job_name).await?;
            
            // 从 Job 状态获取信息
            let job_status = job.status.as_ref();
            let succeeded = job_status.and_then(|s| s.succeeded).unwrap_or(0);
            let failed = job_status.and_then(|s| s.failed).unwrap_or(0);
            let active = job_status.and_then(|s| s.active).unwrap_or(0);
            
            // 从 ConfigMap 读取任务配置
            let configmaps: Api<ConfigMap> = Api::namespaced(client, "backup-system");
            let configmap_name = format!("vector-task-onetime-{}", task_id);
            let configmap = configmaps.get(&configmap_name).await?;
            
            // 从 Vector API 获取进度（如果 Job 运行中）
            let progress = if active > 0 {
                get_vector_progress(task_id).await.ok()
            } else {
                None
            };
            
            Ok(TaskStatusResponse {
                task_id: task_id.to_string(),
                status: if succeeded > 0 {
                    TaskStatus::Completed
                } else if failed > 0 {
                    TaskStatus::Failed
                } else if active > 0 {
                    TaskStatus::Running
                } else {
                    TaskStatus::Pending
                },
                job_succeeded: succeeded,
                job_failed: failed,
                job_active: active,
                progress,
                // ...
            })
        }
    }
}

// 从 ConfigMap 读取任务配置
async fn get_task_config_from_configmap(
    client: Client,
    task_id: &str,
    task_type: TaskType,
) -> Result<TaskConfig> {
    let configmaps: Api<ConfigMap> = Api::namespaced(client, "backup-system");
    let configmap_name = match task_type {
        TaskType::Scheduled => format!("vector-task-scheduled-{}", task_id),
        TaskType::Onetime => format!("vector-task-onetime-{}", task_id),
    };
    
    let configmap = configmaps.get(&configmap_name).await?;
    
    // 从 ConfigMap 的 data 字段读取配置
    if let Some(data) = configmap.data {
        let config_key = match task_type {
            TaskType::Scheduled => format!("task-{}.toml", task_id),
            TaskType::Onetime => "vector.toml".to_string(),
        };
        
        if let Some(vector_config_toml) = data.get(&config_key) {
            // 解析 Vector 配置，提取任务信息
            let task_config = parse_task_config_from_vector_config(vector_config_toml)?;
            Ok(task_config)
        } else {
            Err(Error::ConfigNotFound)
        }
    } else {
        Err(Error::ConfigNotFound)
    }
}

// 列出所有任务（从 ConfigMap 列表）
async fn list_all_tasks(client: Client) -> Result<Vec<TaskInfo>> {
    let configmaps: Api<ConfigMap> = Api::namespaced(client, "backup-system");
    
    // 列出所有周期性任务 ConfigMap
    let scheduled_configmaps = configmaps
        .list(&ListParams::default().labels("type=scheduled"))
        .await?;
    
    // 列出所有一次性任务 ConfigMap
    let onetime_configmaps = configmaps
        .list(&ListParams::default().labels("type=onetime"))
        .await?;
    
    let mut tasks = Vec::new();
    
    // 解析周期性任务
    for cm in scheduled_configmaps.items {
        if let Some(name) = &cm.metadata.name {
            if let Some(task_id) = extract_task_id_from_configmap_name(name) {
                let status = get_task_status(client.clone(), &task_id, TaskType::Scheduled).await?;
                tasks.push(TaskInfo {
                    id: task_id,
                    task_type: TaskType::Scheduled,
                    status: status.status,
                    // ...
                });
            }
        }
    }
    
    // 解析一次性任务
    for cm in onetime_configmaps.items {
        if let Some(name) = &cm.metadata.name {
            if let Some(task_id) = extract_task_id_from_configmap_name(name) {
                let status = get_task_status(client.clone(), &task_id, TaskType::Onetime).await?;
                tasks.push(TaskInfo {
                    id: task_id,
                    task_type: TaskType::Onetime,
                    status: status.status,
                    // ...
                });
            }
        }
    }
    
    Ok(tasks)
}
```

**实现要点:**
- 使用 K8s 客户端库（如 kube-rs、client-go）
- 通过 K8s API 查询 Pod/Job 状态
- 通过 Vector API 查询任务进度
- 从 ConfigMap 读取和解析任务配置
- 无需数据库，所有信息从 K8s 资源获取

### 8.2 Vector 插件使用指南

#### 8.2.1 数据源插件 (Sources)

**S3 数据源:**
- 插件: `vector/sources-aws_s3`
- 文档: https://vector.dev/docs/reference/configuration/sources/aws_s3/
- 关键配置: bucket, key_prefix, compression, region

**Loki 数据源:**
- 插件: `vector/sources-loki` (如果存在) 或使用 HTTP Source
- 替代方案: 使用 `http` source 调用 Loki API
- 关键配置: endpoint, query, headers

**数据库数据源:**
- 插件: `vector/sources-sql` (如果存在) 或使用自定义 source
- 替代方案: 使用 `http` source 调用数据库 API，或开发自定义 source
- 关键配置: connection_string, query, interval

**Prometheus 数据源:**
- 插件: `vector/sources-prometheus` (如果存在)
- 替代方案: 使用 `http` source 调用 Prometheus Query API
- 关键配置: endpoint, query, start_time, end_time

#### 8.2.2 转换插件 (Transforms)

**解压缩:**
- 插件: `vector/transforms-decompress`
- 文档: https://vector.dev/docs/reference/configuration/transforms/decompress/
- 支持格式: gzip, zlib, snappy, lz4

**解析:**
- 插件: `vector/transforms-parse_grok`, `vector/transforms-parse_json`, `vector/transforms-parse_regex`
- 文档: https://vector.dev/docs/reference/configuration/transforms/
- 根据日志格式选择合适的解析器

**过滤:**
- 插件: `vector/transforms-filter`
- 文档: https://vector.dev/docs/reference/configuration/transforms/filter/
- 使用 VRL 条件表达式

**字段操作:**
- 插件: `vector/transforms-add_fields`, `vector/transforms-remove_fields`, `vector/transforms-rename_fields`
- 用于添加备份元数据

#### 8.2.3 目标插件 (Sinks)

**S3 目标:**
- 插件: `vector/sinks-aws_s3`
- 文档: https://vector.dev/docs/reference/configuration/sinks/aws_s3/
- 关键配置: bucket, key_prefix, compression, encoding

**文件目标:**
- 插件: `vector/sinks-file`
- 文档: https://vector.dev/docs/reference/configuration/sinks/file/
- 关键配置: path, filename, compression

### 8.3 代码结构建议

```
project/
├── src/
│   ├── api/                    # 管理端 API 模块
│   │   ├── mod.rs             # API 模块入口
│   │   ├── handlers/          # API 处理器
│   │   │   ├── tasks.rs       # 任务管理 API
│   │   │   ├── clusters.rs    # 集群管理 API
│   │   │   └── health.rs      # 健康检查 API
│   │   ├── models/            # API 数据模型
│   │   │   ├── task.rs        # 任务模型
│   │   │   └── response.rs    # 响应模型
│   │   └── routes.rs          # 路由定义
│   ├── config/                # 配置模块
│   │   ├── mod.rs             # 配置模块入口
│   │   ├── backup_task.rs     # 备份任务配置结构
│   │   ├── data_source.rs     # 数据源配置
│   │   ├── filter.rs          # 过滤规则配置
│   │   ├── target.rs          # 目标存储配置
│   │   └── task_type.rs       # 任务类型定义（周期性/一次性）
│   ├── scheduler/             # 任务调度模块
│   │   ├── mod.rs             # 调度模块入口
│   │   ├── cron_scheduler.rs  # Cron 调度器
│   │   ├── task_queue.rs      # 任务队列
│   │   └── trigger.rs         # 任务触发逻辑
│   ├── vector_manager/        # Vector 实例管理模块
│   │   ├── mod.rs             # Vector 管理模块入口
│   │   ├── scheduled.rs       # 周期性任务 Vector 管理
│   │   ├── onetime.rs         # 一次性任务 Vector 管理
│   │   ├── process_manager.rs # 进程管理
│   │   └── config_manager.rs  # 配置目录管理
│   ├── mapper/                # 数据源映射模块
│   │   ├── mod.rs             # 数据源映射模块入口
│   │   ├── source_mapper.rs   # 数据源映射逻辑
│   │   └── cluster_config.rs  # 集群配置管理
│   ├── filter/                # 过滤规则模块
│   │   ├── mod.rs             # 过滤规则模块入口
│   │   ├── keyword_filter.rs  # 关键字过滤
│   │   ├── regex_filter.rs    # 正则过滤
│   │   ├── field_filter.rs    # 字段过滤
│   │   └── vrl_generator.rs   # VRL 表达式生成
│   ├── vector/                # Vector 配置生成模块
│   │   ├── mod.rs             # Vector 配置生成模块入口
│   │   ├── config_generator.rs # 配置生成器
│   │   ├── source_builder.rs  # Source 配置构建
│   │   ├── transform_builder.rs # Transform 配置构建
│   │   └── sink_builder.rs    # Sink 配置构建
│   ├── k8s/                   # K8s 资源管理模块
│   │   ├── mod.rs             # K8s 模块入口
│   │   ├── client.rs          # K8s 客户端封装
│   │   ├── configmap.rs       # ConfigMap 管理
│   │   ├── pod.rs             # Pod 管理（周期性任务）
│   │   ├── job.rs             # Job 管理（一次性任务）
│   │   └── status.rs          # 状态查询
│   ├── monitor/               # 监控模块
│   │   ├── mod.rs             # 监控模块入口
│   │   ├── task_monitor.rs    # 任务监控
│   │   └── metrics.rs         # 指标收集
│   └── main.rs                # 主程序入口
├── config/
│   ├── cluster_config.yaml    # 集群数据源配置示例
│   └── backup_task.yaml       # 备份任务配置示例
├── migrations/                # 数据库迁移（如果使用数据库）
└── tests/
    ├── unit/                  # 单元测试
    └── integration/           # 集成测试
```

### 8.4 关键实现细节

#### 8.4.1 时间范围处理

- 统一使用 ISO 8601 格式: `2024-01-01T00:00:00Z`
- 支持时区转换
- 在数据源级别应用时间过滤（如果支持）
- 在转换级别进行二次时间过滤（确保精确性）

#### 8.4.2 过滤规则实现

- 关键字过滤: 使用 VRL `contains()` 函数
- 正则过滤: 使用 VRL 正则表达式匹配 `=~`
- 字段过滤: 使用 VRL 比较运算符
- 规则组合: 使用 VRL 逻辑运算符 `and`/`or`

#### 8.4.3 错误处理

- 数据源连接失败: 重试机制，记录错误日志
- 数据解析失败: 跳过错误数据，记录警告
- 目标写入失败: 重试机制，支持死信队列
- 任务超时: 设置超时时间，超时后终止任务

#### 8.4.4 性能优化

- 并行处理多个数据源
- 使用批处理减少 I/O 次数
- 压缩数据传输
- 流式处理大文件

#### 8.4.5 任务可靠性保证

##### 8.4.5.1 Checkpoint 机制

**目的:** 确保任务中断后可以从断点继续执行，避免重复处理数据。

**实现方式:**

1. **Vector Checkpoint 配置:**
```toml
# 在 Vector 配置中启用 checkpoint
data_dir = "/vector/data/checkpoints"

[sources.s3_logs]
type = "aws_s3"
# ... 其他配置
# Vector 会自动记录已处理的文件位置
```

2. **自定义 Checkpoint 管理:**
```rust
// Checkpoint 数据结构
struct TaskCheckpoint {
    task_id: String,
    source_type: String,
    source_id: String,
    last_processed_file: Option<String>,
    last_processed_offset: Option<u64>,
    last_processed_time: Option<DateTime<Utc>>,
    total_processed: u64,
    total_size: u64,
}

// 保存 checkpoint
fn save_checkpoint(task_id: &str, checkpoint: &TaskCheckpoint) -> Result<()> {
    let checkpoint_file = format!("/vector/data/checkpoints/{}.json", task_id);
    let json = serde_json::to_string(checkpoint)?;
    atomic_write(&checkpoint_file, json)?;
    Ok(())
}

// 加载 checkpoint
fn load_checkpoint(task_id: &str) -> Result<Option<TaskCheckpoint>> {
    let checkpoint_file = format!("/vector/data/checkpoints/{}.json", task_id);
    if !exists(&checkpoint_file) {
        return Ok(None);
    }
    let content = read_to_string(&checkpoint_file)?;
    let checkpoint: TaskCheckpoint = serde_json::from_str(&content)?;
    Ok(Some(checkpoint))
}
```

3. **Checkpoint 更新策略:**
- 每处理完一个文件更新一次 checkpoint
- 或按时间间隔更新（如每 5 分钟）
- 使用原子性写入确保 checkpoint 一致性

##### 8.4.5.2 Vector Pod/Job 监控和自动重启

**问题:** 一次性任务执行中 Vector Pod 异常退出或系统重启。

**解决方案（基于 K8s）:**

1. **Pod/Job 监控:**
```rust
// 监控一次性任务 Job 状态
async fn monitor_onetime_job(task_id: &str) {
    let client = Client::try_default().await.unwrap();
    let jobs: Api<Job> = Api::namespaced(client, "backup-system");
    let job_name = format!("vector-task-onetime-{}", task_id);
    
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    
    loop {
        interval.tick().await;
        
        match jobs.get(&job_name).await {
            Ok(job) => {
                if let Some(status) = &job.status {
                    // 检查 Job 是否完成
                    if let Some(_) = &status.completion_time {
                        let succeeded = status.succeeded.unwrap_or(0) > 0;
                        let failed = status.failed.unwrap_or(0) > 0;
                        
                        if succeeded {
                            update_task_status(task_id, TaskStatus::Completed).await;
                            break;
                        } else if failed {
                            // Job 失败，检查是否需要重启
                            let checkpoint = load_checkpoint_from_pvc(task_id).await;
                            
                            if let Some(cp) = checkpoint {
                                // 从 checkpoint 重启
                                log::warn!("Job failed for task {}, restarting from checkpoint", task_id);
                                restart_onetime_job(task_id, Some(cp)).await;
                            } else {
                                update_task_status(task_id, TaskStatus::Failed).await;
                                break;
                            }
                        }
                    }
                    
                    // 检查是否有失败的 Pod
                    if status.failed.unwrap_or(0) > 0 {
                        // 检查 Pod 重启策略和次数
                        // K8s Job 默认会重试，但如果超过限制，需要手动重启
                    }
                }
            }
            Err(kube::Error::Api(ResponseError { code: 404, .. })) => {
                // Job 不存在（可能已被清理）
                update_task_status(task_id, TaskStatus::Completed).await;
                break;
            }
            Err(e) => {
                log::error!("Error monitoring job {}: {}", job_name, e);
            }
        }
    }
}

// 重启一次性任务 Job（从 checkpoint 恢复）
async fn restart_onetime_job(task_id: &str, checkpoint: Option<TaskCheckpoint>) {
    let client = Client::try_default().await.unwrap();
    
    // 1. 删除旧的 Job
    let jobs: Api<Job> = Api::namespaced(client.clone(), "backup-system");
    let job_name = format!("vector-task-onetime-{}", task_id);
    let _ = jobs.delete(&job_name, &DeleteParams::default()).await;
    
    // 2. 从 ConfigMap 读取任务配置
    let configmaps: Api<ConfigMap> = Api::namespaced(client.clone(), "backup-system");
    let configmap_name = format!("vector-task-onetime-{}", task_id);
    let configmap = configmaps.get(&configmap_name).await?;
    
    // 3. 如果有 checkpoint，更新配置
    let mut vector_config = configmap.data
        .and_then(|d| d.get("vector.toml").cloned())
        .unwrap_or_default();
    
    if let Some(cp) = checkpoint {
        vector_config = apply_checkpoint_to_config(&vector_config, &cp)?;
        // 更新 ConfigMap
        let mut updated_configmap = configmap;
        if let Some(data) = &mut updated_configmap.data {
            data.insert("vector.toml".to_string(), vector_config);
        }
        configmaps.replace(&configmap_name, &PostParams::default(), &updated_configmap).await?;
    }
    
    // 4. 重新创建 Job
    let job = build_onetime_job(task_id);
    jobs.create(&PostParams::default(), &job).await?;
    
    // 5. 更新任务状态
    update_task_status(task_id, TaskStatus::Running).await;
    
    // 6. 继续监控
    spawn_job_monitor(task_id);
}
```

2. **管理端重启恢复:**
```rust
// 管理端启动时恢复未完成的任务
async fn recover_incomplete_tasks() {
    let client = Client::try_default().await.unwrap();
    let jobs: Api<Job> = Api::namespaced(client.clone(), "backup-system");
    let configmaps: Api<ConfigMap> = Api::namespaced(client, "backup-system");
    
    // 列出所有一次性任务 ConfigMap
    let onetime_configmaps = configmaps
        .list(&ListParams::default().labels("type=onetime"))
        .await?;
    
    for cm in onetime_configmaps.items {
        if let Some(name) = &cm.metadata.name {
            if let Some(task_id) = extract_task_id_from_configmap_name(name) {
                let job_name = format!("vector-task-onetime-{}", task_id);
                
                // 检查 Job 状态
                match jobs.get(&job_name).await {
                    Ok(job) => {
                        if let Some(status) = &job.status {
                            // 检查 Job 是否还在运行
                            let active = status.active.unwrap_or(0);
                            let succeeded = status.succeeded.unwrap_or(0);
                            let failed = status.failed.unwrap_or(0);
                            
                            if active == 0 && succeeded == 0 && failed > 0 {
                                // Job 失败，尝试从 checkpoint 恢复
                                let checkpoint = load_checkpoint_from_pvc(&task_id).await;
                                if let Some(cp) = checkpoint {
                                    restart_onetime_job(&task_id, Some(cp)).await;
                                }
                            }
                        }
                    }
                    Err(kube::Error::Api(ResponseError { code: 404, .. })) => {
                        // Job 不存在，但 ConfigMap 存在，可能是管理端重启
                        // 检查是否有 checkpoint，如果有则恢复
                        let checkpoint = load_checkpoint_from_pvc(&task_id).await;
                        if let Some(cp) = checkpoint {
                            restart_onetime_job(&task_id, Some(cp)).await;
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}
```

3. **健康检查机制:**
```rust
// Vector Pod 健康检查
async fn check_vector_pod_health(pod_name: &str) -> bool {
    let client = Client::try_default().await.unwrap();
    let pods: Api<Pod> = Api::namespaced(client, "backup-system");
    
    match pods.get(pod_name).await {
        Ok(pod) => {
            if let Some(status) = &pod.status {
                // 检查 Pod 状态
                if let Some(phase) = &status.phase {
                    if phase == "Running" {
                        // 检查容器状态
                        if let Some(container_statuses) = &status.container_statuses {
                            for cs in container_statuses {
                                if let Some(state) = &cs.state {
                                    if state.running.is_some() {
                                        // 检查 Vector API 是否响应（可选）
                                        return check_vector_api_health(pod_name).await;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            false
        }
        Err(_) => false,
    }
}
```

##### 8.4.5.3 任务完成判断

**判断任务是否完成的策略:**

1. **基于数据源完成状态:**
```rust
// 检查所有数据源是否处理完成
async fn is_task_completed(task_id: &str, checkpoint: &Option<TaskCheckpoint>) -> bool {
    let task = load_task_config(task_id).await?;
    
    for data_type in &task.data_types {
        match data_type {
            DataType::Logs => {
                // 检查 S3 文件是否全部处理完
                let all_files = list_s3_files(&task.cluster, &task.time_range).await?;
                let processed_files = get_processed_files(task_id, DataType::Logs).await?;
                
                if all_files.len() != processed_files.len() {
                    return false;
                }
            }
            DataType::Metrics => {
                // 检查指标导出是否完成
                let metrics_exported = check_metrics_export_status(task_id).await?;
                if !metrics_exported {
                    return false;
                }
            }
            // ... 其他数据类型
        }
    }
    
    true
}
```

2. **基于 Vector 进程退出码:**
- Vector 正常退出（退出码 0）通常表示任务完成
- 需要结合 checkpoint 验证数据完整性

3. **基于目标存储验证:**
- 检查目标存储中是否有预期的输出文件
- 验证文件完整性（checksum）

#### 8.4.6 任务进度跟踪

##### 8.4.6.1 进度指标定义

**进度指标包括:**

```rust
struct TaskProgress {
    task_id: String,
    status: TaskStatus,
    progress_percentage: f64,  // 0-100
    
    // 数据源进度
    sources: Vec<SourceProgress>,
    
    // 总体统计
    total_events: u64,
    processed_events: u64,
    failed_events: u64,
    
    // 时间信息
    start_time: DateTime<Utc>,
    estimated_completion: Option<DateTime<Utc>>,
    elapsed_time: Duration,
    
    // 吞吐量
    events_per_second: f64,
    bytes_per_second: f64,
}

struct SourceProgress {
    source_id: String,
    source_type: String,
    status: SourceStatus,
    progress_percentage: f64,
    
    // 文件进度（适用于文件类数据源）
    total_files: Option<u64>,
    processed_files: Option<u64>,
    current_file: Option<String>,
    
    // 事件进度
    total_events: Option<u64>,
    processed_events: u64,
    
    // 数据量
    total_bytes: u64,
    processed_bytes: u64,
}
```

##### 8.4.6.2 进度收集机制

**方式 1: 从 Vector 指标收集**

Vector 提供内部指标，可以通过 API 或日志获取：

```toml
# Vector 配置中启用指标
[api]
enabled = true
address = "127.0.0.1:8686"
```

```rust
// 从 Vector API 获取指标（通过 K8s Service）
async fn collect_vector_metrics(
    client: Client,
    task_id: &str,
    task_type: TaskType,
) -> Result<TaskProgress> {
    // 确定 Vector Pod 名称
    let pod_name = match task_type {
        TaskType::Scheduled => "vector-scheduled".to_string(),
        TaskType::Onetime => {
            // 获取 Job 对应的 Pod
            let jobs: Api<Job> = Api::namespaced(client.clone(), "backup-system");
            let job_name = format!("vector-task-onetime-{}", task_id);
            let job = jobs.get(&job_name).await?;
            
            // 从 Job 获取 Pod 名称（通过 label selector）
            let pods: Api<Pod> = Api::namespaced(client, "backup-system");
            let pod_list = pods.list(&ListParams::default()
                .labels(&format!("job-name={}", job_name))).await?;
            
            pod_list.items.first()
                .and_then(|p| p.metadata.name.clone())
                .ok_or_else(|| Error::PodNotFound)?
        }
    };
    
    // 通过 K8s Port Forward 或 Service 访问 Vector API
    // 方式 1: 使用 K8s Port Forward（推荐用于开发/测试）
    // 方式 2: 创建 Service 暴露 Vector API（推荐用于生产）
    let url = format!("http://{}.backup-system.svc.cluster.local:8686/metrics", pod_name);
    
    let response = reqwest::get(&url).await?;
    let metrics_text = response.text().await?;
    
    // 解析 Prometheus 格式的指标
    let metrics = parse_prometheus_metrics(&metrics_text)?;
    
    // 提取任务相关指标
    let processed_events = get_metric_value(&metrics, "vector_events_processed_total")?;
    let failed_events = get_metric_value(&metrics, "vector_events_failed_total")?;
    
    // 计算进度
    let progress = calculate_progress(task_id, processed_events, failed_events).await?;
    
    Ok(progress)
}
```

**方式 2: 从 Checkpoint 计算进度**

```rust
// 基于 checkpoint 计算进度
async fn calculate_progress_from_checkpoint(
    task_id: &str,
    checkpoint: &TaskCheckpoint,
) -> Result<f64> {
    let task = load_task_config(task_id).await?;
    
    // 计算总工作量
    let total_work = calculate_total_work(&task).await?;
    
    // 计算已完成工作量
    let completed_work = checkpoint.total_processed;
    
    // 计算进度百分比
    let progress = if total_work > 0 {
        (completed_work as f64 / total_work as f64) * 100.0
    } else {
        0.0
    };
    
    Ok(progress.min(100.0))
}
```

**方式 3: 从目标存储验证进度**

```rust
// 通过检查目标存储验证进度
async fn verify_progress_from_target(task_id: &str) -> Result<TaskProgress> {
    let task = load_task_config(task_id).await?;
    
    match &task.target {
        Target::S3 { bucket, prefix } => {
            // 列出目标存储中的文件
            let output_files = list_s3_files(bucket, prefix).await?;
            
            // 根据输出文件数量和大小估算进度
            let total_size: u64 = output_files.iter()
                .map(|f| f.size)
                .sum();
            
            // 与预期输出对比
            let expected_size = estimate_expected_output_size(&task).await?;
            let progress = if expected_size > 0 {
                (total_size as f64 / expected_size as f64) * 100.0
            } else {
                0.0
            };
            
            Ok(TaskProgress {
                progress_percentage: progress.min(100.0),
                // ... 其他字段
            })
        }
        // ... 其他目标类型
    }
}
```

##### 8.4.6.3 进度更新和存储

```rust
// 定期更新任务进度
async fn update_task_progress(task_id: &str) {
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    
    loop {
        interval.tick().await;
        
        // 收集进度信息
        let progress = collect_task_progress(task_id).await?;
        
        // 保存进度到存储
        save_task_progress(task_id, &progress).await?;
        
        // 如果任务完成，退出循环
        if progress.status == TaskStatus::Completed {
            break;
        }
    }
}

// 保存进度（可选：存储到 ConfigMap 或 PVC）
async fn save_task_progress(
    client: Client,
    task_id: &str,
    progress: &TaskProgress,
) -> Result<()> {
    // 方式 1: 存储到 ConfigMap（轻量级，适合进度信息）
    let configmaps: Api<ConfigMap> = Api::namespaced(client.clone(), "backup-system");
    let progress_cm_name = format!("vector-task-progress-{}", task_id);
    
    let json = serde_json::to_string(progress)?;
    let configmap = ConfigMap {
        metadata: ObjectMeta {
            name: Some(progress_cm_name.clone()),
            namespace: Some("backup-system".to_string()),
            ..Default::default()
        },
        data: Some({
            let mut map = BTreeMap::new();
            map.insert("progress.json".to_string(), json);
            map
        }),
        ..Default::default()
    };
    
    // 创建或更新 ConfigMap
    match configmaps.get(&progress_cm_name).await {
        Ok(mut existing) => {
            if let Some(data) = &mut existing.data {
                data.insert("progress.json".to_string(), serde_json::to_string(progress)?);
            }
            configmaps.replace(&progress_cm_name, &PostParams::default(), &existing).await?;
        }
        Err(kube::Error::Api(ResponseError { code: 404, .. })) => {
            configmaps.create(&PostParams::default(), &configmap).await?;
        }
        Err(e) => return Err(e.into()),
    }
    
    // 方式 2: 存储到 PVC（如果需要持久化，如 checkpoint）
    // 使用 PVC 挂载到 Pod，Vector 可以直接写入 checkpoint 文件
    
    Ok(())
}
```

#### 8.4.7 管理端状态和进度查询

##### 8.4.7.1 API 接口设计

```rust
// 获取任务状态
GET /api/v1/tasks/{id}/status

// 响应示例（一次性任务）
{
  "task_id": "onetime-backup-001",
  "status": "running",  // pending, running, completed, failed
  "task_type": "onetime",
  "created_at": "2024-01-01T10:00:00Z",
  "started_at": "2024-01-01T10:00:05Z",
  "updated_at": "2024-01-01T10:15:30Z",
  "k8s_job": {
    "name": "vector-task-onetime-001",
    "namespace": "backup-system",
    "status": {
      "active": 1,
      "succeeded": 0,
      "failed": 0
    }
  },
  "k8s_pod": {
    "name": "vector-task-onetime-001-xxxxx",
    "phase": "Running",
    "container_status": "Running"
  }
}

// 响应示例（周期性任务）
{
  "task_id": "scheduled-backup-001",
  "status": "running",
  "task_type": "scheduled",
  "created_at": "2024-01-01T10:00:00Z",
  "k8s_pod": {
    "name": "vector-scheduled",
    "phase": "Running",
    "container_status": "Running"
  },
  "configmap": {
    "name": "vector-task-scheduled-001",
    "exists": true
  }
}

// 获取任务进度
GET /api/v1/tasks/{id}/progress

// 响应示例
{
  "task_id": "onetime-backup-001",
  "status": "running",
  "progress_percentage": 45.5,
  "sources": [
    {
      "source_id": "s3_logs",
      "source_type": "aws_s3",
      "status": "running",
      "progress_percentage": 60.0,
      "total_files": 100,
      "processed_files": 60,
      "current_file": "logs/2024/01/01/app-060.log.gz",
      "processed_events": 1500000,
      "processed_bytes": 1073741824,
      "events_per_second": 2500.0,
      "bytes_per_second": 1789569.7
    },
    {
      "source_id": "prometheus_metrics",
      "source_type": "prometheus",
      "status": "running",
      "progress_percentage": 30.0,
      "processed_events": 500000,
      "processed_bytes": 536870912
    }
  ],
  "total_events": 2000000,
  "processed_events": 2000000,
  "failed_events": 0,
  "start_time": "2024-01-01T10:00:05Z",
  "elapsed_time": "15m30s",
  "estimated_completion": "2024-01-01T10:35:00Z",
  "events_per_second": 2150.5,
  "bytes_per_second": 1610612.8
}

// 获取任务日志
GET /api/v1/tasks/{id}/logs?level=info&limit=100&offset=0

// 响应示例
{
  "task_id": "onetime-backup-001",
  "logs": [
    {
      "timestamp": "2024-01-01T10:00:05Z",
      "level": "info",
      "message": "Task started",
      "source": "management"
    },
    {
      "timestamp": "2024-01-01T10:00:10Z",
      "level": "info",
      "message": "Vector process started, PID: 12345",
      "source": "vector_manager"
    },
    // ...
  ],
  "total": 150,
  "limit": 100,
  "offset": 0
}

// 获取任务指标
GET /api/v1/tasks/{id}/metrics?start_time=2024-01-01T10:00:00Z&end_time=2024-01-01T10:30:00Z

// 响应示例
{
  "task_id": "onetime-backup-001",
  "metrics": [
    {
      "timestamp": "2024-01-01T10:00:00Z",
      "events_processed": 0,
      "events_per_second": 0.0,
      "bytes_processed": 0,
      "bytes_per_second": 0.0
    },
    {
      "timestamp": "2024-01-01T10:05:00Z",
      "events_processed": 645000,
      "events_per_second": 2150.0,
      "bytes_processed": 483750000,
      "bytes_per_second": 1612500.0
    },
    // ...
  ]
}
```

##### 8.4.7.2 实现代码示例

```rust
// API 处理器
#[get("/tasks/{id}/status")]
async fn get_task_status(
    id: Path<String>,
    task_store: Data<dyn TaskStore>,
) -> Result<Json<TaskStatusResponse>> {
    let task_id = id.into_inner();
    let task = task_store.get_task(&task_id).await?;
    
    // 检查 Vector 进程状态
    let vector_status = if let Some(pid) = task.vector_pid {
        check_vector_process_status(pid).await
    } else {
        ProcessStatus::NotRunning
    };
    
    Ok(Json(TaskStatusResponse {
        task_id: task.id.clone(),
        status: task.status,
        created_at: task.created_at,
        started_at: task.started_at,
        updated_at: task.updated_at,
        vector_pid: task.vector_pid,
        vector_status,
    }))
}

#[get("/tasks/{id}/progress")]
async fn get_task_progress(
    id: Path<String>,
    progress_store: Data<dyn ProgressStore>,
) -> Result<Json<TaskProgressResponse>> {
    let task_id = id.into_inner();
    
    // 从存储获取最新进度
    let progress = progress_store.get_progress(&task_id).await?;
    
    // 如果任务正在运行，实时更新进度
    if progress.status == TaskStatus::Running {
        let latest_progress = collect_task_progress(&task_id).await?;
        progress_store.update_progress(&task_id, &latest_progress).await?;
        Ok(Json(latest_progress))
    } else {
        Ok(Json(progress))
    }
}

// 实时进度收集
async fn collect_task_progress(task_id: &str) -> Result<TaskProgress> {
    let task = load_task_config(task_id).await?;
    
    // 从多个来源收集进度信息
    let mut sources_progress = Vec::new();
    
    for data_type in &task.data_types {
        let source_progress = match data_type {
            DataType::Logs => {
                collect_s3_source_progress(task_id, "s3_logs").await?
            }
            DataType::Metrics => {
                collect_prometheus_source_progress(task_id, "prometheus_metrics").await?
            }
            // ... 其他数据类型
        };
        sources_progress.push(source_progress);
    }
    
    // 计算总体进度
    let total_progress: f64 = sources_progress.iter()
        .map(|s| s.progress_percentage)
        .sum::<f64>() / sources_progress.len() as f64;
    
    // 计算总体统计
    let total_events: u64 = sources_progress.iter()
        .map(|s| s.processed_events)
        .sum();
    
    let processed_events: u64 = sources_progress.iter()
        .map(|s| s.processed_events)
        .sum();
    
    Ok(TaskProgress {
        task_id: task_id.to_string(),
        status: get_task_status(task_id).await?,
        progress_percentage: total_progress,
        sources: sources_progress,
        total_events,
        processed_events,
        failed_events: 0, // 从 Vector 指标获取
        start_time: task.created_at,
        estimated_completion: estimate_completion_time(task_id).await?,
        elapsed_time: calculate_elapsed_time(task_id).await?,
        events_per_second: calculate_throughput(task_id).await?,
        bytes_per_second: calculate_bytes_throughput(task_id).await?,
    })
}
```

##### 8.4.7.3 WebSocket 实时进度推送（可选）

```rust
// WebSocket 实时进度推送
#[get("/tasks/{id}/progress/stream")]
async fn stream_task_progress(
    id: Path<String>,
    ws: WebSocket,
) -> Result<impl Responder> {
    let task_id = id.into_inner();
    
    let (mut sender, _receiver) = ws.split();
    
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(2));
        
        loop {
            interval.tick().await;
            
            // 获取最新进度
            if let Ok(progress) = collect_task_progress(&task_id).await {
                // 发送进度更新
                if let Err(_) = sender.send(Message::Text(
                    serde_json::to_string(&progress).unwrap()
                )).await {
                    break; // 客户端断开连接
                }
                
                // 如果任务完成，发送最终状态后退出
                if progress.status == TaskStatus::Completed 
                    || progress.status == TaskStatus::Failed {
                    break;
                }
            }
        }
    });
    
    Ok(())
}
```

## 9. 配置示例

### 9.1 周期性任务配置

```yaml
task:
  id: scheduled-backup-001
  name: "Daily Cluster Backup"
  type: "scheduled"  # 周期性任务
  enabled: true
  
  schedule:
    type: "cron"  # 或 "interval"
    cron: "0 2 * * *"  # 每天凌晨 2 点执行
    # 或使用 interval: "24h"
    timezone: "UTC"
  
  cluster: tidb-cluster-01
  
  # 周期性任务使用相对时间（相对于执行时间）
  time_range:
    type: "relative"  # 相对时间
    offset: "-24h"     # 备份过去 24 小时的数据
    # 或使用 absolute 绝对时间
    # type: "absolute"
    # start: "2024-01-01T00:00:00Z"
    # end: "2024-01-01T23:59:59Z"
  
  data_types:
    - logs
    - metrics
  
  filters:
    logs:
      enabled: true
      rules:
        - type: keyword
          keywords: ["ERROR", "WARN"]
  
  target:
    type: s3
    bucket: backup-bucket
    prefix: "backups/tidb-cluster-01/daily/"
    compression: "gzip"
  
  options:
    timeout: "2h"
    retry:
      max_attempts: 3
```

### 9.2 一次性任务配置

```yaml
task:
  id: onetime-backup-001
  name: "Ad-hoc Backup for Incident"
  type: "onetime"  # 一次性任务
  enabled: true
  
  # 一次性任务使用绝对时间
  time_range:
    type: "absolute"
    start: "2024-01-01T00:00:00Z"
    end: "2024-01-01T23:59:59Z"
    timezone: "UTC"
  
  cluster: tidb-cluster-01
  
  data_types:
    - logs
    - slowlogs
    - sqlstatements
    - metrics
  
  filters:
    logs:
      enabled: true
      logic: "OR"
      rules:
        - type: keyword
          keywords: ["ERROR", "WARN", "critical"]
        - type: regex
          pattern: ".*timeout.*"
    
    slowlogs:
      enabled: true
    
    sqlstatements:
      enabled: true
      rules:
        - type: field
          field: "execution_time"
          operator: ">"
          value: "1s"
  
  target:
    type: s3
    bucket: backup-bucket
    prefix: "backups/tidb-cluster-01/incident-20240101/"
    compression: "gzip"
  
  options:
    timeout: "4h"
    retry:
      max_attempts: 3
      backoff: "exponential"
```

### 9.3 完整备份任务配置（通用格式）

```yaml
backup_task:
  id: backup-20240101-001
  cluster: tidb-cluster-01
  time_range:
    start: "2024-01-01T00:00:00Z"
    end: "2024-01-01T23:59:59Z"
    timezone: "UTC"
  
  data_types:
    - logs
    - slowlogs
    - sqlstatements
    - metrics
  
  filters:
    logs:
      enabled: true
      logic: "OR"
      rules:
        - type: keyword
          keywords: ["ERROR", "WARN", "critical"]
          case_sensitive: false
        - type: regex
          pattern: ".*timeout.*"
          field: "message"
    
    slowlogs:
      enabled: false
    
    sqlstatements:
      enabled: true
      logic: "AND"
      rules:
        - type: field
          field: "execution_time"
          operator: ">"
          value: "1s"
        - type: keyword
          keywords: ["SELECT", "UPDATE", "DELETE"]
          field: "sql_text"
    
    metrics:
      enabled: false
  
  target:
    type: s3
    bucket: backup-bucket
    prefix: "backups/tidb-cluster-01/2024-01-01/"
    compression: "gzip"
    encryption: true
  
  options:
    parallel_sources: true
    batch_size: 1000
    timeout: "2h"
    retry:
      max_attempts: 3
      backoff: "exponential"
```

### 9.4 集群数据源配置

```yaml
clusters:
  tidb-cluster-01:
    logs:
      s3:
        bucket: "logs-bucket"
        region: "us-west-2"
        prefix: "tidb-cluster-01/logs/"
        compression: "gzip"
      loki:
        endpoint: "http://loki-server:3100"
        query_template: '{cluster="tidb-cluster-01"}'
      parquet:
        bucket: "stats-bucket"
        prefix: "tidb-cluster-01/stats/hourly/"
    
    slowlogs:
      database:
        connection_string: "mysql://user:pass@tidb-server:4000/information_schema"
        table: "slow_query"
        time_field: "time"
      s3:
        bucket: "logs-bucket"
        prefix: "tidb-cluster-01/slowlogs/"
    
    sqlstatements:
      database:
        connection_string: "mysql://user:pass@tidb-server:4000/information_schema"
        table: "statements_summary"
        time_field: "summary_begin_time"
      api:
        endpoint: "http://tidb-server:10080/api/v1/statements"
    
    metrics:
      prometheus:
        endpoint: "http://prometheus:9090"
        query_template: '{cluster="tidb-cluster-01"}'
      victoriametrics:
        endpoint: "http://vm:8428"
        query_template: '{cluster="tidb-cluster-01"}'
```

### 9.5 管理端配置

```yaml
management:
  # API 服务配置
  api:
    host: "0.0.0.0"
    port: 8080
    enable_cors: true
  
  # Kubernetes 配置
  kubernetes:
    # K8s 命名空间
    namespace: "backup-system"
    
    # K8s API 配置（如果不在集群内运行，需要配置）
    # kubeconfig: "/path/to/kubeconfig"
    # 或使用 in-cluster 配置（在 Pod 内运行时自动使用）
  
  # Vector Pod 配置
  vector:
    # Vector 镜像
    image: "vector:latest"
    
    # 周期性任务 Pod 名称
    scheduled_pod_name: "vector-scheduled"
    
    # 一次性任务 Job 配置
    onetime_job:
      # Job 完成后自动清理时间（秒）
      ttl_seconds_after_finished: 3600
  
  # 调度器配置
  scheduler:
    # Cron 调度器配置
    cron:
      enabled: true
      timezone: "UTC"
    
    # 任务队列配置
    queue:
      max_concurrent_tasks: 10
      task_timeout: "4h"
  
  # 监控配置
  monitoring:
    enabled: true
    metrics_port: 9090
    log_level: "info"
  
  # 注意：无需数据库配置，所有任务信息存储在 K8s ConfigMap 中
```

### 9.6 API 请求示例

**创建周期性任务:**
```bash
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Daily Backup",
    "type": "scheduled",
    "schedule": {
      "type": "cron",
      "cron": "0 2 * * *"
    },
    "cluster": "tidb-cluster-01",
    "time_range": {
      "type": "relative",
      "offset": "-24h"
    },
    "data_types": ["logs", "metrics"],
    "target": {
      "type": "s3",
      "bucket": "backup-bucket",
      "prefix": "backups/tidb-cluster-01/daily/"
    }
  }'
```

**创建一次性任务:**
```bash
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Ad-hoc Backup",
    "type": "onetime",
    "time_range": {
      "type": "absolute",
      "start": "2024-01-01T00:00:00Z",
      "end": "2024-01-01T23:59:59Z"
    },
    "cluster": "tidb-cluster-01",
    "data_types": ["logs", "slowlogs", "sqlstatements", "metrics"],
    "target": {
      "type": "s3",
      "bucket": "backup-bucket",
      "prefix": "backups/tidb-cluster-01/incident-20240101/"
    }
  }'
```

**查询任务状态:**
```bash
curl http://localhost:8080/api/v1/tasks/scheduled-backup-001/status
```

**停止任务:**
```bash
curl -X POST http://localhost:8080/api/v1/tasks/scheduled-backup-001/stop
```

## 10. 测试验证

### 10.1 单元测试

- 配置解析测试
- 过滤规则转换测试
- Vector 配置生成测试

### 10.2 集成测试

- 端到端备份流程测试
- 多数据源备份测试
- 过滤功能测试
- 错误处理测试

### 10.3 性能测试

- 大数据量备份测试
- 并发备份测试
- 过滤性能测试

## 11. 附录

### 11.1 Vector 相关资源

- Vector 官方文档: https://vector.dev/docs/
- Vector 插件列表: https://vector.dev/docs/reference/configuration/
- VRL 语言参考: https://vector.dev/docs/reference/vrl/

### 11.2 数据格式参考

- ISO 8601 时间格式: https://en.wikipedia.org/wiki/ISO_8601
- Parquet 格式: https://parquet.apache.org/
- Prometheus 数据格式: https://prometheus.io/docs/instrumenting/exposition_formats/

### 11.3 术语表

- **Cluster**: 集群，一个 TiDB 集群实例
- **Diagnostic Data**: 诊断数据，包括日志、慢查询、SQL 语句、指标等
- **Filter**: 过滤规则，用于筛选需要备份的数据
- **Source**: Vector 数据源插件
- **Transform**: Vector 数据转换插件
- **Sink**: Vector 数据目标插件
- **VRL**: Vector Remap Language，Vector 的表达式语言
