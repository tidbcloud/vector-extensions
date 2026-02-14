# Conprof 拓扑发现：接口与用法说明

本文档按实际请求逐个说明 conprof 拓扑发现用到的 PD API 和 etcd 接口：每条给出**实际可执行的命令**、**返回示例**以及**代码里如何用**。  
配置采用实际部署的写死值：`pd_address: db-pd:2379`，TLS 证书路径 `/etc/vector/tikv-tls/`（ca.crt / tls.crt / tls.key）。

---

## 0. 公共参数（TLS 与基地址）

所有通过 PD 的 HTTP 请求都使用同一套 TLS 与基地址：

- **基地址**：`https://db-pd:2379`（配置中的 `pd_address`，代码里若有 TLS 会加上 `https://`，见 `topology/fetch/mod.rs` 的 `polish_address_impl`）
- **TLS**：与 `ConprofConfig.tls` 对应，即 toml 中的 `ca_file` / `crt_file` / `key_file`，本例中为：
  - `--cacert /etc/vector/tikv-tls/ca.crt`
  - `--cert /etc/vector/tikv-tls/tls.crt`
  - `--key /etc/vector/tikv-tls/tls.key`

下文 curl 均省略重复说明，只写路径与用途。

---

## 1. PD Health：获取健康成员列表

**作用**：拿到当前「健康」的 PD member_id 集合，后面和 PD Members 一起用，只保留健康的 PD 节点。

**实际命令**：

```bash
curl --cacert /etc/vector/tikv-tls/ca.crt --cert /etc/vector/tikv-tls/tls.crt --key /etc/vector/tikv-tls/tls.key \
  https://db-pd:2379/pd/api/v1/health
```

**代码位置**：`src/sources/conprof/topology/fetch/pd.rs`  
- 路径常量：`health_path: "/pd/api/v1/health"`  
- 请求：`GET {pd_address}/pd/api/v1/health`

**返回结构（示例）**：JSON 数组，每项包含 `member_id`、`health`（bool）：

```json
[
  { "member_id": 1205700534785825479, "health": true },
  { "member_id": 8087220927624939195, "health": true },
  { "member_id": 9180028931716664588, "health": true }
]
```

**代码怎么用**：  
在 `get_up_pds` 里先调 `fetch_pd_health()`，得到 `health_resp`，然后筛出 `health == true` 的 `member_id` 放入集合 `health_members`。接下来用 PD Members 返回的 `members`，只保留 `member_id` 在 `health_members` 里的节点，再从中取 `client_urls[0]` 解析为 (host, port)，生成 `InstanceType::PD` 的 `Component`。

---

## 2. PD Members：获取 PD 成员及其 client_urls

**作用**：拿到所有 PD 成员信息；代码只关心 `members[].member_id` 和 `members[].client_urls[0]`，再结合 Health 过滤出在线的 PD，用于生成 PD 拓扑（conprof 要连的 PD 地址）。

**实际命令**：

```bash
curl --cacert /etc/vector/tikv-tls/ca.crt --cert /etc/vector/tikv-tls/tls.crt --key /etc/vector/tikv-tls/tls.key \
  https://db-pd:2379/pd/api/v1/members
```

**代码位置**：`src/sources/conprof/topology/fetch/pd.rs`  
- 路径常量：`members_path: "/pd/api/v1/members"`  
- 请求：`GET {pd_address}/pd/api/v1/members`

**实际返回示例**（你提供的真实响应）：

```json
{
  "header": {
    "cluster_id": 7606556073950805071
  },
  "members": [
    {
      "name": "db-2a7a0917-dlcln6",
      "member_id": 1205700534785825479,
      "peer_urls": [
        "https://db-2a7a0917-pd-dlcln6.db-cluster.tidb2022505199024738304.svc.cluster.local:2380"
      ],
      "client_urls": [
        "https://db-2a7a0917-pd-dlcln6.db-cluster.tidb2022505199024738304.svc.cluster.local:2379"
      ],
      "deploy_path": "/",
      "binary_version": "v9.0.0-beta.2.pre-286-g16fd547",
      "git_hash": "16fd547f5eb30b529f5e4711868408a691debda6"
    },
    {
      "name": "db-2a7a0917-tgxyez",
      "member_id": 8087220927624939195,
      "peer_urls": ["https://db-2a7a0917-pd-tgxyez.db-cluster...:2380"],
      "client_urls": ["https://db-2a7a0917-pd-tgxyez.db-cluster...:2379"],
      ...
    },
    {
      "name": "db-2a7a0917-kcbq3s",
      "member_id": 9180028931716664588,
      "peer_urls": ["https://db-2a7a0917-pd-kcbq3s.db-cluster...:2380"],
      "client_urls": ["https://db-2a7a0917-pd-kcbq3s.db-cluster...:2379"],
      ...
    }
  ],
  "leader": { ... },
  "etcd_leader": { ... }
}
```

**代码怎么用**：  
- 反序列化时只用到顶层 `members` 数组；模型里 `MemberItem` 只有 `member_id` 和 `client_urls`（见 `topology/fetch/models.rs`），`header` / `leader` / `etcd_leader` 等未使用。  
- 对每个 `member`，若其 `member_id` 在 Health 得到的 `health_members` 中，则取 `member.client_urls[0]`（即该 PD 的 client 地址，如 `https://db-2a7a0917-pd-dlcln6....:2379`），用 `utils::parse_host_port` 解析出 host 和 port，插入一个 `Component { instance_type: PD, host, primary_port, secondary_port }`。  
- 因此最终拓扑里的 PD 列表 = Health 为 true 的成员对应的 client_urls，用于后续访问 PD/etcd。

---

## 3. PD Stores：获取 TiKV / TiFlash 存储节点

**作用**：拿到所有 store（TiKV 或 TiFlash），代码根据 `state_name == "up"` 和 `address` / `status_address` 生成 TiKV 或 TiFlash 的 `Component`；conprof 用 `status_address` 对应 secondary_port 做 profile 拉取。

**实际命令**：

```bash
curl --cacert /etc/vector/tikv-tls/ca.crt --cert /etc/vector/tikv-tls/tls.crt --key /etc/vector/tikv-tls/tls.key \
  https://db-pd:2379/pd/api/v1/stores
```

**代码位置**：`src/sources/conprof/topology/fetch/store.rs`  
- 路径常量：`stores_path: "/pd/api/v1/stores"`  
- 请求：`GET {pd_address}/pd/api/v1/stores`

**实际返回示例**（真实响应，`status` 仅保留与拓扑无关的容量/心跳等，代码未使用）：

```json
{
  "count": 5,
  "stores": [
    {
      "store": {
        "id": 187,
        "address": "db-2a7a0917-tikv-rre4fm.db-cluster.tidb2022505199024738304.svc.cluster.local:20160",
        "labels": [
          { "key": "host", "value": "ip-10-0-137-56.us-west-2.compute.internal" },
          { "key": "region", "value": "us-west-2" },
          { "key": "zone", "value": "us-west-2c" }
        ],
        "status_address": "db-2a7a0917-tikv-rre4fm.db-cluster...:20180",
        "state_name": "Up"
      },
      "status": { "capacity": "1.441TiB", "leader_count": 1353, ... }
    },
    {
      "store": {
        "id": 277,
        "address": "db-2a7a0917-write-tiflash-8a72t8.db-cluster...:3930",
        "labels": [
          { "key": "engine_role", "value": "write" },
          { "key": "engine", "value": "tiflash" },
          ...
        ],
        "status_address": "db-2a7a0917-write-tiflash-8a72t8.db-cluster...:20292",
        "state_name": "Up"
      },
      "status": { ... }
    },
    {
      "store": {
        "id": 278,
        "address": "db-2a7a0917-compute-tiflash-cjd0hn.db-cluster...:3930",
        "labels": [
          { "key": "engine", "value": "tiflash_compute" },
          ...
        ],
        "status_address": "db-2a7a0917-compute-tiflash-cjd0hn.db-cluster...:20292",
        "state_name": "Up"
      },
      "status": { ... }
    },
    {
      "store": {
        "id": 1,
        "address": "db-2a7a0917-tikv-072qmp.db-cluster...:20160",
        "labels": [ { "key": "region", "value": "us-west-2" }, { "key": "zone", "value": "us-west-2b" }, ... ],
        "status_address": "db-2a7a0917-tikv-072qmp.db-cluster...:20180",
        "state_name": "Up"
      },
      "status": { ... }
    },
    {
      "store": {
        "id": 12,
        "address": "db-2a7a0917-tikv-b9cplx.db-cluster...:20160",
        "labels": [ ... ],
        "status_address": "db-2a7a0917-tikv-b9cplx.db-cluster...:20180",
        "state_name": "Up"
      },
      "status": { ... }
    }
  ]
}
```

**代码用到的字段**：  
- `store.address`：业务地址（host:port），解析为 `Component` 的 host + primary_port。  
- `store.status_address`：状态/监控地址，解析出 secondary_port，conprof 用该端口拉 profile（TiKV 一般为 20180，TiFlash 为 20292）。  
- `store.state_name`：代码用 `state_name.to_lowercase() == "up"` 判断是否采集，本例 5 个均为 `"Up"`，都会保留。  
- `store.labels`：若存在 `key == "engine"` 且 `value.to_lowercase().contains("tiflash")` 则判为 **TiFlash**，否则为 **TiKV**（见 `parse_instance_type`）。

**按本条实际响应的分类**：  
- **TiKV**（3 个）：id 187、1、12，labels 中无 `engine=tiflash`，address 端口 20160，status_address 端口 20180。  
- **TiFlash**（2 个）：id 277（`engine: "tiflash"`）、id 278（`engine: "tiflash_compute"`，value 含 "tiflash"），address 端口 3930，status_address 端口 20292。  

**代码怎么用**：  
- `get_up_stores` 调用 `fetch_stores()` 得到 `StoresResponse`，遍历 `stores_resp.stores`。  
- 对每个 `store`，若 `is_up(&store)` 为 true（即 state_name 为 "Up"），则从 `store.address` 解析 (host, primary_port)，从 `store.status_address` 解析 secondary_port，用 `parse_instance_type(&store)` 得到 TiKV 或 TiFlash，插入一个 `Component`。  
- 即：PD Stores 接口直接驱动「哪些 TiKV/TiFlash 实例要被 conprof 采集」。

---

## 4. etcd TiDB 拓扑：/topology/tidb/

**作用**：从 etcd 读取 TiDB 实例的拓扑（地址 + status_port），结合 TTL 判断实例是否存活，得到在线的 TiDB 列表用于 conprof 采集。

**实际命令**（etcd 与 PD 同 endpoint，TLS 一致）：

```bash
ETCDCTL_API=3 etcdctl --endpoints=https://db-pd:2379 \
  --cacert=/etc/vector/tikv-tls/ca.crt \
  --cert=/etc/vector/tikv-tls/tls.crt \
  --key=/etc/vector/tikv-tls/tls.key \
  get --prefix "/topology/tidb/"
```

**代码位置**：`src/sources/conprof/topology/fetch/tidb.rs`  
- prefix：`"/topology/tidb/"`  
- 请求：etcd `get(key_prefix, WithPrefix)`，等价于上面 `get --prefix`。

**etcd 中的 key 形态（示例）**：  
- `{prefix}{address}/ttl`：TTL 键，value 与租约相关，用于判断该 address 是否仍存活。  
- `{prefix}{address}/info`：信息键，value 为 JSON，包含 `status_port`（conprof 用做 secondary_port）。

**代码怎么用**：  
- `get_up_tidbs` 先 `fetch_topology_kvs()` 拉取 prefix 下所有 kv。  
- 对每个 kv 解析为 `EtcdTopology::TTL { address, ttl }` 或 `EtcdTopology::Info { address, value }`：TTL 用于 `is_up_impl(ttl)` 得到「仍存活的 address」集合；Info 解析出 (host, port) 和 value.status_port，构造 `Component { instance_type: TiDB, host, primary_port, secondary_port: value.status_port }`。  
- 仅当 address 在「存活」集合中时才把对应 Component 加入结果。可选地，代码中还有 `TIDB_GROUP` 环境变量用于过滤 TiDB 组（与 PD/证书无关）。

---

## 5. etcd TiProxy 拓扑：/topology/tiproxy/

**作用**：与 TiDB 拓扑类似，从 etcd 读取 TiProxy 实例的地址和 status_port，结合 TTL 得到在线的 TiProxy 列表。

**实际命令**：

```bash
ETCDCTL_API=3 etcdctl --endpoints=https://db-pd:2379 \
  --cacert=/etc/vector/tikv-tls/ca.crt \
  --cert=/etc/vector/tikv-tls/tls.crt \
  --key=/etc/vector/tikv-tls/tls.key \
  get --prefix "/topology/tiproxy/"
```

**代码位置**：`src/sources/conprof/topology/fetch/tiproxy.rs`  
- prefix：`"/topology/tiproxy/"`  
- 请求：etcd `get(key_prefix, WithPrefix)`。

**代码怎么用**：  
- 逻辑与 TiDB 拓扑类似：通过 TTL 键判断 address 是否存活，通过 info 键取 address 和 `status_port`（此处为字符串，代码里会 `parse::<u16>()`），只保留存活的 TiProxy，生成 `InstanceType::TiProxy` 的 `Component`。

---

## 小结（与配置/代码对应）

| 序号 | 接口 | 命令/路径 | 代码用途 |
|------|------|-----------|----------|
| 1 | PD Health | `GET https://db-pd:2379/pd/api/v1/health` | 得到健康 member_id 集合，用于过滤 PD Members |
| 2 | PD Members | `GET https://db-pd:2379/pd/api/v1/members` | 取健康成员的 client_urls[0]，生成 PD Component |
| 3 | PD Stores | `GET https://db-pd:2379/pd/api/v1/stores` | 取 state_name==up 的 store，按 address/status_address、labels 生成 TiKV/TiFlash Component |
| 4 | etcd TiDB | `get --prefix /topology/tidb/` | 解析 TTL + info，得到存活 TiDB 的 address 与 status_port，生成 TiDB Component |
| 5 | etcd TiProxy | `get --prefix /topology/tiproxy/` | 同上，生成 TiProxy Component |

以上命令中的 `db-pd:2379` 和 `/etc/vector/tikv-tls/` 三个证书路径均为实际部署的写死配置，与 Vector 中 `pd_address` 和 `tls.ca_file/crt_file/key_file` 一一对应。
