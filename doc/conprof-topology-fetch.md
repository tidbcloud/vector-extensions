# Conprof Topology Discovery: API and Usage

This doc describes each PD API and etcd request used by conprof topology discovery: **executable commands**, **sample responses**, and **how the code uses them**.  
Configuration uses fixed values from the deployment: `pd_address: db-pd:2379`, TLS certs at `/etc/vector/tikv-tls/` (ca.crt / tls.crt / tls.key).

---

## 0. Common Parameters (TLS and Base URL)

All PD HTTP requests share the same TLS and base URL:

- **Base URL**: `https://db-pd:2379` (from `pd_address`; TLS adds `https://` in code—see `topology/fetch/mod.rs` `polish_address_impl`)
- **TLS**: matches `ConprofConfig.tls` (toml `ca_file` / `crt_file` / `key_file`). For this example:
  - `--cacert /etc/vector/tikv-tls/ca.crt`
  - `--cert /etc/vector/tikv-tls/tls.crt`
  - `--key /etc/vector/tikv-tls/tls.key`

Curl examples below omit these and show only paths and purpose.

---

## 1. PD Health: Healthy Member List

**Purpose**: Get the set of healthy PD `member_id`s; combined with PD Members, only healthy PDs are kept.

**Command**:

```bash
curl --cacert /etc/vector/tikv-tls/ca.crt --cert /etc/vector/tikv-tls/tls.crt --key /etc/vector/tikv-tls/tls.key \
  https://db-pd:2379/pd/api/v1/health
```

**Code**: `src/sources/conprof/topology/fetch/pd.rs`  
- Path constant: `health_path: "/pd/api/v1/health"`  
- Request: `GET {pd_address}/pd/api/v1/health`

**Response** (example): JSON array of `member_id` and `health` (bool):

```json
[
  { "member_id": 1205700534785825479, "health": true },
  { "member_id": 8087220927624939195, "health": true },
  { "member_id": 9180028931716664588, "health": true }
]
```

**Usage**: In `get_up_pds`, call `fetch_pd_health()` to get `health_resp`, filter to `health == true` and collect `member_id`s into `health_members`. Then use PD Members `members` and keep only those with `member_id` in `health_members`. Parse `client_urls[0]` as (host, port) and create `Component { instance_type: PD, ... }`.

---

## 2. PD Members: PD Members and client_urls

**Purpose**: Get all PD members; code uses `members[].member_id` and `members[].client_urls[0]`, filters by Health to get online PDs, and builds PD topology (addresses conprof connects to).

**Command**:

```bash
curl --cacert /etc/vector/tikv-tls/ca.crt --cert /etc/vector/tikv-tls/tls.crt --key /etc/vector/tikv-tls/tls.key \
  https://db-pd:2379/pd/api/v1/members
```

**Code**: `src/sources/conprof/topology/fetch/pd.rs`  
- Path: `members_path: "/pd/api/v1/members"`  
- Request: `GET {pd_address}/pd/api/v1/members`

**Response** (example): JSON with `members` array; each has `member_id`, `client_urls`, etc. `header` / `leader` / `etcd_leader` are not used.

**Usage**: Deserialize only `members`; for each member with `member_id` in `health_members`, take `client_urls[0]`, parse to (host, port), insert `Component { instance_type: PD, host, primary_port, secondary_port }`. Final PD list = client_urls of healthy members for PD/etcd access.

---

## 3. PD Stores: TiKV / TiFlash Storage Nodes

**Purpose**: Get all stores (TiKV or TiFlash); code filters by `state_name == "up"` and uses `address` / `status_address` to build TiKV or TiFlash `Component`s. conprof uses `status_address` (secondary_port) for profile fetch.

**Command**:

```bash
curl --cacert /etc/vector/tikv-tls/ca.crt --cert /etc/vector/tikv-tls/tls.crt --key /etc/vector/tikv-tls/tls.key \
  https://db-pd:2379/pd/api/v1/stores
```

**Code**: `src/sources/conprof/topology/fetch/store.rs`  
- Path: `stores_path: "/pd/api/v1/stores"`  
- Request: `GET {pd_address}/pd/api/v1/stores`

**Usage**: `store.address` → (host, primary_port); `store.status_address` → secondary_port (TiKV 20180, TiFlash 20292); `store.state_name == "Up"` for inclusion; `store.labels` with `engine=tiflash` → TiFlash, else TiKV. `get_up_stores` calls `fetch_stores()`, iterates stores, and for each up store inserts a TiKV or TiFlash `Component`.

---

## 4. etcd TiDB Topology: /topology/tidb/

**Purpose**: Read TiDB topology (address + status_port) from etcd; TTL indicates liveness. Online TiDB list is used for conprof.

**Command**:

```bash
ETCDCTL_API=3 etcdctl --endpoints=https://db-pd:2379 \
  --cacert=/etc/vector/tikv-tls/ca.crt \
  --cert=/etc/vector/tikv-tls/tls.crt \
  --key=/etc/vector/tikv-tls/tls.key \
  get --prefix "/topology/tidb/"
```

**Code**: `src/sources/conprof/topology/fetch/tidb.rs`  
- Prefix: `"/topology/tidb/"`  
- Request: etcd `get(key_prefix, WithPrefix)`

**etcd keys**: `{prefix}{address}/ttl` (liveness), `{prefix}{address}/info` (JSON with `status_port`).

**Usage**: `get_up_tidbs` fetches all KVs under prefix; parses TTL and Info; keeps alive addresses; builds `Component { instance_type: TiDB, host, primary_port, secondary_port }` from Info.

---

## 5. etcd TiProxy Topology: /topology/tiproxy/

**Purpose**: Same as TiDB; reads TiProxy address and status_port from etcd; TTL for liveness.

**Command**:

```bash
ETCDCTL_API=3 etcdctl --endpoints=https://db-pd:2379 \
  --cacert=/etc/vector/tikv-tls/ca.crt \
  --cert=/etc/vector/tikv-tls/tls.crt \
  --key=/etc/vector/tikv-tls/tls.key \
  get --prefix "/topology/tiproxy/"
```

**Code**: `src/sources/conprof/topology/fetch/tiproxy.rs`  
- Prefix: `"/topology/tiproxy/"`  
- Request: etcd `get(key_prefix, WithPrefix)`

**Usage**: Same logic as TiDB; TTL for liveness; Info for address and status_port; creates `Component { instance_type: TiProxy, ... }`.

---

## Summary (Config / Code Mapping)

| # | API | Command/Path | Code use |
|---|-----|--------------|----------|
| 1 | PD Health | `GET https://db-pd:2379/pd/api/v1/health` | Healthy member_id set; filter PD Members |
| 2 | PD Members | `GET https://db-pd:2379/pd/api/v1/members` | Healthy members' client_urls[0]; build PD Component |
| 3 | PD Stores | `GET https://db-pd:2379/pd/api/v1/stores` | state_name==up stores; address/status_address/labels → TiKV/TiFlash Component |
| 4 | etcd TiDB | `get --prefix /topology/tidb/` | TTL + info → alive TiDB address and status_port; TiDB Component |
| 5 | etcd TiProxy | `get --prefix /topology/tiproxy/` | Same; TiProxy Component |

`db-pd:2379` and the three cert paths are the fixed deployment config matching `pd_address` and `tls.ca_file/crt_file/key_file` in Vector.
