# conprof jeprof/jeheap 采集模式说明

## 背景

TiKV 使用 jemalloc 时，heap 数据通过 jeprof 兼容的接口暴露（如 `/debug/pprof/heap`）。conprof 支持两种采集方式，由配置项 `jeprof_fetch_mode` 选择。

## jeprof 脚本在 `--raw` + 远程 URL 下实际做了什么

Perl 脚本 `jeprof --raw <url>` 在远程 URL 场景下**并不只是**发一次 HTTP GET，而是：

1. **GET 拉取 profile**  
   用 `URL_FETCHER`（默认 `curl -s --fail`）请求 URL，将响应写入临时文件 `$collected_profile`。

2. **解析 profile 得到 PC 列表**  
   `ReadProfile` 读取该文件，解析 heap 格式（如 `heap profile: ...` 头、栈记录等），得到所有出现过的程序计数器地址集合 `$pcs`。

3. **向服务端拉取符号**  
   `FetchSymbols($pcs)`：将 PC 列表通过 **POST** 发给同 host 的 `/pprof/symbol`，拿到地址→符号名映射；必要时还通过 `c++filt` 做 demangle。

4. **可选：拉取程序名**  
   `FetchProgramName()`：GET `/pprof/cmdline` 得到 binary 名。

5. **输出 “symbolized raw” 格式**  
   `PrintSymbolizedProfile` 输出到 stdout 的内容是：
   - 一行 `--- symbol`
   - 一行 `binary=<program name>`
   - 多行符号表：`0x<addr> <symbol>`
   - 一行 `---`
   - 一行 `--- heap`（或 growth/contention/cpu）
   - **紧接着**把 `$collected_profile` 文件的**原始内容**原样输出（即 GET 得到的 body）

也就是说，**Perl 模式的 stdout = 符号头 + 原始 heap body**，是一份可以离线用 `jeprof --text` 分析、且不再依赖当时进程的“自包含”格式。

## 两种配置模式对比

| 项目           | `jeprof_fetch_mode = "perl"`（默认） | `jeprof_fetch_mode = "rust"`      |
|----------------|--------------------------------------|-----------------------------------|
| 实现           | 起 Perl 进程执行 jeprof 脚本         | 本进程内 Rust：GET heap → 解析 PC → POST symbol → 拼输出 |
| 依赖           | 需要系统有 Perl、curl（TLS 时用你配的 curl） | 仅 Rust/reqwest，无 Perl         |
| 输出内容       | **符号头 + 原始 heap**               | **符号头 + 原始 heap**（与 Perl 一致） |
| 与 jeprof 兼容 | 与 `jeprof --raw` 输出一致           | 与 `jeprof --raw` 输出一致        |
| 离线分析       | 存下来的 blob 可直接 `jeprof --text` | 同上                             |

## 何时用哪种模式

- **用 `perl`**：  
  需要和现有 jeprof 流程完全一致、或下游会把采到的数据存起来以后用 `jeprof --text` 等做离线分析（且希望不再依赖当时进程），或当前 Rust 实现有 bug 需要快速回退。

- **用 `rust`**：  
  不打算依赖 Perl、只做采集与归档，且下游不依赖“带符号头的 jeprof --raw”格式；或后续会在别处做符号解析/展示。

## Rust 模式实现说明

Rust 模式（`jeprof_fetch_mode = "rust"`）已实现与 Perl 等价的流程：

1. GET `/debug/pprof/heap`，得到 body。
2. 解析 heap 文本格式，提取所有 PC；对除第一个外的地址做 FixCallerAddresses（减 1）。
3. POST 这些 PC（`0xaddr1+0xaddr2+...`）到同 base URL 的 `/debug/pprof/symbol`，解析响应得到符号表。
4. GET `/debug/pprof/cmdline` 得到程序名。
5. 按 jeprof 约定拼出：`--- symbol`、`binary=...`、符号行、`---`、`--- heap`、再拼上原始 body。

若 heap 为二进制或解析不到 PC，或 symbol 请求失败，则回退为只返回原始 body（与仅 GET 等价）。
