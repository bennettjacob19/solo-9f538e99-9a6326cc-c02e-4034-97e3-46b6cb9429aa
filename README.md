# 跨机房镜像层分发演练

一个 FastAPI 应用，用网页复现"多台构建机同时拉同一个镜像层"的跨机房分发：
**只发起一次 HTTP 主下载**，快、慢两路同时消费同一份响应；慢端收到约 3/4 后暂停，
网关为它留存正文；快端收齐后慢端续传，缺的尾巴再用条件 Range 回源补齐。
最后两路都按**主下载当时那一版**的 SHA-256 校验。

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
```

- 网页与 API：<http://localhost:8000/>
- 源站（独立服务）：<http://localhost:8001/origin/state>

compose 把源站（`app.origin_app`）和网关（`app.gateway_app`）拆成两个容器，
网关通过 `DRILL_ORIGIN_BASE=http://origin:8001` 回源，源站健康检查通过后网关才启动。

### 本地单进程（源站与网页同一进程）

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

打开 <http://localhost:8000/> 即可。

### 本地双进程（与 compose 拓扑一致）

```bash
DRILL_LAYER_BYTES=8388608 uvicorn app.origin_app:app --port 8001 &
DRILL_ORIGIN_BASE=http://127.0.0.1:8001 uvicorn app.gateway_app:app --port 8000
```

## 演练流程

1. 每次开始演练先把源站重置为 `v1`（长度 = `size_bytes`），并记录基线。
2. 网关向源站发出**唯一一次**主下载 `GET /origin/layer`，响应带强 `ETag`
   （形如 `"v1-<sha256>"`，无 `W/` 前缀）、`Content-Length` 和
   `X-Content-SHA256`。
3. 同一份字节流同时喂给快、慢两个接收端（各自独立计算 SHA-256）。
4. 慢端到达 `pause_fraction`（默认 3/4）后暂停，之后网关把剩余正文尽量放进
   留存缓冲区：先占内存到 `retention_memory_bytes`，再溢写到匿名临时文件，
   两者合计不超过 `retention_bytes`；超出部分直接丢弃。
5. 快端收齐后，慢端从留存缓冲区续传。仍缺的尾段先做**取段前唯一一次版本查询**
   （`GET /origin/state`，每轮最多一次、且只在取段前），再向源站发**每轮唯一一次**
   `Range: bytes=<start>-<end>` + `If-Range: <主下载的强 ETag>` 条件请求，并带
   `X-Backfill-Cap: <本轮实发上限>` 声明本轮硬额度。只有同时满足以下条件才接收补段：
   - 状态码 `206`（`If-Range` 失效时源站按 RFC 9110 返回 `200` 新版本）；
   - 响应 `ETag` 是强校验器且与主下载完全一致；
   - `Content-Range` 的起止与总长同请求及主下载一致；
   - 响应 `X-Content-SHA256` 等于主下载那一版的摘要。
   任一不符则**拒绝补段**，慢端只报缺段，绝不混版本。

### 回源限额怎么算（修复点）

- **本轮补段源站实发的镜像正文不得超过上限**：上限 = `min(还缺字节, 回源额度余量)`。
  升级后的源站识别 `X-Backfill-Cap`，即使 `If-Range` 失效回整层 `200`，也只产出
  不超过该上限的正文（此时改用分块传输，不再声明会被截断的 `Content-Length`）；
  网关自己也按同一上限硬截断读取并主动断开，双保险。
- **被拒收的正文、断开前已发的部分都计入额度**：回源额度按"源站实发"而不是
  "慢端实收"计费。被拒的新版正文网关排空到上限即断开，一个字节也不喂给慢端，
  但源站计数器里这些字节照常计入。
- **结算**：源站每个响应带 `X-Request-Id`，网关断开后轮询 `/origin/state` 里该请求
  的最终 `bytes_sent`。源站还在发送（记录 `in_flight`）或最终计数暂时查不到时，
  网页的"源站为补段实发字节"和"额度余量"显示**待结算**；拿到最终计数后自动更新。
  未升级源站没有 `X-Request-Id` 时，网关退化为按最新一条 `range` 记录结算；它也
  不认 `X-Backfill-Cap`，此时由网关保证自身读取不超上限，源站诚实上报它实际产出
  的字节并照常计入额度。
6. 总额度判断：慢端暂停时的尾段长度若不超过 `留存额度 + 回源额度`，源站未换版时
   两路必须收齐；额度不足或版本已换成新版导致旧版补不齐时，只有慢端报缺段
   （`outcome = slow_missing`，`missing_segments` 精确到字节区间）。取段前就查到
   已换版时也不会把新版混入：补段仍会被 ETag/状态码校验拒绝，慢端只报缺段。

网页实时显示：

- 两路**实收字节**与 SHA-256 校验结果（通过 / 未收齐 / 不匹配）；
- 慢端暂停点、续传状态、缺段区间；
- 取段前版本查询结果（源站版本、ETag 是否与主下载一致、查询次数）；
- 补段的 ETag / 区间校验结果、本轮源站实发上限与结算状态；
- **源站为补段实发字节**（来自源站最终计数器，待结算时显示"待结算…"）与
  **慢端实际收到的补段字节**（被拒收时为 0），两者分开显示；
- **补段额度余量（按源站实发计算）**、**源站累计实发字节**（主下载 + 补段）；
- **网关暂存峰值**（总峰值及内存 / 磁盘各自峰值）；
- 源站是否在暂停时被换成等长新版，以及主下载仍继续发原版的事实。

## 参数

| 参数 | 含义 |
| --- | --- |
| `size_bytes` | 层大小（1 KiB–128 MiB） |
| `chunk_size` / `chunk_delay_ms` | 主下载分块与节流，控制两路快慢节奏 |
| `retention_bytes` | 网关为慢端留存的正文额度（**内存 + 磁盘合计**） |
| `retention_memory_bytes` | 其中允许占用内存的上限，其余溢写磁盘 |
| `backfill_bytes` | 允许额外回源的正文额度 |
| `pause_fraction` | 慢端暂停点，默认 `0.75` |
| `auto_switch_on_pause` | 慢端暂停时把源站换成等长 `v2`（主下载仍发 `v1`） |

`retention_memory_bytes` 不得大于 `retention_bytes`，否则 422。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/api/drill` | 当前演练状态（含源站计数器、留存峰值、补段结算） |
| `POST` | `/api/drill` | 开始演练，body 同参数表 |
| `POST` | `/api/drill/reset` | 取消当前演练并重置源站 |
| `POST` | `/api/drill/switch?version=v2` | 手动换版（演练中会记录来源 `manual`） |
| `GET` | `/api/origin` | 源站状态与两个版本 |
| `GET` | `/origin/layer` | 源站正文（支持单段 Range / If-Range；响应带 `X-Request-Id`，网关补段另带 `X-Backfill-Cap`） |
| `POST` | `/origin/switch` | 源站换版 |
| `POST` | `/origin/reset` | 源站重置（可带 `size_bytes`） |

### 与未升级源站 / 旧客户端的兼容

- **新网关 + 旧源站**：旧源站忽略未知的 `X-Backfill-Cap`、也不回 `X-Request-Id`。
  网关改用 `/origin/state` 中最新一条 `range` 记录结算，并由自身保证读取不超过
  本轮上限；旧源站诚实上报它实际产出的字节，照常计入额度。
- **旧客户端直连源站**：不带 `X-Backfill-Cap` 时源站行为完全不变——
  `If-Range` 命中回 `206`、失配按 RFC 9110 回带 `Content-Length` 的整层 `200`。

## 验证

```bash
pip install -r requirements-dev.txt
python -m pytest -q          # 单元 + 端到端（真实 HTTP 主下载/补段）
python scripts/selftest.py   # 双进程起 origin+gateway，跑 5 个场景并打印表格
```

`scripts/selftest.py` 覆盖：

1. 额度够补尾段 → 两路都收齐、两路 SHA-256 通过、源站只收到 1 次主下载；
2. 留存 + 回源额度都不足 → 快端收齐，**只有慢端报缺段**；
3. 暂停时换等长新版、尾段未全部留存 → 补段被拒（ETag 不符 / 源站回 200），
   被拒收正文不超过本轮实发上限且计入额度，慢端实收补段为 0、只报缺段，
   快端仍按原版校验通过；
4. 暂停时换版但尾段全在留存缓冲区 → 不回源，两路都按原版收齐；
5. 暂停中手动换版 → 记录 `manual`，仍然只有慢端受影响。

`pytest` 另外覆盖：取段前版本查询每轮最多一次、待结算→最终计数的结算流程、
未升级源站（无 `X-Request-Id` / 不认 `X-Backfill-Cap`）的退化结算、以及旧客户端
直连时 `If-Range` 的 `200/206` 语义保持不变。

## 已知限制

- 演练中的"快端/慢端"是同进程内的两个消费者，用来模拟同机房快机与远机房慢机；
  网络层的真实丢包/重传不在本次范围。
- 源站的版本切换与计数器都是进程内状态，重启即清零；多网关实例共享同一源站时
  计数器会累加，需要各自重置后观察。
- 补段按"一片连续尾段"处理；当前设计慢端只会缺尾部连续区间，够用但未实现任意
  多段 holes 的并行补齐。

## 自测记录（2026-11-24）

- `python -m pytest -q`：12 passed（留存缓冲区、等长双版本、端到端两路收齐、
  额度不足、暂停换版、If-Range 失效回 200、补段硬额度与拒收计费、待结算流程、
  未升级源站退化结算、旧客户端 If-Range 语义不变）。
- `python scripts/selftest.py`：5 个场景全部通过；每个补段轮次都校验
  `源站实发 ≤ 本轮上限`、`额度按实发结算`、被拒收时慢端实收补段为 0。
- 真实 HTTP 双进程下另验证：升级源站在 `If-Range` 失配回整层 `200` 时，仍按
  `X-Backfill-Cap` 精确停在上限（记录 `outcome=capped`）。
- 未执行项：本机无 Docker，`docker compose up --build` 未实际构建验证；compose 的
  YAML、健康检查与双服务拆分已核对，并以同拓扑的双进程方式手工跑通。
