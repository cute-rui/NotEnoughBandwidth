# 远程 zstd 卸载（UCX/RoCE RDMA）

本文档介绍 NEB 的远程压缩卸载特性：把服务器出站压缩/入站解压的 zstd 计算，从游戏服务器进程转移到一台独立的物理压缩服务器上执行，两者之间通过 UCX tag matching 在 RoCE（RDMA over Converged Ethernet）网络上通信。

> [!WARNING]
> 这是一个面向大型 modded 服务器的高端特性，需要 RoCE 网卡、已配置的无损（或 ECN）以太网 fabric、以及一台空闲的物理机。它**不会降低压缩延迟**（反而略增），买的是游戏服务器 CPU 与 Netty event loop 的解放。普通服务器请直接使用默认的本地压缩。

## 目录

- [架构总览](#架构总览)
- [线缆协议与 UCX tag 布局](#线缆协议与-ucx-tag-布局)
- [构建指南（Linux）](#构建指南linux)
- [SR-IOV + RoCE 环境搭建](#sr-iov--roce-环境搭建)
- [部署与配置](#部署与配置)
- [故障语义与回退策略](#故障语义与回退策略)
- [限制](#限制)
- [基准测试方法](#基准测试方法)

## 架构总览

NEB 本地压缩的链路是：`Connection.send` 被拦截 → 聚合 20ms → 在 Netty event loop 上用 zstd-jni 压缩。远程卸载把其中的 zstd 计算替换为一次到远程服务的同步往返：

- **客户端库（`libneb_zstd_client.so`）**：Rust cdylib，通过 Java FFM API 被 mod 调用。它维护一个到压缩服务器的 UCX endpoint 池，把每条游戏连接固定（pin）到一个 endpoint 上，保证同连接内严格保序。
- **压缩服务器（`neb-zstd-server`）**：独立物理机上运行的 Rust 服务，持有全部 per-连接 zstd 流式上下文（CCtx/DCtx），参数（level / windowLog / magicless）与 mod 端完全一致，因此产生的帧格式与本地压缩**逐字节兼容**，对玩家客户端完全透明。
- **传输**：UCX tag matching，RoCEv2 网络上走 `rc_verbs`/`rc_mlx5` 传输，内核旁路、零拷贝。

```text
  游戏服务器（可以是 SR-IOV 直通 VF 的 VM）                压缩服务器（物理机）
 ┌───────────────────────────────────────────┐         ┌──────────────────────────────┐
 │  Minecraft Server (NeoForge)              │         │  neb-zstd-server             │
 │  ┌─────────────────────────────────────┐  │         │  ┌────────────────────────┐  │
 │  │ NEB mod                             │  │         │  │ accept 线程            │  │
 │  │   Connection.send 拦截              │  │         │  │  worker 线程 × N       │  │
 │  │   聚合 20ms → flush                 │  │         │  │    per-conn zstd 上下文 │  │
 │  │   ┌───────────────────────────┐     │  │  UCX    │  │    (CCtx/DCtx, 保序)   │  │
 │  │   │ RemoteOffloadManager      │─────┼──┼─tag ───▶│  │    op=compress/        │  │
 │  │   │  FFM downcall（同步）     │◀────┼──┼─match── │  │    decompress/oneshot  │  │
 │  │   └───────────────────────────┘     │  │  over   │  │  GC: 空闲 gc-secs 回收 │  │
 │  │   本地 zstd-jni 兜底（fallback）    │  │  RoCE   │  └────────────────────────┘  │
 │  └─────────────────────────────────────┘  │         └──────────────────────────────┘
 │  libneb_zstd_client.so (Rust cdylib)      │              ▲ 解压仍在两端本地进行：
 └───────────────────────────────────────────┘              │ 玩家客户端无法卸载，
                conn_id % slots → endpoint 池               │ 只有服务端出站压缩与
                                                           │ 服务端入站解压可卸载。
```

要点：

- **只有游戏服务器端受益**。玩家客户端的解压与压缩永远在玩家本地。
- 线缆格式（NEB 聚合包的 zstd 帧）与本地压缩完全一致：magicless 帧、level 3、windowLog 21–25（`contextLevel`，默认 23）、每条消息 flush。老客户端无感知。
- 远程卸载是**同步**调用：一次压缩 = 一次请求-响应往返。收益是把压缩移出游戏服务器 CPU 和 event loop，代价是约 +5%–25% 的单次压缩延迟（详见[限制](#限制)）。

## 线缆协议与 UCX tag 布局

### 请求 / 响应消息

协议与传输层无关，路由信息（连接归属、响应路由）全部由 UCX tag 携带，消息体只承载操作与数据。

请求 = 12 字节头 + payload：

```text
┌---------┬---------┬-----------┬----------------┬----------------┬===========···
│ op: u8  │ rsv: u8 │ rsv: u16  │ raw_size: u32  │ len: u32       │ payload
└---------┴---------┴-----------┴----------------┴----------------┴===========···
```

- `raw_size` 仅 `OP_DECOMPRESS` 使用（期望解压大小，取自 NEB 聚合线缆格式中的 S varint）。

响应 = 8 字节头 + payload：

```text
┌-------------┬----------------┬===========···
│ status: i32 │ len: u32       │ payload
└-------------┴----------------┴===========···
```

### 操作码

| op | 名称 | 语义 |
|---|---|---|
| 0 | `OP_HELLO` | 服务端在 accept 新 endpoint 后主动宣告（非请求） |
| 1 | `OP_COMPRESS` | 在 per-连接上下文上做有状态流式压缩（每次调用 flush） |
| 2 | `OP_DECOMPRESS` | 在 per-连接上下文上做有状态流式解压 |
| 3 | `OP_COMPRESS_ONESHOT` | 无状态一次性压缩（完整帧），用于关闭上下文复用的连接（对应 mod 配置 `playersDoNotUseContext`） |
| 4 | `OP_RESET` | 丢弃该连接在服务端的全部上下文（连接关闭时调用） |

### 状态码

| status | 名称 | 含义 |
|---|---|---|
| 0 | `STATUS_OK` | 成功 |
| -1 | `STATUS_BAD_REQUEST` | 请求非法（未知 op、头损坏等） |
| -2 | `STATUS_ZSTD_ERROR` | zstd 库错误 |
| -3 | `STATUS_PARAM_MISMATCH` | HELLO 参数与客户端配置不一致 |
| -4 | `STATUS_MESSAGE_TOO_LARGE` | 请求 payload 超过 `max_payload` |
| -5 | `STATUS_INTERNAL_ERROR` | 服务端内部错误 |

### UCX tag 布局

64 位 tag 承载全部路由信息：

```text
┌-------------┬--------------┬--------┬----------------┐
│ ep_id: 16   │ reserved: 15 │ resp:1 │ conn_id: 32    │
└-------------┴--------------┴--------┴----------------┘
 bit 63..48      bit 47..33     bit 32   bit 31..0
```

- `ep_id`：服务端在 accept 每个 endpoint 时分配，并通过 HELLO 消息告知客户端。
- `resp`：响应标志位（bit 32）。请求 tag 置 0，响应 tag = 请求 tag 置 1。
- `conn_id`：游戏连接 id。

工作流程：客户端连接到服务端后，先等待接收 HELLO 消息（tag 掩码 `HELLO_MASK = resp位 | conn_id全1`，匹配保留 conn_id `0xFFFFFFFF` 的服务端宣告）。HELLO payload 为 16 字节：magic `"NEB1"`（4B）+ `level:u8` + `window_log:u8` + flags（`magicless`，1B）+ 保留 1B + `max_payload:u32`。客户端**必须先校验 HELLO 参数与自身配置一致**，否则两侧产生的 zstd 帧不兼容（此时握手失败，见[故障语义](#故障语义与回退策略)）。

此后客户端把每条游戏连接固定到一个 endpoint（`conn_id % endpoint池大小`）：同连接的所有请求落在同一个服务端 worker 上，上下文保持在那个 worker 本地，天然保序、无需跨 worker 同步。

## 构建指南（Linux）

> [!NOTE]
> 只能在装有 RDMA 软件栈的 Linux 上构建。Windows/macOS 无法链接 UCX。

### 依赖

- Rust 工具链（stable，edition 2021）
- RDMA 栈二选一：
  - 发行版 **rdma-core** ≥ 28（`rdma-core-devel` / `libibverbs-dev`、`librdmacm-dev`）
  - 或 **MLNX_OFED** ≥ 5.0（Mellanox 网卡推荐）
- **UCX** ≥ 1.10（需要 `ucp_tag_send_nbx`/`ucp_tag_recv_nbx` API）：
  - 发行版包（如 `ucx-devel`），或
  - 源码构建：
    ```bash
    git clone https://github.com/openucx/ucx.git && cd ucx
    ./autogen.sh
    ./contrib/configure-release --prefix=/opt/ucx --with-verbs --with-rdmacm
    make -j$(nproc) && sudo make install
    ```

### 构建

```bash
cd native
# 若 UCX 装在非标准路径，先让链接器/运行时找到它：
export PKG_CONFIG_PATH=/opt/ucx/lib/pkgconfig:$PKG_CONFIG_PATH   # 如有 .pc
export LD_LIBRARY_PATH=/opt/ucx/lib:$LD_LIBRARY_PATH

cargo build --release
```

产物（`native/target/release/`）：

| 产物 | 用途 | 部署位置 |
|---|---|---|
| `libneb_zstd_client.so` | FFM 客户端库 | 游戏服务器，路径写入 mod 配置 `remoteOffloadLibrary` |
| `neb-zstd-server` | 压缩服务可执行文件 | 压缩服务器 |

## SR-IOV + RoCE 环境搭建

最重的运维环节。典型拓扑：游戏服务器是 KVM/Proxmox 虚拟机，通过 SR-IOV VF 直通获得 RDMA 能力；压缩服务器是物理机（可用 PF 或另一个 VF）。任何一环出错都表现为"不通"，请按顺序逐层验证。

### 1. BIOS

- 开启 **SR-IOV** 与 **IOMMU**（Intel VT-d / AMD-Vi）。

### 2. 网卡固件（Mellanox ConnectX 系列）

```bash
# 开启 SR-IOV 并设定 VF 数量
mst start
mlxconfig -d /dev/mst/<dev> set SRIOV_EN=1 NUM_OF_VFS=N
# 如需将端口设为以太网模式（RoCE 需要）：
mlxconfig -d /dev/mst/<dev> set LINK_TYPE_P1=2
# 修改后需冷重启（或固件 reset）生效
```

### 3. Host 上生成 VF

```bash
echo N > /sys/class/net/<pf>/device/sriov_numvfs
```

要持久化请写入 systemd unit 或 udev 规则。

### 4. VF 直通给虚拟机

- 在 KVM/Proxmox 中把 VF 的 PCI 设备 passthrough 给游戏服务器 VM。
- VM 内安装 Mellanox 驱动（mlx5，内核自带或 MLNX_OFED）+ rdma-core 用户态。

### 5. RoCEv2 GID 验证

VM（或物理机）内：

```bash
show_gids        # 确认存在 RoCE v2 的 GID（通常 index 对应 IPv4）
ibv_devinfo      # 确认端口 state: PORT_ACTIVE
```

### 6. 交换机侧

RoCE 对丢包极敏感，二选一：

- **无损方案**：配置 PFC（Priority Flow Control），把 RoCE 流量划入无损优先级；
- **有损方案**：配置 ECN + ETS（DCQCN 拥塞控制）。

同时确保**端到端 MTU 一致**（建议 9000 jumbo，但全链路必须统一）。

### 7. 链路验证

```bash
# UCX 侧：确认 rc_verbs / rc_mlx5 传输可用
ucx_info -d | grep -E 'rc_verbs|rc_mlx5'

# 裸 verbs 侧：两端互跑 ib_write_bw 验证 RDMA 链路吞吐
#   服务端: ib_write_bw
#   客户端: ib_write_bw <server_ip>
```

`ucx_info -d` 找不到 RC 传输时，先回到第 5/6 步排查，不要在 UCX 之上调试。

## 部署与配置

### 启动压缩服务器

```bash
./neb-zstd-server \
    --listen 0.0.0.0:19999 \        # 数据面监听地址（默认值）
    --metrics-listen 0.0.0.0:9100 \ # 指标端点（默认值），off 可关闭
    --threads 16 \                  # worker 线程数（默认：全部可用核）
    --level 3 \                     # zstd 压缩级别（默认值）
    --window-log 23 \               # zstd windowLog（默认值）
    --max-payload 8388608 \         # 单请求最大 payload 字节数（默认值，8MB）
    --gc-secs 600                   # per-连接上下文空闲回收秒数（默认值）
```

### mod 配置对照

在 `config/NotEnoughBandwidthConfig.json` 中：

| mod 配置项 | 默认值 | 对应 server 参数 | 说明 |
|---|---|---|---|
| `remoteOffloadEnabled` | `false` | — | 总开关，默认关闭（纯本地压缩） |
| `remoteOffloadLibrary` | `""` | — | `libneb_zstd_client.so` 的绝对路径 |
| `remoteOffloadAddress` | `"127.0.0.1:19999"` | `--listen` | 压缩服务器地址，必须与 server 监听地址一致 |
| `remoteOffloadWorkers` | `8` | — | 客户端库到 server 的 endpoint 池大小（slots）；游戏连接按 `conn_id % slots` 固定到 endpoint |
| `contextLevel` | `23` | `--window-log` | **必须一致**，否则 HELLO 握手失败 |

> [!WARNING]
> **参数一致性是硬性要求**：server 的 `--level` 必须等于 mod 的压缩 level（3），`--window-log` 必须等于 mod 的 `contextLevel`。客户端收到 HELLO 后会逐项校验，不一致即握手失败（`STATUS_PARAM_MISMATCH`），该连接按故障语义处理。同理，`--max-payload` 不应小于服务器可能产生的最大聚合包。

## 监控指标（Metrics）

server 内置 Prometheus 文本格式的指标端点，默认 `http://<host>:9100/metrics`，可直接被 vmagent（VictoriaMetrics）或 Prometheus 抓取；`--metrics-listen off` 可关闭。端点是明文 HTTP 且无鉴权，请只对监控网段开放（防火墙/安全组限制）。

### 指标清单

| 指标 | 类型 | 标签 | 说明 |
|---|---|---|---|
| `neb_zstd_endpoints` | gauge | — | 存活的 UCX endpoint 数（客户端 slot 连接） |
| `neb_zstd_contexts` | gauge | — | 存活的 per-连接 zstd 上下文数 |
| `neb_zstd_connections_accepted_total` | counter | — | 接受并完成 HELLO 的连接数 |
| `neb_zstd_connections_dropped_total` | counter | `reason` = transport / hard_cap / wrap / hello | 断开（或未完成接受）的连接数 |
| `neb_zstd_contexts_evicted_total` | counter | `reason` = gc / endpoint | 上下文回收数（GC 过期 / 端点断开连带清理） |
| `neb_zstd_requests_total` | counter | `op` = compress / decompress / oneshot / reset / invalid × `status` = ok / bad_request / zstd_error / too_large | 请求处理数 |
| `neb_zstd_request_bytes_total` | counter | `op` × `direction` = in / out | 处理的 payload 字节数 |
| `neb_zstd_request_duration_seconds` | histogram | `op`（50µs–50ms 共 10 桶） | 实际 zstd 计算耗时（不含网络） |

### vmagent 抓取配置

vmagent 直接使用 Prometheus 格式的 scrape 配置（`-promscrape.config`）：

```yaml
scrape_configs:
  - job_name: neb-zstd-server
    scrape_interval: 15s
    static_configs:
      - targets: ["<压缩服务器IP>:9100"]
```

### 常用 MetricsQL

压缩率（越小越好，对照 README 的 7.6%~39%）：

```promql
sum(rate(neb_zstd_request_bytes_total{op="compress",direction="out"}[5m]))
  / sum(rate(neb_zstd_request_bytes_total{op="compress",direction="in"}[5m]))
```

压缩延迟 p99：

```promql
histogram_quantile(0.99, sum(rate(neb_zstd_request_duration_seconds_bucket{op="compress"}[5m])) by (le))
```

异常告警参考：`rate(neb_zstd_connections_dropped_total{reason="transport"}[5m]) > 0`（传输层不稳）、`neb_zstd_contexts` 持续增长（GC 失效或连接泄漏）。

## 故障语义与回退策略

压缩上下文在远端，任何远端故障（server 重启、网络中断、传输错误）都会使流式窗口历史丢失——两端立刻 desync，**无法就地恢复**（上下文流无法重新同步）。NEB 采用如下策略：

- **任意 native 层错误**（UCX 传输错误、超时、握手失败、非零 status）→ `RemoteOffloadManager.markBroken()`：
  - 卸载服务被标记为不可用，**此后所有新连接回退到本地 zstd-jni 压缩**（行为与未开启卸载完全一致）；
  - **出错的那条连接被断开并重连**——这是唯一安全的恢复方式，因为旧连接的流式上下文两端都已不可信。玩家表现为一次短暂掉线。
- **静默切换是禁止的**：绝不能在不重置上下文的情况下把一条连接从远程切到本地或反之。
- **server 侧上下文 GC**：per-连接上下文在空闲 `--gc-secs`（默认 600s）后自动回收；游戏连接正常关闭时 mod 会发送 `OP_RESET` 主动释放。
- **`kill -9` server 是安全的**：server 不持有任何磁盘状态，全部上下文都在内存中。杀掉后游戏侧表现为上述故障语义（连接重连 + 回退本地），重启 server 即可恢复服务。

## 限制

- **明文传输**：卸载链路上的游戏流量不加密，仅限可信内网 / 同机柜部署。
- **卸载范围**：只有服务端出站压缩与服务端入站解压可卸载；玩家客户端的解压（与压缩）永远在玩家本地。
- **延迟**：同步调用，单次压缩 ≈ 本地压缩耗时 + 5%–25%（大包时传输占比上升；1MB @25GbE 单向传输约 320µs，压缩回程只有原始大小的 7.6%–40%，更便宜）。20ms flush 节奏下约 +0.5–1ms，对 gameplay 无感，但**不要指望它降低延迟**。
- **内存转移而非消除**：per-连接上下文内存从游戏服务器挪到压缩服务器。100 玩家、windowLog=25 时约 3.2GB，请为压缩服务器准备足够内存。
- **平台**：仅 Linux；游戏服务器若是 VM，需要 SR-IOV VF 直通（或裸金属）。

## 基准测试方法

对比两组：**local**（默认本地 zstd-jni）与 **远程卸载**（本特性）。

- **负载矩阵**：聚合包大小 1KB、8KB、64KB、256KB、1MB、2MB × 连接数 1、100。
- **测量指标**：
  - 单次压缩/解压**延迟**（p50/p99，卸载组含网络往返）；
  - 端到端**吞吐**（MB/s）；
  - **游戏服务器 CPU 占用**（重点：Netty event loop 线程的 CPU 时间与阻塞时间）。验收目标：100 连接场景下游戏服务器压缩 CPU 下降 ≥ 70%。
- **压缩率回归**：卸载组与本地组的压缩率必须逐点一致（参数相同则帧格式相同）；线缆格式不变，可用既有统计（游戏内 `Alt+N`）交叉验证。
- **故障注入**：卸载组运行中 `kill -9` server，验证连接重连、新连接回退本地、无坏包；重启 server 后新连接恢复卸载。
