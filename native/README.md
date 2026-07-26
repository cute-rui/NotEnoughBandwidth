# NEB native（远程 zstd 卸载）

NEB 远程压缩卸载特性的 Rust workspace：把游戏服务器的 zstd 压缩/解压计算，通过 UCX tag matching（RoCE/RDMA）卸载到一台独立的物理压缩服务器。完整架构、协议、SR-IOV/RoCE 环境搭建与部署说明见 [docs/rdma-offload.md](../docs/rdma-offload.md)。

## Workspace 布局

| crate | 说明 |
|---|---|
| `neb-offload-core` | 共享库：与传输无关的线缆协议（请求/响应头、HELLO、UCX tag 布局）与 zstd 流式上下文封装 |
| `ucx-ffi` | UCX UCP API 的最小安全封装：endpoint 建立（listen/connect/accept）与带超时的阻塞式 tag 收发 |
| `neb-zstd-server` | 压缩服务可执行文件：持有全部 per-连接 zstd 上下文，多 worker 线程处理压缩/解压请求 |
| `neb-zstd-client` | cdylib 客户端库（`libneb_zstd_client.so`）：供 Java mod 通过 FFM 同步调用 |

## 构建

仅支持装有 RDMA 软件栈（rdma-core ≥ 28 或 MLNX_OFED ≥ 5.0）与 UCX ≥ 1.10 的 Linux：

```bash
cd native
cargo build --release
```

产物在 `native/target/release/`：`neb-zstd-server`（部署到压缩服务器）与 `libneb_zstd_client.so`（部署到游戏服务器，路径写入 mod 配置 `remoteOffloadLibrary`）。详细依赖与排障见 [docs/rdma-offload.md](../docs/rdma-offload.md)。

## CI

`.github/workflows/native-ci.yml`：在 GitHub 容器内安装 UCX（`libucx-dev`），执行 `cargo fmt --check`、workspace 构建、单元测试，以及端到端测试（`NEB_E2E=1`，真实 UCX client↔server 往返）。由于 runner 没有 RDMA 网卡，e2e 强制 `UCX_TLS=tcp,self` 走 TCP 传输——协议与两个二进制全覆盖，RDMA 专属路径需在真实硬件上按 [docs/rdma-offload.md](../docs/rdma-offload.md) 手动验证。
