# Mooncache

Mooncache 是一个面向大规模并行 LLM API 访问的分布式 API Response KVCache。它不是模型内部的 transformer KV tensor cache，而是一个参考 Mooncake 架构思想裁剪出来的分布式响应对象缓存：用户请求先进入 Gateway，Gateway 对请求做严格指纹计算并查询缓存；命中时直接返回缓存响应，未命中时调用后端供应商 API，并在完整成功响应后写回缓存。

当前版本是第一阶段可运行骨架，重点验证核心数据流、分层边界、测试覆盖和本地开发拓扑。生产化还需要接入真实供应商配置、真实鉴权、多副本 Master/Store 部署、etcd 持久化状态和 SSD 二级缓存完整实现。

## 目标

- 为 OpenAI Responses API 兼容请求提供分布式缓存前置层。
- 在严格相同、确定性的请求重复出现时低延迟返回缓存响应。
- 减少供应商 API 调用次数、成本和尾延迟。
- 支持 Gateway、Store Node 和 Master 横向扩展。
- 优先使用 DRAM 保存热对象，后续使用 SSD 作为冷对象二级缓存。
- 同时支持 streaming 和 non-streaming 响应。
- 对相同 fingerprint 的并发 miss 使用 singleflight 合并，只让一个 leader 调供应商 API。
- 提供运维控制台，用于观察健康状态、容量、租户、节点、供应商、缓存对象、告警和审计。

## 非目标

- 不存储 transformer 原始 KV tensor。
- 不负责 prefill / decode 调度。
- 不管理 GPU / VRAM。
- 第一阶段不依赖 RDMA 或 Mooncake Transfer Engine。
- 不做语义相似命中。
- 不做 prompt prefix 复用。
- 第一阶段不做多 Region active-active。
- 不缓存失败、未完成、中断或不安全的供应商响应。

## 架构概览

```mermaid
flowchart LR
  Client[Client SDK / App] --> Gateway[API Gateway Cluster]
  Gateway -->|metadata RPC| Master[Master Leader]
  Master <--> Etcd[(etcd)]
  Master --> Standby[Standby Masters]
  Gateway -->|read/write chunks| StoreA[Store Node A\nDRAM + SSD]
  Gateway -->|read/write chunks| StoreB[Store Node B\nDRAM + SSD]
  Gateway -->|miss| Vendor[Vendor API]
  Control[Control Panel] --> Admin[Admin API]
  Admin --> Master
  Admin --> Gateway
  Admin --> StoreA
  Admin --> StoreB
  Metrics[(Metrics / Logs / Traces)] <-- Gateway
  Metrics <-- Master
  Metrics <-- StoreA
  Metrics <-- StoreB
```

### Gateway

Gateway 是请求入口。职责包括：

- 暴露 OpenAI Responses API 兼容接口。
- 鉴权并识别 tenant。
- 对完整请求体和缓存相关 header 做规范化。
- 计算严格 cache fingerprint。
- 判断请求是否可缓存。
- 处理 cache hit / miss / bypass / cache-only。
- 对相同 fingerprint 的并发 miss 做 singleflight 合并。
- 未命中时调用供应商 API。
- 对完整成功响应执行缓存写回。
- 对 streaming 响应捕获 SSE event 序列和最终聚合 JSON。
- 输出 cache 决策 header、metrics、trace 和日志。

### Master

Master 是控制平面，不代理响应字节。职责包括：

- 管理 tenant 级对象元数据。
- 管理 Store 节点和 segment 容量。
- 分配 DRAM / SSD chunk。
- 维护 replica 位置和对象生命周期状态。
- 发放和刷新读 lease。
- 执行 tenant quota。
- 触发 eviction。
- 管理 soft pin / hard pin。
- 处理节点 drain / failure 元数据。
- 后续通过 etcd 支撑 HA 状态。

### Store Node

Store Node 是数据平面。职责包括：

- 管理本地 DRAM segment。
- 后续管理本地 SSD cache 目录或设备。
- 提供对象 chunk 读写。
- 新对象优先写入 DRAM。
- 后续将冷对象异步落到 SSD。
- SSD hit 后尽量提升回 DRAM。
- 上报 heartbeat、容量、压力和磁盘指标。

### Admin API 与 Control Panel

Admin API 负责运维操作，Control Panel 是 React + TypeScript + Vite 实现的管理控制台。

当前角色模型：

| 角色 | 权限 |
| --- | --- |
| Viewer | 查看健康状态、metrics、cache stats、节点状态、audit log、tenant 摘要 |
| Operator | Viewer 权限 + 节点 drain、手动 warmup、cache remove、告警确认 |
| Admin | Operator 权限 + tenant policy、vendor config、quota、RBAC 管理 |

## 核心缓存策略

### Cache primitive

本项目中的 “KVCache” 指 API response object cache，不是模型运行时内部的 key/value tensor cache。

缓存对象包括：

- non-streaming 的完整 JSON 响应；
- streaming 的原始 SSE event 序列；
- streaming 完成后聚合出的最终 JSON 响应。

### Cache key

cache key 使用严格 fingerprint，核心输入包括：

- tenant；
- vendor；
- resolved model version；
- endpoint / schema / adapter version；
- 完整规范化请求体；
- cache-relevant headers / policy。

默认只自动缓存确定性请求。随机请求默认 bypass，除非调用方显式要求 exact replay。

### 写入规则

只提交完整成功的供应商响应：

1. Gateway 调用 Master `PutStart` 预留对象位置。
2. Gateway 将响应对象写入 Store。
3. Gateway 调用 Master `PutEnd`，对象才对读路径可见。
4. 如果供应商失败、stream 中断、客户端取消或写入失败，对象不会变成 committed。

### 失败策略

默认 fail-open：缓存层故障时尽量回退到供应商 API。例外：

- auth 失败；
- quota 冲突；
- 显式 cache-only 请求；
- idempotency 冲突；
- 明确不能安全回退的状态。

## 请求流程

### Non-streaming hit

1. Client 调用 `POST /v1/responses`。
2. Gateway 鉴权并计算 fingerprint。
3. Gateway 向 Master 查询 committed replicas。
4. Master 返回 lease 和可读 replica 位置。
5. Gateway 从 Store 读取完整 JSON 响应。
6. Gateway 返回缓存响应和 cache decision headers。

### Non-streaming miss

1. Gateway 计算 fingerprint。
2. Gateway 检查 singleflight。
3. 第一个请求成为 leader，其他相同 fingerprint 请求等待。
4. Leader 调用供应商 API。
5. Gateway 返回供应商响应。
6. 响应完整且可缓存时写回 Store，并通过 Master commit。
7. 等待者复用 leader 的最终结果。

### Streaming hit

1. Client 以 streaming 模式调用 `POST /v1/responses`。
2. Gateway 命中缓存对象。
3. Gateway 读取已存储的 SSE event 序列。
4. Gateway 按 OpenAI Responses-compatible SSE schema replay。
5. replay 可以比原始供应商流更快，但必须保持 event order、event type、usage metadata 和 terminal event 语义。

### Streaming miss

1. Gateway 成为 leader 或加入已有 in-flight stream。
2. Leader 调用供应商 API 并转发 SSE 给调用方。
3. Gateway 捕获原始 SSE event，同时构建最终聚合 JSON。
4. stream 完整成功后提交两份 artifact：
   - 原始 SSE event 序列，用于未来 streaming hit；
   - 最终聚合 JSON，用于未来 non-streaming hit。
5. 如果 stream 失败、中断或不符合缓存规则，不提交缓存对象。

## 项目结构

```text
.
├── Cargo.toml
├── docker-compose.yml
├── apps/
│   ├── gateway/
│   ├── master/
│   ├── store-node/
│   └── admin-api/
├── crates/
│   ├── common/
│   ├── protocol/
│   ├── fingerprint/
│   ├── master/
│   ├── store/
│   ├── gateway/
│   └── admin-api/
├── control-panel/
│   ├── package.json
│   ├── index.html
│   └── src/
├── tests/
│   └── integration/
└── docs/
    └── superpowers/
```

## Rust workspace

Workspace members：

| Crate / App | 作用 |
| --- | --- |
| `mooncache-common` | 通用 ID、错误、metrics 类型 |
| `mooncache-protocol` | Responses API、cache header、admin protocol 类型 |
| `mooncache-fingerprint` | canonical JSON、请求分类、cache key 计算 |
| `mooncache-master` | metadata、quota、lease、allocation、eviction |
| `mooncache-store` | DRAM chunk store、SSD store 边界、checksum |
| `mooncache-gateway` | cache flow、singleflight、vendor adapter、streaming replay |
| `mooncache-admin-api` | RBAC、audit、admin service |
| `mooncache-gateway-app` | 本地 Gateway HTTP 服务 |
| `mooncache-master-app` | 本地 Master HTTP 服务 |
| `mooncache-store-node-app` | 本地 Store Node HTTP 服务 |
| `mooncache-admin-api-app` | 本地 Admin API HTTP 服务 |

## 快速开始

### 前置要求

- Rust stable
- Cargo
- Node.js / npm
- Docker CLI 可选，仅用于 docker-compose 本地拓扑

### 运行 Rust 测试

```bash
cargo test --workspace
```

### 运行 Rust lint

```bash
cargo clippy --workspace --all-targets -- -D warnings
```

### 格式检查

```bash
cargo fmt --check
```

### 运行 Gateway 本地服务

```bash
cargo run -p mooncache-gateway-app -- --bind-addr=127.0.0.1:8080
```

健康检查：

```bash
curl http://127.0.0.1:8080/healthz
```

本地 Responses API 示例：

```bash
curl -s http://127.0.0.1:8080/v1/responses \
  -H 'authorization: Bearer test-api-key' \
  -H 'content-type: application/json' \
  -d '{"model":"gpt-test","input":"hello"}'
```

开发态 Gateway 会返回 mock vendor 响应，并对可缓存请求走本地 MemoryStore 缓存路径。

### 运行其他本地服务

```bash
cargo run -p mooncache-master-app -- --bind-addr=127.0.0.1:8081
cargo run -p mooncache-store-node-app -- --bind-addr=127.0.0.1:8082
cargo run -p mooncache-admin-api-app -- --bind-addr=127.0.0.1:8083
```

各服务都支持 `--help` 查看参数：

```bash
cargo run -p mooncache-gateway-app -- --help
cargo run -p mooncache-master-app -- --help
cargo run -p mooncache-store-node-app -- --help
cargo run -p mooncache-admin-api-app -- --help
```

## Control Panel

进入前端目录：

```bash
cd control-panel
```

安装依赖：

```bash
npm install
```

运行测试：

```bash
npm test
```

生产构建：

```bash
npm run build
```

开发服务：

```bash
npm run dev
```

Control Panel 页面包括：

- Overview
- Cache Analytics
- Nodes
- Tenants
- Vendors
- Cache Operations
- Audit Log
- Alerts

## Docker Compose 本地拓扑

仓库包含 `docker-compose.yml`，定义：

- etcd
- master
- store-node
- gateway
- admin-api
- control-panel

构建前端静态资源：

```bash
cd control-panel
npm install
npm run build
cd ..
```

启动拓扑：

```bash
docker compose up
```

> 注意：当前环境中 Docker CLI 未完成验证。compose 文件是本地开发拓扑草案，真实运行前需要在有 Docker 的环境里执行 `docker compose config` 和 `docker compose up` 验证。

## HTTP 端口

默认端口：

| 服务 | API 端口 | Metrics 端口 |
| --- | ---: | ---: |
| Gateway | `8080` | `9090` |
| Master | `8081` | `9091` |
| Store Node | `8082` | `9092` |
| Admin API | `8083` | `9093` |
| Control Panel | `3000` | - |

## 当前实现状态

已实现：

- Rust workspace 基础结构。
- 共享 domain types。
- Responses protocol 和 cache headers。
- Fingerprint / canonical JSON / eligibility。
- Master metadata、quota、lease、allocation、eviction 基础能力。
- MemoryStore chunk 读写。
- Gateway non-streaming cache flow。
- Gateway streaming capture / replay 基础能力。
- singleflight 合并并发 miss。
- Admin API RBAC / audit / metrics / cache debug。
- React control panel 基础页面和测试。
- 本地 Axum app binaries。
- docker-compose 本地拓扑草案。

开发态限制：

- Gateway app 当前使用 `GatewayState::new_for_test`、`MemoryStore` 和 `MockVendorAdapter`。
- 本地鉴权使用 `Bearer test-api-key`。
- Master app / Store app / Admin app 是本地可运行 HTTP 骨架，不是完整生产集群控制面。
- SSD 二级缓存已有边界和设计方向，但生产级冷热迁移仍需后续完善。
- etcd HA 状态持久化仍需生产化接入。
- 真实 vendor adapter、tenant config 和 API key 管理仍需后续实现。

## 验证命令

建议提交前运行：

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cd control-panel && npm test && npm run build
```

当前已验证结果：

- `cargo fmt --check`：通过
- `cargo clippy --workspace --all-targets -- -D warnings`：通过
- `cargo test --workspace`：`100 passed`, `23 suites`, `1 ignored`
- `cargo test -p mooncache-gateway-app`：`4 passed`
- `control-panel npm test`：`4 files`, `16 tests` 通过
- `control-panel npm run build`：通过

## 设计文档

详细 PRD 和设计见：

```text
docs/superpowers/specs/2026-07-03-distributed-api-response-kvcache-design.md
```

实施计划见：

```text
docs/superpowers/plans/2026-07-03-distributed-api-response-kvcache.md
```

## 安全说明

- 不要提交 `.env`、真实 API key、供应商 token 或生产凭证。
- 生产环境必须替换当前开发态 mock auth / mock vendor。
- Admin API 写操作需要真实认证、授权、审计和最小权限控制。
- Store Node 生产环境不应允许未授权直连读写。
- cache key、audit log 和 control panel 中展示的 fingerprint 应始终使用 redacted 形式。

## 后续路线

推荐下一阶段优先级：

1. 接入真实 vendor adapter 和 vendor config。
2. 接入真实 tenant config、API key 管理和认证中间件。
3. 将 Master 状态持久化到 etcd，并支持 leader / standby。
4. 完成 SSD cold tier、DRAM promotion 和容量水位 eviction。
5. 将 Gateway / Master / Store 之间的本地调用替换为清晰的 RPC 边界。
6. 补齐 Prometheus metrics、structured tracing 和 dashboard。
7. 在真实 Docker / Kubernetes 环境做端到端部署验证。
8. 增加 Playwright E2E 覆盖 Control Panel 关键操作流。
