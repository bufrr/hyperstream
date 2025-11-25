# Hyperliquid 数据流水线指南

**状态**: ✅ 生产就绪 - 全部 6 个主题已验证
**数据源模式**: 文件模式（基于节点数据文件的批次格式）
**批次格式支持**: ✅ `_by_block` 文件的 Batch wrapper 解析
**已知限制**:
- ❌ **区块哈希不可用**: `replica_cmds` 文件不包含区块哈希，`hl.blocks.hash` 字段永远为空
- ❌ **节点文件限制**: `node_trades` 目录不存在（trades 从 fills 中提取）
- ✅ **交易哈希可用**: 所有其他主题（transactions、fills、orders、trades、misc_events）均包含哈希字段

---

## 概要总结

本指南记录了 Hyperliquid 区块链数据流式传输的完整流程，包含 6 个 Kafka 主题的数据采集、解析和验证。

### 哈希字段可用性总结

| 主题 | 哈希可用性 | 数据源 | 说明 |
|------|----------|--------|------|
| hl.blocks | ❌ **不可用** | replica_cmds 无区块哈希 | 永远为空字符串，需要 Explorer API 获取 |
| hl.transactions | ✅ **可用** | `signed_action_bundles[i][0]` | 区块链共识生成的官方交易哈希 |
| hl.fills | ✅ **可用** | node_fills_by_block | 交易哈希包含在 fill 数据中 |
| hl.orders | ✅ **可用** | node_order_statuses_by_block | 订单关联的交易哈希 |
| hl.trades | ✅ **可用** | 继承自 fills | 与 fill 相同的交易哈希 |
| hl.misc_events | ✅ **可用** | misc_events_by_block | 事件关联的交易哈希 |

**关键发现**:
- ✅ **5/6 主题有哈希**: transactions、fills、orders、trades、misc_events 均包含完整的交易哈希
- ❌ **仅区块哈希缺失**: 本地节点文件不包含区块哈希，这是唯一的限制
- 💡 **获取区块哈希**: 如需区块哈希，必须使用 Hyperliquid Explorer WebSocket API (`explorerBlock`)

### 关键特性

✅ **完整的主题覆盖**: 所有 6 个主题已实现并验证
✅ **批次格式支持**: `_by_block` 文件的 `{block_number, block_time, local_time, events: [...]}` 结构
✅ **可配置性能限制**: 资源限制可通过 config.toml 配置
✅ **智能文件选择**: 按修改时间排序（最新优先），确保监控活跃文件
✅ **skip_historical 模式**: 仅处理新数据，从文件末尾开始
✅ **MessagePack + JSONL 双格式支持**: blocks 用 MessagePack，其他用 JSONL
⚠️ **区块哈希留空**: 所有本地数据源均无此字段 - 仅 Explorer RPC 可获取

### 实现状态

| 主题 | 状态 | 数据源 | 批次格式 | 验证 |
|------|------|--------|---------|------|
| hl.blocks | ✅ 已实现 | replica_cmds | 每行一个区块 | ✅ 128+ 记录 |
| hl.transactions | ✅ 已实现 | replica_cmds | 每行一个区块 | ✅ 128+ 记录 |
| hl.fills | ✅ 已实现 | node_fills_by_block | Batch wrapper | ✅ 248+ 记录 |
| hl.orders | ✅ 已实现 | node_order_statuses_by_block | Batch wrapper | ✅ 162+ 记录 |
| hl.trades | ✅ 已实现 | node_fills_by_block (聚合) | Batch wrapper | ✅ 248+ 记录 |
| hl.misc_events | ✅ 已实现 | misc_events_by_block | Batch wrapper | ✅ 49+ 记录 |

### 性能特性

**可配置性能限制** (config.toml):
- `max_concurrent_tailers` (默认: 64) - 限制并发文件处理任务
- `skip_historical` (默认: false) - 从文件末尾开始，仅处理新数据

**验证结果** (2025-11-25, skip_historical=true):
- ✅ 全部 6 个主题成功发出记录
- ✅ 实时处理新数据（skip_historical=true 工作正常）
- ✅ 批次格式正确解析（block_number, block_time, events）
- ✅ INFO 级别日志显示所有主题

---

## 数据源概览

### 本地文件

| 文件 | 格式 | 批次结构 | 内容 |
|------|------|---------|------|
| replica_cmds | JSONL | 每行一个区块 | 区块元数据 + 交易和响应数据 |
| node_fills_by_block | JSONL | **Batch wrapper** | 持仓填充（带 block_number） |
| node_order_statuses_by_block | JSONL | **Batch wrapper** | 订单状态（带 block_number） |
| misc_events_by_block | JSONL | **Batch wrapper** | 系统事件（带 block_number） |
| ~~node_trades~~ | ❌ 不存在 | - | trades 从 fills 中提取 |

### 批次格式 (Batch Wrapper)

`_by_block` 文件使用批次封装格式：

```json
{
  "local_time": "2025-11-25T08:33:18.508962742",
  "block_time": "2025-11-25T08:33:18.111517886",
  "block_number": 807847463,
  "events": [
    // 实际事件数据数组
    {"user": "0x...", "coin": "ETH", "px": "2896.9", ...},
    {"user": "0x...", "coin": "BTC", "px": "87351.0", ...}
  ]
}
```

**字段说明**:
- `local_time`: 节点本地记录时间（ISO8601）
- `block_time`: 区块链共识时间（ISO8601）
- `block_number`: 真实区块高度（u64）
- `events`: 事件数组（fills、orders、misc_events 等）

### 目录结构

```
~/hl-data/
├── replica_cmds/
│   └── 2025-11-24T09:08:24Z/         # 时间戳目录
│       └── 20251125/
│           └── 807840000             # JSONL (每行一个区块)
├── node_fills_by_block/
│   └── hourly/
│       └── 20251125/
│           └── 8                     # JSONL (批次格式)
├── node_order_statuses_by_block/
│   └── hourly/
│       └── 20251125/
│           └── 8                     # JSONL (批次格式)
└── misc_events_by_block/
    └── hourly/
        └── 20251125/
            └── 8                     # JSONL (批次格式)
```

---

## 主题映射

### 1. hl.blocks

**数据源**: `replica_cmds/**/*/` (JSONL)

**Schema**:
```json
{
  "height": 807847463,
  "time": 1764059598111,
  "hash": "",              // 不可用
  "proposer": "0x...",
  "numTxs": 1285,
  "round": 807847463
}
```

**字段映射**:
| 字段 | 来源 | 说明 |
|------|------|------|
| height | `abci_block.round` | 使用 round 作为 height |
| time | `abci_block.time` | ISO8601→毫秒 |
| hash | ❌ **永远为空** | `replica_cmds` 文件不包含区块哈希 |
| proposer | `abci_block.proposer` | 区块提议者地址 |
| numTxs | `signed_action_bundles.len()` | 交易计数 |
| round | `abci_block.round` | ABCI 轮次号 |

**哈希限制**:
- ❌ **区块哈希不可用**: `replica_cmds` 数据源不包含区块哈希
- 💡 **获取方式**: 如需区块哈希，必须使用 Explorer WebSocket API (`explorerBlock`)
- 📝 **代码位置**: `blocks.rs:171` - 硬编码为空字符串

**状态**: ✅ 完成 | ⚠️ 区块哈希永远为空（数据源限制）

---

### 2. hl.transactions

**数据源**: `replica_cmds/**/*` (JSONL)

**Schema**:
```json
{
  "time": 1764059598111,
  "user": "0xb6a766f531fa8e222f460df11d62b0f84b7b65f3",
  "hash": "",
  "action": {
    "type": "order",
    "orders": [...]
  },
  "block": 807847463,
  "error": null
}
```

**字段映射**:
| 字段 | 来源 | 说明 |
|------|------|------|
| time | `abci_block.time` | ISO8601→毫秒 |
| user | `resps.Full[i].user` | ✅ 从响应直接获取 |
| hash | `signed_action_bundles[i].0` | ✅ **可用** - 共识生成的交易哈希 |
| action | `signed_actions[i].action` | 完整 action 对象 |
| block | `abci_block.round` | 使用 round 作为 block |
| error | `resps.Full[i].res` | ✅ 从响应直接获取 |

**哈希可用性**:
- ✅ **交易哈希可用**: `replica_cmds` 数据包含交易哈希
- 📝 **数据结构**: `signed_action_bundles` 是 tuple 数组 `[hash, bundle]`
- 📝 **提取位置**: `transactions.rs:207-212` - `BundleWithHash(hash, bundle)` 解构
- 💾 **存储位置**:
  - Payload 中的 `hash` 字段
  - DataRecord 的 `tx_hash` 元数据字段（line 239）
- ℹ️ **说明**: 这是区块链共识生成的官方交易哈希

**状态**: ✅ 完成

---

### 3. hl.fills

**数据源**: `node_fills_by_block/**/*` (JSONL with Batch wrapper)

**Batch 结构示例**:
```json
{
  "local_time": "2025-11-25T08:33:18.508962742",
  "block_time": "2025-11-25T08:33:18.111517886",
  "block_number": 807847463,
  "events": [
    ["0x638b9e1f...", {
      "coin": "MON",
      "px": "0.032693",
      "sz": "33983.0",
      "side": "B",
      "time": 1764059598111,
      "startPosition": "0.0",
      "dir": "Open Long",
      "closedPnl": "0.0",
      "hash": "0xa95532d3...",
      "oid": 248252470514,
      "crossed": true,
      "fee": "0.499952",
      "tid": 128259158939093,
      "feeToken": "USDC"
    }]
  ]
}
```

**输出 Schema** (tuple format):
```json
["0x638b9e1f...", {
  "coin": "MON",
  "px": "0.032693",
  "sz": "33983.0",
  "side": "B",
  "time": 1764059598111,
  "startPosition": "0.0",
  "dir": "Open Long",
  "closedPnl": "0.0",
  "hash": "0xa95532d3...",
  "oid": 248252470514,
  "crossed": true,
  "fee": "0.499952",
  "tid": 128259158939093,
  "feeToken": "USDC"
}]
```

**解析流程**:
1. 解析 Batch wrapper，提取 `block_number`、`block_time`、`events`
2. 遍历 `events` 数组（每个元素是 `[user, fill_details]` tuple）
3. 注入 `block_height = batch.block_number`
4. 输出为 tuple 格式：`[user, fillDetails]`

**独有数据**:
- ✅ **手续费**: 每笔交易的确切费用和币种
- ✅ **盈亏**: 已实现盈亏（closedPnl）
- ✅ **仓位跟踪**: startPosition、dir（方向变化）
- ✅ **实际执行价格**: 包含滑点的真实成交价
- ✅ **Maker/Taker**: crossed 标识流动性提供方
- ✅ **Trade ID**: 唯一交易标识符（tid）
- ✅ **Block Number**: 从 batch 中获取真实区块高度
- ✅ **交易哈希**: fill 数据包含 tx hash

**哈希可用性**:
- ✅ **交易哈希可用**: `node_fills_by_block` 数据包含 hash 字段
- 📝 **代码位置**: `fills.rs:98, 114` - 提取并规范化哈希
- 💾 **存储位置**: 同时存储在 payload 和 DataRecord 的 `tx_hash` 元数据字段

**状态**: ✅ 完成（支持 Batch 格式 + tuple 输出 + 哈希可用）

---

### 4. hl.orders

**数据源**: `node_order_statuses_by_block/**/*` (JSONL with Batch wrapper)

**Batch 结构示例**:
```json
{
  "local_time": "2025-11-25T08:33:18.509256245",
  "block_time": "2025-11-25T08:33:18.111517886",
  "block_number": 807847463,
  "events": [
    {
      "time": "2025-11-25T08:33:18.111517886",
      "user": "0x365e0c115f...",
      "hash": "0x0df718d8a6...",
      "builder": null,
      "status": "open",
      "order": {
        "coin": "kPEPE",
        "side": "B",
        "limitPx": "0.004453",
        "sz": "233318.0",
        "oid": 248252469859,
        "timestamp": 1764059598111,
        "orderType": "Limit",
        "origSz": "233318.0",
        "tif": "Alo"
      }
    }
  ]
}
```

**输出 Schema**:
```json
{
  "user": "0x365e0c115f...",
  "hash": "0x0df718d8a6...",
  "time": "2025-11-25T08:33:18.111517886",
  "status": "open",
  "coin": "kPEPE",
  "side": "B",
  "limitPx": "0.004453",
  "sz": "233318.0",
  "oid": 248252469859
}
```

**解析流程**:
1. 解析 Batch wrapper，提取 `block_number`、`events`
2. 遍历 `events` 数组
3. 从 `order` 对象中提取字段并扁平化
4. 注入 `block_height = batch.block_number`

**独有数据**:
- ✅ **订单状态**: open/partial/filled/cancelled/rejected
- ✅ **剩余数量**: sz（当前剩余）vs origSz（原始数量）
- ✅ **订单生命周期**: 从创建到完成的状态变化历史
- ✅ **Block Number**: 从 batch 中获取真实区块高度
- ✅ **交易哈希**: order status 数据包含 tx hash

**哈希可用性**:
- ✅ **交易哈希可用**: `node_order_statuses_by_block` 数据包含 hash 字段
- 📝 **数据来源**: 订单状态更新事件关联的交易哈希
- 💾 **输出位置**: 包含在 payload 的 `hash` 字段中

**状态**: ✅ 完成（支持 Batch 格式 + 字段扁平化 + 哈希可用）

---

### 5. hl.trades

**数据源**: `node_fills_by_block/**/*` (从 fills 中提取)

**Schema**:
```json
{
  "coin": "MON",
  "side": "B",
  "px": "0.032693",
  "sz": "33983.0",
  "time": 1764059598111,
  "hash": "0xa95532d3...",
  "tid": 128259158939093,
  "users": ["0x638b9e1f...", "0x162cc7c8..."]
}
```

**提取逻辑**:
- 从 fills 数据中提取 trade 信息
- 1 个 crossed fill → 1 个 trade
- 聚合买卖双方用户地址到 `users` 数组

**哈希可用性**:
- ✅ **交易哈希可用**: 继承自 fills 数据的 hash 字段
- 📝 **数据来源**: 从 `node_fills_by_block` 提取
- 💾 **输出位置**: 包含在 trade payload 的 `hash` 字段中

**状态**: ✅ 完成（从 fills 提取，无需单独文件，哈希可用）

---

### 6. hl.misc_events

**数据源**: `misc_events_by_block/**/*` (JSONL with Batch wrapper)

**Batch 结构示例**:
```json
{
  "local_time": "2025-11-25T08:33:18.508962742",
  "block_time": "2025-11-25T08:33:18.111517886",
  "block_number": 807847463,
  "events": [
    {
      "time": "2025-11-25T08:33:18.111517886",
      "hash": "0x000...",
      "inner": {
        "type": "funding",
        "coin": "BTC",
        "fundingRate": "0.00001234",
        "user": "0x..."
      }
    }
  ]
}
```

**输出 Schema**:
```json
{
  "time": "2025-11-25T08:33:18.111517886",
  "hash": "0x000...",
  "inner": {
    "type": "funding",
    "coin": "BTC",
    "fundingRate": "0.00001234"
  }
}
```

**解析流程**:
1. 解析 Batch wrapper，提取 `block_number`、`events`
2. 遍历 `events` 数组
3. 从 `inner.user` 提取用户（如果存在）
4. 注入 `block_height = batch.block_number`

**哈希可用性**:
- ✅ **交易哈希可用**: `misc_events_by_block` 数据包含 hash 字段
- 📝 **数据来源**: 系统事件关联的交易哈希
- 💾 **输出位置**: 包含在 payload 的 `hash` 字段中

**状态**: ✅ 完成（支持 Batch 格式 + 哈希可用）

---

## 完整数据流

### 数据源拓扑

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Hyperliquid 节点文件                                                          │
└─────────────────────────────────────────────────────────────────────────────┘
                        │
                        ├── replica_cmds/* (JSONL, 每行一个区块)
                        │   ├─> hl.blocks ✅
                        │   │   • height, time, proposer, numTxs
                        │   │
                        │   └─> hl.transactions ✅ [REQUEST 层]
                        │       • 全部 52 种交易类型
                        │       • 用户原始请求参数
                        │       • 错误信息
                        │
                        ├── node_fills_by_block/* (JSONL, Batch 格式)
                        │   ├─> hl.fills ✅ [OUTCOME 层]
                        │   │   • 实际成交价格/数量
                        │   │   • 手续费 + 盈亏
                        │   │   • 仓位变化
                        │   │   • block_number（真实区块高度）
                        │   │
                        │   └─> hl.trades ✅
                        │       • 交易撮合数据
                        │       • 从 fills 中提取
                        │
                        ├── node_order_statuses_by_block/* (JSONL, Batch 格式)
                        │   └─> hl.orders ✅ [STATE 层]
                        │       • 订单状态 (open/partial/filled)
                        │       • 剩余数量 vs 原始数量
                        │       • block_number（真实区块高度）
                        │
                        └── misc_events_by_block/* (JSONL, Batch 格式)
                            └─> hl.misc_events ✅
                                • 系统事件
                                • block_number（真实区块高度）
```

---

## 配置

### 代理配置 (config.toml)

```toml
mode = "file"

[node]
node_id = "hl-agent-1"
data_dir = "~/hl-data"

[watcher]
watch_paths = [
    "replica_cmds",
    "node_fills_by_block",
    "node_order_statuses_by_block",
    "misc_events_by_block"
]
poll_interval_ms = 100
skip_historical = true    # 从文件末尾开始，仅处理新数据

[sorter]
endpoint = "http://127.0.0.1:50051"  # gRPC 模式
# output_dir = "/tmp/output"         # 或使用文件模式
batch_size = 100

[checkpoint]
db_path = "~/.hl-agent/checkpoint.db"
```

**注意**:
- ❌ `node_trades` 已从 watch_paths 移除（目录不存在，trades 从 fills 提取）
- ✅ 使用 `_by_block` 变体以获取 block_number 元数据

---

## Checkpoint 机制

**数据库**: SQLite with WAL mode (`~/.hl-agent/checkpoint.db`)

**安全 Offset 计算**:
- 公式: `safe_offset = current_offset + chunk.len() - parser.backlog_len()`
- 从数据库更新 checkpoint 为 safe_offset

**为什么重要**:
- Parser buffer 可能包含不完整的行/Batch
- Checkpoint 必须指向最后**完全处理**的字节
- 重启时从 checkpoint 安全恢复，无数据丢失

---

## 测试验证总结 (2025-11-25)

### 压力测试结果

**测试配置**:
- 配置: `skip_historical = true`（仅处理新数据）
- 并发 tailers: 68 个文件同时处理
- 日志级别: INFO（所有主题可见）

**验证结果**:
| 主题 | 记录数 | 状态 |
|------|--------|------|
| hl.blocks | 128+ | ✅ 工作正常 |
| hl.transactions | 128+ | ✅ 工作正常 |
| hl.fills | 248+ | ✅ 工作正常 |
| hl.trades | 248+ | ✅ 工作正常 |
| hl.orders | 162+ | ✅ 工作正常 |
| hl.misc_events | 49+ | ✅ 工作正常 |

**关键验证**:
- ✅ Batch 格式正确解析（block_number, block_time, events）
- ✅ skip_historical=true 正确工作（从文件末尾开始）
- ✅ 实时处理新数据（timestamps: 2025-11-25 08:33:18）
- ✅ 所有主题在 INFO 级别可见
- ✅ Fills 输出为 tuple 格式：`[user, fillDetails]`
- ✅ Orders 字段正确扁平化

---

## 构建和部署

### 前置条件
```bash
sudo apt-get install protobuf-compiler  # Debian/Ubuntu
brew install protobuf                    # macOS
```

### 构建
```bash
cd hl-agent
cargo build --release
# 输出: target/release/hl-agent
```

### 运行
```bash
export RUST_LOG=info
HL_AGENT_CONFIG=config.toml ./target/release/hl-agent
```

---

## 参考文档

- `CLAUDE.md` - 项目概览
- `ORDER_BOOK_SERVER_ANALYSIS.md` - 批次格式分析（发现 Batch wrapper 结构）
- `ALLIUM_COMPARISON.md` - Allium schema 对比
- `REVIEW_REPORT_NOV_2025.md` - 2025-11 代码审查报告
- `examples/mock_sorter.rs` - 测试基础设施
- `examples/verify_schemas.rs` - Schema 验证工具

---

**文档版本**: v6.0
**最后更新**: 2025-11-25
**状态**: ✅ 生产就绪 - 全部 6 个主题已验证（含 Batch 格式支持）
