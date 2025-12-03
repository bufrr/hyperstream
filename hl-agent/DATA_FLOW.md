# Hyperliquid Data Pipeline Guide

**Status**: Production Ready - All 6 topics verified
**Architecture**: File mode + Redis hash lookup (ws-agent populates Redis)
**Batch Format Support**: `_by_block` files with Batch wrapper parsing
**Hash Status**:
- **Block Hash**: via Redis (populated by ws-agent from Explorer WebSocket)
- **Transaction Hash**: fills, orders, trades, misc_events all contain unique hashes
- **transactions**: No unique hash (data source limitation, set to empty)
- **Node file limitation**: `node_trades` directory does not exist (trades extracted from fills)

---

## Summary

This guide documents the complete data streaming pipeline for Hyperliquid blockchain, covering data collection, parsing, and verification for 6 Kafka topics.

### Hash Field Availability Summary

| Topic | Hash Status | Source | Notes |
|-------|-------------|--------|-------|
| hl.blocks | Available | Redis (ws-agent) | Real-time via Explorer WebSocket |
| hl.transactions | Empty | - | Data source doesn't provide unique tx hash |
| hl.fills | Available | node_fills_by_block | Unique transaction hash |
| hl.orders | Available | node_order_statuses_by_block | Unique transaction hash |
| hl.trades | Available | Inherited from fills | Unique transaction hash |
| hl.misc_events | Available | misc_events_by_block | Unique transaction hash |

**Key Findings**:
- 5/6 topics have hashes: blocks (via Redis), fills, orders, trades, misc_events
- transactions has no hash: `replica_cmds` only contains bundle hash (non-unique), set to empty to avoid confusion
- Block hash: Retrieved from Redis (populated by ws-agent)

### Key Features

- **Complete Topic Coverage**: All 6 topics implemented and verified
- **Batch Format Support**: `_by_block` files with `{block_number, block_time, local_time, events: [...]}` structure
- **Configurable Performance Limits**: Resource limits configurable via config.toml
- **Smart File Selection**: Sorted by modification time (newest first), ensures active files are monitored
- **skip_historical Mode**: Process only new data, start from end of files
- **MessagePack + JSONL Dual Format Support**: blocks use MessagePack, others use JSONL
- **Redis Hash Lookup**: Block hashes retrieved from Redis (populated by ws-agent)
- **Line Count Persistence**: Checkpoint tracks `line_count` for accurate block height calculation after restart
- **Optimized Startup**: Line counting only for `replica_cmds` files; other large files skip immediately

### Implementation Status

| Topic | Status | Source | Batch Format | Verified |
|-------|--------|--------|--------------|----------|
| hl.blocks | Implemented | replica_cmds | One block per line | 128+ records |
| hl.transactions | Implemented | replica_cmds | One block per line | 128+ records |
| hl.fills | Implemented | node_fills_by_block | Batch wrapper | 248+ records |
| hl.orders | Implemented | node_order_statuses_by_block | Batch wrapper | 162+ records |
| hl.trades | Implemented | node_fills_by_block (aggregation) | Batch wrapper | 248+ records |
| hl.misc_events | Implemented | misc_events_by_block | Batch wrapper | 49+ records |

### Performance Characteristics

**Configurable Performance Limits** (config.toml):
- `max_concurrent_tailers` (default: 64) - Limit concurrent file processing tasks
- `skip_historical` (default: false) - Start from end of files, process only new data

---

## Data Source Overview

### Local Files

| File | Format | Batch Structure | Content |
|------|--------|-----------------|---------|
| replica_cmds | JSONL | One block per line | Block metadata + transaction and response data |
| node_fills_by_block | JSONL | **Batch wrapper** | Position fills (with block_number) |
| node_order_statuses_by_block | JSONL | **Batch wrapper** | Order status (with block_number) |
| misc_events_by_block | JSONL | **Batch wrapper** | System events (with block_number) |
| ~~node_trades~~ | Does not exist | - | trades extracted from fills |

### Batch Format (Batch Wrapper)

`_by_block` files use batch wrapper format:

```json
{
  "local_time": "2025-11-25T08:33:18.508962742",
  "block_time": "2025-11-25T08:33:18.111517886",
  "block_number": 807847463,
  "events": [
    // Actual event data array
    {"user": "0x...", "coin": "ETH", "px": "2896.9", ...},
    {"user": "0x...", "coin": "BTC", "px": "87351.0", ...}
  ]
}
```

**Field Description**:
- `local_time`: Node local recording time (ISO8601)
- `block_time`: Blockchain consensus time (ISO8601)
- `block_number`: Real block height (u64)
- `events`: Event array (fills, orders, misc_events, etc.)

### Directory Structure

```
~/hl-data/
├── replica_cmds/
│   └── 2025-11-24T09:08:24Z/         # Timestamp directory
│       └── 20251125/
│           └── 807840000             # JSONL (one block per line)
├── node_fills_by_block/
│   └── hourly/
│       └── 20251125/
│           └── 8                     # JSONL (batch format)
├── node_order_statuses_by_block/
│   └── hourly/
│       └── 20251125/
│           └── 8                     # JSONL (batch format)
└── misc_events_by_block/
    └── hourly/
        └── 20251125/
            └── 8                     # JSONL (batch format)
```

### replica_cmds File Structure Details

**File Naming and Block Height Relationship**:

```
File path: replica_cmds/2025-11-26T04:53:40Z/20251126/808750000
                                                      ↑
                                                 Starting block height
```

**Line Number and Block Height Formula**:
```
Block Height = Filename + Line Count + 1
```

Where `Line Count` is the 0-indexed line number (first line = line count 0).

| Line Count | Block Height | Calculation |
|------------|--------------|-------------|
| 0 (first line) | 808750001 | 808750000 + 0 + 1 |
| 99 | 808750100 | 808750000 + 99 + 1 |
| 9999 | 808760000 | 808750000 + 9999 + 1 |

**Important**: The file is named with the starting block, but the first line contains `starting_block + 1`. For example, file `808750000` contains blocks 808750001 to 808760000.

**Round Number vs Block Height**:

| Concept | Description | Example Value |
|---------|-------------|---------------|
| **Block Height** | Hyperliquid blockchain block height | 808,750,001 |
| **Round Number** | CometBFT/Tendermint consensus round number | 1,087,330,263 |

- Round != Block Height: Values are completely different (~278 million difference)
- Round is an internal consensus mechanism identifier
- Block Height is the publicly exposed blockchain block number

---

## Topic Mapping

### 1. hl.blocks

**Data Source**: `replica_cmds/**/*/` (JSONL)

**Schema**:
```json
{
  "height": 808750001,
  "time": 1764059598111,
  "hash": "0x...",
  "proposer": "0x...",
  "numTxs": 1,
  "round": 1087330263
}
```

**Field Mapping**:
| Field | Source | Notes |
|-------|--------|-------|
| height | **filename + line_count + 1** | Not round, calculated block height |
| time | `abci_block.time` | ISO8601 -> milliseconds |
| hash | **Redis (ws-agent)** | Retrieved from Redis cache |
| proposer | `abci_block.proposer` | Block proposer address |
| numTxs | `signed_action_bundles.len()` | Number of submitted bundles (not processed transactions) |
| round | `abci_block.round` | CometBFT consensus round number (different from height!) |

**Important**: `round` and `height` are different values!
- `round`: CometBFT internal consensus round number (e.g., 1,087,330,263)
- `height`: Public blockchain block height (e.g., 808,750,001)
- Calculation: `height = filename + line_count + 1` (line_count is 0-indexed)

**Hash Retrieval**:
- Block hash retrieved from Redis
- Redis populated by ws-agent (separate binary)
- Key format: `block:{height}` -> hash

**Status**: Complete

---

### 2. hl.transactions

**Data Source**: `replica_cmds/**/*` (JSONL)

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

**Field Mapping**:
| Field | Source | Notes |
|-------|--------|-------|
| time | `abci_block.time` | ISO8601 -> milliseconds |
| user | `resps.Full[i].user` | Directly from response |
| hash | - | **Set to empty** - data source doesn't provide unique hash |
| action | `signed_actions[i].action` | Complete action object |
| block | `abci_block.round` | Using round as block |
| error | `resps.Full[i].res` | Directly from response |

**Hash Not Available**:
- **Data source limitation**: `replica_cmds` only contains bundle hash (non-unique)
- **Why set to empty**: Multiple transactions in the same bundle share the same hash, cannot uniquely identify individual transactions
- **Alternative**: Use fills/orders data for unique transaction hashes

**Status**: Complete

---

### 3. hl.fills

**Data Source**: `node_fills_by_block/**/*` (JSONL with Batch wrapper)

**Batch Structure Example**:
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

**Output Schema** (tuple format):
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

**Parsing Flow**:
1. Parse Batch wrapper, extract `block_number`, `block_time`, `events`
2. Iterate `events` array (each element is `[user, fill_details]` tuple)
3. Inject `block_height = batch.block_number`
4. Output as tuple format: `[user, fillDetails]`

**Unique Data**:
- **Fees**: Exact fee and token for each trade
- **PnL**: Realized PnL (closedPnl)
- **Position Tracking**: startPosition, dir (direction change)
- **Actual Execution Price**: Real fill price including slippage
- **Maker/Taker**: crossed indicates liquidity provider
- **Trade ID**: Unique trade identifier (tid)
- **Block Number**: Real block height from batch
- **Transaction Hash**: fill data contains tx hash

**Status**: Complete (Batch format + tuple output + hash available)

---

### 4. hl.orders

**Data Source**: `node_order_statuses_by_block/**/*` (JSONL with Batch wrapper)

**Batch Structure Example**:
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

**Output Schema**:
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

**Parsing Flow**:
1. Parse Batch wrapper, extract `block_number`, `events`
2. Iterate `events` array
3. Extract fields from `order` object and flatten
4. Inject `block_height = batch.block_number`

**Unique Data**:
- **Order Status**: open/partial/filled/cancelled/rejected
- **Remaining Quantity**: sz (current remaining) vs origSz (original quantity)
- **Order Lifecycle**: Status change history from creation to completion
- **Block Number**: Real block height from batch
- **Transaction Hash**: order status data contains tx hash

**Status**: Complete (Batch format + field flattening + hash available)

---

### 5. hl.trades

**Data Source**: `node_fills_by_block/**/*` (extracted from fills)

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

**Extraction Logic**:
- Extract trade information from fills data
- 1 crossed fill -> 1 trade
- Aggregate buyer and seller addresses to `users` array

**Hash Available**: Inherited from fills data hash field

**Status**: Complete (extracted from fills, no separate file needed, hash available)

---

### 6. hl.misc_events

**Data Source**: `misc_events_by_block/**/*` (JSONL with Batch wrapper)

**Batch Structure Example**:
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

**Output Schema**:
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

**Parsing Flow**:
1. Parse Batch wrapper, extract `block_number`, `events`
2. Iterate `events` array
3. Extract user from `inner.user` (if present)
4. Inject `block_height = batch.block_number`

**Hash Available**: `misc_events_by_block` data contains hash field

**Status**: Complete (Batch format + hash available)

---

## Complete Data Flow

### Data Source Topology

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Hyperliquid Node Files                                                      │
└─────────────────────────────────────────────────────────────────────────────┘
                        │
                        ├── replica_cmds/* (JSONL, one block per line)
                        │   │   [OPTIMIZED: Single combined parser]
                        │   │   Parse JSON once → ~50% faster (87ms vs 152-176ms)
                        │   │
                        │   ├─> hl.blocks
                        │   │   • height, time, proposer, numTxs
                        │   │   • hash from Redis (ws-agent)
                        │   │
                        │   └─> hl.transactions [REQUEST layer]
                        │       • All 52 transaction types
                        │       • User original request parameters
                        │       • Error information
                        │
                        ├── node_fills_by_block/* (JSONL, Batch format)
                        │   ├─> hl.fills [OUTCOME layer]
                        │   │   • Actual fill price/quantity
                        │   │   • Fees + PnL
                        │   │   • Position changes
                        │   │   • block_number (real block height)
                        │   │
                        │   └─> hl.trades
                        │       • Trade matching data
                        │       • Extracted from fills
                        │
                        ├── node_order_statuses_by_block/* (JSONL, Batch format)
                        │   └─> hl.orders [STATE layer]
                        │       • Order status (open/partial/filled)
                        │       • Remaining vs original quantity
                        │       • block_number (real block height)
                        │
                        └── misc_events_by_block/* (JSONL, Batch format)
                            └─> hl.misc_events
                                • System events
                                • block_number (real block height)
```

---

## Runtime Architecture

The system uses a two-binary architecture for clean separation of concerns:

### Binary Overview

| Binary | Purpose | Data Flow |
|--------|---------|-----------|
| **ws-agent** | WebSocket -> Redis | Subscribes to Explorer WS, stores block hashes in Redis |
| **hl-agent** | Files -> gRPC/JSON | Reads node files, queries Redis for hashes, outputs records |

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           System Architecture                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────┐                        ┌─────────────────┐            │
│  │   ws-agent      │ ─────────────────────> │     Redis       │            │
│  │ (Explorer WS)   │   block:{height}->hash │  (hash cache)   │            │
│  └─────────────────┘                        └────────┬────────┘            │
│         │                                            │                     │
│         │ wss://rpc.hyperliquid.xyz/ws               │ GET block:{height}  │
│         ▼                                            ▼                     │
│  ┌─────────────────┐                        ┌─────────────────┐            │
│  │ Hyperliquid     │                        │   hl-agent      │            │
│  │ Explorer API    │                        │  (file mode)    │            │
│  └─────────────────┘                        └────────┬────────┘            │
│                                                      │                     │
│                                                      ▼                     │
│                                             ┌─────────────────┐            │
│  ┌─────────────────┐                        │  File Watchers  │            │
│  │ Hyperliquid     │ ──────────────────────>│  + Tailers      │            │
│  │ Node Files      │   replica_cmds, etc.   │  + Activity     │            │
│  └─────────────────┘                        │    Monitor      │            │
│                                             └────────┬────────┘            │
│                                                      │                     │
│                                                      ▼                     │
│                                             ┌─────────────────┐            │
│                                             │  gRPC Sorter    │            │
│                                             │  or JSON Files  │            │
│                                             └─────────────────┘            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### ws-agent

Standalone binary that:
1. Connects to Hyperliquid Explorer WebSocket (`wss://rpc.hyperliquid.xyz/ws`)
2. Subscribes to `explorerBlock` channel
3. Stores block hashes in Redis with TTL (key format: `block:{height}`)
4. Runs independently of hl-agent

**Usage**:
```bash
./target/release/ws_agent \
    --ws-url wss://rpc.hyperliquid.xyz/ws \
    --redis-url redis://127.0.0.1:6379 \
    --ttl 86400
```

### hl-agent

Main data processing binary that:
1. Watches Hyperliquid node files for changes
2. Spawns tailers for active files (parallel processing)
3. **Monitors file activity** and shuts down inactive tailers (5-minute timeout)
4. Tails files incrementally with checkpoint recovery
5. Parses data into 6 Kafka topics
6. Queries Redis for block hashes (populated by ws-agent)
7. Outputs to gRPC sorter or JSON files

**File Activity Monitor**:
- Polls file sizes every 10 seconds
- Tracks growth/stagnation per file
- Automatically shuts down tailers for inactive files (default: 5 minutes)
- Ensures continuous metrics during file transitions
- Prevents resource waste on completed files

**Usage**:
```bash
HL_AGENT_CONFIG=config.toml ./target/release/hl-agent
```

### Module Structure

```
src/
├── main.rs                 # Entry point
├── runner/
│   ├── mod.rs              # Shared types and utilities
│   └── file_mode.rs        # File mode runner
├── parsers/
│   ├── mod.rs              # Parser routing + Parser trait
│   ├── batch.rs            # Generic batch wrapper BatchEnvelope<T>
│   ├── buffered.rs         # Buffered line parser abstraction
│   ├── schemas.rs          # Shared output schemas (Block, Transaction)
│   ├── block_merger.rs     # Block hash merger
│   ├── hash_store.rs       # Redis hash lookup
│   ├── replica_cmds.rs     # **OPTIMIZED** ReplicaCmdsParser (replica_cmds -> hl.blocks + hl.transactions)
│   ├── blocks.rs           # [DEPRECATED] BlocksParser (legacy, not used)
│   ├── transactions.rs     # [DEPRECATED] TransactionsParser (legacy, not used)
│   ├── fills.rs            # FillsParser (node_fills_by_block -> hl.fills)
│   ├── trades.rs           # TradesParser (node_fills_by_block -> hl.trades)
│   ├── orders.rs           # OrdersParser (node_order_statuses_by_block -> hl.orders)
│   └── misc_events.rs      # MiscEventsParser (misc_events_by_block -> hl.misc_events)
├── config.rs               # Configuration management
├── checkpoint.rs           # Progress checkpointing (with line_count)
├── tailer.rs               # File tailer + TailerConfig
├── watcher.rs              # File system watcher
├── sources/
│   └── mod.rs              # Data sources module
└── bin/
    └── ws_agent.rs         # WebSocket agent binary
```

### Key Types

**TailerConfig** (src/tailer.rs):
```rust
/// Configuration for file tailers.
#[derive(Clone)]
pub struct TailerConfig {
    pub checkpoint_db: Arc<CheckpointDB>,
    pub record_sink: Arc<dyn RecordSink>,
    pub batch_size: usize,
    pub poll_interval: Duration,
    pub bulk_load_warn_bytes: u64,
    pub bulk_load_abort_bytes: u64,
    pub cancel_token: CancellationToken,
}
```

**Parser Trait** (src/parsers/mod.rs):
```rust
pub trait Parser: Send {
    fn parse(&mut self, file_path: &Path, data: &[u8]) -> Result<Vec<DataRecord>>;
    fn backlog_len(&self) -> usize;
    fn set_initial_line_count(&mut self, count: u64) {}  // For block height calculation
    fn get_line_count(&self) -> u64 { 0 }
}
```

### HashStore Component

HashStore provides block hash lookup via Redis:

```
┌───────────────────────────────────────────────────┐
│                   HashStore                        │
├───────────────────────────────────────────────────┤
│  Layer 1: LRU Cache (Memory)                      │
│  ├── Capacity: 100,000 entries                    │
│  ├── Access: O(1)                                 │
│  └── Purpose: Hot data fast access                │
├───────────────────────────────────────────────────┤
│  Layer 2: Redis (Network)                         │
│  ├── Key: block:{height}                          │
│  ├── Value: hash string                           │
│  └── Purpose: Shared cache populated by ws-agent  │
└───────────────────────────────────────────────────┘
```

**Data Flow**:
1. ws-agent receives `explorerBlock` -> writes to Redis
2. hl-agent queries HashStore -> checks LRU cache first -> falls back to Redis
3. Cache miss returns empty hash (block may be too old)

### File Lifecycle Management

The agent implements intelligent file lifecycle management to handle continuous file creation by Hyperliquid nodes:

```
┌────────────────────────────────────────────────────────────────┐
│                    File Lifecycle Flow                          │
├────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. File Detection                                              │
│     └─> Watcher detects new file                               │
│         └─> Spawn tailer task                                  │
│             └─> Create shutdown channel (mpsc)                 │
│                 └─> Track in FileActivity map                  │
│                                                                 │
│  2. Active Processing (file growing)                           │
│     └─> Tailer reads 8MiB chunks                               │
│     └─> Activity Monitor polls every 10s                       │
│         └─> File size increases → reset activity timer        │
│         └─> Update last_size and last_checked                  │
│                                                                 │
│  3. Inactivity Detection (file complete)                       │
│     └─> No file growth for 5 minutes                           │
│         └─> Activity Monitor sends shutdown signal             │
│             └─> Tailer receives signal via try_recv()          │
│                 └─> Graceful exit (checkpoint saved)           │
│                     └─> Remove from active_tailers map         │
│                                                                 │
│  4. Parallel Processing (overlap period)                       │
│     └─> Old file (file_N) still processing                     │
│     └─> New file (file_N+1) detected and started              │
│     └─> Both process in parallel (continuous metrics)          │
│     └─> Old file shuts down after 5min timeout                 │
│     └─> New file continues → no metric gap                     │
│                                                                 │
└────────────────────────────────────────────────────────────────┘
```

**Key Components**:

1. **TailerHandle** (src/runner/file_mode.rs:26-29):
   - Stores JoinHandle for task management
   - Contains shutdown_tx (mpsc::Sender) for signaling

2. **FileActivity** (src/runner/file_mode.rs:31-34):
   - Tracks last_size (u64) for growth detection
   - Records last_checked (SystemTime) for timeout calculation

3. **Activity Monitor Task** (src/runner/file_mode.rs:506-617):
   - Polls every 10 seconds
   - Compares current file size with last_size
   - Calculates inactivity duration
   - Sends shutdown signal when timeout exceeded

4. **Shutdown Channel Pattern** (src/tailer.rs):
   - Two-level checking for responsiveness:
     - Immediate: try_recv() at loop start
     - During idle: select! in sleep_or_shutdown helper
   - Enables instant wake from sleep on shutdown signal

**Timeline Example** (verified in production):
```
14:55:52 - Agent starts
         - Files 815250000, 815240000 detected
         - Both tailers spawned

15:00:52 - File 815240000 inactive for 5min
         - Shutdown signal sent
         - Tailer exits gracefully

15:08:40 - New file 815260000 detected
         - Tailer spawned immediately
         - Parallel processing with 815250000

15:13:42 - File 815250000 inactive for 5min
         - Shutdown signal sent
         - Only 815260000 remains active

Result: Zero downtime, continuous data flow
```

### Configuration Example

```toml
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
skip_historical = true
inactive_timeout_sec = 300  # 5 minutes (default)

[sorter]
endpoint = "http://127.0.0.1:50051"
batch_size = 100

[checkpoint]
db_path = "~/.hl-agent/checkpoint.db"
redis_url = "redis://127.0.0.1:6379"
```

### Performance Characteristics

| Metric | Description |
|--------|-------------|
| Block Hash | Retrieved from Redis (populated by ws-agent) |
| Startup Delay | ~1s (Redis connection) |
| Memory Usage | ~5MB (LRU cache, 100,000 entries) |
| Network Dependency | Redis connection required |
| Cache Miss | Returns empty hash |

#### replica_cmds Parse Optimization (December 2025)

**Before Optimization** (Separate parsers):
- Parse latency: 152-176ms per 100-record batch
- JSON deserialized twice (BlocksParser + TransactionsParser)

**After Optimization** (Combined parser):
- Parse latency: 87ms per 100-record batch
- JSON deserialized once (ReplicaCmdsParser)
- **Performance improvement: 43-50% reduction**

The combined parser parses JSON once and generates both hl.blocks and hl.transactions from the same parsed structure, significantly reducing CPU overhead and improving throughput for replica_cmds files (the highest-volume data source).

---

## Checkpoint Mechanism

**Database**: SQLite with WAL mode (`~/.hl-agent/checkpoint.db`)

**Schema**:
```sql
CREATE TABLE checkpoints (
    file_path TEXT PRIMARY KEY,
    byte_offset INTEGER NOT NULL,
    file_size INTEGER NOT NULL,
    last_modified_ts INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    line_count INTEGER NOT NULL DEFAULT 0
);
```

**Key Fields**:
| Field | Purpose |
|-------|---------|
| `byte_offset` | Position in file to resume reading |
| `line_count` | Number of lines processed (critical for block height calculation) |

**Safe Offset Calculation**:
- Formula: `safe_offset = current_offset + chunk.len() - parser.backlog_len()`
- Update checkpoint in database to safe_offset

**Line Count Persistence**:
- `line_count` is persisted for `replica_cmds` files
- Used to calculate block height: `height = filename + line_count + 1`
- When `skip_historical=true`, newlines are counted in the skipped portion (only for `replica_cmds`)
- Other file types don't need `line_count` and skip immediately without counting

**Why Important**:
- Parser buffer may contain incomplete lines/Batches
- Checkpoint must point to last **fully processed** byte
- Resume safely from checkpoint on restart, no data loss
- `line_count` ensures correct block height calculation after restart

---

## Build and Deploy

### Prerequisites
```bash
sudo apt-get install protobuf-compiler  # Debian/Ubuntu
brew install protobuf                    # macOS
```

### Build
```bash
cd hl-agent
cargo build --release
# Output: target/release/hl-agent, target/release/ws_agent
```

### Run

**Start ws-agent first** (populates Redis):
```bash
./target/release/ws_agent \
    --ws-url wss://rpc.hyperliquid.xyz/ws \
    --redis-url redis://127.0.0.1:6379
```

**Then start hl-agent**:
```bash
export RUST_LOG=info
HL_AGENT_CONFIG=config.toml ./target/release/hl-agent
```

---

## Reference Documentation

- `CLAUDE.md` - Project overview
- `ORDER_BOOK_SERVER_ANALYSIS.md` - Batch format analysis (discovered Batch wrapper structure)
- `ALLIUM_COMPARISON.md` - Allium schema comparison
- `examples/mock_sorter.rs` - Test infrastructure

### Key Code Paths

| Function | File Path |
|----------|-----------|
| Entry point | `src/main.rs` |
| File mode runner | `src/runner/file_mode.rs` |
| Parser routing | `src/parsers/mod.rs` |
| Shared output schemas | `src/parsers/schemas.rs` |
| Batch wrapper generic | `src/parsers/batch.rs` |
| Buffered line parser | `src/parsers/buffered.rs` |
| Block hash merger | `src/parsers/block_merger.rs` |
| Hash store (Redis) | `src/parsers/hash_store.rs` |
| **Optimized combined parser** | `src/parsers/replica_cmds.rs` |
| WebSocket agent | `src/bin/ws_agent.rs` |

---

**Document Version**: v12.0
**Last Updated**: 2025-12-02
**Status**: Production Ready - All 6 topics verified (file mode + Redis hash lookup + optimized parsing)
**Architecture**: Two-binary system (ws-agent + hl-agent) with intelligent file lifecycle management
**Recent Changes**:
- **replica_cmds Parse Optimization** (Dec 2025): Single combined parser eliminates duplicate JSON deserialization
  - Parse latency: 152-176ms → 87ms (43-50% improvement)
  - JSON parsed once instead of twice for both hl.blocks and hl.transactions
  - Maintains full backward compatibility with all 5 Codex-identified issues fixed
  - New file: `src/parsers/replica_cmds.rs` (replaces separate BlocksParser + TransactionsParser)
- **File Activity Monitor**: Automatic shutdown of inactive tailers (5-minute timeout)
- **Graceful Shutdown**: Two-level shutdown checking (try_recv + select!) for instant response
- **Parallel Processing**: Multiple files processed simultaneously for zero-downtime transitions
- **Resource Efficiency**: Automatic cleanup prevents CPU waste on completed files
- **Verified in Production**: Optimized parser running stably since 2025-12-02 18:16 UTC
