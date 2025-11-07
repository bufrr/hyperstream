# Hyperstream Sorter Proto Review

## Current Design Assessment
### Schema efficiency
- `DataBatch` only includes `source` and `records`, so every batch must repeat bulky per-record routing metadata (`topic`, `partition_key`). Introducing shared batch-level routing info (with per-record overrides) would cut repeated bytes on the wire.
- `DataRecord.timestamp` is an unsigned 64-bit integer. Milliseconds since epoch fit in 48 bits for decades, so the extra 16 bits provide little value but do not materially hurt wire size because protobuf uses varints.
- `payload` is raw bytes with no declared encoding or compression hint, forcing the sorter to treat every payload as opaque and preventing payload-aware validation.

### Scalability
- One `DataBatch` can theoretically contain unbounded records or bytes. Without an explicit `max_records`, `max_bytes`, or `compression` field, a single oversized batch could strand memory or defeat flow control at 100k+ records/sec.
- Unidirectional fire-and-forget streaming (`rpc StreamData(stream DataBatch)`) provides throughput but makes backpressure opaque. Agents cannot tell whether the sorter is lagging or dropping data.
- `SourceMetadata` lacks a monotonically increasing batch or sequence number, so redelivery or replay ordering cannot be detected if an agent restarts.

### Extensibility
- Field numbering is dense (1..6) without reserved gaps, so adding high-traffic optional fields later will require renumbering or appending at the end of the message, which is still safe but less tidy.
- No `oneof` or nested types limit forwards-compatibility; however, the absence of `reserved` statements means retired fields could be accidentally reused.

### Network efficiency
- Strings for `topic`, `partition_key`, `node_id`, and `agent_id` recur in every record/batch. Using enums or small integer identifiers (mapped out-of-band) would lower per-record bytes and CPU spent on UTF-8 validation.
- `byte_offset` is sent every batch even though many sources will not be file-based; marking it optional or using a `oneof` for offset types would avoid unnecessary bytes for stream-only agents.

### Deduplication keys
- `block_height + tx_hash` works for block-bound events, but Hyperliquid streams may include non-transactional updates (order book deltas, system events) that lack a `tx_hash`. Two different transactions can also have the same height, so dedup still requires `tx_hash` to be globally unique and case-normalized. Absent `chain_id` or `source_id`, data from multiple chains/nodes at the same height could collide.

## Identified Issues
1. **No batch identity or sequencing (proto/sorter.proto:7-25)** – Without `batch_id`, `sequence_id`, or `ingestion_watermark`, the sorter cannot detect gaps, duplicates, or replays when agents retry.
2. **Opaque payload contract (proto/sorter.proto:32-42)** – `bytes payload` lacks `content_type`, `encoding`, or `compression` descriptors, preventing validation, schema evolution, or mixed encodings (JSON vs MsgPack) within the same stream.
3. **Per-record routing metadata duplicated (proto/sorter.proto:32-42)** – `topic` and `partition_key` repeat for every `DataRecord`, inflating bandwidth and CPU for large homogeneous batches.
4. **Missing runtime introspection RPCs** – The service exposes only `StreamData`. Operators cannot query health, stats, or agent registration state via gRPC, so troubleshooting needs out-of-band tooling.
5. **Deduplication coverage gaps** – Non-transactional events have no stable dedup key. Cross-chain or multi-agent setups lack `chain_id`, `shard_id`, or even `source_epoch`, so `block_height` collisions remain possible.
6. **No schema/version negotiation** – Agents cannot advertise protocol version or feature set, making rolling upgrades riskier.

## Recommendations
1. **Add batch-level envelope fields** (`BatchId`, `record_count`, `uncompressed_bytes`, optional `compression` enum). Justification: enables quick validation, partial ingestion, and size guardrails per batch, improving stability at 100k+ records/sec.
2. **Introduce routing header vs per-record overrides** – Move `topic`, `partition_key`, and future `kafka_headers` into `DataBatch` with optional per-record overrides via `oneof routing`. Justification: reduces repeated strings, lowering wire size and CPU.
3. **Extend `SourceMetadata`** with `chain_id`, `agent_version`, `protocol_version`, and a monotonically increasing `sequence_id`. Justification: simplifies deduplication, compatibility checks, and replay detection without needing payload inspection.
4. **Describe payload semantics** – Add fields like `bytes payload`, `string content_type`, optional `Compression compression`, and `SchemaVersion schema_version`. Justification: allows the sorter to validate payloads, enables mixed encodings, and prepares for future binary formats.
5. **Reserve field ranges** (e.g., `reserved 10 to 19;`) for future use in each message. Justification: prevents accidental reuse and makes schema evolution predictable.
6. **Augment the service definition** with unary RPCs such as `CheckHealth(Empty)`, `GetStats(Empty)`, or `RegisterAgent(SourceMetadata)`. Justification: improves observability and gives agents a supported way to confirm connectivity/backpressure before streaming.
7. **Strengthen dedup keys** – Define a dedicated `RecordIdentity` message containing `node_id`, `chain_id`, `block_height`, `tx_hash`, `log_index` (optional), and/or a 128-bit UUID. Justification: ensures every event has a unique, comparable identifier even when `tx_hash` is absent.

## Migration Strategy
- **Phase 1 (compatibility mode):** Add new optional fields (`batch_id`, `sequence_id`, `content_type`, etc.) with high field numbers so existing agents keep working. Sorter treats missing fields with sensible defaults.
- **Phase 2 (dual-write):** Update agents to populate the new fields while still filling legacy ones (`topic`, `partition_key`). Sorter logs when legacy-only data arrives to track rollout progress.
- **Phase 3 (enforcement):** Once adoption reaches 100%, enforce presence of new identifiers and envelope metadata server-side; optionally deprecate redundant per-record routing strings.
- **Service evolution:** Introduce health/stats RPCs under new method names so old clients ignore them. After rollout, document the new RPCs and require agents to call `CheckHealth` before streaming.
- **Dedup upgrade:** Implement sorter-side detection that prefers the new `RecordIdentity` when available, falling back to `block_height + tx_hash` only during the migration window.
