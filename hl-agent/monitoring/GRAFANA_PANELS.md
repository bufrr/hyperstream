# Grafana Panel Configurations for hl-agent

## Panel 1: Block Validation Failures (Total Count)

**Type**: Stat Panel
**Query**:
```promql
sum(hl_agent_block_validation_failures_total) or vector(0)
```

**Settings**:
- Visualization: Stat
- Unit: none
- Show: Value (not rate)
- Color mode: Value
- Thresholds:
  - 0: Green
  - 1: Red

**Why this query**:
- `sum(...)` aggregates across all failure reasons
- `or vector(0)` shows `0` instead of "No Data" when counter hasn't been incremented
- **NO `rate()` function** - we want the cumulative total, not failures per second

---

## Panel 2: Latest Block Height Processing

**Type**: Stat Panel
**Query**:
```promql
hl_agent_latest_block_height
```

**Settings**:
- Visualization: Stat
- Unit: none
- Show: Value
- Description: "Latest block height currently being processed"

**Alternative - Total Blocks Processed (counter)**:
```promql
hl_agent_records_emitted_total{topic="hl.blocks"}
```
Shows cumulative total blocks processed since agent startup instead of current block height.

---

## Panel 3: Blocks & Transactions Throughput

**Type**: Time Series Graph
**Query A (Blocks per second)**:
```promql
rate(hl_agent_records_emitted_total{topic="hl.blocks"}[1m])
```

**Query B (Transactions per second)**:
```promql
rate(hl_agent_records_emitted_total{topic="hl.transactions"}[1m])
```

**Settings**:
- Visualization: Time series
- Unit: ops/sec (operations per second)
- Legend: {{topic}}
- Y-axis: Left axis for both queries

---

## Panel 4: Total Records Emitted by Topic

**Type**: Bar Gauge or Table
**Query**:
```promql
sum by(topic) (hl_agent_records_emitted_total)
```

**Settings**:
- Visualization: Bar gauge (horizontal) or Table
- Unit: short (thousands, millions)
- Display mode: All values
- Sort: By value (descending)

---

## Panel 5: Parse Errors by Type

**Type**: Time Series or Stat
**Query**:
```promql
sum by(parser_type, error_type) (hl_agent_parse_errors_total) or vector(0)
```

**Settings**:
- Visualization: Time series (to see when errors occurred)
- Unit: none
- Legend: {{parser_type}}: {{error_type}}

---

## Panel 6: Parse Latency (p95)

**Type**: Time Series
**Query**:
```promql
histogram_quantile(0.95, sum by(le, parser_type) (rate(hl_agent_parse_duration_seconds_bucket[5m])))
```

**Settings**:
- Visualization: Time series
- Unit: seconds (s)
- Legend: {{parser_type}} p95
- Description: "95th percentile parse latency by parser type"

---

## Panel 7: Batch Send Success Rate

**Type**: Time Series
**Query A (Success rate %)**:
```promql
100 * rate(hl_agent_batches_sent_total{status="success"}[1m]) / (rate(hl_agent_batches_sent_total{status="success"}[1m]) + rate(hl_agent_batches_sent_total{status="failure"}[1m]))
```

**Query B (Total batches/sec)**:
```promql
sum(rate(hl_agent_batches_sent_total[1m]))
```

**Settings**:
- Visualization: Time series
- Unit: percent (0-100) for Query A, ops/sec for Query B
- Y-axis: Left for Query A, Right for Query B

---

## Panel 8: Redis Cache Hit Rate

**Type**: Stat or Time Series
**Query**:
```promql
100 * rate(hl_agent_redis_cache_total{result="hit"}[1m]) / (rate(hl_agent_redis_cache_total{result="hit"}[1m]) + rate(hl_agent_redis_cache_total{result="miss"}[1m]))
```

**Settings**:
- Visualization: Stat (for current value) or Time series (for trend)
- Unit: percent (0-100)
- Thresholds:
  - 0-80%: Red
  - 80-95%: Yellow
  - 95-100%: Green

---

## Panel 9: File Read Latency (Average)

**Type**: Time Series
**Query**:
```promql
rate(hl_agent_file_read_duration_seconds_sum[5m]) / rate(hl_agent_file_read_duration_seconds_count[5m])
```

**Settings**:
- Visualization: Time series
- Unit: seconds (s)
- Description: "Average time to read 8MiB file chunks"

---

## Panel 10: Checkpoint Write Latency (p95)

**Type**: Time Series
**Query**:
```promql
histogram_quantile(0.95, sum by(le) (rate(hl_agent_checkpoint_duration_seconds_bucket[5m])))
```

**Settings**:
- Visualization: Time series
- Unit: seconds (s)
- Description: "95th percentile checkpoint write latency"

---

## Recommended Dashboard Layout

```
+----------------------------------+----------------------------------+
|  Block Validation Failures       |  Latest Block Height             |
|  (Stat - should show 0)          |  (Stat - total blocks processed) |
+----------------------------------+----------------------------------+
|                                                                     |
|  Blocks & Transactions Throughput (Time Series)                    |
|  - Blocks/sec (blue line)                                          |
|  - Transactions/sec (orange line)                                  |
|                                                                     |
+---------------------------------------------------------------------+
|                                                                     |
|  Total Records Emitted by Topic (Bar Gauge)                        |
|  hl.blocks: 50,234                                                 |
|  hl.transactions: 32,145,678                                       |
|  hl.orders: 12,345,678                                             |
|  ...                                                                |
|                                                                     |
+----------------------------------+----------------------------------+
|  Parse Errors (Time Series)      |  Parse Latency p95              |
|  (Should be flat at 0)           |  (Time Series)                  |
+----------------------------------+----------------------------------+
|  Batch Send Success Rate         |  Redis Cache Hit Rate           |
|  (Should be near 100%)           |  (Should be >95%)               |
+----------------------------------+----------------------------------+
```

---

## Common Issues & Fixes

### Issue: "No Data" shown in panel

**Cause**: Counter hasn't been incremented yet (value is 0)

**Fix**: Add `or vector(0)` to the query:
```promql
# Before (shows "No Data"):
sum(hl_agent_block_validation_failures_total)

# After (shows 0):
sum(hl_agent_block_validation_failures_total) or vector(0)
```

### Issue: Panel shows rate instead of total

**Cause**: Using `rate()` on a counter when you want cumulative total

**Fix**: Remove `rate()` function:
```promql
# Wrong (shows rate):
rate(hl_agent_block_validation_failures_total[5m])

# Correct (shows total):
sum(hl_agent_block_validation_failures_total) or vector(0)
```

### Issue: Graph drops to zero when agent restarts

**Cause**: Prometheus counters are in-memory and reset on process restart

**Fix**: This is expected behavior. To persist metrics across restarts, consider:
1. Use Prometheus remote write to long-term storage
2. Add restart annotations to dashboards
3. Use `increase()` with long time windows to smooth restarts:
   ```promql
   increase(hl_agent_records_emitted_total{topic="hl.blocks"}[1h])
   ```

---

## Alerting Rules

### High Parse Error Rate
```yaml
alert: HighParseErrorRate
expr: rate(hl_agent_parse_errors_total[5m]) > 1
for: 5m
labels:
  severity: warning
annotations:
  summary: "High parse error rate detected"
  description: "Parser {{ $labels.parser_type }} has {{ $value }} errors/sec"
```

### Low Throughput
```yaml
alert: LowThroughput
expr: rate(hl_agent_records_emitted_total{topic="hl.blocks"}[5m]) < 5
for: 10m
labels:
  severity: warning
annotations:
  summary: "Low block processing throughput"
  description: "Only {{ $value }} blocks/sec (expected >10)"
```

### Block Validation Failures
```yaml
alert: BlockValidationFailures
expr: increase(hl_agent_block_validation_failures_total[5m]) > 0
for: 1m
labels:
  severity: critical
annotations:
  summary: "Block validation failures detected"
  description: "{{ $value }} blocks failed validation in last 5 minutes"
```

---

**Document Version**: v1.0
**Last Updated**: 2025-12-02
**Related Files**: `monitoring/GRAFANA_QUERIES.md`, `src/metrics.rs`
