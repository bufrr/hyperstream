# Grafana Query Guide for hl-agent Metrics

## Overview

This guide explains the correct PromQL queries for hl-agent metrics in Grafana dashboards.

## Counter Metrics (Total Numbers)

Counter metrics ending in `_total` are **monotonically increasing** values that represent cumulative counts. They should NOT use `rate()` unless you want to see the rate of change.

### Block Validation Failures

**Metric**: `hl_agent_block_validation_failures_total`

**What it tracks**: Total number of blocks dropped due to validation failures (cumulative)

**WRONG Query (shows rate, not total)**:
```promql
rate(hl_agent_block_validation_failures_total[5m])
```
❌ This shows failures per second, not total count

**CORRECT Query (shows total number)**:
```promql
sum(hl_agent_block_validation_failures_total) or vector(0)
```
✅ This shows the cumulative total number of failures

**Why `or vector(0)`?**
- If no failures have occurred, the metric doesn't exist yet
- `or vector(0)` ensures the panel shows `0` instead of "No Data"
- Once failures occur, the actual count will be displayed

### Records Emitted

**Metric**: `hl_agent_records_emitted_total{topic="hl.blocks"}`

**WRONG Query (shows rate)**:
```promql
rate(hl_agent_records_emitted_total{topic="hl.blocks"}[5m])
```
❌ Shows blocks per second

**CORRECT Query (shows total)**:
```promql
hl_agent_records_emitted_total{topic="hl.blocks"}
```
✅ Shows total blocks emitted since startup

**For Multiple Topics**:
```promql
sum by(topic) (hl_agent_records_emitted_total)
```
✅ Shows total per topic

## When to Use rate()

Use `rate()` only when you want to see **throughput** (events per second):

### Blocks Throughput (blocks/sec)
```promql
rate(hl_agent_records_emitted_total{topic="hl.blocks"}[1m])
```
✅ Shows blocks per second over 1-minute window

### Transactions Throughput (tx/sec)
```promql
rate(hl_agent_records_emitted_total{topic="hl.transactions"}[1m])
```
✅ Shows transactions per second over 1-minute window

### Validation Failures Rate (failures/sec)
```promql
rate(hl_agent_block_validation_failures_total[5m])
```
✅ Shows validation failures per second (useful for detecting spikes)

## Complete Metric Reference

### Counters (use without rate for totals)

| Metric | Description | Total Query | Rate Query |
|--------|-------------|-------------|------------|
| `hl_agent_records_emitted_total{topic}` | Records emitted per topic | `sum by(topic) (hl_agent_records_emitted_total)` | `sum by(topic) (rate(hl_agent_records_emitted_total[1m]))` |
| `hl_agent_block_validation_failures_total{reason}` | Block validation failures | `sum(hl_agent_block_validation_failures_total) or vector(0)` | `sum(rate(hl_agent_block_validation_failures_total[5m]))` |
| `hl_agent_parse_errors_total{parser_type,error_type}` | Parse errors | `sum by(parser_type) (hl_agent_parse_errors_total)` | `sum by(parser_type) (rate(hl_agent_parse_errors_total[5m]))` |
| `hl_agent_checkpoint_errors_total{operation}` | Checkpoint errors | `sum(hl_agent_checkpoint_errors_total)` | `sum(rate(hl_agent_checkpoint_errors_total[5m]))` |
| `hl_agent_batches_sent_total{status}` | Batches sent | `sum by(status) (hl_agent_batches_sent_total)` | `sum by(status) (rate(hl_agent_batches_sent_total[1m]))` |
| `hl_agent_redis_cache_total{result}` | Redis cache hits/misses | `sum by(result) (hl_agent_redis_cache_total)` | `sum by(result) (rate(hl_agent_redis_cache_total[1m]))` |

### Histograms (always use rate for percentiles)

| Metric | Description | p95 Latency Query | Avg Latency Query |
|--------|-------------|-------------------|-------------------|
| `hl_agent_parse_duration_seconds{parser_type}` | Parse latency | `histogram_quantile(0.95, sum by(le,parser_type) (rate(hl_agent_parse_duration_seconds_bucket[5m])))` | `sum by(parser_type) (rate(hl_agent_parse_duration_seconds_sum[5m])) / sum by(parser_type) (rate(hl_agent_parse_duration_seconds_count[5m]))` |
| `hl_agent_checkpoint_duration_seconds{operation}` | Checkpoint write latency | `histogram_quantile(0.95, sum by(le) (rate(hl_agent_checkpoint_duration_seconds_bucket[5m])))` | `rate(hl_agent_checkpoint_duration_seconds_sum[5m]) / rate(hl_agent_checkpoint_duration_seconds_count[5m])` |
| `hl_agent_sorter_send_duration_seconds{status}` | Sorter send latency | `histogram_quantile(0.95, sum by(le) (rate(hl_agent_sorter_send_duration_seconds_bucket[5m])))` | `rate(hl_agent_sorter_send_duration_seconds_sum[5m]) / rate(hl_agent_sorter_send_duration_seconds_count[5m])` |
| `hl_agent_file_read_duration_seconds` | File read latency | `histogram_quantile(0.95, rate(hl_agent_file_read_duration_seconds_bucket[5m]))` | `rate(hl_agent_file_read_duration_seconds_sum[5m]) / rate(hl_agent_file_read_duration_seconds_count[5m])` |

### Gauges (use directly, no rate needed)

| Metric | Description | Query |
|--------|-------------|-------|
| `hl_agent_latest_block_height` | Latest block height being processed | `hl_agent_latest_block_height` |

## Dashboard Panel Examples

### Panel 1: Total Block Validation Failures (Stat Panel)

**Query**:
```promql
sum(hl_agent_block_validation_failures_total) or vector(0)
```

**Panel Settings**:
- Visualization: Stat
- Unit: none
- Show: Value
- Color: Green (if 0), Red (if > 0)
- Thresholds: 0 (green), 1 (red)

### Panel 2: Validation Failures by Reason (Table)

**Query**:
```promql
sum by(reason) (hl_agent_block_validation_failures_total) or vector(0)
```

**Panel Settings**:
- Visualization: Table
- Transform: Labels to fields
- Columns: reason, Value

### Panel 3: Blocks & Transactions Throughput (Time Series)

**Query A (Blocks)**:
```promql
rate(hl_agent_records_emitted_total{topic="hl.blocks"}[1m])
```

**Query B (Transactions)**:
```promql
rate(hl_agent_records_emitted_total{topic="hl.transactions"}[1m])
```

**Panel Settings**:
- Visualization: Time series
- Unit: ops/sec
- Legend: {{topic}}

### Panel 4: Total Records Emitted (Stat Panel)

**Query**:
```promql
sum by(topic) (hl_agent_records_emitted_total)
```

**Panel Settings**:
- Visualization: Stat
- Unit: short
- Show: Value
- Display mode: All values

## Common Mistakes

### ❌ Mistake 1: Using rate() for cumulative totals
```promql
# WRONG - Shows rate of change, not total
rate(hl_agent_block_validation_failures_total[5m])
```

```promql
# CORRECT - Shows cumulative total
sum(hl_agent_block_validation_failures_total) or vector(0)
```

### ❌ Mistake 2: Missing `or vector(0)` for zero values
```promql
# WRONG - Shows "No Data" when count is 0
sum(hl_agent_block_validation_failures_total)
```

```promql
# CORRECT - Shows 0 when count is 0
sum(hl_agent_block_validation_failures_total) or vector(0)
```

### ❌ Mistake 3: Not using sum() for multi-label counters
```promql
# WRONG - May show multiple series if multiple reasons exist
hl_agent_block_validation_failures_total
```

```promql
# CORRECT - Aggregates across all reasons for total count
sum(hl_agent_block_validation_failures_total) or vector(0)
```

## Quick Reference Table

| What You Want | Query Pattern | Example |
|---------------|---------------|---------|
| **Total count** | `sum(metric_total) or vector(0)` | `sum(hl_agent_block_validation_failures_total) or vector(0)` |
| **Count by label** | `sum by(label) (metric_total)` | `sum by(reason) (hl_agent_block_validation_failures_total)` |
| **Rate (per second)** | `rate(metric_total[window])` | `rate(hl_agent_records_emitted_total[1m])` |
| **Rate by label** | `sum by(label) (rate(metric_total[window]))` | `sum by(topic) (rate(hl_agent_records_emitted_total[1m]))` |
| **p95 latency** | `histogram_quantile(0.95, sum by(le) (rate(metric_bucket[5m])))` | `histogram_quantile(0.95, sum by(le) (rate(hl_agent_parse_duration_seconds_bucket[5m])))` |
| **Average latency** | `rate(metric_sum[5m]) / rate(metric_count[5m])` | `rate(hl_agent_parse_duration_seconds_sum[5m]) / rate(hl_agent_parse_duration_seconds_count[5m])` |

## Troubleshooting

### "No Data" in panel

**Possible causes**:
1. Metric hasn't been incremented yet (counter is 0)
2. Wrong metric name
3. Agent not running or not exposing metrics

**Solutions**:
- Add `or vector(0)` to show 0 instead of "No Data"
- Verify metric exists: `curl http://localhost:9090/metrics | grep metric_name`
- Check agent logs for errors

### Panel shows rate instead of total

**Problem**: Using `rate()` for a counter metric

**Solution**: Remove `rate()` and use raw counter value:
```promql
# Change from:
rate(hl_agent_block_validation_failures_total[5m])

# To:
sum(hl_agent_block_validation_failures_total) or vector(0)
```

### Panel shows multiple series for same metric

**Problem**: Counter has multiple labels but not aggregated

**Solution**: Use `sum by(label)` to aggregate:
```promql
# Change from:
hl_agent_block_validation_failures_total

# To:
sum by(reason) (hl_agent_block_validation_failures_total)
# Or for total across all reasons:
sum(hl_agent_block_validation_failures_total) or vector(0)
```

---

**Document Version**: v1.0
**Last Updated**: 2025-12-02
**Related Files**: `src/metrics.rs`, `monitoring/README.md`
