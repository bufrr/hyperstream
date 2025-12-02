# HL-Agent Monitoring Stack

Complete Prometheus + Grafana monitoring setup for hl-agent metrics.

## Quick Start

**Prerequisites**: hl-agent must be running and exposing metrics on port 9090.

```bash
# 1. Ensure hl-agent is running with metrics enabled
cd ../  # Go to hl-agent root directory
cargo run --release &

# 2. Start monitoring stack
cd monitoring
chmod +x start.sh
./start.sh
```

Then open http://localhost:3000 in your browser (login: admin/admin).

The dashboard will show "No data" until hl-agent starts processing files and emitting metrics.

## Services

- **Prometheus**: http://localhost:9091 - Metrics scraping and storage
- **Grafana**: http://localhost:3000 - Visualization dashboards

## Dashboard Panels

The `HL-Agent Metrics` dashboard includes:

1. **Records Emitted** - Per-second rate by topic (hl.blocks, hl.transactions, etc.)
2. **Batches Sent** - Success/error rates for sorter batches
3. **Parse Duration (p95)** - 95th percentile latency by parser type
4. **Sorter Send Duration (p95)** - 95th percentile batch send latency
5. **Checkpoint Duration (p95)** - 95th percentile checkpoint write latency
6. **Parse Errors** - Per-second error rate by parser and error type
7. **Block Validation Failures** - Per-second validation failure rate by reason
8. **Redis Cache Hit Rate** - Percentage of Redis cache hits
9. **Totals** - Cumulative counters for records and batches

## Configuration

### Prometheus Scraping

Prometheus scrapes three targets:
- **hl-agent**: `host.docker.internal:9090` (main application metrics) - 10s interval
- **prometheus**: `localhost:9090` (self-monitoring) - 15s interval
- **grafana**: `grafana:3000` (Grafana metrics) - 15s interval

**Note**: Ensure hl-agent is running with metrics enabled:
```toml
# config.toml
[metrics]
bind_addr = "0.0.0.0:9090"
```

### Data Retention

- **Time-based**: 15 days
- **Size-based**: 2GB maximum
- **WAL compression**: Enabled for disk efficiency

### Security

- **Grafana password**: Set via `GRAFANA_ADMIN_PASSWORD` environment variable (defaults to "admin")
- **Image versions**: Pinned to Prometheus v2.52.0 and Grafana 10.4.2
- **Config mounts**: Read-only to prevent accidental modification
- **Health checks**: Both services have HTTP health checks for reliable startup

### Grafana Provisioning

Dashboards and datasources are automatically provisioned on startup from:
- `grafana/provisioning/datasources/` - Prometheus datasource
- `grafana/provisioning/dashboards/` - Dashboard configuration
- `grafana/dashboards/` - Dashboard JSON files

## Manual Commands

```bash
# Start stack
docker compose up -d

# View logs
docker compose logs -f

# View specific service logs
docker compose logs -f prometheus
docker compose logs -f grafana

# Stop stack
docker compose down

# Stop and remove volumes (reset data)
docker compose down -v
```

## Troubleshooting

### No metrics in Grafana

1. Check hl-agent is running and exposing metrics:
   ```bash
   curl http://localhost:9090/metrics
   ```

2. Check Prometheus is scraping successfully:
   - Open http://localhost:9091/targets
   - Look for `hl-agent` job with state "UP"

3. Check Grafana datasource:
   - Settings → Data Sources → Prometheus
   - Click "Test" - should show "Data source is working"

### Dashboard not appearing

1. Check dashboard provisioning:
   ```bash
   docker compose logs grafana | grep -i dashboard
   ```

2. Manually import dashboard:
   - Dashboards → Import
   - Upload `grafana/dashboards/hl-agent.json`

### Port conflicts

If ports 3000 or 9091 are already in use, edit `docker-compose.yml`:
```yaml
services:
  prometheus:
    ports:
      - "9092:9090"  # Change 9091 → 9092
  grafana:
    ports:
      - "3001:3000"  # Change 3000 → 3001
```

## Metrics Reference

### Latency Histograms
- `hl_agent_parse_duration_seconds{parser_type}` - Parse time by parser
- `hl_agent_sorter_send_duration_seconds{status}` - Batch send time
- `hl_agent_checkpoint_duration_seconds{operation}` - Checkpoint write time
- `hl_agent_file_read_duration_seconds` - File chunk read time

### Counters
- `hl_agent_records_emitted_total{topic}` - Records emitted by topic
- `hl_agent_batches_sent_total{status}` - Batches sent (success/error)
- `hl_agent_parse_errors_total{parser_type,error_type}` - Parse errors
- `hl_agent_block_validation_failures_total{reason}` - Block validation failures
- `hl_agent_redis_cache_total{result}` - Redis cache hits/misses
- `hl_agent_checkpoint_errors_total{operation}` - Checkpoint errors

## Dashboard Panels

 - Records Emitted (per second, idle-safe): `hl_agent:records_emitted:idle_safe` shows per-topic throughput with idle fallback to last-seen topics so the panel stays at 0 instead of going blank when idle.
 - Batches Sent (per second, idle-safe): `hl_agent:batches_sent:idle_safe` surfaces per-status batch rate with idle fallback to last-seen statuses.
- Transactions/Blocks/Orders/Fills/Trades/Misc Parse Duration (p50/p95/p99): `hl_agent:parse_duration_seconds:<quantile>:idle_safe{parser_type=...}` percentile latency per parser with idle-safe recording rules.
- Sorter Send Duration (p95): `hl_agent:sorter_send_duration_seconds:p95:idle_safe` histogram quantile with idle fallback to last buckets.
- Checkpoint Duration (p95): `hl_agent:checkpoint_duration_seconds:p95:idle_safe` histogram quantile with idle fallback to last buckets.
- Parse Errors (per second): `rate(hl_agent_parse_errors_total[1m])`; remains empty until the first error is emitted.
- Block Validation Failures (rate w/ idle fallback): `hl_agent:block_validation_failures:idle_safe` by reason, falls back to last totals when idle.
- Redis Cache Hit Rate: `hl_agent:redis_cache:hit_rate:idle_safe` smoothed hit ratio with idle fallback.
- Totals: `sum(hl_agent_records_emitted_total)` and `sum(hl_agent_batches_sent_total)` cumulative counts.

## Data Retention

- **Prometheus**: 15 days (configurable in `prometheus.yml`)
- **Grafana**: Persistent volumes preserve dashboards and settings across restarts
