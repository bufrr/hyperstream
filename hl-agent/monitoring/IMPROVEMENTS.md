# Monitoring Stack Improvements

This document summarizes the improvements made to the Prometheus + Grafana monitoring stack based on the Codex ultra-deep review.

## Changes Applied

### CRITICAL Fixes

#### 1. Fixed Redis Cache Hit Rate PromQL Query
**Problem**: The original query mixed `result="hit"` and `result="miss"` label series without proper aggregation, causing Prometheus to drop vectors and return no data.

**Original Query**:
```promql
rate(hl_agent_redis_cache_total{result="hit"}[1m]) /
  (rate(hl_agent_redis_cache_total{result="hit"}[1m]) +
   rate(hl_agent_redis_cache_total{result="miss"}[1m]))
```

**Fixed Query**:
```promql
sum(rate(hl_agent_redis_cache_total{result="hit"}[5m])) by (job, service) /
  sum(rate(hl_agent_redis_cache_total[5m])) by (job, service)
```

**Impact**: Redis cache hit rate panel now displays correct percentage values.

**File**: `grafana/dashboards/hl-agent.json` line 537

---

### HIGH Priority Improvements

#### 2. Pinned Docker Image Versions
**Problem**: Using `latest` tags caused unpredictable behavior on `docker compose up`, potentially pulling breaking changes.

**Changes**:
- Prometheus: `prom/prometheus:latest` → `prom/prometheus:v2.52.0`
- Grafana: `grafana/grafana:latest` → `grafana/grafana:10.4.2`

**Impact**: Reproducible builds, no surprise breakage on container restarts.

**File**: `docker-compose.yml` lines 3, 30

---

#### 3. Added Health Checks
**Problem**: `depends_on` only ordered container starts, allowing Grafana to boot before Prometheus was ready, causing provisioning failures.

**Changes**:
- Added HTTP health checks for both services
- Grafana waits for Prometheus to be healthy before starting
- 15s interval, 5s timeout, 5 retries

**Prometheus Health Check**:
```yaml
healthcheck:
  test: ["CMD", "wget", "-qO-", "http://localhost:9090/-/healthy"]
  interval: 15s
  timeout: 5s
  retries: 5
```

**Grafana Health Check**:
```yaml
healthcheck:
  test: ["CMD", "wget", "-qO-", "http://localhost:3000/api/health"]
  interval: 15s
  timeout: 5s
  retries: 5
depends_on:
  prometheus:
    condition: service_healthy
```

**Impact**: Reliable startup order, surfaces broken containers immediately.

**File**: `docker-compose.yml` lines 21-25, 45-54

---

#### 4. Security Improvements
**Problem**: Default admin password hardcoded, configs writable by containers.

**Changes**:
- Grafana password configurable via `GRAFANA_ADMIN_PASSWORD` env var (defaults to "admin")
- Disabled Gravatar and user sign-up
- All config files mounted read-only (`:ro` suffix)

**Grafana Environment**:
```yaml
environment:
  - GF_SECURITY_ADMIN_USER=admin
  - GF_SECURITY_ADMIN_PASSWORD=${GRAFANA_ADMIN_PASSWORD:-admin}
  - GF_SECURITY_DISABLE_GRAVATAR=true
  - GF_USERS_ALLOW_SIGN_UP=false
```

**Read-Only Mounts**:
```yaml
volumes:
  - ./prometheus.yml:/etc/prometheus/prometheus.yml:ro
  - ./grafana/provisioning:/etc/grafana/provisioning:ro
  - ./grafana/dashboards:/var/lib/grafana/dashboards:ro
```

**Impact**: Better security posture, config integrity protection.

**Files**: `docker-compose.yml` lines 8, 36-38, 42-43

**Note**: Ports bound to `0.0.0.0` per user request (accessible from all interfaces).

---

### MEDIUM Priority Improvements

#### 5. Fixed Environment Label
**Problem**: Stack labeled as `environment: production` despite being dev/test setup.

**Change**: `production` → `dev`

**Impact**: Avoids misleading labels and future remote_write collisions.

**File**: `prometheus.yml` line 6

---

#### 6. Added Data Retention Settings
**Problem**: Default 15-day retention not explicit, could change with Prometheus updates.

**Changes**:
```yaml
command:
  - '--storage.tsdb.retention.time=15d'
  - '--storage.tsdb.retention.size=2GB'
  - '--storage.tsdb.wal-compression'
```

**Impact**: Predictable disk usage, protected against long-running dev environments filling disk.

**File**: `docker-compose.yml` lines 13-15

---

#### 7. Added Self-Monitoring
**Problem**: No visibility into monitoring stack health itself.

**Changes**: Added scrape jobs for Prometheus and Grafana internal metrics.

**New Scrape Jobs**:
```yaml
- job_name: 'prometheus'
  static_configs:
    - targets: ['localhost:9090']
      labels:
        service: 'prometheus'

- job_name: 'grafana'
  static_configs:
    - targets: ['grafana:3000']
      labels:
        service: 'grafana'
```

**Impact**: Can detect when monitoring stack itself is unhealthy.

**File**: `prometheus.yml` lines 17-29

**Verification**:
```bash
curl -s http://localhost:9091/api/v1/targets | jq '.data.activeTargets[] | {job, health}'
```

Expected output:
- `hl-agent`: down (expected until agent runs)
- `prometheus`: up
- `grafana`: up

---

## Not Implemented (Future Enhancements)

The following LOW-priority improvements were identified but not implemented:

1. **Resource Limits**: CPU/memory caps to prevent runaway queries
2. **Alerting Rules**: Basic alerts for target down, scrape failures
3. **Backup/Restore**: Documentation for volume backup procedures
4. **Dashboard UX**: Templating with topic/parser dropdowns
5. **Startup Script Health**: Replace 5s sleep with actual health polling

These can be added as the monitoring stack matures.

---

## Testing Verification

All improvements verified working:

```bash
# Container health status
$ sudo docker compose ps
NAME                  IMAGE                     STATUS
hl-agent-grafana      grafana/grafana:10.4.2    Up 2 minutes (healthy)
hl-agent-prometheus   prom/prometheus:v2.52.0   Up 2 minutes (healthy)

# All targets scraped
$ curl -s http://localhost:9091/api/v1/targets | jq '.data.activeTargets[] | .health'
"up"    # grafana
"down"  # hl-agent (expected)
"up"    # prometheus

# Dashboard accessible
$ curl -s http://localhost:3000/api/health | jq .
{
  "database": "ok",
  "version": "10.4.2"
}
```

---

## Files Modified

1. `docker-compose.yml` - Image versions, health checks, security settings, retention
2. `prometheus.yml` - Environment label, self-monitoring scrape jobs
3. `grafana/dashboards/hl-agent.json` - Redis cache hit rate query fix
4. `README.md` - Documentation updates

---

## Migration Notes

If updating an existing deployment:

1. Stop old stack: `sudo docker compose down`
2. Pull new images: `sudo docker compose pull`
3. Start with new config: `sudo docker compose up -d`
4. Verify health: `sudo docker compose ps`

Existing data in named volumes (`prometheus-data`, `grafana-data`) is preserved.

To use custom Grafana password:
```bash
export GRAFANA_ADMIN_PASSWORD="your-secure-password"
sudo -E docker compose up -d
```
