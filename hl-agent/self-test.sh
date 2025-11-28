#!/bin/bash
set -euo pipefail

#############################################################################
# Hyperliquid hl-agent Self-Test Script
#
# This script performs a comprehensive end-to-end test of the hl-agent:
# 1. Builds the project
# 2. Starts mock gRPC sorter
# 3. Runs the agent with all data sources
# 4. Verifies all 6 topics produce correct records
# 5. Analyzes performance and data quality
# 6. Generates detailed test report
#
# Usage: ./self-test.sh [options]
#   --quick         Quick test (3 minutes, verifies all 6 topics)
#   --full          Full test (5 minutes, comprehensive verification)
#   --keep-output   Don't clean up test output after completion
#   --help          Show this help message
#############################################################################

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
TEST_DIR="/tmp/hl-agent-test"
OUTPUT_DIR="${TEST_DIR}/output"
MOCK_LOG="${TEST_DIR}/mock-sorter.log"
AGENT_LOG="${TEST_DIR}/agent.log"
CHECKPOINT_DB="${HOME}/.hl-agent/checkpoint-self-test.db"
TEST_CONFIG="${TEST_DIR}/test-config.toml"
REPORT_FILE="${TEST_DIR}/test-report.md"
AGENT_RUST_LOG="${HL_AGENT_RUST_LOG:-${RUST_LOG:-info}}"

# Test parameters (defaults)
TEST_DURATION=30  # 30 seconds default
KEEP_OUTPUT=false
DATA_DIR="${HOME}/hl-data"

# Process arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --quick)
            TEST_DURATION=30  # 30 seconds quick test
            shift
            ;;
        --full)
            TEST_DURATION=180  # 3 minutes - comprehensive verification
            shift
            ;;
        --keep-output)
            KEEP_OUTPUT=true
            shift
            ;;
        --help)
            head -20 "$0" | tail -17
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

#############################################################################
# Helper Functions
#############################################################################

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}$1${NC}"
    echo -e "${GREEN}========================================${NC}"
}

cleanup() {
    log_step "Cleaning Up"

    # Kill processes
    pkill -f "mock_sorter" 2>/dev/null || true
    pkill -f "hl-agent" 2>/dev/null || true
    sleep 2

    # Force kill if still running
    pkill -9 -f "mock_sorter" 2>/dev/null || true
    pkill -9 -f "hl-agent" 2>/dev/null || true

    if [ "$KEEP_OUTPUT" = false ]; then
        log_info "Removing test artifacts..."
        rm -rf "${TEST_DIR}"
        rm -f "${CHECKPOINT_DB}"*
    else
        log_info "Keeping test output at: ${TEST_DIR}"
    fi

    log_success "Cleanup complete"
}

# Trap to ensure cleanup on exit
trap cleanup EXIT INT TERM

check_prerequisites() {
    log_step "Checking Prerequisites"

    # Check if data directory exists
    if [ ! -d "${DATA_DIR}" ]; then
        log_error "Data directory not found: ${DATA_DIR}"
        log_info "Please ensure Hyperliquid node data is available"
        exit 1
    fi

    # Check for required data sources
    local missing=0
    for dir in replica_cmds node_fills_by_block node_order_statuses_by_block misc_events_by_block; do
        if [ ! -d "${DATA_DIR}/${dir}" ]; then
            log_warning "Missing data source: ${dir}"
            missing=$((missing + 1))
        else
            local file_count=$(find "${DATA_DIR}/${dir}" -type f 2>/dev/null | wc -l)
            log_info "Found ${dir}: ${file_count} files"
        fi
    done

    if [ $missing -eq 4 ]; then
        log_error "No data sources found in ${DATA_DIR}"
        exit 1
    fi

    # Check for protobuf compiler
    if ! command -v protoc &> /dev/null; then
        log_warning "protoc not found - will try to build anyway"
    fi

    # Check for jq (for analysis)
    if ! command -v jq &> /dev/null; then
        log_warning "jq not found - analysis will be limited"
    fi

    # Check for Python 3 (for detailed analysis)
    if ! command -v python3 &> /dev/null; then
        log_warning "python3 not found - detailed analysis will be skipped"
    fi

    log_success "Prerequisites check complete"
}

setup_test_environment() {
    log_step "Setting Up Test Environment"

    # Clean up old test artifacts
    log_info "Cleaning previous test artifacts..."
    rm -rf "${TEST_DIR}"
    rm -f "${CHECKPOINT_DB}"*

    # Create test directories
    mkdir -p "${TEST_DIR}"
    mkdir -p "${OUTPUT_DIR}"

    # Create test configuration
    log_info "Creating test configuration..."
    cat > "${TEST_CONFIG}" <<EOF
[node]
node_id = "self-test"
data_dir = "${DATA_DIR}"

[watcher]
watch_paths = [
    "replica_cmds",
    "node_fills_by_block",
    "node_order_statuses_by_block",
    "misc_events_by_block"
]
poll_interval_ms = 100
skip_historical = true  # Production mode: skip historical data, process only new arrivals

[sorter]
endpoint = "http://127.0.0.1:50051"
batch_size = 100

[checkpoint]
db_path = "${CHECKPOINT_DB}"
EOF

    log_success "Test environment ready"
}

build_project() {
    log_step "Building Project"

    log_info "Running cargo build --release..."
    if cargo build --release --quiet 2>&1 | tee "${TEST_DIR}/build.log"; then
        log_success "Build successful"
    else
        log_error "Build failed - see ${TEST_DIR}/build.log"
        exit 1
    fi

    # Verify binaries exist
    if [ ! -f "target/release/hl-agent" ]; then
        log_error "hl-agent binary not found"
        exit 1
    fi

    if [ ! -f "target/release/examples/mock_sorter" ]; then
        log_error "mock_sorter binary not found"
        exit 1
    fi

    if [ ! -f "target/release/ws-agent" ]; then
        log_warning "ws-agent binary not found - WebSocket agent tests will be skipped"
    fi

    log_success "Binaries ready"
}

run_ws_agent_briefly() {
    log_step "Running WebSocket Agent (ws-agent)"

    if [ ! -f "target/release/ws-agent" ]; then
        log_warning "ws-agent not found - skipping"
        return
    fi

    # Check if Redis is available
    if ! command -v redis-cli &> /dev/null; then
        log_warning "redis-cli not found - skipping ws-agent test"
        return
    fi

    if ! redis-cli ping &> /dev/null; then
        log_warning "Redis not running - skipping ws-agent test"
        return
    fi

    log_info "Starting ws-agent to stream blocks from Hyperliquid Explorer WebSocket..."
    log_info "ws-agent will connect to wss://rpc.hyperliquid.xyz/ws and store blocks in Redis"

    # Run ws-agent for 10 seconds to verify it connects and receives blocks
    local ws_log="${TEST_DIR}/ws-agent.log"
    timeout 10 ./target/release/ws-agent > "${ws_log}" 2>&1 &
    local ws_pid=$!

    log_info "ws-agent started (PID: $ws_pid), waiting 10 seconds..."
    sleep 10

    # Check if ws-agent ran successfully (it should have received some blocks)
    if wait $ws_pid 2>/dev/null; then
        log_info "ws-agent completed (timeout reached)"
    fi

    # Check if blocks were stored in Redis
    local block_count=$(redis-cli keys "block:*" 2>/dev/null | wc -l)
    if [ "$block_count" -gt 0 ]; then
        log_success "ws-agent verified: ${block_count} blocks stored in Redis"

        # Show a sample block
        local sample_key=$(redis-cli keys "block:*" 2>/dev/null | head -1)
        if [ -n "$sample_key" ]; then
            log_info "Sample block: $sample_key"
            redis-cli get "$sample_key" 2>/dev/null | head -c 200
            echo ""
        fi
    else
        # Check log for any connection errors
        if grep -q "Connected to Explorer WebSocket" "${ws_log}" 2>/dev/null; then
            log_success "ws-agent connected to WebSocket (no blocks received yet - may need more time)"
        elif grep -q "Failed to connect" "${ws_log}" 2>/dev/null; then
            log_warning "ws-agent failed to connect to WebSocket"
            tail -10 "${ws_log}"
        else
            log_warning "ws-agent status unknown - check ${ws_log}"
        fi
    fi
}

clean_previous_state() {
    log_step "Cleaning Previous State"

    log_info "Removing mock output directory..."
    rm -rf "${OUTPUT_DIR}"
    mkdir -p "${OUTPUT_DIR}"

    log_info "Removing checkpoint database..."
    rm -f "${CHECKPOINT_DB}"*

    log_success "Previous state cleaned"
}

start_mock_sorter() {
    log_step "Starting Mock Sorter"

    log_info "Launching mock gRPC server..."
    RUST_LOG=info ./target/release/examples/mock_sorter \
        --listen-addr 127.0.0.1:50051 \
        --stats-interval-ms 5000 \
        --output-dir "${OUTPUT_DIR}" \
        > "${MOCK_LOG}" 2>&1 &

    local mock_pid=$!

    # Wait for mock sorter to start
    log_info "Waiting for mock sorter to initialize..."
    sleep 3

    # Check if still running
    if ! kill -0 $mock_pid 2>/dev/null; then
        log_error "Mock sorter failed to start"
        cat "${MOCK_LOG}"
        exit 1
    fi

    log_success "Mock sorter running (PID: $mock_pid)"
}

start_agent() {
    log_step "Starting hl-agent"

    log_info "Launching agent with test configuration (RUST_LOG=${AGENT_RUST_LOG})..."
    HL_AGENT_CONFIG="${TEST_CONFIG}" \
    RUST_LOG="${AGENT_RUST_LOG}" \
    ./target/release/hl-agent \
        > "${AGENT_LOG}" 2>&1 &

    local agent_pid=$!

    # Wait for agent to start
    log_info "Waiting for agent to initialize..."
    sleep 3

    # Check if still running
    if ! kill -0 $agent_pid 2>/dev/null; then
        log_error "Agent failed to start"
        tail -50 "${AGENT_LOG}"
        exit 1
    fi

    log_success "Agent running (PID: $agent_pid)"
}

monitor_progress() {
    log_step "Running Test (${TEST_DURATION} seconds)"

    local start_time=$(date +%s)
    local end_time=$((start_time + TEST_DURATION))

    log_info "Test will run until $(date -d @${end_time} '+%H:%M:%S')"
    log_info "Press Ctrl+C to stop early"
    echo ""

    # Monitor progress
    local last_batch_count=0
    while [ $(date +%s) -lt $end_time ]; do
        sleep 5

        # Count batches
        local batch_count=$(find "${OUTPUT_DIR}" -name "batch-*.json" 2>/dev/null | wc -l)
        local new_batches=$((batch_count - last_batch_count))
        last_batch_count=$batch_count

        # Calculate remaining time
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))
        local remaining=$((end_time - current_time))

        # Show progress
        echo -ne "\r${BLUE}Progress:${NC} ${elapsed}s / ${TEST_DURATION}s | "
        echo -ne "${GREEN}Batches:${NC} ${batch_count} (+${new_batches}/5s) | "
        echo -ne "${YELLOW}Remaining:${NC} ${remaining}s    "
    done

    echo ""
    log_success "Test duration completed"
}

print_mock_server_stats() {
    log_step "Mock Server Statistics"

    if [ ! -f "${MOCK_LOG}" ]; then
        log_warning "Mock server log not found"
        return
    fi

    # Strip ANSI codes and get the last stats line from mock sorter
    local last_stats=$(sed 's/\x1b\[[0-9;]*m//g' "${MOCK_LOG}" | grep "sorter stats" | tail -1)

    if [ -z "$last_stats" ]; then
        log_warning "No stats found in mock server log"
        return
    fi

    # Parse and display key metrics from the stats line
    echo ""
    log_info "Final mock sorter statistics:"
    echo ""

    # Extract metrics using grep/sed (after stripping ANSI codes)
    local total_batches=$(echo "$last_stats" | grep -oP 'total_batches=\K[0-9]+' || echo "N/A")
    local total_records=$(echo "$last_stats" | grep -oP 'total_records=\K[0-9]+' || echo "N/A")
    local total_bytes=$(echo "$last_stats" | grep -oP 'total_bytes=\K[0-9]+' || echo "N/A")
    local records_per_sec=$(echo "$last_stats" | grep -oP 'records_per_sec=\K[0-9.]+' || echo "N/A")
    local mb_per_sec=$(echo "$last_stats" | grep -oP 'mb_per_sec=\K[0-9.]+' || echo "N/A")
    local unique_tx=$(echo "$last_stats" | grep -oP 'unique_tx=\K[0-9]+' || echo "N/A")
    local unique_block_heights=$(echo "$last_stats" | grep -oP 'unique_block_heights=\K[0-9]+' || echo "N/A")
    local invalid_records=$(echo "$last_stats" | grep -oP 'invalid_records=\K[0-9]+' || echo "N/A")
    local topics_coverage=$(echo "$last_stats" | grep -oP 'topics_coverage="?\K[^" ]+' || echo "N/A")
    local topic_totals=$(echo "$last_stats" | grep -oP 'topic_totals=\K[^ ]+.*topic_rates' | sed 's/ *topic_rates$//' || echo "N/A")

    # Format bytes to MB
    if [ "$total_bytes" != "N/A" ] && [ "$total_bytes" -gt 0 ] 2>/dev/null; then
        local total_mb=$(echo "scale=2; $total_bytes / 1048576" | bc)
        total_bytes="${total_mb} MB"
    fi

    echo -e "  ${GREEN}Throughput:${NC}"
    echo "    Total Batches:     ${total_batches}"
    echo "    Total Records:     ${total_records}"
    echo "    Total Data:        ${total_bytes}"
    echo "    Records/sec:       ${records_per_sec}"
    echo "    MB/sec:            ${mb_per_sec}"
    echo ""
    echo -e "  ${GREEN}Data Quality:${NC}"
    echo "    Unique Tx Hashes:  ${unique_tx}"
    echo "    Unique Blocks:     ${unique_block_heights}"
    echo "    Invalid Records:   ${invalid_records}"
    echo ""
    echo -e "  ${GREEN}Topic Coverage:${NC} ${topics_coverage}"
    echo "    ${topic_totals}"
    echo ""

    # Show last few stats lines for trend
    log_info "Recent stats history (last 5 intervals):"
    sed 's/\x1b\[[0-9;]*m//g' "${MOCK_LOG}" | grep "sorter stats" | tail -5 | while read line; do
        local ts=$(echo "$line" | grep -oP '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}' || echo "")
        local coverage=$(echo "$line" | grep -oP 'topics_coverage="?\K[^" ]+' || echo "?/?")
        local rps=$(echo "$line" | grep -oP 'records_per_sec=\K[0-9.]+' || echo "0")
        local batches=$(echo "$line" | grep -oP 'total_batches=\K[0-9]+' || echo "0")
        echo "    [${ts}] coverage=${coverage} records/s=${rps} batches=${batches}"
    done
    echo ""
}

analyze_results() {
    log_step "Analyzing Results"

    # Print mock server stats first
    print_mock_server_stats

    # Count total batches
    local total_batches=$(find "${OUTPUT_DIR}" -name "batch-*.json" 2>/dev/null | wc -l)
    log_info "Total batches produced: ${total_batches}"

    if [ $total_batches -eq 0 ]; then
        log_error "No batches produced - test failed"
        log_info "Check logs:"
        log_info "  Mock sorter: ${MOCK_LOG}"
        log_info "  Agent: ${AGENT_LOG}"
        exit 1
    fi

    # Use Python for detailed analysis if available
    if command -v python3 &> /dev/null; then
        log_info "Running detailed analysis..."

        python3 <<'PYTHON_SCRIPT' > "${TEST_DIR}/analysis.json"
import json
import os
import sys
from collections import Counter

output_dir = os.environ.get('OUTPUT_DIR', '/tmp/hl-agent-test/output')
total_batches = int(os.environ.get('TOTAL_BATCHES', '0'))

# Sample batches for analysis
sample_interval = max(1, total_batches // 1000)  # Sample ~1000 batches
topics = Counter()
sample_records = {}
total_records = 0

for i in range(0, total_batches, sample_interval):
    batch_file = f"{output_dir}/batch-{i}.json"
    if os.path.exists(batch_file):
        try:
            with open(batch_file) as f:
                data = json.load(f)
                for record in data.get('records', []):
                    topic = record.get('topic')
                    if topic:
                        topics[topic] += 1
                        total_records += 1

                        # Save first sample of each topic
                        if topic not in sample_records:
                            # Decode payload
                            payload_hex = record.get('payload_hex', '')
                            try:
                                payload_bytes = bytes.fromhex(payload_hex)
                                payload = json.loads(payload_bytes.decode('utf-8'))
                            except:
                                payload = None

                            sample_records[topic] = {
                                'topic': topic,
                                'has_payload': payload is not None
                            }
        except Exception as e:
            print(f"Error reading {batch_file}: {e}", file=sys.stderr)

# Output results
result = {
    'total_batches': total_batches,
    'sampled_batches': len([i for i in range(0, total_batches, sample_interval)
                           if os.path.exists(f"{output_dir}/batch-{i}.json")]),
    'total_records_sampled': total_records,
    'topics': dict(topics),
    'sample_records': sample_records
}

print(json.dumps(result, indent=2))
PYTHON_SCRIPT

        # Parse analysis results
        if [ -f "${TEST_DIR}/analysis.json" ]; then
            log_success "Analysis complete"
        else
            log_warning "Analysis failed - continuing with basic checks"
        fi
    else
        log_warning "Skipping detailed analysis (python3 not available)"
    fi
}

verify_topics() {
    log_step "Verifying Topics"

    local success=true

    if [ -f "${TEST_DIR}/analysis.json" ]; then
        # Extract topics from analysis
        local topics=$(python3 -c "import json; data=json.load(open('${TEST_DIR}/analysis.json')); print(' '.join(sorted(data['topics'].keys())))")

        log_info "Topics found: ${topics}"

        # Check for 5 required topics (always present)
        local required_topics=("hl.blocks" "hl.transactions" "hl.fills" "hl.trades" "hl.orders")
        for topic in "${required_topics[@]}"; do
            if echo "$topics" | grep -q "$topic"; then
                log_success "✓ ${topic}"
            else
                log_error "✗ ${topic} - NOT FOUND"
                success=false
            fi
        done

        # Check for optional topic (misc_events written hourly, may not appear with skip_historical=true)
        if echo "$topics" | grep -q "hl.misc_events"; then
            log_success "✓ hl.misc_events (optional - hourly writes)"
        else
            log_warning "○ hl.misc_events - NOT FOUND (expected with skip_historical=true in short tests)"
        fi
    else
        # Basic verification using jq if available
        if command -v jq &> /dev/null; then
            log_info "Performing basic topic check..."

            local found_topics=$(find "${OUTPUT_DIR}" -name "batch-*.json" | head -100 | \
                xargs jq -r '.records[].topic' 2>/dev/null | sort -u)

            log_info "Topics found in first 100 batches:"
            echo "$found_topics" | while read topic; do
                log_info "  - ${topic}"
            done
        else
            log_warning "Cannot verify topics (jq not available)"
        fi
    fi

    if [ "$success" = false ]; then
        log_error "Topic verification failed - not all required topics found"
        return 1
    fi

    log_success "All 5 required topics verified (misc_events optional with skip_historical=true)"
    return 0
}

generate_report() {
    log_step "Generating Test Report"

    cat > "${REPORT_FILE}" <<'EOF'
# Hyperliquid hl-agent Self-Test Report

**Test Date**: $(date)
**Test Duration**: ${TEST_DURATION} seconds
**Status**:
EOF

    # Add status
    if [ -f "${TEST_DIR}/analysis.json" ]; then
        echo "✅ **PASSED**" >> "${REPORT_FILE}"
    else
        echo "⚠️ **COMPLETED WITH WARNINGS**" >> "${REPORT_FILE}"
    fi

    cat >> "${REPORT_FILE}" <<EOF

---

## Test Configuration

- **Data Directory**: ${DATA_DIR}
- **Output Directory**: ${OUTPUT_DIR}
- **Checkpoint Database**: ${CHECKPOINT_DB}
- **Mock Sorter Log**: ${MOCK_LOG}
- **Agent Log**: ${AGENT_LOG}

---

## Results Summary

EOF

    if [ -f "${TEST_DIR}/analysis.json" ]; then
        ANALYSIS_FILE="${TEST_DIR}/analysis.json" python3 <<'PYTHON_REPORT'
import json
import os

with open(os.environ['ANALYSIS_FILE']) as f:
    data = json.load(f)

print(f"**Total Batches**: {data['total_batches']:,}")
print(f"**Sampled Batches**: {data['sampled_batches']:,}")
print(f"**Records Sampled**: {data['total_records_sampled']:,}")
print(f"**Estimated Total Records**: ~{data['total_records_sampled'] * (data['total_batches'] // max(1, data['sampled_batches'])):,}")
print()
print("### Topic Distribution")
print()
print("| Topic | Records | Percentage |")
print("|-------|---------|------------|")

topics = data['topics']
total = sum(topics.values())
for topic in sorted(topics.keys()):
    count = topics[topic]
    pct = (count / total * 100) if total > 0 else 0
    print(f"| {topic} | {count:,} | {pct:.2f}% |")

print()
print("### Sample Records")
print()
for topic in sorted(data['sample_records'].keys()):
    sample = data['sample_records'][topic]
    print(f"**{topic}**:")
    print(f"- Payload Decoded: {'✅' if sample['has_payload'] else '❌'}")
    print()
PYTHON_REPORT
    fi >> "${REPORT_FILE}"

    cat >> "${REPORT_FILE}" <<EOF

---

## Logs

### Agent Log (last 50 lines)
\`\`\`
$(tail -50 "${AGENT_LOG}")
\`\`\`

### Mock Sorter Log (last 30 lines)
\`\`\`
$(tail -30 "${MOCK_LOG}")
\`\`\`

---

## Files Generated

- Test Report: ${REPORT_FILE}
- Analysis Data: ${TEST_DIR}/analysis.json
- Batch Files: ${OUTPUT_DIR}/batch-*.json

---

**End of Report**
EOF

    log_success "Report generated: ${REPORT_FILE}"
}

show_summary() {
    log_step "Test Summary"

    echo ""
    if [ -f "${TEST_DIR}/analysis.json" ]; then
        ANALYSIS_FILE="${TEST_DIR}/analysis.json" python3 <<'PYTHON_SUMMARY'
import json
import os

with open(os.environ['ANALYSIS_FILE']) as f:
    data = json.load(f)

print(f"  Total Batches:    {data['total_batches']:,}")
print(f"  Topics Found:     {len(data['topics'])}/6")
print(f"  Records Sampled:  {data['total_records_sampled']:,}")
print()
print("  Topic Breakdown:")
topics = data['topics']
for topic in sorted(topics.keys()):
    print(f"    - {topic}: {topics[topic]:,} records")
PYTHON_SUMMARY
    fi

    echo ""
    log_info "Test artifacts:"
    log_info "  Report:  ${REPORT_FILE}"
    log_info "  Output:  ${OUTPUT_DIR}"
    log_info "  Logs:    ${TEST_DIR}/*.log"
    echo ""

    if [ -f "${TEST_DIR}/analysis.json" ]; then
        local topic_count=$(python3 -c "import json; print(len(json.load(open('${TEST_DIR}/analysis.json'))['topics']))")
        if [ "$topic_count" -eq 6 ]; then
            log_success "✅ TEST PASSED - All 6 topics verified"
            return 0
        else
            log_warning "⚠️  TEST INCOMPLETE - Only ${topic_count}/6 topics found"
            return 1
        fi
    else
        log_warning "⚠️  TEST COMPLETED - Manual verification recommended"
        return 1
    fi
}

#############################################################################
# Main Test Flow
#############################################################################

main() {
    log_step "Hyperliquid hl-agent Self-Test"
    log_info "Test mode: $([ $TEST_DURATION -eq 30 ] && echo 'Quick (30s)' || echo "Full (${TEST_DURATION}s)")"

    check_prerequisites
    setup_test_environment
    build_project

    # Step 1: Run ws-agent briefly to verify it works
    run_ws_agent_briefly

    # Step 2: Clean previous state (mock output + checkpoint)
    clean_previous_state

    # Step 3: Start mock server and hl-agent
    start_mock_sorter
    start_agent

    # Export for Python scripts
    export OUTPUT_DIR
    export TOTAL_BATCHES=0

    monitor_progress

    # Stop processes before analysis
    log_info "Stopping test processes..."
    pkill -f "hl-agent" 2>/dev/null || true
    pkill -f "mock_sorter" 2>/dev/null || true
    sleep 2

    # Update batch count for analysis
    export TOTAL_BATCHES=$(find "${OUTPUT_DIR}" -name "batch-*.json" 2>/dev/null | wc -l)

    analyze_results
    verify_topics
    generate_report

    show_summary
}

# Run main function
main
