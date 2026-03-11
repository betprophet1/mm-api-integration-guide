#!/bin/bash
#
# Weekend Test Runner - Shell Wrapper with Auto-Restart
# Keeps the test runner alive for the entire weekend
#

set -e

# Configuration
HOURS=48  # Run for 48 hours (entire weekend)
TEST_DURATION=10  # Each test runs for 10 minutes
SCRIPT_DIR="/Users/tranlam/Documents/GitHub/mm-api-integration-guide"
LOG_DIR="$SCRIPT_DIR/weekend_logs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Create log directory
mkdir -p "$LOG_DIR"

# Log file
WRAPPER_LOG="$LOG_DIR/wrapper_${TIMESTAMP}.log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$WRAPPER_LOG"
}

log "========================================"
log "Weekend Test Runner Starting"
log "Duration: $HOURS hours"
log "Test duration: $TEST_DURATION minutes per test"
log "========================================"

cd "$SCRIPT_DIR"

# Calculate end time
END_TIME=$(date -v+${HOURS}H +%s)

# Counter for restarts
RESTART_COUNT=0
MAX_CONSECUTIVE_FAILURES=5
CONSECUTIVE_FAILURES=0

while [ $(date +%s) -lt $END_TIME ]; do
    REMAINING_HOURS=$(( ($END_TIME - $(date +%s)) / 3600 ))
    
    log ""
    log "Starting test runner (Restart #$RESTART_COUNT)"
    log "Remaining time: $REMAINING_HOURS hours"
    
    # Run the Python script
    python3 weekend_test_runner.py \
        --hours $REMAINING_HOURS \
        --test-duration $TEST_DURATION \
        2>&1 | tee -a "$LOG_DIR/runner_${TIMESTAMP}_${RESTART_COUNT}.log"
    
    EXIT_CODE=$?
    
    if [ $EXIT_CODE -eq 0 ]; then
        log "Test runner completed successfully (exit code 0)"
        CONSECUTIVE_FAILURES=0
    else
        log "Test runner exited with code $EXIT_CODE"
        CONSECUTIVE_FAILURES=$((CONSECUTIVE_FAILURES + 1))
        
        if [ $CONSECUTIVE_FAILURES -ge $MAX_CONSECUTIVE_FAILURES ]; then
            log "ERROR: Too many consecutive failures ($CONSECUTIVE_FAILURES). Stopping."
            exit 1
        fi
    fi
    
    RESTART_COUNT=$((RESTART_COUNT + 1))
    
    # Check if we still have time
    if [ $(date +%s) -ge $END_TIME ]; then
        log "Time limit reached. Stopping."
        break
    fi
    
    # Wait before restart
    WAIT_TIME=60
    log "Waiting $WAIT_TIME seconds before restart..."
    sleep $WAIT_TIME
done

log ""
log "========================================"
log "Weekend Test Runner Finished"
log "Total restarts: $RESTART_COUNT"
log "========================================"

# Generate summary report
log "Generating summary report..."

python3 << 'EOF'
import json
import glob
from datetime import datetime

# Find all progress files
progress_files = glob.glob('weekend_runner_progress_*.json')

if not progress_files:
    print("No progress files found")
    exit(0)

# Load and merge all results
all_results = []
all_markets = set()

for pf in progress_files:
    try:
        with open(pf, 'r') as f:
            data = json.load(f)
            all_results.extend(data.get('test_results', []))
            all_markets.update(data.get('markets_tested', []))
    except:
        pass

# Generate summary
print("\n" + "="*70)
print("WEEKEND TEST SUMMARY")
print("="*70 + "\n")

print(f"Total tests executed: {len(all_results)}")
print(f"Unique markets tested: {len(all_markets)}")

if all_results:
    completed = sum(1 for r in all_results if r.get('status') == 'completed')
    failed = sum(1 for r in all_results if r.get('status') == 'failed')
    timeout = sum(1 for r in all_results if r.get('status') == 'timeout')
    error = sum(1 for r in all_results if r.get('status') == 'error')
    
    print(f"\nStatus breakdown:")
    print(f"  ✅ Completed: {completed} ({completed/len(all_results)*100:.1f}%)")
    print(f"  ❌ Failed: {failed} ({failed/len(all_results)*100:.1f}%)")
    print(f"  ⏰ Timeout: {timeout} ({timeout/len(all_results)*100:.1f}%)")
    print(f"  🔥 Error: {error} ({error/len(all_results)*100:.1f}%)")
    
    # Test type breakdown
    test_types = {}
    for r in all_results:
        test = r.get('test', 'unknown')
        test_types[test] = test_types.get(test, 0) + 1
    
    print(f"\nTests by type:")
    for test, count in sorted(test_types.items()):
        print(f"  {test}: {count}")

print("\n" + "="*70)

EOF

log "Summary report complete"
