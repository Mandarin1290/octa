#!/bin/bash
# Shadow trading status checker — run anytime during 48h loop
# Usage: bash scripts/shadow_status.sh

LOG_DIR="/home/n-b/Octa/octa/var/logs"
MAIN_LOG="/tmp/shadow_trading_main.log"

echo "=== Shadow Trading Status ($(date -u '+%Y-%m-%d %H:%M UTC')) ==="
echo ""

# Loop running?
if pgrep -f "shadow_trading_loop" > /dev/null 2>&1; then
    LOOP_PID=$(pgrep -f "shadow_trading_loop")
    START=$(ps -o lstart= -p "$LOOP_PID" 2>/dev/null | xargs -I{} date -d "{}" +%s 2>/dev/null || echo 0)
    NOW=$(date +%s)
    ELAPSED=$(( (NOW - START) / 3600 ))
    echo "Loop: ✅ RUNNING (PID=$LOOP_PID, elapsed ~${ELAPSED}h)"
else
    echo "Loop: ❌ NOT RUNNING"
fi

# TWS up?
timeout 6 python3 -c "
from ib_insync import IB
ib = IB()
ib.connect('127.0.0.1', 7497, clientId=198, timeout=4, readonly=True)
print('TWS: ✅ UP accounts=' + str(ib.managedAccounts()))
ib.disconnect()
" 2>/dev/null || echo "TWS: ❌ NOT REACHABLE"

echo ""
echo "=== Run Statistics ==="
ALL_LOGS=("$LOG_DIR"/shadow_execution_*.log)
TOTAL=${#ALL_LOGS[@]}
if [ -f "${ALL_LOGS[0]}" ]; then
    SUCCESS=$(grep -rl "Exit code: 0\|✅ All" "$LOG_DIR"/shadow_execution_*.log 2>/dev/null | wc -l)
    # Count logs where run completed (has summary JSON)
    COMPLETED=$(grep -rl '"data_source": "tws"' "$LOG_DIR"/shadow_execution_*.log 2>/dev/null | wc -l)
    FAILED=$(( TOTAL - COMPLETED ))
    echo "Total log files: $TOTAL"
    echo "Completed runs: $COMPLETED"
    echo "Incomplete/failed: $FAILED"
    if [ $COMPLETED -gt 0 ]; then
        RATE=$(( COMPLETED * 100 / TOTAL ))
        echo "Completion rate: ${RATE}%"
    fi
else
    echo "No run logs yet"
fi

echo ""
echo "=== Latest Run ==="
LATEST=$(ls -t "$LOG_DIR"/shadow_execution_*.log 2>/dev/null | head -1)
if [ -n "$LATEST" ]; then
    echo "File: $LATEST"
    python3 /tmp/parse_signal.py "$LATEST" 2>/dev/null || \
    grep -E "last=|signal|confidence|reason" "$LATEST" | tail -5
fi

echo ""
echo "=== Main Loop (last 10 lines) ==="
tail -10 "$MAIN_LOG" 2>/dev/null

echo ""
echo "=== Monitor (last 5 lines) ==="
tail -5 /tmp/shadow_monitor.log 2>/dev/null || echo "(monitor not running)"
