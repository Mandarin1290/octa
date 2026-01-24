#!/usr/bin/env bash
set -euo pipefail
REPO=/home/n-b/Octa
PY=$REPO/.venv/bin/python
RUN_ID=stock_alpha_run_001
OUT=$REPO/reports/stock_alpha_run_001.jsonl
LOG=$REPO/reports/stock_alpha_run_001.log
:> "$OUT"
:> "$LOG"
while IFS= read -r sym || [[ -n "$sym" ]]; do
  if [[ -z "$sym" ]]; then
    continue
  fi
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) START $sym" | tee -a "$LOG"
  $PY $REPO/scripts/train_multiframe_symbol.py --symbol "$sym" --include-5m --include-1m --config $REPO/configs/dev.yaml --run-id "$RUN_ID" --mode train 2>&1 | tee -a "$LOG"
  # aggregate decision.json files for this symbol (uppercase norm)
  up=$(echo "$sym" | tr '[:lower:]' '[:upper:]')
  find "$REPO/reports" -path "*/cascade/$RUN_ID/$up/*/decision.json" -type f -print0 | while IFS= read -r -d '' f; do
    $PY - <<PY - "$f" "$RUN_ID" >> "$OUT"
import sys, json
f = sys.argv[1]
run_id = sys.argv[2]
try:
    obj = json.load(open(f))
except Exception as e:
    print(json.dumps({"type":"symbol","asset_profile":"stock","gate_stage":None,"status":"ERR","fail_reasons":[f"json_read_error:{e}"],"symbol":None}))
else:
    out = dict(obj)
    out["type"] = "symbol"
    out["asset_profile"] = "stock"
    out["gate_stage"] = out.get("timeframe")
    out["run_id"] = run_id
    print(json.dumps(out, ensure_ascii=False))
PY
  done
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) DONE $sym" | tee -a "$LOG"
done < $REPO/reports/pass_symbols_stock.txt

echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) BATCH COMPLETE" | tee -a "$LOG"
