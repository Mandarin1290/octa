#!/usr/bin/env bash
set -euo pipefail
for i in 1 2 3 4 5; do
  echo "===== RUN $i ====="
  scripts/tws_e2e.sh || { echo "FAIL at run $i"; exit 1; }
  sleep 2
done
echo "✅ 5/5 clean runs"
