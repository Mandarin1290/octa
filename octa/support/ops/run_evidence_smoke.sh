#!/usr/bin/env bash
set -euo pipefail

RUN_ID="${1:-pytest_lanes_$(date -u +%Y%m%dT%H%M%SZ)}"
BASE="octa/var/evidence/$RUN_ID"
FAST="$BASE/fast_not_slow"
SLOW="$BASE/slow"
export FAST SLOW
mkdir -p "$FAST" "$SLOW"

set -o pipefail
PYTHONFAULTHANDLER=1 pytest -q -m "not slow" 2>&1 | tee "$FAST/pytest.txt"
ec_fast=$?
echo "$ec_fast" > "$FAST/exitcode.txt"
python - <<'PY'
import json, os
out=os.environ["FAST"]
text=open(os.path.join(out,"pytest.txt"),encoding="utf-8").read()
failed=[ln.split("FAILED ",1)[1].strip() for ln in text.splitlines() if ln.startswith("FAILED ")]
summary={"lane":"not_slow","pytest_exitcode":int(open(os.path.join(out,"exitcode.txt")).read().strip() or 1),"tests_passed":open(os.path.join(out,"exitcode.txt")).read().strip()=="0","failing_tests":failed}
with open(os.path.join(out,"summary.json"),"w",encoding="utf-8") as f: json.dump(summary,f,ensure_ascii=False,indent=2)
PY
sha256sum "$FAST/summary.json" "$FAST/pytest.txt" "$FAST/exitcode.txt" > "$FAST/hashes.sha256"

PYTHONFAULTHANDLER=1 pytest -q -m "slow" 2>&1 | tee "$SLOW/pytest.txt"
ec_slow=$?
echo "$ec_slow" > "$SLOW/exitcode.txt"
python - <<'PY'
import json, os
out=os.environ["SLOW"]
text=open(os.path.join(out,"pytest.txt"),encoding="utf-8").read()
failed=[ln.split("FAILED ",1)[1].strip() for ln in text.splitlines() if ln.startswith("FAILED ")]
summary={"lane":"slow","pytest_exitcode":int(open(os.path.join(out,"exitcode.txt")).read().strip() or 1),"tests_passed":open(os.path.join(out,"exitcode.txt")).read().strip()=="0","failing_tests":failed}
with open(os.path.join(out,"summary.json"),"w",encoding="utf-8") as f: json.dump(summary,f,ensure_ascii=False,indent=2)
PY
sha256sum "$SLOW/summary.json" "$SLOW/pytest.txt" "$SLOW/exitcode.txt" > "$SLOW/hashes.sha256"

python -V 2>&1 | tee "$BASE/python_version.txt"
pip freeze 2>&1 | tee "$BASE/pip_freeze.txt"
env | sort | grep -E 'OPENGAMMA|REDIS|PYTHON|VIRTUAL_ENV' | tee "$BASE/env_excerpt.txt" || true
echo "DONE BASE=$BASE FAST=$FAST SLOW=$SLOW"
