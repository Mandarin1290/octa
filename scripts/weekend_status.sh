#!/bin/bash
# Weekend screening status — run anytime
echo "=== Weekend Screening Status ($(date -u '+%Y-%m-%d %H:%M UTC')) ==="
echo ""

python3 - <<'PYEOF'
import glob, json, os

evidence_base = 'octa/var/evidence'

candidates = []
pass_1d = []
screened = set()

for result_json in glob.glob(evidence_base + '/universe_screen*/results/*.json'):
    sym = os.path.basename(result_json).replace('.json', '')
    screened.add(sym)
    try:
        d = json.load(open(result_json))
        pr = d.get('paper_ready', False)
        stages = d.get('stages', [])
        h1_pass = any(isinstance(s, dict) and s.get('timeframe') == '1H' and s.get('status') == 'PASS' for s in stages)
        if pr and h1_pass:
            candidates.append(sym)
        elif d.get('artifact_summary', {}).get('valid_tradeable_artifacts', 0) > 0:
            pass_1d.append(sym)
    except:
        pass

total_q = 0
remaining = 0
try:
    q = json.load(open('octa/var/screening_queue.json'))['queue']
    total_q = len(q)
    remaining = len([s for s in q if s not in screened])
except:
    pass

running = 0
for pre in glob.glob(evidence_base + '/universe_screen*/pre_manifest.json'):
    if not os.path.exists(pre.replace('pre_manifest.json', 'summary.json')):
        running += 1

print(f"Candidates (1D+1H): {len(set(candidates))} / 5 needed")
for s in sorted(set(candidates)):
    print(f"  ✅ {s}")
print(f"")
print(f"Screened: {len(screened)} / {total_q} ({total_q - remaining} done, {remaining} remaining)")
print(f"Running batches: {running}")
print(f"1D-only near-misses: {len(set(pass_1d))} → {sorted(set(pass_1d))[:10]}")
PYEOF

echo ""
echo "=== Screening loop ==="
pgrep -f "run_universe_screening" > /dev/null && echo "✅ RUNNING (PID=$(pgrep -f run_universe_screening))" || echo "❌ NOT RUNNING"

echo ""
echo "=== Last log lines ==="
tail -8 /tmp/weekend_screening_main.log 2>/dev/null
