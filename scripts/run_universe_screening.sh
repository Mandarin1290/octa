#!/usr/bin/env bash
# Continuous Universe Screening Loop
# Fires batches of BATCH_SIZE symbols from screening_queue.json
# Runs MAX_PARALLEL batches simultaneously
# Stops when STOP_THRESHOLD 1D+1H candidates found
#
# Usage: bash scripts/run_universe_screening.sh [--dry-run]

set -euo pipefail
cd "$(dirname "$0")/.."

QUEUE_FILE="octa/var/screening_queue.json"
PROGRESS_FILE="octa/var/screening_progress.json"
EVIDENCE_BASE="octa/var/evidence"
CONFIG="configs/p03_research.yaml"
BATCH_SIZE=25
MAX_PARALLEL=2
STOP_THRESHOLD=5
CHECK_INTERVAL=120   # seconds between checks
LOG_FILE="/tmp/universe_screening_$(date +%Y%m%d).log"

DRY_RUN=false
if [[ "${1:-}" == "--dry-run" ]]; then DRY_RUN=true; fi

log() { echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] $*" >> "$LOG_FILE"; echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] $*"; }

# ── helpers ──────────────────────────────────────────────────────────────────

count_candidates() {
    # Count symbols with paper_ready=True AND 1H stage PASS across ALL evidence dirs.
    # Checks the 1H stage explicitly because paper_ready can be True even when a non-required
    # downstream TF (30M/5M) fails — in which case reason is non-empty but not a blocker.
    python3 - <<'PYEOF'
import glob, json, os

candidates = []
for result_json in glob.glob('octa/var/evidence/universe_screen_*/results/*.json'):
    try:
        d = json.load(open(result_json))
        if not d.get('paper_ready'):
            continue
        # Must have 1H stage with status=PASS (exclude 1D-only promotions)
        stages = d.get('stages', [])
        h1_pass = any(
            isinstance(s, dict) and s.get('timeframe') == '1H' and s.get('status') == 'PASS'
            for s in stages
        )
        if h1_pass:
            sym = os.path.basename(result_json).replace('.json', '')
            candidates.append(sym)
    except:
        pass

print(len(set(candidates)))
PYEOF
}

count_running_batches() {
    # A batch is RUNNING if it has run_manifest.json or pre_manifest.json but NOT summary.json yet
    python3 - <<'PYEOF'
import glob, os

seen = set()
running = 0

# Check pre_manifest (written immediately by fire_batch)
for m in glob.glob('octa/var/evidence/universe_screen_*/pre_manifest.json'):
    batch_dir = os.path.dirname(m)
    if batch_dir in seen:
        continue
    if not os.path.exists(os.path.join(batch_dir, 'summary.json')):
        seen.add(batch_dir)
        running += 1

# Check run_manifest (written by training process; may or may not have pre_manifest)
for m in glob.glob('octa/var/evidence/universe_screen_*/run_manifest.json'):
    batch_dir = os.path.dirname(m)
    if batch_dir in seen:
        continue
    if not os.path.exists(os.path.join(batch_dir, 'summary.json')):
        seen.add(batch_dir)
        running += 1

print(running)
PYEOF
}

get_next_batch() {
    # Returns comma-separated list of BATCH_SIZE next unscreened symbols
    python3 - <<PYEOF
import glob, json, os, sys

queue_file = "$QUEUE_FILE"
evidence_base = "$EVIDENCE_BASE"
batch_size = $BATCH_SIZE

try:
    q = json.load(open(queue_file))['queue']
except:
    sys.exit(1)

# Symbols already processed (have a result file)
done = set()
for f in glob.glob(evidence_base + '/universe_screen_*/results/*.json'):
    done.add(os.path.basename(f).replace('.json',''))

# Running: check pre_manifest.json (written immediately by fire_batch before training starts)
# and run_manifest.json (written by training process, may use different key names).
for pre in glob.glob(evidence_base + '/universe_screen_*/pre_manifest.json'):
    if not os.path.exists(pre.replace('pre_manifest.json', 'summary.json')):
        try:
            m = json.load(open(pre))
            for sym in m.get('symbols', []):
                done.add(sym)
        except: pass
for manifest in glob.glob(evidence_base + '/universe_screen_*/run_manifest.json'):
    if not os.path.exists(manifest.replace('run_manifest.json', 'summary.json')):
        try:
            m = json.load(open(manifest))
            for sym in m.get('symbols_override', m.get('symbols', [])):
                done.add(sym)
        except: pass

remaining = [s for s in q if s not in done]
batch = remaining[:batch_size]
print(','.join(batch) if batch else '')
PYEOF
}

fire_batch() {
    local batch="$1"
    local ts
    ts=$(date -u '+%Y%m%dT%H%M%SZ')
    local run_id="universe_screen_auto_${ts}"
    local out_log="/tmp/${run_id}.log"
    local evidence_dir="${EVIDENCE_BASE}/${run_id}"

    log "Firing batch: run_id=$run_id symbols=$batch"
    if [[ "$DRY_RUN" == "true" ]]; then
        log "[DRY RUN] Would run: python -m octa.support.ops.run_training --config $CONFIG --symbols $batch --run-id $run_id"
        return
    fi
    # Write pre_manifest immediately so get_next_batch() excludes these symbols
    # even before the training process writes run_manifest.json.
    mkdir -p "$evidence_dir"
    python3 -c "
import json, sys
syms = '$batch'.split(',')
with open('${evidence_dir}/pre_manifest.json', 'w') as f:
    json.dump({'run_id': '${run_id}', 'symbols': syms, 'fired_at': '${ts}'}, f)
"
    nohup python -m octa.support.ops.run_training \
        --config "$CONFIG" \
        --symbols "$batch" \
        --run-id "$run_id" \
        > "$out_log" 2>&1 &
    log "Fired PID=$! log=$out_log"
}

report_status() {
    python3 - <<'PYEOF'
import glob, json, os
from collections import defaultdict

evidence_base = 'octa/var/evidence'
candidates = []
screened_total = set()
pass_1d = []
fail_both = []
batches = []

for result_json in sorted(glob.glob(evidence_base + '/universe_screen_*/results/*.json')):
    batch = result_json.split('/')[3]
    sym = os.path.basename(result_json).replace('.json','')
    screened_total.add(sym)
    try:
        d = json.load(open(result_json))
        pr = d.get('paper_ready', False)
        stages = d.get('stages', [])
        h1_pass = any(
            isinstance(s, dict) and s.get('timeframe') == '1H' and s.get('status') == 'PASS'
            for s in stages
        )
        if pr and h1_pass:
            candidates.append(sym)
        elif d.get('artifact_summary',{}).get('valid_tradeable_artifacts',0) > 0:
            pass_1d.append(sym)
        else:
            fail_both.append(sym)
    except: pass

print(f"  Total screened: {len(screened_total)}")
print(f"  1D+1H PASS: {len(set(candidates))} → {sorted(set(candidates))}")
print(f"  1D only:    {len(set(pass_1d))}")
print(f"  Both fail:  {len(set(fail_both))}")
PYEOF
}

# ── main loop ─────────────────────────────────────────────────────────────────

log "=== Universe Screening Loop started ==="
log "Config: $CONFIG | BatchSize: $BATCH_SIZE | MaxParallel: $MAX_PARALLEL | StopAt: $STOP_THRESHOLD"

while true; do
    candidates=$(count_candidates)
    log "Candidates so far: $candidates / $STOP_THRESHOLD"

    if [[ "$candidates" -ge "$STOP_THRESHOLD" ]]; then
        log "🎯 STOP THRESHOLD REACHED — Found $candidates validated 1D+1H candidates!"
        report_status
        log "=== Phase 1 COMPLETE. Run: bash scripts/phase2_prerequisites.sh ==="
        exit 0
    fi

    running=$(count_running_batches)
    log "Running batches: $running / $MAX_PARALLEL"

    while [[ "$running" -lt "$MAX_PARALLEL" ]]; do
        next_batch=$(get_next_batch)
        if [[ -z "$next_batch" ]]; then
            log "Queue exhausted — no more symbols to screen."
            report_status
            log "=== Universe fully screened. Candidates: $candidates ==="
            exit 1
        fi
        fire_batch "$next_batch"
        running=$(( running + 1 ))
        sleep 5   # stagger starts slightly
    done

    log "Status:"
    report_status
    log "Sleeping ${CHECK_INTERVAL}s..."
    sleep "$CHECK_INTERVAL"
done
