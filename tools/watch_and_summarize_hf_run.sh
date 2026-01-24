#!/usr/bin/env bash
set -euo pipefail

run_id="$1"
runner_pid="${2:-}"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
log_dir="$repo_root/reports/training_30m/$run_id"

cd "$repo_root"

python_bin="python3"
if [[ -x "/home/n-b/Octa/.venv/bin/python" ]]; then
    python_bin="/home/n-b/Octa/.venv/bin/python"
fi

find_runner_pid() {
    local run_id="$1"
    local pid_file="$log_dir/runner.pid"
    if [[ -n "${runner_pid:-}" ]]; then
        echo "$runner_pid"
        return 0
    fi
    if [[ -f "$pid_file" ]]; then
        cat "$pid_file"
        return 0
    fi

    # Try to find the batch runner process by its command line.
    # Example cmd: python scripts/run_batch_train_multiframe.py ... --run-id <run_id>
    ps -eo pid=,cmd= \
        | grep -F "scripts/run_batch_train_multiframe.py" \
        | grep -F -- "--run-id $run_id" \
        | grep -v grep \
        | awk 'NR==1{print $1}'
}

is_pid_alive() {
    local pid="$1"
    if [[ -z "$pid" ]]; then
        return 1
    fi
    kill -0 "$pid" 2>/dev/null
}

runner_pid="$(find_runner_pid "$run_id" || true)"
if [[ -n "$runner_pid" ]]; then
    echo "$runner_pid" > "$log_dir/runner.pid" || true
    echo "runner_pid=$runner_pid"
    while is_pid_alive "$runner_pid"; do
        echo "runner_alive=1"; sleep 30
    done
    echo "runner_alive=0"
else
    echo "runner_pid not found; falling back to pids.json monitoring" >&2
fi

# After the runner exits, wait until no per-symbol training processes remain.
while true; do
    remaining=$(ps -eo cmd= | grep -F "scripts/train_multiframe_symbol.py" | grep -F -- "--run-id $run_id" | grep -v grep | wc -l | tr -d ' ')
    echo "remaining_train_procs=$remaining"
    if [[ "$remaining" == "0" ]]; then
        break
    fi
    sleep 30
done

"$python_bin" /home/n-b/Octa/scripts/summarize_training_30m_run.py --run-id "$run_id"
