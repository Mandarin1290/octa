#!/usr/bin/env bash
# Wrapper to run the auto pipeline daemon with correct PYTHONPATH and working dir
set -euo pipefail
cd "$(dirname "$0")/.."
export PYTHONPATH="$PWD"
# Use an absolute python path to ensure systemd user service finds the interpreter
PYTHON_EXEC="/home/n-b/miniconda/bin/python"
if [ ! -x "$PYTHON_EXEC" ]; then
	PYTHON_EXEC="$(command -v python || command -v python3)"
fi
exec "$PYTHON_EXEC" scripts/auto_pipeline_daemon.py
