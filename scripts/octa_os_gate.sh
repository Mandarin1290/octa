#!/usr/bin/env bash
set -euo pipefail

python -m compileall -q .
pytest -q tests/test_octa_os_brain.py
ruff check octa/os scripts/octa_os_start.py scripts/octa_os_stop.py tests/test_octa_os_brain.py
