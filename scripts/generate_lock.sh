#!/usr/bin/env bash
set -euo pipefail
pip-compile --output-file=requirements-lock.txt requirements-runtime.txt || echo 'pip-compile not available; use pip-tools to generate lockfile'
