# Dev Setup

## Install dev dependencies (Poetry)
- `poetry install`
- Optional: `poetry install --with dev`

## Verification commands
- `python -m compileall -q .`
- `pytest -q tests/test_imports.py --maxfail=1 --disable-warnings`
- `ruff check .`
- `black --check .`
- `mypy .`

## Offline environments
- Offline environments require pre-provisioned wheels or a base image with Poetry + dev deps installed.
- Commands (normal environment):
  - `poetry install --with dev`
  - `poetry run black --check .`
  - `poetry run mypy .`
