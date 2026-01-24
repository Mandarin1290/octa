**ENGINEERING GATES**

This document describes Tier-1 quality gates enforced by CI and local tooling.

Enforced checks
- Tests: `pytest` must pass. Unit + integration tests included.
- Coverage: minimum `80%` overall. Enforced with `coverage report --fail-under=80` in CI.
- Type checking: `mypy .` must pass.
- Linting: `ruff check .` must pass.
- Formatting: `black --check .` must pass.
- Security: `bandit -r . -lll` must not report high severity issues.
- Import boundaries: project includes tests that assert forbidden import rules (`tests/test_import_rules.py`).

Local developer workflow
1. Install dependencies (recommended in a virtualenv):

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install pre-commit
pre-commit install
```

2. Run checks locally (fast feedback):

```bash
pre-commit run --all-files
pytest -q
coverage run -m pytest && coverage report --fail-under=80
```

Notes
- CI runs on GitHub Actions (`.github/workflows/ci.yml`). The workflow installs dependencies and runs the checks listed above.
- Pre-commit runs `black`, `ruff`, `mypy`, and `bandit` hooks to catch issues before commits.
- If you need to relax the coverage threshold for a PR, discuss with maintainers — the threshold protects against regressions in test coverage.
