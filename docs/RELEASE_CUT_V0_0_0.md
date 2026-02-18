# Release Cut v0.0.0 Recovery Report

Date (UTC): 2026-02-18T21:11:17Z
Branch: v000_finish_local_only
Readiness commit: 2c00933999cee7af55ffcc585762efd352ca7652
Existing tag v0.0.0 target: 57b3f8681a9eaab455e77f8df30e9ad3b525129f
New tag used: v0.0.0-rc1

## Safety State (Phase 0)
- `git status --porcelain=v1`: clean before extraction
- `git stash list`: preserved (`stash@{0}: On v000_finish_local_only: WIP before v0.0.0 release cut`)
- `v0.0.0` already existed and pointed to an older commit.

## Path Mapping (Phase 1)
| Intended File | Actual Path in This Repo | Resolution |
|---|---|---|
| Smoke chain CLI + summary writer | `scripts/octa_smoke_chain.py` | Not present in HEAD; restored from `stash@{0}^3` |
| Risk fail-closed test | `tests/test_risk_fail_closed.py` | Not present in HEAD; restored from `stash@{0}^3` |
| Ruff config | `pyproject.toml` | Not present in HEAD; restored from `stash@{0}^3` |
| Release manifest | `octa/var/releases/v0.0.0/manifest.json` | Exists at expected release-guard path; normalized to non-placeholder template |

## Stash Extraction Scope (Phase 2)
Applied only readiness subset from stash untracked parent (`stash@{0}^3`):
- `scripts/octa_smoke_chain.py`
- `tests/test_risk_fail_closed.py`
- `pyproject.toml`

Plus required runtime/test support for recovered risk test:
- `octa/execution/risk_fail_closed.py`
- `octa/execution/risk_fail_closed_harness.py`
- `octa/core/data/quality/series_validator.py` (required by autopilot import path used by smoke chain)

No full stash apply/pop/drop was performed.

## Implemented Readiness Fixes (Phase 3)
- Offline-safe smoke chain mode with explicit `SKIP` + `OFFLINE_SAFE` reason for IBKR/TWS probe steps.
- Deterministic offline-safe run-id suffix via stable SHA256 over config contents + ordered steps + flags + limit.
- Risk test isolation with `OCTA_MODE=dev` fixture scoped to test execution.
- Minimal conservative Ruff policy in `pyproject.toml` (`E`, `F` only).
- Release manifest template retained at expected path with structured null fields and notes (no fake clean-state claim).

## Commands Executed
- `git rev-parse --abbrev-ref HEAD`
- `git status --porcelain=v1`
- `git stash list`
- `git tag --list | grep -E '^v0\.0\.0$' || true`
- `git show-ref --tags -d | grep -E 'refs/tags/v0\.0\.0' || true`
- `git rev-list -n 1 v0.0.0`
- `git stash show --name-status --include-untracked stash@{0}`
- `git show stash@{0}^3:<path> > <path>` for subset files
- `python -m compileall -q .`
- `pytest -q tests/test_risk_fail_closed.py`
- `python scripts/octa_smoke_chain.py --autopilot-config configs/dev.yaml --limit 3 --ibkr-config configs/execution_ibkr.yaml --offline-safe`
- `ruff check .`

## Verification Results (Phase 5)
1. `python -m compileall -q .` -> PASS
2. `pytest -q tests/test_risk_fail_closed.py` -> PASS (2 passed)
3. Offline-safe smoke chain -> PASS, exit 0
   - `ibkr_api_ready` -> `status=SKIP`, `reason=OFFLINE_SAFE`
   - `autopilot_universe_train` -> PASS
4. `tests/test_smoke_chain_parser.py` -> not present in this branch layout
5. `ruff check .` -> FAIL (pre-existing repository-wide issues; 2317 reported)

## Remaining Blockers / Next Steps
- Existing `v0.0.0` tag still points to older commit `57b3f8681a9eaab455e77f8df30e9ad3b525129f`.
- If strict semver release guard is enforced, finalize release by either:
  1. moving/replacing `v0.0.0` to `2c00933999cee7af55ffcc585762efd352ca7652` per release policy, or
  2. promoting `v0.0.0-rc1` to `v0.0.0` via policy-approved retag.
- Keep stash intact until broader WIP reconciliation is explicitly scheduled.
