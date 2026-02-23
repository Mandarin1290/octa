# O C T Λ
Institutional Risk-First Quant Architecture

# OCTA OS

## One Entrypoint
- Start: `python scripts/octa_os_start.py --config configs/dev.yaml --policy configs/policy.yaml --mode shadow`
- Stop: `python scripts/octa_os_stop.py`
- Optional one-shot tick: add `--once`.

## Brain Model
- Brain states: `INIT`, `START_SERVICES`, `SENSE`, `RUNBOOK_DECIDE`, `WAIT`, `WAIT_FOR_BROKER`, `WAIT_FOR_ELIGIBLE`, `TRAINING_TICK`, `EXECUTION_TICK`, `COMMIT_PHASE_1`, `COMMIT_PHASE_2`, `COMMIT_SEND`, `RECOVER_BACKOFF`, `HALT`.
- Each tick executes exactly one runbook decision, writes evidence, and appends hash-chain state.
- Default mode is `shadow`.

## Hard Safety
- If no trade-eligible artifacts exist, Brain enters `WAIT_FOR_ELIGIBLE` and execution is not called.
- WAIT-family states do not allow order-send semantics.
- Unknown or risk-unsafe sensors fail closed (`NO ORDER`).
- Live mode is unreachable unless both are true:
  - CLI flag `--arm-live`
  - Valid token file `octa/var/state/live_armed.json` with non-expired `expires_at_utc`

## Eligibility Source of Truth
- Primary source: approved artifacts via `octa.models.approved_loader`.
- Trade eligibility requires approved artifacts for both `1D` and `1H` per symbol.
- Compatibility fallback: `octa/var/state/blessed_models.jsonl` if approved loader is unavailable or empty.

### Preferred Bless/Promote Workflow
- Promote candidate to approved store:
1. `python -m octa.models.ops.promote --candidate <path/to/model.cbm> --symbol AAPL --timeframe 1D --signing-key <path/to/key>`
2. `python -m octa.models.ops.promote --candidate <path/to/model.cbm> --symbol AAPL --timeframe 1H --signing-key <path/to/key>`
- OS then discovers eligibility from approved artifacts.

### Compatibility Blessed Registry (optional)
- Path: `octa/var/state/blessed_models.jsonl`
- Append-only JSONL; latest row per symbol is used.
- Symbol is eligible only when both fields are `PASS`: `performance_1d`, `performance_1h`.

## 2-Phase Commit
- Intent (phase 1): `octa/var/state/order_intents/<order_id>.json`
- Approval (phase 2): `octa/var/state/order_approvals/<order_id>.json`
- Commit send occurs only when:
  - approval is `approved=true`
  - sensors still safe
  - live token is valid at commit time for live mode

## Evidence and Chain
- Tick evidence: `octa/var/evidence/<run_id>/os_tick_<ts>.json`
- Additive identity artifact: `octa/var/evidence/<run_id>/run_identity.json`
- Registry: `octa/var/state/os_registry.json`
- Hash chain: `octa/var/state/os_chain.jsonl`

## OS Gate Definition
- `python -m compileall -q .`
- `pytest -q tests/test_octa_os_brain.py`
- Optional lint scope only:
  - `ruff check octa/os scripts/octa_os_start.py scripts/octa_os_stop.py scripts/octa_autopilot.py scripts/octa_smoke_chain.py tests/test_octa_os_brain.py`
- Convenience helper:
  - `scripts/octa_os_gate.sh`

## Known Baseline Issues (legacy)
- Repo-wide `pytest -q` currently has pre-existing unrelated collection failure in `tests/test_v000_finish_paper_ready_local_only_synth.py` due missing `resolve_parquet_for_symbol_tf` import target.
- Repo-wide `ruff check .` currently fails on pre-existing baseline violations outside OS scope.

## Systemd Autostart
Use `systemd/octa-os.service.example` as template:
1. Copy to `/etc/systemd/system/octa-os.service`
2. `sudo systemctl daemon-reload`
3. `sudo systemctl enable --now octa-os.service`

## Service Wrappers
- `dashboard_service`, `alerts_service`, `broker_service`, `training_service`, `execution_service`
- Capability mapping is in `configs/policy.yaml` and enforced by Brain.
