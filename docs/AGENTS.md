# Repository Guidelines

## Session Start (Strict)
- At the start of every session, read `docs/AGENT_STATE.md` first.
- Follow its Non-negotiables.
- Continue from "Next Steps" unless the user overrides.
- Keep changes minimal, auditable, fail-closed.

## Session Control & State Management (Strict)

### Context Budget Policy
- Monitor "context left".
- When context left ≤ 25% (or user requests), you MUST:
  1) Produce a compact HANDOVER block (≤25 lines) including:
     - current goal
     - changes made (files)
     - evidence (tests / logs / smoke paths)
     - current blockers
     - exact next commands / prompts
  2) If and only if the system state is stable and verified, append a milestone to docs/AGENT_STATE.md.
  3) Instruct the user to restart Codex:
     - `exit`
     - `codex`
     - then paste:
       "Read docs/AGENT_STATE.md and continue from the last HANDOVER / Next Steps."
- NEVER update AGENT_STATE.md during debugging or unstable states.

### AGENT_STATE Update Policy (HARD RULE)
- docs/AGENT_STATE.md MUST be updated automatically ONLY when:
  1) change is fully implemented
  2) verification / tests / smoke runs completed successfully
  3) no active debugging or partial work remains
  4) system is in a stable, consistent state
- NEVER update AGENT_STATE.md during debugging, exploration, failed experiments, or incomplete tasks.
- When updating:
  - append a concise milestone entry
  - include: what changed, files, evidence, next steps
  - NEVER overwrite history.

## Project Structure & Module Organization
- Core training code lives in `octa_training/` (pipelines, evaluation, gates, packaging).
- Ops and orchestration scripts are in `scripts/` (e.g., `scripts/octa_autopilot.py`, `scripts/train_multiframe_symbol.py`).
- Support modules span `octa_core/`, `octa_ops/`, and other domain packages.
- Configs are in `configs/` and `config/` (notably HF defaults and cascade presets).
- Tests live in `tests/` and `octa_tests/`.
- Data and artifacts are in `raw/`, `reports/`, `state/`, `artifacts/`, `mlruns/`.

## Build, Test, and Development Commands
- Set up env:
  - `python -m venv .venv && source .venv/bin/activate`
  - `pip install -r requirements.txt`
- Run training (single symbol):
  - `python -m octa_training.run_train --symbol <SYM> --package --evaluate`
- Run multi-timeframe cascade:
  - `python scripts/train_multiframe_symbol.py --symbol <SYM> --config configs/cascade_hf.yaml`
- Run autopilot (universe to paper):
  - `python scripts/octa_autopilot.py --config configs/autonomous_paper.yaml`
- Tests and linters:
  - `pytest -q`
  - `ruff check .`
  - `black --check .`
  - `mypy .`

## Coding Style & Naming Conventions
- Python codebase; follow PEP 8 and keep types explicit where possible.
- Linters/formatters: `ruff`, `black`, and `mypy` are expected to pass.
- Naming: modules and functions are `snake_case`; classes are `PascalCase`.
- Config files are YAML; keep keys descriptive and align with existing patterns.

## Testing Guidelines
- Primary framework: `pytest` (see `pytest.ini`).
- Tests are named `test_*.py` and functions `test_*`.
- Keep tests deterministic; avoid depending on external services or mutable data.

## Commit & Pull Request Guidelines
- Git history is not available in this workspace, so no enforced commit style is known.
- Recommended: short imperative subject (e.g., "Add strict cascade guard").
- PRs should include: clear description, risk/behavior notes, and relevant test results.

## Security & Configuration Tips
- Prefer `configs/` presets for training; HF defaults are merged automatically.
- Strict cascade is enforced in multi-timeframe runs; 5m/1m are opt-in and exit-only.
