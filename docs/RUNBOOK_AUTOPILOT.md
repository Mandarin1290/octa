# Runbook: Autonomous Universe→Paper (OCTA)

## Daily schedule (cron example)
- 02:00 UTC: run training gates + promote PASS to paper
- 03:00 UTC: run paper runner
- 23:00 UTC: run paper evaluation + produce promotion candidates

## Commands
- Install deps: `pip install -r requirements.txt`
- Run universe→gates→cascade training: `./.venv/bin/python scripts/octa_autopilot.py --config configs/autonomous_paper.yaml`

## Failures
- Any stage error should leave `artifacts/runs/{run_id}/summary.json` and registry entries.
- Fail-closed: missing parquet / missing pkl / missing sha -> no trade.

## Logs / Evidence
- Run outputs: `artifacts/runs/{run_id}/*`
- Registry DB: `artifacts/registry.sqlite3`
- Ledger (paper): `artifacts/ledger_paper/ledger.log`
