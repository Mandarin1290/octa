# Monitoring

## Overview
This module provides a lightweight monitoring API and dashboard for the OCTA cascade pipeline.
It reads from existing artifacts:
- DuckDB metrics: `octa/var/metrics/metrics.duckdb`
- Run artifacts: `octa/var/artifacts/runs/<run_id>/...`
- Debug parquets under `runs/<run_id>/reports/`

## API
Run the FastAPI server:

```bash
python -m octa.core.monitoring.api.app
```

Endpoints:
- `/runs/latest`
- `/runs/{run_id}/overview`
- `/runs/{run_id}/layer/{layer}/debug`
- `/runs/{run_id}/candidates`
- `/metrics/query`

## Dashboard
Run Streamlit:

```bash
streamlit run octa/core/monitoring/dashboard/app.py
```

Pages:
- Latest Run Overview
- Layer Drilldown
- Candidates
- Metrics Explorer

## Telegram Notifications
Set the environment variables:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

Notifications are sent on training and monitoring smoke completion.

## Local Env (.env.local)

Create a `.env.local` file in the repo root for local ops defaults:

```
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
TELEGRAM_ALWAYS_SEND=0
OCTA_ALERT_DROP_PCT=0.7
```

Explicit shell exports override `.env.local`.

## Monitoring Smoke

```bash
python -m octa.support.ops.run_monitoring_smoke
```

This reads the latest run, loads metrics, prints a summary, and sends Telegram if env vars are set.
