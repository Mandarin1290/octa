# Daily AltData Refresh Runbook

## One-shot refresh

```bash
python -m octa.support.scheduler.run_daily
```

## Daemon scheduler

```bash
python -m octa.support.scheduler.run_daily --daemon --hour 2 --minute 0
```

## Telegram notifications

```bash
export TELEGRAM_BOT_TOKEN=...
export TELEGRAM_CHAT_ID=...
python -m octa.support.scheduler.run_daily --notify-telegram
```

## Audit logs

- JSON: `octa/var/audit/altdata_refresh/altdata_refresh_<UTC timestamp>.json`
- Summary: `octa/var/audit/altdata_refresh/altdata_refresh_<UTC timestamp>.md`

## Notes

- The scheduler sets `OCTA_DAILY_REFRESH=1` and `OCTA_ALLOW_NET=1` for the refresh job.
- Trading/backtests remain offline-first and do not fetch live network data.
