**Readiness Dashboard (Terminal + CSV)**

Purpose
- Produce an operator-grade, facts-only snapshot of system readiness derived from the append-only ledger and current in-memory state.

Features
- Terminal summary with: event counts, latest gate events, incident count, margin & capacity summary.
- CSV snapshot for BI ingestion (flat key,value rows).
- Deterministic: ledger events are sorted by timestamp; given same ledger and inputs output is reproducible.

Usage
- Run `python -m octa_reports.readiness <ledger-file> --csv snapshot.csv` to print and write CSV.

Notes
- The module does not assume event schemas. It looks for common event keys: `event`, `type`, `ts` and `margin.evaluation`/`capacity.report` payloads when present.
