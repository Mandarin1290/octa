# Reporting Standard

This document describes the reporting outputs produced by `octa_reports.generator`.

Facts-only: all reports are reproducible from ledger audit events and artifacts. No subjective language.

Reports
-------
- Monthly Performance: generated from `performance.nav` audit events; outputs a Markdown table and CSV of month,NAV,return.
- Risk Report: computes max drawdown, approximate Sharpe and period volatility from NAV series.
- Incident Summary: lists `incident.created` events with timeline notes.
- Strategy Contribution: aggregates `trade.executed` events by `strategy_id` and outputs Markdown + CSV.

Reproducibility
---------------
All reports are deterministic given the ledger contents. To regenerate, replay the ledger and run the generator.
