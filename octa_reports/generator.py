from __future__ import annotations

import csv
import io
from datetime import datetime
from typing import Dict, List, Tuple

from octa_ledger.performance import (
    max_drawdown,
    periodic_returns_from_prices,
    sharpe,
)
from octa_ledger.store import LedgerStore


def _parse_nav_events(ledger: LedgerStore) -> List[Tuple[str, float]]:
    items = []
    for e in ledger.by_action("performance.nav"):
        p = e.get("payload", {})
        ts = p.get("date") or e.get("timestamp")
        ts_s = str(ts) if ts is not None else ""
        nav = float(p.get("nav", 0.0))
        items.append((ts_s, nav))
    # sort by timestamp
    items.sort(key=lambda x: x[0])
    return items


def monthly_performance_markdown(ledger: LedgerStore) -> Tuple[str, str]:
    """Return (markdown, csv_text) for monthly performance report based on `performance.nav` events."""
    navs = _parse_nav_events(ledger)
    if not navs:
        return ("# Monthly Performance\n\nNo data\n", "date,nav,return\n")

    # group by month YYYY-MM
    by_month: Dict[str, float] = {}
    for ts, nav in navs:
        dt = datetime.fromisoformat(ts)
        key = f"{dt.year:04d}-{dt.month:02d}"
        by_month[key] = nav

    months = sorted(by_month.keys())
    rows = [(m, by_month[m]) for m in months]

    # compute returns between consecutive months
    csv_buf = io.StringIO()
    writer = csv.writer(csv_buf)
    writer.writerow(["month", "nav", "return"])
    md_lines = [
        "# Monthly Performance",
        "",
        "| Month | NAV | Return |",
        "|---:|---:|---:|",
    ]
    prev = None
    for m, nav in rows:
        r = ""
        if prev is not None:
            r = f"{(nav / prev - 1.0):.6f}"
        writer.writerow([m, f"{nav:.6f}", r])
        md_lines.append(f"| {m} | {nav:.6f} | {r} |")
        prev = nav

    return ("\n".join(md_lines), csv_buf.getvalue())


def risk_report_markdown(ledger: LedgerStore) -> str:
    navs = _parse_nav_events(ledger)
    if not navs:
        return "# Risk Report\n\nNo data\n"
    prices = [nav for _, nav in navs]
    returns = periodic_returns_from_prices(prices)
    md = ["# Risk Report", ""]
    dd, dur = max_drawdown(prices)
    md.append(f"- Max drawdown: {dd:.6f} ({dur} periods)")
    md.append(f"- Annualized Sharpe (approx): {sharpe(returns):.6f}")
    vol = 0.0
    if returns:
        mu = sum(returns) / len(returns)
        var = sum((r - mu) ** 2 for r in returns) / len(returns)
        vol = var**0.5
    md.append(f"- Volatility (period): {vol:.6f}")
    return "\n".join(md)


def incident_summary_markdown(ledger: LedgerStore) -> str:
    incs = ledger.by_action("incident.created")
    if not incs:
        return "# Incident Summary\n\nNo incidents\n"
    lines = ["# Incident Summary", ""]
    for e in incs:
        p = e.get("payload", {})
        iid = e.get("event_id")
        ts = e.get("timestamp")
        typ = p.get("type")
        sev = p.get("severity")
        title = p.get("title")
        lines.append(f"- {ts} | {iid} | {typ} | severity={sev} | {title}")
        # include timeline notes
        notes = ledger.by_action("incident.timeline")
        for n in notes:
            npay = n.get("payload", {})
            if npay.get("incident_id") == iid:
                lines.append(f"  - {n.get('timestamp')}: {npay.get('note')}")
    return "\n".join(lines)


def strategy_contribution_table(ledger: LedgerStore) -> Tuple[str, str]:
    # look for trade.executed events with payload containing strategy_id and pnl
    trades = ledger.by_action("trade.executed")
    contrib: Dict[str, float] = {}
    total = 0.0
    for t in trades:
        p = t.get("payload", {})
        sid = p.get("strategy_id", "unknown")
        pnl = float(p.get("pnl", 0.0))
        contrib[sid] = contrib.get(sid, 0.0) + pnl
        total += pnl

    # CSV
    csv_buf = io.StringIO()
    writer = csv.writer(csv_buf)
    writer.writerow(["strategy_id", "pnl", "pct_of_total"])
    md_lines = [
        "# Strategy Contribution",
        "",
        "| Strategy | PnL | % of Total |",
        "|---|---:|---:|",
    ]
    for sid, pnl in sorted(contrib.items(), key=lambda x: -x[1]):
        pct = (pnl / total * 100.0) if total != 0 else 0.0
        writer.writerow([sid, f"{pnl:.6f}", f"{pct:.4f}"])
        md_lines.append(f"| {sid} | {pnl:.6f} | {pct:.4f}% |")

    return ("\n".join(md_lines), csv_buf.getvalue())


def generate_all_reports(ledger: LedgerStore) -> Dict[str, Tuple[str, str]]:
    """Return a dict of report_name -> (markdown, csv) where csv may be empty string."""
    mp_md, mp_csv = monthly_performance_markdown(ledger)
    risk_md = risk_report_markdown(ledger)
    inc_md = incident_summary_markdown(ledger)
    strat_md, strat_csv = strategy_contribution_table(ledger)
    return {
        "monthly_performance": (mp_md, mp_csv),
        "risk": (risk_md, ""),
        "incidents": (inc_md, ""),
        "strategy_contribution": (strat_md, strat_csv),
    }


__all__ = [
    "monthly_performance_markdown",
    "risk_report_markdown",
    "incident_summary_markdown",
    "strategy_contribution_table",
    "generate_all_reports",
]
