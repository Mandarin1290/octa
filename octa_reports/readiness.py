"""Readiness dashboard: terminal summary and CSV snapshot reproducible from ledger.

Reads an append-only ledger of JSON events (one JSON object per line) and
produces a facts-only readiness summary and a CSV snapshot for BI ingestion.
"""

from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple


def load_ledger(path: str) -> List[Dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return []
    events: List[Dict[str, Any]] = []
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except Exception:
                # skip malformed lines
                continue

    # sort by timestamp deterministically if present
    def ts_key(ev: Dict[str, Any]):
        t = ev.get("ts") or ev.get("time") or ev.get("timestamp")
        if not isinstance(t, str):
            return datetime.min
        try:
            return datetime.fromisoformat(t.replace("Z", "+00:00"))
        except Exception:
            return datetime.min

    events.sort(key=ts_key)
    return events


def summarize(
    events: List[Dict[str, Any]], current_state: Dict[str, Any] | None = None
) -> Dict[str, Any]:
    # Facts-only aggregation
    counters: Counter = Counter()
    latest_gate = {}
    incidents: List[Dict[str, Any]] = []
    margin = {}
    capacity = {}

    for ev in events:
        et = ev.get("event") or ev.get("type") or ev.get("evt")
        if isinstance(et, dict):
            et = et.get("name")
        if not et:
            continue
        counters[et] += 1
        if et in ("gate_event", "correlation_gate.evaluation", "margin.evaluation"):
            latest_gate[et] = ev
        if (
            et in ("incident", "incident_report", "drill_incident")
            or ev.get("type") == "incident"
        ):
            incidents.append(ev)
        if et == "margin.evaluation" or ev.get("event") == "margin.evaluation":
            margin = ev
        if et == "capacity.report" or ev.get("event") == "capacity.report":
            capacity = ev

    # incorporate provided current_state overrides
    if current_state:
        latest_gate.update(current_state.get("gates", {}))
        if "margin" in current_state:
            margin.update(current_state["margin"])
        if "capacity" in current_state:
            capacity.update(current_state["capacity"])

    # Use last event timestamp for deterministic snapshots when available
    last_ts = None
    if events:
        ev_ts = (
            events[-1].get("ts")
            or events[-1].get("time")
            or events[-1].get("timestamp")
        )
        last_ts = ev_ts

    summary = {
        "timestamp": (
            last_ts if last_ts is not None else datetime.utcnow().isoformat() + "Z"
        ),
        "event_counts": dict(counters),
        "latest_gate_events": latest_gate,
        "incidents_count": len(incidents),
        "incidents": incidents,
        "margin": margin,
        "capacity": capacity,
    }
    return summary


def format_text(summary: Dict[str, Any]) -> str:
    lines = []
    lines.append(f"Readiness snapshot: {summary.get('timestamp')}")
    lines.append(f"Incidents: {summary.get('incidents_count')}")
    lines.append("Event counts:")
    for k, v in sorted(summary.get("event_counts", {}).items()):
        lines.append(f" - {k}: {v}")

    lines.append("Latest gates:")
    for k, ev in sorted(summary.get("latest_gate_events", {}).items()):
        lines.append(f" - {k}: {json.dumps(ev, sort_keys=True)}")

    lines.append("Margin summary:")
    if summary.get("margin"):
        m = summary["margin"]
        lines.append(
            f" - utilization: {m.get('margin_utilization')} headroom: {m.get('headroom')}"
        )
    else:
        lines.append(" - none")

    lines.append("Capacity summary:")
    if summary.get("capacity"):
        c = summary["capacity"]
        lines.append(f" - {json.dumps(c, sort_keys=True)}")
    else:
        lines.append(" - none")

    return "\n".join(lines)


def write_csv(snapshot_path: str, summary: Dict[str, Any]) -> None:
    p = Path(snapshot_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    # Flatten a simple key,value CSV for ingestion
    rows: List[Tuple[str, str]] = []
    rows.append(("timestamp", summary.get("timestamp", "")))
    rows.append(("incidents_count", str(summary.get("incidents_count", 0))))
    for k, v in sorted(summary.get("event_counts", {}).items()):
        rows.append((f"event_count:{k}", str(v)))
    # margin
    if summary.get("margin"):
        m = summary["margin"]
        rows.append(("margin_utilization", str(m.get("margin_utilization", ""))))
        rows.append(("margin_headroom", str(m.get("headroom", ""))))
    # capacity
    if summary.get("capacity"):
        c = summary["capacity"]
        rows.append(("capacity", json.dumps(c, sort_keys=True)))

    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["key", "value"])
        for k, v in rows:
            w.writerow([k, v])


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("ledger", help="path to ledger file (newline JSON)")
    ap.add_argument(
        "--csv", help="output csv snapshot path", default="readiness_snapshot.csv"
    )
    args = ap.parse_args()
    ev = load_ledger(args.ledger)
    s = summarize(ev)
    print(format_text(s))
    write_csv(args.csv, s)
