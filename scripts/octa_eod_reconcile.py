#!/usr/bin/env python3
"""EOD reconciliation: broker positions vs local state, NAV snapshot, ledger chain check.

Runs at 20:05 UTC (16:05 ET) via systemd timer.
Broker is ground truth for position state.

Exit codes:
  0 — reconcile completed (discrepancies may exist but are logged)
  1 — fatal error (broker unreachable, unhandled exception)
"""
from __future__ import annotations

import datetime
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


_LEDGER_DIR = "artifacts/ledger_paper"
_POSITION_STATE_FILE = "position_state.json"


def _now_utc_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _parse_net_liquidation(summary: List[Any]) -> Optional[float]:
    """Extract NetLiquidation value from IBKR accountSummary() list.

    IBKR returns AccountSummaryRow objects; when serialized to dict they have
    'tag' and 'value' keys (ib_insync) or similar. Tries multiple key names
    for robustness across ib_insync versions.
    """
    for item in summary:
        if not isinstance(item, dict):
            continue
        tag = (
            item.get("tag")
            or item.get("Tag")
            or item.get("account_tag")
            or ""
        )
        if str(tag) == "NetLiquidation":
            raw = item.get("value") or item.get("Value")
            if raw is not None:
                try:
                    return float(raw)
                except (ValueError, TypeError):
                    pass
    return None


def run_eod_reconcile(
    *,
    ledger_dir: str = _LEDGER_DIR,
    evidence_dir: Optional[str] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Run EOD reconciliation.

    Returns a result dict. Never raises — errors are captured in result["errors"].
    """
    from octa_ledger.events import AuditEvent
    from octa_ledger.store import LedgerStore

    run_id = f"eod_{datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}"
    ledger = LedgerStore(ledger_dir)
    ev_dir = Path(evidence_dir) if evidence_dir else Path("octa/var/evidence") / run_id
    ev_dir.mkdir(parents=True, exist_ok=True)

    result: Dict[str, Any] = {
        "run_id": run_id,
        "ts": _now_utc_iso(),
        "dry_run": dry_run,
        "broker_available": False,
        "broker_nav": None,
        "local_positions": {},
        "broker_positions": {},
        "discrepancies": [],
        "chain_ok": False,
        "errors": [],
    }

    # 1. Load local position state
    pstate_path = Path(ledger_dir) / _POSITION_STATE_FILE
    local_positions: Dict[str, Any] = {}
    if pstate_path.exists():
        try:
            raw = json.loads(pstate_path.read_text(encoding="utf-8"))
            local_positions = dict(raw.get("positions") or {})
        except Exception as exc:
            result["errors"].append(f"position_state_load_failed:{exc}")
    result["local_positions"] = local_positions

    # 2. Connect to broker (only when OCTA_BROKER_MODE=ib_insync and not dry_run)
    broker_mode = str(os.getenv("OCTA_BROKER_MODE") or "sandbox")
    broker_positions: Dict[str, Any] = {}
    broker_nav: Optional[float] = None

    if broker_mode == "ib_insync" and not dry_run:
        adapter = None
        try:
            from octa_vertex.broker.ibkr_ib_insync import (
                IBKRIBInsyncAdapter,
                IBKRIBInsyncConfig,
            )

            ib_cfg = IBKRIBInsyncConfig.from_env()
            adapter = IBKRIBInsyncAdapter(ib_cfg)
            snap = adapter.account_snapshot()
            broker_positions = adapter.get_positions()
            broker_nav = _parse_net_liquidation(snap.get("summary") or [])
            result["broker_available"] = True
            result["broker_nav"] = broker_nav
        except Exception as exc:
            result["errors"].append(f"broker_connect_failed:{exc}")
        finally:
            if adapter is not None:
                try:
                    adapter.ib.disconnect()
                except Exception:
                    pass

    result["broker_positions"] = broker_positions

    # 3. Compare positions — broker is ground truth
    discrepancies: List[Dict[str, Any]] = []
    all_symbols = set(local_positions.keys()) | set(broker_positions.keys())
    for sym in sorted(all_symbols):
        local_entry = local_positions.get(sym)
        broker_entry = broker_positions.get(sym)
        local_qty = float(local_entry.get("exposure", 0.0) if isinstance(local_entry, dict) else 0.0)
        broker_qty = float(broker_entry.get("qty", 0.0) if isinstance(broker_entry, dict) else 0.0)
        if abs(local_qty - broker_qty) > 1e-6:
            discrepancies.append({
                "symbol": sym,
                "local_qty": local_qty,
                "broker_qty": broker_qty,
                "delta": round(broker_qty - local_qty, 8),
            })
    result["discrepancies"] = discrepancies

    # 4. Write NAV snapshot to ledger (when broker NAV available)
    if broker_nav is not None:
        try:
            ledger.append(AuditEvent.create(
                actor="eod_reconcile",
                action="performance.nav",
                payload={
                    "ts": _now_utc_iso(),
                    "run_id": run_id,
                    "nav": broker_nav,
                    "source": "broker_eod",
                },
                severity="INFO",
            ))
        except Exception as exc:
            result["errors"].append(f"nav_event_write_failed:{exc}")

    # 5. Ledger chain integrity check
    try:
        chain_ok = ledger.verify_chain()
    except Exception as exc:
        chain_ok = False
        result["errors"].append(f"chain_verify_failed:{exc}")
    result["chain_ok"] = chain_ok

    # 6. Write reconcile result event to ledger
    try:
        ledger.append(AuditEvent.create(
            actor="eod_reconcile",
            action="eod_reconcile.result",
            payload={
                "ts": _now_utc_iso(),
                "run_id": run_id,
                "broker_nav": broker_nav,
                "discrepancy_count": len(discrepancies),
                "chain_ok": chain_ok,
                "errors": result["errors"],
            },
            severity="ERROR" if (discrepancies or not chain_ok or result["errors"]) else "INFO",
        ))
    except Exception as exc:
        result["errors"].append(f"result_event_write_failed:{exc}")

    # 7. Write evidence JSON
    try:
        ev_path = ev_dir / "eod_reconcile.json"
        ev_path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    except Exception as exc:
        result["errors"].append(f"evidence_write_failed:{exc}")

    # 8. Per-symbol P&L snapshot (non-blocking — errors captured in result)
    try:
        from octa_accounting.symbol_pnl import compute_symbol_pnl

        pnl_output = Path(ledger_dir) / "symbol_pnl.json"
        pstate_str = str(pstate_path) if pstate_path.exists() else None
        symbol_pnl = compute_symbol_pnl(
            ledger_path=ledger_dir,
            positions_path=pstate_str,
            output_path=str(pnl_output),
        )
        result["symbol_pnl_symbols"] = sorted(symbol_pnl.keys())
    except Exception as exc:
        result["errors"].append(f"symbol_pnl_failed:{exc}")

    # 9. Print summary
    print(
        f"[eod_reconcile] {run_id}"
        f"  nav={broker_nav}"
        f"  discrepancies={len(discrepancies)}"
        f"  chain_ok={chain_ok}",
        flush=True,
    )
    for d in discrepancies:
        print(
            f"  DISCREPANCY: {d['symbol']}"
            f"  local={d['local_qty']}"
            f"  broker={d['broker_qty']}"
            f"  delta={d['delta']:+.6f}",
            flush=True,
        )
    for err in result["errors"]:
        print(f"  ERROR: {err}", file=sys.stderr, flush=True)

    return result


def main() -> int:
    dry_run = os.getenv("OCTA_EOD_DRY_RUN", "").lower() in ("1", "true", "yes")
    try:
        result = run_eod_reconcile(dry_run=dry_run)
    except Exception as exc:
        print(f"[eod_reconcile] FATAL: {exc}", file=sys.stderr, flush=True)
        return 1
    return 0 if not result.get("errors") else 1


if __name__ == "__main__":
    raise SystemExit(main())
