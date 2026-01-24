from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


def load_intent_positions(*, ndjson_path: str) -> Dict[str, float]:
    """Best-effort positions derived from paper intent logs.

    This parses `paper_trade_log.ndjson` written by `octa_ops.autopilot.paper_runner`.

    Important: these are *intent-based* positions (net BUY-SELL qty), not confirmed fills.
    """

    p = Path(ndjson_path)
    if not p.exists() or not p.is_file():
        return {}

    pos: Dict[str, float] = {}
    try:
        with p.open("r", encoding="utf-8") as fh:
            for line in fh:
                s = line.strip()
                if not s:
                    continue
                try:
                    obj = json.loads(s)
                except Exception:
                    continue

                if not isinstance(obj, dict):
                    continue
                if str(obj.get("decision") or "") != "PLACE_PAPER_ORDER":
                    continue

                intent = obj.get("intent")
                if not isinstance(intent, dict):
                    continue

                symbol = str(intent.get("symbol") or "").strip()
                if not symbol:
                    continue

                side = str(intent.get("side") or "").upper().strip()
                try:
                    qty = float(intent.get("qty") or 0.0)
                except Exception:
                    qty = 0.0

                if qty == 0.0:
                    continue

                signed = qty if side == "BUY" else (-qty)
                pos[symbol] = float(pos.get(symbol, 0.0) + signed)
    except Exception:
        return {}

    # Drop near-zeros for readability
    out: Dict[str, float] = {}
    for k, v in pos.items():
        if abs(float(v)) > 1e-12:
            out[k] = float(v)
    return out


def load_registry_positions(
    *,
    registry_root: str = "artifacts",
    statuses_allowlist: Optional[Tuple[str, ...]] = None,
) -> Dict[str, float]:
    """Best-effort net positions from the autopilot registry orders table.

    This is still not a fill-confirmed view, but it is closer to a canonical run ledger
    than parsing intent logs.
    """

    statuses_allowlist = statuses_allowlist or (
        "PENDING",
        "SUBMITTED",
        "SUBMIT",
        "FILLED",
        "PARTIALLY_FILLED",
        "ACCEPTED",
        "ACK",
        "OPEN",
    )

    db = Path(registry_root) / "registry.sqlite3"
    if not db.exists() or not db.is_file():
        return {}

    pos: Dict[str, float] = {}
    try:
        with sqlite3.connect(str(db)) as c:
            c.row_factory = sqlite3.Row
            rows = c.execute("SELECT symbol, side, qty, status FROM orders").fetchall()
            for r in rows:
                symbol = str(r["symbol"] or "").strip()
                if not symbol:
                    continue
                side = str(r["side"] or "").upper().strip()
                try:
                    qty = float(r["qty"] or 0.0)
                except Exception:
                    qty = 0.0
                st = str(r["status"] or "").upper().strip()

                # normalize some common variants from brokers
                if st == "SUBMITTED":
                    st = "SUBMITTED"
                if st == "SUBMIT_FAILED" or st.startswith("REJECT") or st.startswith("BLOCKED"):
                    continue

                if st not in set(statuses_allowlist):
                    continue

                if qty == 0.0:
                    continue
                signed = qty if side == "BUY" else (-qty)
                pos[symbol] = float(pos.get(symbol, 0.0) + signed)
    except Exception:
        return {}

    out: Dict[str, float] = {}
    for k, v in pos.items():
        if abs(float(v)) > 1e-12:
            out[k] = float(v)
    return out


def load_broker_positions(*, broker_mode: Optional[str] = None) -> Dict[str, float]:
    """Best-effort positions from broker adapter account snapshot.

    For sandbox adapters this is expected to be empty.
    """

    mode = str(broker_mode or os.getenv("OCTA_BROKER_MODE") or "sandbox").strip().lower()
    try:
        if mode == "ib_insync":
            from octa_vertex.broker.ibkr_ib_insync import (  # type: ignore
                IBKRIBInsyncAdapter,
                IBKRIBInsyncConfig,
            )

            b = IBKRIBInsyncAdapter(IBKRIBInsyncConfig.from_env())
        else:
            # sandbox adapter
            from octa_vertex.broker.ibkr_contract import IBKRConfig, IBKRContractAdapter

            b = IBKRContractAdapter(IBKRConfig(rate_limit_per_minute=1, supported_instruments=[]))

        snap = b.account_snapshot() or {}
        positions = snap.get("positions")
        if not isinstance(positions, list):
            return {}
        out: Dict[str, float] = {}
        for p in positions:
            if not isinstance(p, dict):
                continue
            sym = str(p.get("symbol") or p.get("instrument") or "").strip()
            if not sym:
                continue
            try:
                qty = float(p.get("qty") if p.get("qty") is not None else p.get("position") or 0.0)
            except Exception:
                qty = 0.0
            if abs(qty) > 1e-12:
                out[sym] = float(qty)
        return out
    except Exception:
        return {}


def load_positions(
    *,
    source: str = "auto",
    paper_log_path: str = "artifacts/paper_trade_log.ndjson",
    registry_root: str = "artifacts",
    broker_mode: Optional[str] = None,
) -> Tuple[Dict[str, float], str]:
    """Load positions with a safe precedence order.

    Returns (positions, source_used).
    """

    s = str(source or "auto").strip().lower()
    if s == "intent":
        return load_intent_positions(ndjson_path=paper_log_path), "intent"
    if s == "registry":
        return load_registry_positions(registry_root=registry_root), "registry"
    if s == "broker":
        return load_broker_positions(broker_mode=broker_mode), "broker"

    # auto
    bp = load_broker_positions(broker_mode=broker_mode)
    if bp:
        return bp, "broker"
    rp = load_registry_positions(registry_root=registry_root)
    if rp:
        return rp, "registry"
    return load_intent_positions(ndjson_path=paper_log_path), "intent"


def parse_json_map(*, s: Optional[str]) -> Dict[str, Any]:
    if not s:
        return {}
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


__all__ = [
    "load_intent_positions",
    "load_registry_positions",
    "load_broker_positions",
    "load_positions",
    "parse_json_map",
]
