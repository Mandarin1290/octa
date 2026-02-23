from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from octa.core.governance.governance_audit import (
    EVENT_EXECUTION_PREFLIGHT,
    EVENT_PORTFOLIO_PREFLIGHT,
    GovernanceAudit,
)
from octa.core.portfolio.preflight import PreflightConfig, run_preflight

from .broker_router import BrokerRouter, BrokerRouterConfig
from .carry import generate_carry_intents, load_json_file, resolve_carry_rates
from .evidence_selection import build_ml_selection
from .notifier import ExecutionNotifier
from .risk_fail_closed import incident_to_dict, safe_decide
from .risk_engine import RiskDecision, RiskEngine, RiskEngineConfig


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True, default=str), encoding="utf-8")


@dataclass(frozen=True)
class ExecutionConfig:
    mode: str = "dry-run"
    asset_class: Optional[str] = None
    max_symbols: int = 0
    evidence_dir: Path = Path("octa") / "var" / "evidence" / "execution_default"
    base_evidence_dir: Path = Path("octa") / "var" / "evidence"
    training_run_id: Optional[str] = None
    loop: bool = False
    cycle_seconds: int = 60
    max_cycles: int = 1
    enable_live: bool = False
    i_understand_live_risk: bool = False
    enable_carry: bool = False
    carry_config_path: Path = Path("octa") / "var" / "config" / "carry_config.json"
    carry_rates_path: Path = Path("octa") / "var" / "config" / "carry_rates.json"
    enable_carry_live: bool = False
    i_understand_carry_risk: bool = False


def _ml_multiplier(level: int) -> float:
    if level <= 0:
        return 1.0
    if level == 1:
        return 1.25
    if level == 2:
        return 1.5
    return 2.0


def _intent_order_id(prefix: str, symbol: str, cycle: int) -> str:
    return f"{prefix}_{symbol}_{cycle}"


def run_execution(cfg: ExecutionConfig) -> Dict[str, Any]:
    cfg.evidence_dir.mkdir(parents=True, exist_ok=True)
    run_log_dir = cfg.evidence_dir
    notifier = ExecutionNotifier(cfg.evidence_dir)
    risk_engine = RiskEngine(RiskEngineConfig())

    gov_audit = GovernanceAudit(run_id=cfg.evidence_dir.name)
    gov_audit.emit(
        EVENT_EXECUTION_PREFLIGHT,
        {
            "mode": cfg.mode,
            "enable_live": cfg.enable_live,
            "max_symbols": cfg.max_symbols,
            "training_run_id": cfg.training_run_id,
            "evidence_dir": str(cfg.evidence_dir),
        },
    )
    carry_cfg = load_json_file(cfg.carry_config_path) if cfg.enable_carry else {}
    supported_instruments = tuple(
        sorted(
            {
                str(x.get("instrument", "")).strip()
                for x in (carry_cfg.get("instruments") if isinstance(carry_cfg.get("instruments"), list) else [])
                if isinstance(x, dict) and str(x.get("instrument", "")).strip()
            }
        )
    )
    broker = BrokerRouter(
        BrokerRouterConfig(
            mode=cfg.mode,
            enable_live=cfg.enable_live,
            i_understand_live_risk=cfg.i_understand_live_risk,
            enable_carry_live=cfg.enable_carry_live,
            i_understand_carry_risk=cfg.i_understand_carry_risk,
            supported_instruments=supported_instruments,
        )
    )

    cycle_count = max(1, int(cfg.max_cycles) if cfg.loop else 1)
    notifier.emit(
        "execution_start",
        {
            "mode": cfg.mode,
            "run_id": cfg.evidence_dir.name,
            "loop": bool(cfg.loop),
            "max_cycles": cycle_count,
        },
    )

    ml_orders: List[Dict[str, Any]] = []
    carry_orders: List[Dict[str, Any]] = []
    blocks: List[Dict[str, Any]] = []
    state: Dict[str, Any] = {"last_rebalance_ts": None}
    last_scaling: Dict[str, int] = {}

    nav = 100000.0
    ml_current_gross = 0.0
    carry_current_gross = 0.0
    carry_current_net = 0.0
    leverage = 1.0

    for cycle_idx in range(1, cycle_count + 1):
        cycle_dir = run_log_dir / f"cycle_{cycle_idx:03d}"
        cycle_dir.mkdir(parents=True, exist_ok=True)
        sel = build_ml_selection(
            evidence_out_dir=cfg.evidence_dir,
            base_evidence_dir=cfg.base_evidence_dir,
            run_id=cfg.training_run_id,
        )
        eligible_rows = list(sel["eligible_rows"])
        if cfg.asset_class:
            eligible_rows = [r for r in eligible_rows if str(r.get("asset_class", "")).lower() == str(cfg.asset_class).lower()]
        eligible_rows = sorted(eligible_rows, key=lambda x: str(x.get("symbol", "")))
        if cfg.max_symbols > 0:
            eligible_rows = eligible_rows[: int(cfg.max_symbols)]

        if not eligible_rows:
            blocks.append({"cycle": cycle_idx, "strategy": "ml", "reason": "no_symbols_entry_eligible"})

        for row in eligible_rows:
            symbol = str(row.get("symbol", "")).upper()
            scaling_level = int(row.get("scaling_level", 0))
            last_level = last_scaling.get(symbol, 0)
            if scaling_level < last_level:
                notifier.emit(
                    "ml_scaling_reduced",
                    {
                        "symbol": symbol,
                        "from_level": last_level,
                        "to_level": scaling_level,
                        "reason": "latest_evidence_scaling_drop",
                    },
                )
            last_scaling[symbol] = scaling_level

            blocked, ml_decision_obj, incident = safe_decide(
                decide_fn=risk_engine.decide_ml,
                decide_kwargs={
                    "nav": nav,
                    "scaling_level": scaling_level,
                    "current_gross_exposure_pct": ml_current_gross,
                },
                evidence_dir=cfg.evidence_dir,
                strategy="ml",
                symbol=symbol,
                cycle=cycle_idx,
            )
            if blocked:
                reason = "risk=ERROR => BLOCK"
                blocks.append({"cycle": cycle_idx, "strategy": "ml", "symbol": symbol, "reason": reason})
                notifier.emit(
                    "risk_block",
                    {
                        "strategy": "ml",
                        "instrument": symbol,
                        "reason": reason,
                        "incident": incident_to_dict(incident) if incident is not None else None,
                    },
                )
                continue
            assert ml_decision_obj is not None
            ml_decision = ml_decision_obj
            if not ml_decision.allow:
                blocks.append({"cycle": cycle_idx, "strategy": "ml", "symbol": symbol, "reason": ml_decision.reason})
                notifier.emit(
                    "risk_block",
                    {
                        "strategy": "ml",
                        "instrument": symbol,
                        "reason": ml_decision.reason,
                    },
                )
                continue

            side = "BUY"
            qty = round(ml_decision.final_size / max(1.0, nav / 100.0), 6)
            order = {
                "order_id": _intent_order_id("ml", symbol, cycle_idx),
                "instrument": symbol,
                "qty": qty,
                "side": side,
                "order_type": "MKT",
            }
            result = broker.place_order(strategy="ml", order=order)
            ml_orders.append(
                {
                    "cycle": cycle_idx,
                    "symbol": symbol,
                    "side": side,
                    "qty": qty,
                    "scaling_level": scaling_level,
                    "multiplier": _ml_multiplier(scaling_level),
                    "risk_decision": ml_decision.__dict__,
                    "broker_result": result,
                }
            )
            notifier.emit(
                "ml_trade_intent",
                {
                    "symbol": symbol,
                    "side": side,
                    "size": qty,
                    "scaling_level": scaling_level,
                    "reason": ml_decision.reason,
                },
            )

        carry_status = {
            "enabled": False,
            "disabled_reason": "carry_disabled",
            "source": None,
            "instruments_considered": 0,
            "intents": 0,
            "timestamp_utc": _utc_now_iso(),
        }
        if cfg.enable_carry:
            snapshot = broker.account_snapshot()
            rates_info = resolve_carry_rates(
                carry_cfg=carry_cfg,
                rates_file_path=cfg.carry_rates_path,
                broker_snapshot=snapshot,
            )
            if not rates_info.get("enabled", False):
                carry_status = {
                    "enabled": False,
                    "disabled_reason": str(rates_info.get("disabled_reason", "carry_rates_unavailable")),
                    "source": rates_info.get("source"),
                    "instruments_considered": 0,
                    "intents": 0,
                    "timestamp_utc": _utc_now_iso(),
                }
            else:
                intents, signal_status = generate_carry_intents(
                    carry_cfg=carry_cfg,
                    rates=dict(rates_info.get("rates", {})),
                    state=state,
                    now_utc=datetime.now(timezone.utc),
                )
                carry_status = {
                    "enabled": True,
                    "disabled_reason": None,
                    "source": rates_info.get("source"),
                    "instruments_considered": signal_status.get("instruments_considered", 0),
                    "intents": len(intents),
                    "timestamp_utc": _utc_now_iso(),
                }
                for idx, intent in enumerate(intents, start=1):
                    live_mode = str(cfg.mode).lower() == "live"
                    blocked, decision_obj, incident = safe_decide(
                        decide_fn=risk_engine.decide_carry,
                        decide_kwargs={
                            "nav": nav,
                            "carry_confidence": float(intent.confidence),
                            "expected_net_carry_after_costs": float(intent.expected_net_carry_after_costs),
                            "funding_cost": float(intent.funding_cost),
                            "carry_drawdown": float(carry_cfg.get("carry_pnl_drawdown", 0.0)),
                            "current_carry_gross_exposure_pct": carry_current_gross,
                            "current_carry_net_exposure_pct": carry_current_net,
                            "current_pair_exposure_pct": float(carry_cfg.get("pair_exposure_pct", 0.0)),
                            "leverage": leverage,
                            "live_mode": live_mode,
                            "pnl_available": bool(carry_cfg.get("pnl_available", not live_mode)),
                        },
                        evidence_dir=cfg.evidence_dir,
                        strategy="carry",
                        symbol=str(intent.instrument),
                        cycle=cycle_idx,
                    )
                    if blocked:
                        reason = "risk=ERROR => BLOCK"
                        blocks.append(
                            {
                                "cycle": cycle_idx,
                                "strategy": "carry",
                                "instrument": intent.instrument,
                                "reason": reason,
                            }
                        )
                        notifier.emit(
                            "risk_block",
                            {
                                "strategy": "carry",
                                "instrument": intent.instrument,
                                "reason": reason,
                                "incident": incident_to_dict(incident) if incident is not None else None,
                            },
                        )
                        continue
                    assert decision_obj is not None
                    decision: RiskDecision = decision_obj
                    if not decision.allow:
                        blocks.append(
                            {
                                "cycle": cycle_idx,
                                "strategy": "carry",
                                "instrument": intent.instrument,
                                "reason": decision.reason,
                            }
                        )
                        notifier.emit(
                            "risk_block",
                            {
                                "strategy": "carry",
                                "instrument": intent.instrument,
                                "reason": decision.reason,
                            },
                        )
                        continue
                    qty = round(decision.final_size / max(1.0, nav / 100.0), 6)
                    side = "BUY" if intent.direction == "LONG_BASE" else "SELL"
                    order = {
                        "order_id": _intent_order_id("carry", intent.instrument, cycle_idx * 1000 + idx),
                        "instrument": intent.instrument,
                        "qty": qty,
                        "side": side,
                        "order_type": "MKT",
                    }
                    result = broker.place_order(strategy="carry", order=order)
                    carry_orders.append(
                        {
                            "cycle": cycle_idx,
                            "instrument": intent.instrument,
                            "direction": intent.direction,
                            "asset_class": intent.asset_class,
                            "qty": qty,
                            "expected_net_carry_after_costs": intent.expected_net_carry_after_costs,
                            "funding_cost": intent.funding_cost,
                            "risk_decision": decision.__dict__,
                            "broker_result": result,
                        }
                    )
                    notifier.emit(
                        "carry_trade_intent",
                        {
                            "instrument": intent.instrument,
                            "direction": intent.direction,
                            "size": qty,
                            "expected_net_carry": intent.expected_net_carry_after_costs,
                            "funding_cost": intent.funding_cost,
                            "reason": intent.reason,
                        },
                    )
                state["last_rebalance_ts"] = _utc_now_iso()

        _write_json(cycle_dir / "carry_status.json", carry_status)
        _write_json(
            cycle_dir / "cycle_summary.json",
            {
                "cycle": cycle_idx,
                "timestamp_utc": _utc_now_iso(),
                "ml_orders_count": len([x for x in ml_orders if x["cycle"] == cycle_idx]),
                "carry_orders_count": len([x for x in carry_orders if x["cycle"] == cycle_idx]),
                "blocks_count": len([x for x in blocks if x["cycle"] == cycle_idx]),
                "carry_status": carry_status,
            },
        )

        notifier.emit(
            "cycle_summary",
            {
                "cycle": cycle_idx,
                "ml_orders": len([x for x in ml_orders if x["cycle"] == cycle_idx]),
                "carry_orders": len([x for x in carry_orders if x["cycle"] == cycle_idx]),
                "blocks": len([x for x in blocks if x["cycle"] == cycle_idx]),
                "carry_status": carry_status,
            },
        )

        if cfg.loop and cycle_idx < cycle_count:
            time.sleep(max(0, int(cfg.cycle_seconds)))

    # --- Portfolio preflight overlay ---
    preflight_positions: Dict[str, float] = {}
    for order in ml_orders:
        sym = str(order.get("symbol", ""))
        preflight_positions[sym] = preflight_positions.get(sym, 0.0) + float(order.get("qty", 0.0))
    preflight_result = run_preflight(
        positions=preflight_positions,
        nav=nav,
        returns_by_symbol={},
    )
    gov_audit.emit(
        EVENT_PORTFOLIO_PREFLIGHT,
        {
            "ok": preflight_result.ok,
            "reason": preflight_result.reason,
            "checks": preflight_result.checks,
        },
    )
    _write_json(cfg.evidence_dir / "portfolio_preflight.json", {
        "ok": preflight_result.ok,
        "reason": preflight_result.reason,
        "blocked_symbols": preflight_result.blocked_symbols,
        "checks": preflight_result.checks,
    })

    summary = {
        "run_id": cfg.evidence_dir.name,
        "timestamp_utc": _utc_now_iso(),
        "mode": cfg.mode,
        "ml_orders": len(ml_orders),
        "carry_orders": len(carry_orders),
        "blocks": len(blocks),
        "carry_enabled": bool(cfg.enable_carry),
        "selection_dir": str(cfg.evidence_dir / "selection"),
        "portfolio_preflight_ok": preflight_result.ok,
    }
    _write_json(cfg.evidence_dir / "execution_summary.json", summary)
    _write_json(cfg.evidence_dir / "ml_orders.json", ml_orders)
    _write_json(cfg.evidence_dir / "carry_orders.json", carry_orders)
    _write_json(cfg.evidence_dir / "risk_blocks.json", blocks)

    notifier.emit(
        "execution_shutdown",
        {
            "run_id": cfg.evidence_dir.name,
            "mode": cfg.mode,
            "ml_orders": len(ml_orders),
            "carry_orders": len(carry_orders),
            "blocks": len(blocks),
        },
    )
    return summary
