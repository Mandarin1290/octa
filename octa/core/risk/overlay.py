from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from .limits import RiskBudget, budget_from_cfg


@dataclass(frozen=True)
class OverlayDecision:
    allow: bool
    adjusted_qty: float
    reason: str
    diagnostics: Dict[str, Any]


def compute_risk_budget(
    portfolio_state: Mapping[str, Any],
    overlay_cfg: Mapping[str, Any],
    regime_state: Mapping[str, Any],
) -> RiskBudget:
    regime_label = str(regime_state.get("label", "RISK_ON"))
    scale = _regime_scale(regime_label)
    drawdown = float(portfolio_state.get("drawdown", 0.0) or 0.0)
    soft_dd = float(overlay_cfg.get("soft_drawdown", 0.08))
    hard_dd = float(overlay_cfg.get("hard_drawdown", 0.12))
    scale *= _drawdown_scale(drawdown, soft_dd, hard_dd)
    return budget_from_cfg(overlay_cfg, scale)


def apply_overlay(
    signals: Iterable[Mapping[str, Any]],
    portfolio_state: Mapping[str, Any],
    market_state: Mapping[str, Any],
    overlay_cfg: Mapping[str, Any],
    regime_state: Mapping[str, Any],
) -> List[Mapping[str, Any]]:
    results: List[Mapping[str, Any]] = []
    budget = compute_risk_budget(portfolio_state, overlay_cfg, regime_state)
    exposure_used = float(portfolio_state.get("exposure_used", 0.0))
    per_bucket = _bucket_exposure(portfolio_state)
    gross_short = float(portfolio_state.get("gross_short_exposure", 0.0))
    max_gross_short = float(overlay_cfg.get("max_gross_short", 0.3)) * budget.risk_multiplier
    reasons: List[Dict[str, Any]] = []

    for sig in signals:
        qty = float(sig.get("qty", 0.0))
        symbol = str(sig.get("symbol", ""))
        side = str(sig.get("side", "BUY")).upper()
        bucket = _bucket_for(symbol, market_state)
        decision = _apply_single(
            qty=qty,
            budget=budget,
            exposure_used=exposure_used,
            bucket=bucket,
            per_bucket=per_bucket,
            side=side,
            gross_short=gross_short,
            max_gross_short=max_gross_short,
            market_state=market_state,
            overlay_cfg=overlay_cfg,
        )
        reasons.append({"symbol": symbol, "decision": decision.reason, "diag": decision.diagnostics})
        if decision.allow and decision.adjusted_qty > 0.0:
            adjusted = dict(sig)
            adjusted["qty"] = decision.adjusted_qty
            results.append(adjusted)
            exposure_used += decision.adjusted_qty
            per_bucket[bucket] = per_bucket.get(bucket, 0.0) + decision.adjusted_qty
            if side == "SELL":
                gross_short += decision.adjusted_qty

    _write_overlay_audit(
        signals=list(signals),
        results=results,
        budget=budget,
        regime_state=regime_state,
        reasons=reasons,
    )
    return results


def _apply_single(
    *,
    qty: float,
    budget: RiskBudget,
    exposure_used: float,
    bucket: str,
    per_bucket: Dict[str, float],
    side: str,
    gross_short: float,
    max_gross_short: float,
    market_state: Mapping[str, Any],
    overlay_cfg: Mapping[str, Any],
) -> OverlayDecision:
    if budget.risk_multiplier <= 0.0:
        return OverlayDecision(False, 0.0, "regime_halt", {"risk_multiplier": budget.risk_multiplier})

    max_pos = budget.max_position_pct * budget.risk_multiplier
    max_port = float(overlay_cfg.get("max_portfolio_exposure", 0.5)) * budget.risk_multiplier
    max_bucket = budget.max_sector_pct * budget.risk_multiplier
    max_single = budget.max_single_asset_risk * budget.risk_multiplier
    require_borrowable = bool(overlay_cfg.get("require_borrowable", False))
    borrowable = market_state.get("borrowable", True)

    if qty > max_pos:
        return OverlayDecision(False, 0.0, "max_position", {"qty": qty, "max_pos": max_pos})
    if exposure_used + qty > max_port:
        return OverlayDecision(False, 0.0, "max_portfolio", {"exposure_used": exposure_used, "max_port": max_port})
    if per_bucket.get(bucket, 0.0) + qty > max_bucket:
        return OverlayDecision(False, 0.0, "max_bucket", {"bucket": bucket, "max_bucket": max_bucket})
    if qty > max_single:
        return OverlayDecision(False, 0.0, "max_single_asset", {"qty": qty, "max_single": max_single})
    if side == "SELL":
        if require_borrowable and not borrowable:
            return OverlayDecision(False, 0.0, "borrow_unavailable", {})
        if gross_short + qty > max_gross_short:
            return OverlayDecision(False, 0.0, "max_gross_short", {"gross_short": gross_short, "max": max_gross_short})
    return OverlayDecision(True, qty, "ok", {})


def _bucket_for(symbol: str, market_state: Mapping[str, Any]) -> str:
    asset_class = str(market_state.get("asset_class", "")).lower()
    if asset_class:
        return asset_class
    sym = symbol.upper()
    if sym.endswith("USD"):
        return "fx"
    return "equity"


def _bucket_exposure(portfolio_state: Mapping[str, Any]) -> Dict[str, float]:
    buckets = portfolio_state.get("bucket_exposure")
    if isinstance(buckets, dict):
        return {str(k): float(v) for k, v in buckets.items()}
    return {}


def _regime_scale(label: str) -> float:
    label = label.upper()
    if label == "RISK_ON":
        return 1.0
    if label == "REDUCE":
        return 0.5
    if label in {"RISK_OFF", "HALT"}:
        return 0.0
    return 0.5


def _drawdown_scale(drawdown: float, soft: float, hard: float) -> float:
    if drawdown >= hard:
        return 0.0
    if drawdown >= soft:
        return 0.5
    return 1.0


def _write_overlay_audit(
    *,
    signals: List[Mapping[str, Any]],
    results: List[Mapping[str, Any]],
    budget: RiskBudget,
    regime_state: Mapping[str, Any],
    reasons: List[Dict[str, Any]],
) -> None:
    root = Path("octa") / "var" / "audit" / "risk_overlay"
    root.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).isoformat()
    safe_ts = ts.replace(":", "").replace("-", "").replace(".", "")
    payload = {
        "timestamp": ts,
        "budget": budget.__dict__,
        "regime_state": dict(regime_state),
        "signals": signals,
        "results": results,
        "reasons": reasons,
    }
    path = root / f"overlay_{safe_ts}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
