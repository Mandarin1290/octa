from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

import json
import math

import pandas as pd

from octa.core.execution.costs import CostConfig, apply_costs, estimate_costs


@dataclass(frozen=True)
class ScoreReport:
    run_id: str
    gate: str
    timeframe: str
    score: float
    metrics: Dict[str, Any]
    ok: bool
    errors: List[str]


def score_run(
    pnl_series: pd.Series,
    trades: List[Mapping[str, Any]],
    market_ctx: Mapping[str, Any],
    cfg: Mapping[str, Any],
    *,
    run_id: str,
    gate: str,
    timeframe: str,
    mode: str = "paper",
) -> ScoreReport:
    errors: List[str] = []
    mode_cfg = cfg.get(mode, {}) if isinstance(cfg.get(mode), dict) else {}
    fee_mult = float(mode_cfg.get("fee_multiplier", 1.0))
    spread_mult = float(mode_cfg.get("spread_multiplier", 1.0))
    slip_mult = float(mode_cfg.get("slippage_multiplier", 1.0))
    try:
        cost_cfg = CostConfig(
            fee_bps=float(cfg.get("fee_bps", 1.0)) * fee_mult,
            spread_bps=float(cfg.get("spread_bps", 0.5)) * spread_mult,
            slippage_bps=float(cfg.get("slippage_bps", 0.5)) * slip_mult,
            min_cost_bps=float(cfg.get("min_cost_bps", 0.0)),
            max_cost_bps=float(cfg.get("max_cost_bps", 20.0)),
            stress_multiplier=float(cfg.get("stress_multiplier", 1.0)),
        )
    except Exception as exc:
        errors.append(f"cost_config_error:{exc}")
        cost_cfg = CostConfig(stress_multiplier=2.0)

    costs = estimate_costs(trades, market_ctx, cost_cfg)
    net_returns = apply_costs(pnl_series.tolist(), costs)
    net_series = pd.Series(net_returns, index=pnl_series.index)
    metrics = _compute_metrics(net_series, costs)

    score = _score_from_metrics(metrics, cfg)
    ok = bool(metrics) and not errors

    report = ScoreReport(
        run_id=run_id,
        gate=gate,
        timeframe=timeframe,
        score=score,
        metrics=metrics,
        ok=ok,
        errors=errors,
    )
    _write_scoring_artifact(report)
    _write_scoring_audit(report, costs, cfg)
    return report


def _compute_metrics(net_series: pd.Series, costs: Any) -> Dict[str, Any]:
    if net_series.empty:
        return {}
    ann = _annualization_factor(net_series.index)
    mean_ann = net_series.mean() * ann
    vol_ann = net_series.std(ddof=0) * math.sqrt(ann)
    sharpe = float(mean_ann / vol_ann) if vol_ann > 0 else 0.0
    equity = (1.0 + net_series).cumprod()
    roll_max = equity.cummax()
    drawdown = equity / roll_max - 1.0
    max_dd = float(abs(drawdown.min()))
    total_periods = len(net_series)
    years = total_periods / ann if ann > 0 else 0.0
    cagr = float(equity.iloc[-1] ** (1.0 / years) - 1.0) if years > 0 else 0.0
    trades = int(costs.diagnostics.get("trade_count", 0)) if costs else 0
    worst = float(net_series.sort_values().head(max(1, int(0.05 * len(net_series)))).mean())
    cost_frac = float(costs.total_cost_bps / 10000.0) if costs else 0.0
    return {
        "cagr": cagr,
        "sharpe": sharpe,
        "max_drawdown": max_dd,
        "trade_count": trades,
        "tail_cvar": worst,
        "cost_fraction": cost_frac,
    }


def _score_from_metrics(metrics: Dict[str, Any], cfg: Mapping[str, Any]) -> float:
    if not metrics:
        return 0.0
    sharpe = float(metrics.get("sharpe", 0.0))
    cagr = float(metrics.get("cagr", 0.0))
    max_dd = float(metrics.get("max_drawdown", 0.0))
    tail = float(metrics.get("tail_cvar", 0.0))
    cost_frac = float(metrics.get("cost_fraction", 0.0))
    score = (0.4 * sharpe) + (0.3 * cagr) - (0.2 * max_dd) - (0.1 * abs(tail))
    score = score - cost_frac * float(cfg.get("cost_penalty", 0.5))
    return score


def _annualization_factor(index: pd.Index) -> float:
    if len(index) < 2:
        return 252.0
    deltas = index.to_series().diff().dropna()
    med = deltas.median()
    if not isinstance(med, pd.Timedelta):
        return 252.0
    sec = med.total_seconds()
    if sec >= 20 * 3600:
        return 252.0
    if sec >= 30 * 60:
        return 252.0 * 24.0
    return 252.0 * 6.5 * 60.0


def _write_scoring_artifact(report: ScoreReport) -> None:
    root = Path("octa") / "var" / "artifacts" / "scoring" / report.run_id / report.gate / report.timeframe
    root.mkdir(parents=True, exist_ok=True)
    path = root / "score.json"
    path.write_text(json.dumps(report.__dict__, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _write_scoring_audit(report: ScoreReport, costs: Any, cfg: Mapping[str, Any]) -> None:
    root = Path("octa") / "var" / "audit" / "scoring"
    root.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).isoformat()
    safe_ts = ts.replace(":", "").replace("-", "").replace(".", "")
    payload = {
        "timestamp": ts,
        "run_id": report.run_id,
        "gate": report.gate,
        "timeframe": report.timeframe,
        "score": report.score,
        "metrics": report.metrics,
        "costs": costs.__dict__ if costs else {},
        "cfg": dict(cfg),
        "ok": report.ok,
        "errors": report.errors,
    }
    path = root / f"scoring_{safe_ts}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
