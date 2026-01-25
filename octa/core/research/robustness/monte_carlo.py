from __future__ import annotations

import json
import math
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

import pandas as pd


@dataclass(frozen=True)
class MonteCarloReport:
    run_id: str
    gate: str
    timeframe: str
    metrics: Dict[str, Any]
    ok: bool
    errors: List[str]


def run_monte_carlo(
    net_trade_returns: Iterable[float],
    cfg: Mapping[str, Any],
    seed: int,
    *,
    run_id: str,
    gate: str,
    timeframe: str,
) -> MonteCarloReport:
    errors: List[str] = []
    returns = list(net_trade_returns)
    if not returns:
        report = MonteCarloReport(run_id, gate, timeframe, {}, False, ["empty_returns"])
        _write_artifacts(report)
        return report

    n_sims = int(cfg.get("n_sims", 200))
    block = int(cfg.get("block_size", 5))
    dd_limit = float(cfg.get("dd_limit", 0.15))
    stress_mults = cfg.get("stress_multipliers", [1.0, 1.5, 2.0])

    rng = random.Random(seed)
    metrics = []
    for _ in range(n_sims):
        sample = _block_bootstrap(returns, block, rng)
        for mult in stress_mults:
            stressed = [r / float(mult) for r in sample]
            metrics.append(_metrics_for(stressed))

    prob_dd = sum(1 for m in metrics if m["max_drawdown"] > dd_limit) / max(len(metrics), 1)
    sharpe_vals = [m["sharpe"] for m in metrics]
    mdd_vals = [m["max_drawdown"] for m in metrics]
    worst_months = [m["worst_month"] for m in metrics]
    summary = {
        "sharpe_mean": _mean(sharpe_vals),
        "mdd_mean": _mean(mdd_vals),
        "worst_month_mean": _mean(worst_months),
        "prob_dd_breach": prob_dd,
    }
    report = MonteCarloReport(run_id, gate, timeframe, summary, True, errors)
    _write_artifacts(report)
    return report


def _block_bootstrap(returns: List[float], block_size: int, rng: random.Random) -> List[float]:
    if block_size <= 1:
        return [returns[rng.randrange(len(returns))] for _ in range(len(returns))]
    blocks = []
    n = len(returns)
    while len(blocks) < n:
        start = rng.randrange(0, max(1, n - block_size + 1))
        blocks.extend(returns[start : start + block_size])
    return blocks[:n]


def _metrics_for(returns: List[float]) -> Dict[str, Any]:
    series = pd.Series(returns)
    ann = 252.0
    mean_ann = series.mean() * ann
    vol_ann = series.std(ddof=0) * math.sqrt(ann)
    sharpe = float(mean_ann / vol_ann) if vol_ann > 0 else 0.0
    equity = (1.0 + series).cumprod()
    roll_max = equity.cummax()
    dd = equity / roll_max - 1.0
    max_dd = float(abs(dd.min()))
    worst_month = float(series.sort_values().head(max(1, int(0.05 * len(series)))).mean())
    return {"sharpe": sharpe, "max_drawdown": max_dd, "worst_month": worst_month}


def _mean(vals: List[float]) -> float:
    return float(sum(vals) / len(vals)) if vals else 0.0


def _write_artifacts(report: MonteCarloReport) -> None:
    root = Path("octa") / "var" / "artifacts" / "robustness" / report.run_id / report.gate / report.timeframe
    root.mkdir(parents=True, exist_ok=True)
    path = root / "mc.json"
    path.write_text(json.dumps(report.__dict__, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    audit_root = Path("octa") / "var" / "audit" / "robustness"
    audit_root.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).isoformat()
    safe_ts = ts.replace(":", "").replace("-", "").replace(".", "")
    audit_path = audit_root / f"mc_{safe_ts}.json"
    audit_path.write_text(json.dumps(report.__dict__, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
