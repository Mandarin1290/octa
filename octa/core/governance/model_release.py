from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Mapping

from .champion_challenger import decide_champion


@dataclass(frozen=True)
class ReleaseDecision:
    released: bool
    reason: str
    diagnostics: Dict[str, Any]


def decide_release(
    validation_report: Mapping[str, Any],
    scoring_report: Mapping[str, Any],
    mc_report: Mapping[str, Any],
    thresholds: Mapping[str, Any],
) -> ReleaseDecision:
    required = [validation_report, scoring_report, mc_report]
    if any(r is None for r in required):
        return ReleaseDecision(False, "missing_reports", {})

    if not validation_report.get("ok", False):
        return ReleaseDecision(False, "validation_failed", {"errors": validation_report.get("errors")})
    if not scoring_report.get("ok", False):
        return ReleaseDecision(False, "scoring_failed", {"errors": scoring_report.get("errors")})
    if not mc_report.get("ok", False):
        return ReleaseDecision(False, "mc_failed", {"errors": mc_report.get("errors")})

    metrics = scoring_report.get("metrics", {}) or {}
    min_sharpe = float(thresholds.get("min_sharpe", 1.0))
    max_dd = float(thresholds.get("max_drawdown", 0.12))
    min_trades = int(thresholds.get("min_trades", 50))
    max_cv = float(thresholds.get("max_split_cv", 0.5))
    mc_dd_prob = float(thresholds.get("mc_dd_prob_max", 0.1))

    sharpe = float(metrics.get("sharpe", 0.0))
    mdd = float(metrics.get("max_drawdown", 0.0))
    trades = int(metrics.get("trade_count", 0))
    split_cv = float(validation_report.get("aggregate_metrics", {}).get("sharpe_cv", 0.0) or 0.0)
    prob_dd = float(mc_report.get("prob_dd_breach", 1.0) or 1.0)

    if sharpe < min_sharpe:
        return ReleaseDecision(False, "min_sharpe", {"sharpe": sharpe, "min": min_sharpe})
    if mdd > max_dd:
        return ReleaseDecision(False, "max_drawdown", {"mdd": mdd, "max": max_dd})
    if trades < min_trades:
        return ReleaseDecision(False, "min_trades", {"trades": trades, "min": min_trades})
    if split_cv > max_cv:
        return ReleaseDecision(False, "split_cv", {"split_cv": split_cv, "max": max_cv})
    if prob_dd > mc_dd_prob:
        return ReleaseDecision(False, "mc_dd_prob", {"prob": prob_dd, "max": mc_dd_prob})

    return ReleaseDecision(True, "release_ok", {})


def update_registry(
    decision: ReleaseDecision,
    model_artifacts: Mapping[str, Any],
    registry_path: Path,
    thresholds: Mapping[str, Any],
) -> None:
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    existing = {}
    if registry_path.exists():
        try:
            existing = json.loads(registry_path.read_text(encoding="utf-8"))
        except Exception:
            existing = {}

    challenger_score = float(model_artifacts.get("score", 0.0))
    champion_score = float(existing.get("score", 0.0))
    stability_ok = bool(model_artifacts.get("stability_ok", False))
    min_improve = float(thresholds.get("min_improvement", 0.05))
    champ_decision = decide_champion(
        challenger_score=challenger_score,
        champion_score=champion_score,
        min_improvement=min_improve,
        stability_ok=stability_ok,
    )

    payload = dict(existing)
    if decision.released and champ_decision.promote:
        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "score": challenger_score,
            "artifacts": dict(model_artifacts),
            "decision": decision.reason,
        }
    registry_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    _write_release_audit(decision, model_artifacts, champ_decision, registry_path)


def _write_release_audit(
    decision: ReleaseDecision,
    model_artifacts: Mapping[str, Any],
    champ_decision: Any,
    registry_path: Path,
) -> None:
    root = Path("octa") / "var" / "audit" / "model_release"
    root.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).isoformat()
    safe_ts = ts.replace(":", "").replace("-", "").replace(".", "")
    payload = {
        "timestamp": ts,
        "decision": decision.__dict__,
        "model_artifacts": dict(model_artifacts),
        "champion_decision": champ_decision.__dict__,
        "registry_path": str(registry_path),
    }
    path = root / f"release_{safe_ts}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
