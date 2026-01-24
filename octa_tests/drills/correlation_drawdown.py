from __future__ import annotations

from datetime import datetime
from typing import Callable


def run_correlation_drawdown(
    correlation_fn: Callable[[], dict],
    drawdown_fn: Callable[[float], dict],
    incident_recorder: Callable[[dict], None],
    sentinel_api,
):
    ts = datetime.utcnow().isoformat() + "Z"
    corr_res = correlation_fn()
    score = float(corr_res.get("score", 0.0))
    # trigger drawdown ladder if score above threshold (caller decides)
    if score >= 0.7:
        # simulate drawdown
        dd = 0.12
        res = drawdown_fn(dd)
        incident = {
            "ts": ts,
            "type": "correlation_drawdown",
            "corr_score": score,
            "drawdown": dd,
            "actions": res,
        }
        incident_recorder(incident)
        try:
            sentinel_api.set_gate(3, "correlation_drawdown")
        except Exception:
            pass
        return {"pass": False, "incident": incident}
    return {"pass": True, "corr_res": corr_res}
