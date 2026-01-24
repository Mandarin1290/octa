from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional


@dataclass
class KvpUpdate:
    asset_class: str
    passed: bool
    sharpe: Optional[float]
    max_drawdown: Optional[float]


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        fv = float(v)
        if fv != fv:  # NaN
            return None
        return fv
    except Exception:
        return None


def _extract_strategy_metrics(metrics_summary: Optional[Dict[str, Any]]) -> KvpUpdate:
    ms = metrics_summary or {}
    # MetricsSummary dict shape varies; we keep this resilient.
    sharpe = _safe_float(ms.get("sharpe"))
    max_dd = _safe_float(ms.get("max_drawdown"))
    return KvpUpdate(asset_class="unknown", passed=False, sharpe=sharpe, max_drawdown=max_dd)


def update_kvp_summary(
    *,
    state_dir: Path,
    asset_class: str,
    passed: bool,
    metrics_summary: Optional[Dict[str, Any]],
    filename: str = "kvp_summary.json",
) -> None:
    """Update a small aggregate KVP summary file.

    This is intentionally non-invasive: it only writes aggregates and never
    changes training/gating behavior.
    """

    state_dir = Path(state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)
    out_path = state_dir / filename

    upd = _extract_strategy_metrics(metrics_summary)
    upd.asset_class = str(asset_class or "unknown")
    upd.passed = bool(passed)

    data: Dict[str, Any]
    if out_path.exists():
        try:
            data = json.loads(out_path.read_text())
        except Exception:
            data = {}
    else:
        data = {}

    ac = upd.asset_class
    entry = data.get(ac) if isinstance(data.get(ac), dict) else {}

    n = int(entry.get("n", 0) or 0) + 1
    pass_n = int(entry.get("pass_n", 0) or 0) + (1 if upd.passed else 0)

    # running mean (ignore None)
    def _update_mean(key: str, new_val: Optional[float]) -> None:
        if new_val is None:
            return
        mean_key = f"{key}_mean"
        cnt_key = f"{key}_n"
        old_cnt = int(entry.get(cnt_key, 0) or 0)
        old_mean = _safe_float(entry.get(mean_key))
        if old_mean is None or old_cnt <= 0:
            entry[mean_key] = float(new_val)
            entry[cnt_key] = 1
            return
        entry[mean_key] = float((old_mean * old_cnt + float(new_val)) / (old_cnt + 1))
        entry[cnt_key] = old_cnt + 1

    _update_mean("sharpe", upd.sharpe)
    _update_mean("max_drawdown", upd.max_drawdown)

    entry["n"] = n
    entry["pass_n"] = pass_n
    entry["pass_rate"] = float(pass_n) / float(n) if n > 0 else 0.0
    entry["last_updated"] = datetime.utcnow().isoformat() + "Z"

    # Guidance is advisory only.
    guidance = []
    if entry.get("pass_rate", 0.0) < 0.2 and n >= 20:
        guidance.append("Low pass-rate: consider reviewing data quality / gates for this asset class.")
    if upd.sharpe is not None and upd.sharpe < 0.5 and n >= 10:
        guidance.append("Low strategy Sharpe: consider feature review or longer horizon.")
    entry["guidance"] = guidance

    data[ac] = entry

    out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2))
