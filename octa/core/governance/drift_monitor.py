from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from octa_ledger.store import LedgerStore


@dataclass(frozen=True)
class DriftDecision:
    disabled: bool
    reason: str
    streak: int
    kpi: float
    diagnostics: Dict[str, Any]


def evaluate_drift(
    *,
    ledger_dir: str,
    model_key: str,
    gate: str,
    timeframe: str,
    bucket: str,
    cfg: Mapping[str, Any],
) -> DriftDecision:
    ledger = LedgerStore(ledger_dir)
    navs = _collect_navs(ledger)
    window_days = int(cfg.get("window_days", 20))
    breach_days = int(cfg.get("breach_days", 3))
    kpi_threshold = float(cfg.get("kpi_threshold", 0.2))

    kpi = _rolling_sharpe(navs, window_days)
    state = _load_state(model_key)
    streak = int(state.get("streak", 0))
    disabled = bool(state.get("disabled", False))
    reason = "ok"

    if not navs or kpi is None:
        return DriftDecision(False, "insufficient_nav", streak, 0.0, {"nav_count": len(navs)})

    if kpi < kpi_threshold:
        streak += 1
        reason = "kpi_below_threshold"
    else:
        streak = 0

    if streak >= breach_days:
        disabled = True
        reason = "drift_breach"
        champion_path = _champion_path(gate, timeframe, bucket)
        _write_drift_audit(model_key, timeframe, bucket, kpi, streak, cfg, champion_path)

    _save_state(model_key, {"streak": streak, "disabled": disabled, "kpi": kpi, "updated_at": _now_iso()})
    return DriftDecision(disabled, reason, streak, kpi, {"threshold": kpi_threshold, "window_days": window_days})


def _collect_navs(ledger: LedgerStore) -> List[tuple[datetime, float]]:
    res: List[tuple[datetime, float]] = []
    for e in ledger.by_action("performance.nav"):
        p = e.get("payload", {})
        ts = p.get("date") or e.get("timestamp")
        try:
            dt = datetime.fromisoformat(str(ts))
        except Exception:
            continue
        try:
            nav = float(p.get("nav", 0.0))
        except Exception:
            nav = 0.0
        res.append((dt, nav))
    res.sort(key=lambda x: x[0])
    return res


def _rolling_sharpe(navs: List[tuple[datetime, float]], window_days: int) -> Optional[float]:
    if len(navs) < window_days + 1:
        return None
    window = navs[-(window_days + 1) :]
    returns = []
    for i in range(1, len(window)):
        prev = window[i - 1][1]
        curr = window[i][1]
        if prev == 0:
            continue
        returns.append((curr / prev) - 1.0)
    if len(returns) < 2:
        return None
    mean = sum(returns) / len(returns)
    var = sum((r - mean) ** 2 for r in returns) / len(returns)
    std = var ** 0.5
    if std == 0:
        return 0.0
    return mean / std


def _state_path(model_key: str) -> Path:
    root = Path("octa") / "var" / "registry" / "models" / "drift"
    root.mkdir(parents=True, exist_ok=True)
    return root / f"{model_key}.json"


def _load_state(model_key: str) -> Dict[str, Any]:
    path = _state_path(model_key)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_state(model_key: str, payload: Mapping[str, Any]) -> None:
    path = _state_path(model_key)
    path.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2), encoding="utf-8")


def _write_drift_audit(
    model_key: str,
    timeframe: str,
    bucket: str,
    kpi: float,
    streak: int,
    cfg: Mapping[str, Any],
    champion_path: str,
) -> None:
    root = Path("octa") / "var" / "audit" / "drift_monitor"
    root.mkdir(parents=True, exist_ok=True)
    ts = _now_iso()
    safe_ts = ts.replace(":", "").replace("-", "").replace(".", "")
    payload = {
        "timestamp": ts,
        "model_key": model_key,
        "timeframe": timeframe,
        "bucket": bucket,
        "kpi": kpi,
        "streak": streak,
        "cfg": dict(cfg),
        "rollback_candidate": champion_path,
    }
    path = root / f"drift_{safe_ts}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _champion_path(gate: str, timeframe: str, bucket: str) -> str:
    return str(Path("octa") / "var" / "registry" / "models" / gate / timeframe / bucket / "champion.json")
