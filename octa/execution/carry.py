from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_json_file(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def resolve_carry_rates(
    *,
    carry_cfg: Dict[str, Any],
    rates_file_path: Path,
    broker_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    # Primary deterministic source: explicit rates in carry config.
    cfg_rates = carry_cfg.get("rates")
    if isinstance(cfg_rates, dict) and cfg_rates:
        return {"enabled": True, "source": "carry_config.rates", "rates": {str(k): float(v) for k, v in cfg_rates.items()}}

    # Secondary deterministic source: local file.
    file_rates = load_json_file(rates_file_path)
    if isinstance(file_rates.get("rates"), dict) and file_rates.get("rates"):
        return {
            "enabled": True,
            "source": str(rates_file_path),
            "rates": {str(k): float(v) for k, v in dict(file_rates.get("rates", {})).items()},
        }

    # Tertiary source: broker snapshot if available.
    if broker_snapshot and isinstance(broker_snapshot.get("funding_rates"), dict) and broker_snapshot.get("funding_rates"):
        return {
            "enabled": True,
            "source": "broker_snapshot.funding_rates",
            "rates": {str(k): float(v) for k, v in dict(broker_snapshot.get("funding_rates", {})).items()},
        }

    return {
        "enabled": False,
        "source": None,
        "rates": {},
        "disabled_reason": "no_deterministic_carry_rate_source",
    }


@dataclass(frozen=True)
class CarryIntent:
    instrument: str
    direction: str
    asset_class: str
    confidence: float
    rate_diff: float
    expected_net_carry_after_costs: float
    funding_cost: float
    reason: str


def _hours_since(ts_iso: Optional[str], now: datetime) -> Optional[float]:
    if not ts_iso:
        return None
    try:
        dt = datetime.fromisoformat(str(ts_iso))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0.0, (now - dt).total_seconds() / 3600.0)
    except Exception:
        return None


def generate_carry_intents(
    *,
    carry_cfg: Dict[str, Any],
    rates: Dict[str, float],
    state: Dict[str, Any],
    now_utc: Optional[datetime] = None,
) -> Tuple[List[CarryIntent], Dict[str, Any]]:
    now = now_utc or datetime.now(timezone.utc)
    instruments = carry_cfg.get("instruments") if isinstance(carry_cfg.get("instruments"), list) else []
    if not instruments:
        return [], {"enabled": False, "disabled_reason": "no_instruments_configured"}

    min_rate_diff = float(carry_cfg.get("min_rate_diff", 0.0))
    enter_threshold = float(carry_cfg.get("enter_threshold", min_rate_diff))
    exit_threshold = float(carry_cfg.get("exit_threshold", 0.0))
    max_vol = float(carry_cfg.get("max_volatility", 999.0))
    min_hold_days = float(carry_cfg.get("min_hold_days", 1.0))
    rebalance_interval_hours = float(carry_cfg.get("rebalance_interval_hours", 24.0))
    if enter_threshold <= exit_threshold:
        return [], {"enabled": False, "disabled_reason": "invalid_hysteresis_thresholds"}

    intents: List[CarryIntent] = []
    considered = 0
    for item in instruments:
        if not isinstance(item, dict):
            continue
        considered += 1
        instrument = str(item.get("instrument", "")).strip()
        base_ccy = str(item.get("base_ccy", "")).strip().upper()
        quote_ccy = str(item.get("quote_ccy", "")).strip().upper()
        asset_class = str(item.get("asset_class", "fx_carry")).strip().lower() or "fx_carry"
        if not instrument or not base_ccy or not quote_ccy:
            continue

        if base_ccy not in rates or quote_ccy not in rates:
            continue
        rate_diff = float(rates.get(quote_ccy, 0.0)) - float(rates.get(base_ccy, 0.0))
        funding_cost = float(item.get("funding_cost", carry_cfg.get("funding_cost_default", 0.0)))
        expected_net = rate_diff - funding_cost
        volatility = float(item.get("volatility", carry_cfg.get("volatility_default", 0.0)))
        if volatility > max_vol:
            continue

        last_ts = (state.get("last_rebalance_ts") if isinstance(state, dict) else None) or None
        hours = _hours_since(last_ts, now)
        if hours is not None and hours < rebalance_interval_hours:
            continue
        if hours is not None and (hours / 24.0) < min_hold_days:
            continue

        direction = "LONG_BASE" if rate_diff >= 0.0 else "SHORT_BASE"
        margin = abs(rate_diff) - enter_threshold
        if abs(rate_diff) < min_rate_diff:
            continue
        if margin < 0.0:
            continue
        if expected_net <= 0.0:
            continue
        confidence = min(1.0, max(0.0, margin / max(1e-9, enter_threshold)))

        intents.append(
            CarryIntent(
                instrument=instrument,
                direction=direction,
                asset_class=asset_class,
                confidence=confidence,
                rate_diff=rate_diff,
                expected_net_carry_after_costs=expected_net,
                funding_cost=funding_cost,
                reason="rate_diff_and_net_carry_pass",
            )
        )

    status = {
        "enabled": True,
        "disabled_reason": None,
        "source": "carry_signal_policy_v0",
        "instruments_considered": considered,
        "intents": len(intents),
        "timestamp_utc": _utc_now_iso(),
    }
    return intents, status
