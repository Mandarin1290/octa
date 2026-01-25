from __future__ import annotations

from statistics import mean, pstdev
from typing import Any, Mapping


def build(payloads: Mapping[str, Any], *, asof_ts: str | None = None) -> dict[str, float]:
    cot = payloads.get("cot", {})
    rows = cot.get("rows", []) if isinstance(cot, dict) else []
    features: dict[str, float] = {}
    if not rows:
        return features

    asof = _parse_ts(asof_ts) if asof_ts else None
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        release_ts = _parse_ts(row.get("release_ts"))
        if asof is not None and release_ts is not None and release_ts > asof:
            continue
        market_id = str(row.get("market_id", "")).strip().lower()
        if not market_id:
            continue
        grouped.setdefault(market_id, []).append(row)

    z_scores: list[float] = []
    for market_id, items in grouped.items():
        items.sort(key=lambda r: r.get("report_date") or "")
        history = [_net_position(r) for r in items if _net_position(r) is not None]
        latest = history[-1] if history else None
        if latest is None:
            continue
        features[f"cot_net_position_{market_id}"] = latest
        latest_nc = _nc_net(items[-1])
        if latest_nc is not None:
            features[f"cot_noncommercial_net_{market_id}"] = latest_nc
        oi = _as_float(items[-1].get("open_interest"))
        if oi is not None:
            features[f"cot_open_interest_{market_id}"] = oi

        z = _z_score(history)
        if z is not None:
            features[f"cot_net_z_{market_id}"] = z
            features[f"cot_extreme_flag_{market_id}"] = 1.0 if abs(z) >= 2.0 else 0.0
            z_scores.append(abs(z))

    if z_scores:
        score = min(1.0, mean(z_scores) / 3.0)
        features["cot_risk_score"] = score

    return features


def _net_position(row: Mapping[str, Any]) -> float | None:
    nc_long = _as_float(row.get("noncommercial_long"))
    nc_short = _as_float(row.get("noncommercial_short"))
    oi = _as_float(row.get("open_interest"))
    if nc_long is None or nc_short is None or oi in (None, 0.0):
        return None
    return (nc_long - nc_short) / oi


def _nc_net(row: Mapping[str, Any]) -> float | None:
    nc_long = _as_float(row.get("noncommercial_long"))
    nc_short = _as_float(row.get("noncommercial_short"))
    if nc_long is None or nc_short is None:
        return None
    return nc_long - nc_short


def _z_score(values: list[float]) -> float | None:
    if len(values) < 10:
        return None
    mean_val = mean(values)
    std_val = pstdev(values)
    if std_val == 0:
        return None
    return (values[-1] - mean_val) / std_val


def _parse_ts(value: Any) -> Any | None:
    if not value:
        return None
    from datetime import datetime, timezone

    try:
        dt = datetime.fromisoformat(str(value))
    except Exception:
        try:
            dt = datetime.strptime(str(value), "%Y-%m-%d")
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _as_float(value: Any) -> float | None:
    try:
        return float(value)
    except Exception:
        return None
