from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Mapping


def build(payloads: Mapping[str, Any], *, asof_ts: str | None = None) -> dict[str, float]:
    gdelt = payloads.get("gdelt", {})
    edgar = payloads.get("edgar", {})

    features: dict[str, float] = {}

    gdelt_rows = gdelt.get("rows", []) if isinstance(gdelt, dict) else []
    status = _gdelt_status(gdelt)
    status_code = float(_status_code(status))
    features["event_risk_status"] = status_code
    features["event_risk_status_ok"] = 1.0 if status == "ok" else 0.0
    features["event_risk_status_missing_cache"] = 1.0 if status == "missing_cache" else 0.0
    features["event_risk_status_net_error"] = 1.0 if status == "net_error" else 0.0
    asof_dt = _parse_ts(asof_ts) if asof_ts else None
    event_scores = _compute_event_scores(gdelt_rows, asof_dt)
    features.update(event_scores)

    filings = edgar.get("filings", []) if isinstance(edgar, dict) else []
    features["edgar_event_count"] = float(len(filings))

    return features


def _gdelt_status(gdelt: Any) -> str:
    if not isinstance(gdelt, dict):
        return "missing_cache"
    status = str(gdelt.get("status", "")).strip().lower()
    if status in {"ok", "net_error"}:
        return status
    if gdelt.get("rows"):
        return "ok"
    return "missing_cache"


def _status_code(status: str) -> int:
    return {"ok": 0, "missing_cache": 1, "net_error": 2}.get(status, 1)


def _compute_event_scores(rows: list[dict[str, Any]], asof_dt: datetime | None) -> dict[str, float]:
    if not rows:
        return {"event_risk_score": 0.0}
    filtered = []
    for row in rows:
        release_ts = _parse_ts(row.get("release_ts")) or _release_from_asof(row.get("asof_date"))
        if asof_dt is not None and release_ts is not None and release_ts > asof_dt:
            continue
        filtered.append(row)

    if not filtered:
        return {"event_risk_score": 0.0}

    scores: dict[str, float] = {}
    by_query: dict[str, dict[str, float]] = {}
    for row in filtered:
        query_id = str(row.get("query_id", "")).strip().lower()
        window = str(row.get("window", "")).strip().lower()
        metric = str(row.get("metric", "")).strip().lower()
        try:
            value = float(row.get("value"))
        except Exception:
            continue
        if metric not in {"volume", "volume_intensity"} or not query_id:
            continue
        by_query.setdefault(query_id, {})[window] = value

    risk_vals = []
    for query_id, windows in by_query.items():
        val_1d = windows.get("1d")
        val_7d = windows.get("7d")
        if val_1d is not None:
            scores[f"gdelt_{query_id}_1d"] = val_1d
            risk_vals.append(_cap01(val_1d))
        if val_7d is not None:
            scores[f"gdelt_{query_id}_7d"] = val_7d

    if risk_vals:
        risk = sum(risk_vals) / float(len(risk_vals))
    else:
        risk = 0.0
    scores["event_risk_score"] = _cap01(risk)

    if "gdelt_conflict_1d" in scores:
        scores["conflict_intensity"] = _cap01(scores["gdelt_conflict_1d"])
    if "gdelt_recession_1d" in scores:
        scores["recession_intensity"] = _cap01(scores["gdelt_recession_1d"])

    if "gdelt_conflict_7d" in scores and "gdelt_conflict_1d" in scores:
        scores["event_risk_delta_1d"] = scores["gdelt_conflict_1d"] - scores["gdelt_conflict_7d"]

    scores["event_risk_high_flag"] = 1.0 if risk >= 0.7 else 0.0
    return scores


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
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


def _release_from_asof(asof_date: Any) -> datetime | None:
    try:
        dt = datetime.fromisoformat(str(asof_date))
    except Exception:
        try:
            dt = datetime.strptime(str(asof_date), "%Y-%m-%d")
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt + timedelta(hours=23, minutes=59, seconds=59)


def _cap01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))
