from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from octa.core.utils.typing_safe import as_float


def build(payloads: Mapping[str, Any], *, asof_ts: str | None = None) -> dict[str, float]:
    gdelt = payloads.get("gdelt", {})
    edgar = payloads.get("edgar", {})
    news_events = payloads.get("news_events", {})
    scheduled_events = payloads.get("scheduled_events", {})

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

    # News/event RSS layer (institutional + high-quality free media)
    news_rows = news_events.get("rows", []) if isinstance(news_events, dict) else []
    news_scores = _compute_news_scores(news_rows, asof_dt)
    features.update(news_scores)
    scheduled_rows = scheduled_events.get("rows", []) if isinstance(scheduled_events, dict) else []
    features.update(_compute_scheduled_scores(scheduled_rows, asof_dt))

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
        raw_value = row.get("value")
        if raw_value is None:
            continue
        value = as_float(raw_value, default=float("nan"))
        if value != value:  # NaN
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


# ── News/event RSS layer features ─────────────────────────────────────────────

def _compute_news_scores(
    rows: list[dict[str, Any]], asof_dt: datetime | None
) -> dict[str, float]:
    """
    Compute risk-context features from the news_events RSS layer.

    Features (all prefixed news_):
      news_event_count          total deduplicated events (filtered to ≤ asof_dt)
      news_tier1_count          Tier 1 (official institution) event count
      news_tier3_count          Tier 3 (business media) event count
      news_risk_score           importance-weighted risk score, Tier 1 weighted 2× Tier 3
      news_critical_flag        1.0 if any event has severity="critical"
      news_official_flag        1.0 if any Tier 1 event has severity ≥ "high"
      news_central_bank_flag    1.0 if any event_type="rates" or "central_bank" with sev ≥ "high"
      news_energy_flag          1.0 if any event_type="energy" with sev ≥ "high"
      news_geopolitics_flag     1.0 if any event_type="geopolitics" with sev ≥ "critical"
      news_status               0=ok_with_data, 1=missing_cache, 2=empty
    """
    if not rows:
        return {
            "news_event_count": 0.0,
            "news_tier1_count": 0.0,
            "news_tier3_count": 0.0,
            "news_risk_score": 0.0,
            "news_critical_flag": 0.0,
            "news_official_flag": 0.0,
            "news_central_bank_flag": 0.0,
            "news_energy_flag": 0.0,
            "news_geopolitics_flag": 0.0,
            "news_status": 1.0,
        }

    _severity_order = {"low": 0, "medium": 1, "high": 2, "critical": 3}

    filtered: list[dict[str, Any]] = []
    for row in rows:
        published_at = str(row.get("published_at", "")).strip()
        if asof_dt is not None and published_at:
            pub_dt = _parse_ts(published_at)
            if pub_dt is not None and pub_dt > asof_dt:
                continue  # future-dated — skip (leakage guard)
        filtered.append(row)

    if not filtered:
        return {
            "news_event_count": 0.0,
            "news_tier1_count": 0.0,
            "news_tier3_count": 0.0,
            "news_risk_score": 0.0,
            "news_critical_flag": 0.0,
            "news_official_flag": 0.0,
            "news_central_bank_flag": 0.0,
            "news_energy_flag": 0.0,
            "news_geopolitics_flag": 0.0,
            "news_status": 1.0,
        }

    tier1_count = 0
    tier3_count = 0
    score_tier1: list[float] = []
    score_tier3: list[float] = []
    has_critical = False
    has_official_high = False
    has_cb_high = False
    has_energy_high = False
    has_geo_critical = False

    for row in filtered:
        tier = int(row.get("source_tier", 3))
        importance = float(row.get("importance_score", 0.0))
        event_type = str(row.get("event_type", "")).lower()
        severity = str(row.get("severity", "low")).lower()
        sev_rank = _severity_order.get(severity, 0)

        if tier <= 1:
            tier1_count += 1
            score_tier1.append(importance)
        else:
            tier3_count += 1
            score_tier3.append(importance)

        if sev_rank >= _severity_order["critical"]:
            has_critical = True
        if tier <= 1 and sev_rank >= _severity_order["high"]:
            has_official_high = True
        if event_type in {"rates", "central_bank"} and sev_rank >= _severity_order["high"]:
            has_cb_high = True
        if event_type == "energy" and sev_rank >= _severity_order["high"]:
            has_energy_high = True
        if event_type == "geopolitics" and sev_rank >= _severity_order["critical"]:
            has_geo_critical = True

    # Tier-weighted risk score: Tier 1 contributes with weight 2, Tier 3 with weight 1
    weighted_sum = 2.0 * sum(score_tier1) + 1.0 * sum(score_tier3)
    weighted_count = 2.0 * tier1_count + 1.0 * tier3_count
    news_risk_score = _cap01(weighted_sum / weighted_count) if weighted_count > 0 else 0.0

    return {
        "news_event_count": float(len(filtered)),
        "news_tier1_count": float(tier1_count),
        "news_tier3_count": float(tier3_count),
        "news_risk_score": news_risk_score,
        "news_critical_flag": 1.0 if has_critical else 0.0,
        "news_official_flag": 1.0 if has_official_high else 0.0,
        "news_central_bank_flag": 1.0 if has_cb_high else 0.0,
        "news_energy_flag": 1.0 if has_energy_high else 0.0,
        "news_geopolitics_flag": 1.0 if has_geo_critical else 0.0,
        "news_status": 0.0,
    }


def _compute_scheduled_scores(rows: list[dict[str, Any]], asof_dt: datetime | None) -> dict[str, float]:
    base = {
        "scheduled_event_count": 0.0,
        "scheduled_tier1_count": 0.0,
        "scheduled_central_bank_flag": 0.0,
        "scheduled_macro_window_flag": 0.0,
        "scheduled_energy_window_flag": 0.0,
        "scheduled_event_bonus": 0.0,
        "scheduled_status": 1.0,
    }
    if not rows or asof_dt is None:
        return base

    active_rows: list[dict[str, Any]] = []
    central_bank_flag = False
    macro_window_flag = False
    energy_window_flag = False
    bonus = 0.0

    for row in rows:
        known_at = _parse_ts(row.get("known_at"))
        if known_at is not None and known_at > asof_dt:
            continue
        scheduled_at = _parse_ts(row.get("scheduled_at"))
        if scheduled_at is None:
            continue
        pre_window_h = max(float(row.get("pre_window_hours", 24.0) or 24.0), 0.0)
        post_window_h = max(float(row.get("post_window_hours", 2.0) or 2.0), 0.0)
        window_start = scheduled_at - timedelta(hours=pre_window_h)
        window_end = scheduled_at + timedelta(hours=post_window_h)
        if not (window_start <= asof_dt <= window_end):
            continue
        active_rows.append(row)
        event_type = str(row.get("event_type", "")).lower()
        tier = int(row.get("source_tier", 3))
        severity_floor = str(row.get("severity_floor", "medium")).lower()
        sev_bonus = {"low": 0.02, "medium": 0.04, "high": 0.07, "critical": 0.10}.get(severity_floor, 0.04)
        tier_mult = 1.0 if tier <= 1 else 0.6
        bonus = min(0.15, bonus + sev_bonus * tier_mult)
        if tier <= 1 and event_type in {"central_bank", "rates"}:
            central_bank_flag = True
        if event_type in {"central_bank", "rates", "inflation", "labor", "growth"}:
            macro_window_flag = True
        if event_type == "energy":
            energy_window_flag = True

    if not active_rows:
        return base

    tier1_count = sum(1 for row in active_rows if int(row.get("source_tier", 3)) <= 1)
    return {
        "scheduled_event_count": float(len(active_rows)),
        "scheduled_tier1_count": float(tier1_count),
        "scheduled_central_bank_flag": 1.0 if central_bank_flag else 0.0,
        "scheduled_macro_window_flag": 1.0 if macro_window_flag else 0.0,
        "scheduled_energy_window_flag": 1.0 if energy_window_flag else 0.0,
        "scheduled_event_bonus": bonus,
        "scheduled_status": 0.0,
    }
