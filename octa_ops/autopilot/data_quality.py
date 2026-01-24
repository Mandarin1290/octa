from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

from octa_training.core.io_parquet import load_parquet

from .types import GateDecision, normalize_timeframe, timeframe_seconds


@dataclass
class DataQualityPolicy:
    max_duplicate_frac: float = 0.001
    max_missing_frac: float = 0.02
    min_spacing_match_frac: float = 0.95
    spacing_tolerance_seconds: int = 120
    require_utc_timezone: bool = True
    allow_naive_timestamps: bool = False


def _spacing_stats(idx: pd.DatetimeIndex) -> Dict[str, Any]:
    if len(idx) < 3:
        return {"n": len(idx), "median_s": None, "p90_s": None, "match_frac": None}
    deltas = idx.to_series().diff().dropna().dt.total_seconds().astype(float)
    if deltas.empty:
        return {"n": len(idx), "median_s": None, "p90_s": None, "match_frac": None}
    return {
        "n": int(len(idx)),
        "median_s": float(deltas.median()),
        "p90_s": float(deltas.quantile(0.9)),
    }


def evaluate_data_quality(
    *,
    symbol: str,
    timeframe: str,
    parquet_path: str,
    asset_class: str,
    policy: DataQualityPolicy,
) -> GateDecision:
    tf = normalize_timeframe(timeframe)
    exp_s = timeframe_seconds(tf)

    try:
        df = load_parquet(Path(parquet_path))
    except Exception as e:
        return GateDecision(symbol=symbol, timeframe=tf, stage="data_quality", status="FAIL", reason="data_load_failed", details={"error": str(e), "path": parquet_path})

    if not isinstance(df.index, pd.DatetimeIndex):
        return GateDecision(
            symbol=symbol,
            timeframe=tf,
            stage="data_quality",
            status="FAIL",
            reason="timestamp_not_datetimeindex",
            details={"path": parquet_path},
        )

    idx = df.index

    # timezone expectations
    if policy.require_utc_timezone:
        if idx.tz is None:
            if not policy.allow_naive_timestamps:
                return GateDecision(symbol=symbol, timeframe=tf, stage="data_quality", status="FAIL", reason="timestamp_timezone_missing", details={"path": parquet_path})
            idx = idx.tz_localize("UTC")
        else:
            try:
                idx = idx.tz_convert("UTC")
            except Exception:
                return GateDecision(symbol=symbol, timeframe=tf, stage="data_quality", status="FAIL", reason="timestamp_tz_convert_failed", details={"path": parquet_path, "tz": str(idx.tz)})

    # monotonic + duplicates
    is_monotonic = bool(idx.is_monotonic_increasing)
    # NOTE: core loader already drops duplicates, but keep this check for vendor/path anomalies.
    dup_frac = float(pd.Series(idx).duplicated().mean()) if len(idx) else 0.0
    if not is_monotonic:
        return GateDecision(symbol=symbol, timeframe=tf, stage="data_quality", status="FAIL", reason="timestamp_not_monotonic", details={"dup_frac": dup_frac, "path": parquet_path})
    if dup_frac > policy.max_duplicate_frac:
        return GateDecision(symbol=symbol, timeframe=tf, stage="data_quality", status="FAIL", reason="too_many_duplicates", details={"dup_frac": dup_frac, "path": parquet_path})

    # spacing check (only when we know expected seconds)
    details: Dict[str, Any] = {"path": parquet_path, "dup_frac": dup_frac, "n": int(len(idx))}
    st = _spacing_stats(idx)
    details.update(st)

    if exp_s is not None and len(idx) >= 3:
        deltas = idx.to_series().diff().dropna().dt.total_seconds().astype(float)
        tol = float(policy.spacing_tolerance_seconds)
        match = (deltas.sub(float(exp_s)).abs() <= tol)
        match_frac = float(match.mean()) if len(match) else 0.0
        details["expected_s"] = exp_s
        details["match_frac"] = match_frac
        if match_frac < policy.min_spacing_match_frac:
            return GateDecision(symbol=symbol, timeframe=tf, stage="data_quality", status="FAIL", reason="spacing_not_timeframe_like", details=details)

        # missing bars (continuous markets only: fx/crypto)
        ac = str(asset_class or "").lower()
        if ac in {"fx", "crypto"}:
            span_s = float((idx[-1] - idx[0]).total_seconds())
            expected_n = int(span_s // float(exp_s)) + 1 if span_s > 0 else len(idx)
            missing_frac = 0.0
            if expected_n > 0:
                missing_frac = float(max(0, expected_n - len(idx)) / expected_n)
            details["expected_n"] = expected_n
            details["missing_frac"] = missing_frac
            if missing_frac > policy.max_missing_frac:
                return GateDecision(symbol=symbol, timeframe=tf, stage="data_quality", status="FAIL", reason="too_many_missing_bars", details=details)

    return GateDecision(symbol=symbol, timeframe=tf, stage="data_quality", status="PASS", reason=None, details=details)


def write_quality_outputs(
    *,
    run_dir: str,
    decisions: List[GateDecision],
    timeframes: List[str],
) -> Tuple[str, str]:
    out_dir = Path(run_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # matrix
    rows: Dict[str, Dict[str, str]] = {}
    for d in decisions:
        rows.setdefault(d.symbol, {})[normalize_timeframe(d.timeframe)] = d.status
    tf_cols = [normalize_timeframe(t) for t in timeframes]
    mat = []
    for sym in sorted(rows.keys()):
        r = {"symbol": sym}
        for tf in tf_cols:
            r[tf] = rows[sym].get(tf, "")
        mat.append(r)
    df = pd.DataFrame(mat)
    matrix_path = out_dir / "data_quality_matrix.csv"
    df.to_csv(matrix_path, index=False)

    details_path = out_dir / "data_quality_details.json"
    payload = [
        {
            "symbol": d.symbol,
            "timeframe": normalize_timeframe(d.timeframe),
            "status": d.status,
            "reason": d.reason,
            "details": d.details,
        }
        for d in decisions
    ]
    details_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    return str(matrix_path), str(details_path)
