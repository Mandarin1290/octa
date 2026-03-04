from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from octa.core.utils.typing_safe import as_float
from octa.core.data.io.timeseries_integrity import (
    validate_timeseries_integrity,
    write_quarantine_entry,
)
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


def _is_continuous_market(asset_class: str) -> bool:
    ac = str(asset_class or "").strip().lower()
    return ac in {"fx", "forex", "crypto", "cryptocurrency"}


def evaluate_data_quality(
    *,
    symbol: str,
    timeframe: str,
    parquet_path: str,
    asset_class: str,
    policy: DataQualityPolicy,
    quarantine_dir: Optional[Path] = None,
) -> GateDecision:
    tf = normalize_timeframe(timeframe)
    exp_s = timeframe_seconds(tf)

    # --- Pre-flight integrity check for Futures 1D ---
    # All Futures 1D parquets in raw/Futures_Parquet have a corrupt 'datetime'
    # index (price float strings instead of timestamps).  Detect this before
    # load_parquet() crashes with a generic "missing time column" error.
    ac_lower = str(asset_class or "").lower().strip()
    if ac_lower in {"futures", "future"} and tf == "1D":
        try:
            raw_df = pd.read_parquet(str(parquet_path))
        except Exception as e:
            return GateDecision(
                symbol=symbol,
                timeframe=tf,
                stage="data_quality",
                status="FAIL",
                reason="FUTURES_1D_CORRUPT_DATA:UNREADABLE",
                details={"error": str(e), "path": parquet_path},
            )
        ok, reason, integrity_details = validate_timeseries_integrity(
            raw_df, asset_class, tf, parquet_path
        )
        if not ok:
            if quarantine_dir is not None:
                write_quarantine_entry(
                    quarantine_dir,
                    path=parquet_path,
                    reason=reason,
                    asset_class=asset_class,
                    timeframe=tf,
                )
            return GateDecision(
                symbol=symbol,
                timeframe=tf,
                stage="data_quality",
                status="FAIL",
                reason=reason,
                details=integrity_details,
            )

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

    ac = str(asset_class or "").lower()
    is_continuous = _is_continuous_market(ac)
    # 1D bars on non-continuous markets legitimately include weekend/holiday gaps.
    strict_spacing = bool(exp_s is not None and len(idx) >= 3 and (tf != "1D" or is_continuous))
    if strict_spacing:
        deltas = idx.to_series().diff().dropna().dt.total_seconds().astype(float)
        tol = float(policy.spacing_tolerance_seconds)
        if is_continuous and tf == "1D":
            # FX/crypto daily: market is 24/5 (not 24/7) — exclude weekend and holiday
            # gaps before checking regular business-day spacing.
            # Regular business-day delta is ~exp_s (86400 s); any delta > 1.5× exp_s
            # (≈ 36 h) is a non-trading-day gap (weekend Fri→Mon = 3 days, holiday = 2-4 days).
            weekend_threshold_s = as_float(exp_s) * 1.5
            comparable = deltas[deltas <= weekend_threshold_s]
            details["fx_1d_weekend_threshold_s"] = float(weekend_threshold_s)
            details["fx_1d_weekend_gaps_excluded"] = int(len(deltas) - len(comparable))
        elif is_continuous:
            # Intraday continuous markets (fx/crypto): evaluate all deltas.
            comparable = deltas
        elif tf == "1D":
            # Non-continuous 1D (equity/futures/index): no strict spacing (already skipped
            # via strict_spacing gate above) — kept for completeness.
            comparable = deltas
        else:
            # Session markets (equities/futures/options): ignore known session breaks
            # and evaluate only intraday-adjacent deltas.
            max_intraday_gap_s = as_float(exp_s) * 4.0
            comparable = deltas[deltas <= max_intraday_gap_s]
            details["max_intraday_gap_s"] = max_intraday_gap_s
            details["compared_deltas_n"] = int(len(comparable))
            details["ignored_session_gap_n"] = int(len(deltas) - len(comparable))
        match = comparable.sub(as_float(exp_s)).abs() <= tol
        match_frac = float(match.mean()) if len(match) else 0.0
        details["expected_s"] = exp_s
        details["match_frac"] = match_frac
        if match_frac < policy.min_spacing_match_frac:
            return GateDecision(symbol=symbol, timeframe=tf, stage="data_quality", status="FAIL", reason="spacing_not_timeframe_like", details=details)

        # missing bars: continuous intraday only (fx/crypto hourly or sub-hourly).
        # Skipped for 1D because weekend/holiday calendar gaps inflate expected_n.
        if is_continuous and tf != "1D":
            span_s = float((idx[-1] - idx[0]).total_seconds())
            expected_n = int(span_s // as_float(exp_s)) + 1 if span_s > 0 else len(idx)
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
