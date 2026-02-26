"""Aviation traffic (OpenSky ADS-B) macro proxy features.

Data source (MANDATORY): OpenSky Network REST API
- Endpoint: GET https://opensky-network.org/api/states/all
- No authentication (free tier)

Purpose (STRICT):
- Macro / demand proxy
- Regime & risk signal
- Exogenous feature input for 1D and 1H models only

Explicitly NOT:
- Trading feed
- HFT / latency optimization
- Entry trigger
- Position sizing input

Storage (MANDATORY):
/home/n-b/Octa/altdata/aviation/
  region=EU/
  region=US/
  region=ASIA/
  region=GLOBAL/
    YYYY-MM-DD.parquet

Parquet: snappy compression, UTC timestamps.

Public interface (MANDATORY):
- get_aviation_features(timestamp, region) -> dict

All other functions are internal (prefixed with _).
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
import requests

from octa.core.utils.typing_safe import as_float

__all__ = ["get_aviation_features"]


# Fixed bounding boxes (MANDATORY)
# Values are approximate and intentionally simple (no geo libraries).
BBOX: dict[str, dict[str, float]] = {
    "US": {"lat_min": 15.0, "lat_max": 72.0, "lon_min": -170.0, "lon_max": -50.0},
    "EU": {"lat_min": 35.0, "lat_max": 72.0, "lon_min": -10.0, "lon_max": 40.0},
    "ASIA": {"lat_min": 5.0, "lat_max": 55.0, "lon_min": 60.0, "lon_max": 150.0},
}

REGIONS = ("US", "EU", "ASIA", "GLOBAL")


@dataclass(frozen=True)
class AviationConfig:
    base_dir: Path = Path("/home/n-b/Octa/altdata/aviation")
    interval_minutes: int = 5
    # Quality gate
    min_aircraft_count: int = 50
    # If last observation is older than 2x interval => invalid (NaN)
    max_gap_multiplier: float = 2.0


def _floor_to_interval(ts: pd.Timestamp, minutes: int) -> pd.Timestamp:
    ts = pd.Timestamp(ts).tz_convert("UTC") if ts.tzinfo else pd.Timestamp(ts, tz="UTC")
    freq = f"{int(minutes)}min"
    return ts.floor(freq)


def _region_mask(lat: pd.Series, lon: pd.Series, region: str) -> pd.Series:
    if region == "GLOBAL":
        return pd.Series(True, index=lat.index)
    box = BBOX.get(region)
    if not box:
        return pd.Series(False, index=lat.index)
    return (
        (lat >= box["lat_min"])
        & (lat <= box["lat_max"])
        & (lon >= box["lon_min"])
        & (lon <= box["lon_max"])
    )


def _safe_float(x: Any) -> float:
    try:
        v = float(x)
        if math.isfinite(v):
            return v
        return float("nan")
    except Exception:
        return float("nan")


def _fetch_opensky_states(timeout_s: int = 10) -> dict:
    url = "https://opensky-network.org/api/states/all"
    r = requests.get(url, timeout=timeout_s)
    r.raise_for_status()
    return r.json()


def _states_to_frame(payload: dict) -> pd.DataFrame:
    # OpenSky returns: {time: int, states: [[icao24, callsign, origin_country, time_position, last_contact,
    #   longitude, latitude, baro_altitude, on_ground, velocity, true_track, vertical_rate, sensors, geo_altitude,
    #   squawk, spi, position_source], ...]}
    states = payload.get("states")
    if not states:
        return pd.DataFrame()

    cols = [
        "icao24",
        "callsign",
        "origin_country",
        "time_position",
        "last_contact",
        "longitude",
        "latitude",
        "baro_altitude",
        "on_ground",
        "velocity",
        "true_track",
        "vertical_rate",
        "sensors",
        "geo_altitude",
        "squawk",
        "spi",
        "position_source",
    ]
    df = pd.DataFrame(states, columns=cols)
    # keep only fields we need; avoid storing per-flight history beyond current snapshot
    keep = ["longitude", "latitude", "on_ground", "velocity", "vertical_rate"]
    df = df[keep]
    # coerce numerics
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["velocity"] = pd.to_numeric(df["velocity"], errors="coerce")
    df["vertical_rate"] = pd.to_numeric(df["vertical_rate"], errors="coerce")
    df["on_ground"] = df["on_ground"].astype("boolean")
    df = df.dropna(subset=["longitude", "latitude"])
    return df


def _compute_region_features(df: pd.DataFrame, region: str) -> dict[str, float]:
    # Apply region filter
    if df.empty:
        return {
            "flights_active": float("nan"),
            "flights_ground": float("nan"),
            "mean_speed": float("nan"),
            "activity_ratio": float("nan"),
        }

    mask = _region_mask(df["latitude"], df["longitude"], region)
    sub = df.loc[mask]
    if sub.empty:
        return {
            "flights_active": 0.0,
            "flights_ground": 0.0,
            "mean_speed": float("nan"),
            "activity_ratio": float("nan"),
        }

    aircraft_count = float(len(sub))
    ground = float(sub["on_ground"].fillna(False).astype(bool).sum())
    airborne = float(max(0.0, aircraft_count - ground))
    airborne_ratio = airborne / aircraft_count if aircraft_count > 0 else float("nan")

    # Mean speed based on airborne only (m/s)
    v = sub.loc[~sub["on_ground"].fillna(False).astype(bool), "velocity"].dropna()
    mean_speed = float(v.mean()) if len(v) > 0 else float("nan")

    return {
        "flights_active": aircraft_count,
        "flights_ground": ground,
        "mean_speed": mean_speed,
        "activity_ratio": float(airborne_ratio),
    }


def _derive_features(history: pd.DataFrame) -> pd.DataFrame:
    """Compute derived features in a leakage-safe way.

    Required outputs (ONLY these):
    LEVEL-1:
      - flights_active
      - flights_ground
      - mean_speed
      - activity_ratio
    LEVEL-2:
      - traffic_momentum (Δ vs prior period)
      - traffic_zscore (rolling 30D, computed from past only)
      - congestion_index
      - shock_flag (|z| > 2.5)
    """
    df = history.copy()
    for c in ["flights_active", "flights_ground", "mean_speed", "activity_ratio"]:
        if c not in df.columns:
            df[c] = np.nan

    # Δ vs prior period (no look-ahead)
    df["traffic_momentum"] = df["flights_active"] - df["flights_active"].shift(1)

    # 30D z-score, computed from past only (shift(1) on rolling stats)
    roll_mean = df["flights_active"].rolling("30D", min_periods=24).mean().shift(1)
    roll_std = df["flights_active"].rolling("30D", min_periods=24).std(ddof=0).shift(1)
    df["traffic_zscore"] = (df["flights_active"] - roll_mean) / (roll_std.replace(0, np.nan))

    # congestion index: activity vs past baseline (past-only)
    eps = 1e-9
    df["congestion_index"] = df["flights_active"] / (roll_mean.abs() + eps)

    z = df["traffic_zscore"].astype(float)
    df["shock_flag"] = ((z.abs() > 2.5) & z.notna()).astype(float)

    # Keep only the allowed feature set
    keep = [
        "flights_active",
        "flights_ground",
        "mean_speed",
        "activity_ratio",
        "traffic_momentum",
        "traffic_zscore",
        "congestion_index",
        "shock_flag",
    ]
    return df[keep]


def _region_day_path(base_dir: Path, region: str, day: str) -> Path:
    return base_dir / f"region={region}" / f"{day}.parquet"


def _append_row(base_dir: Path, region: str, ts: pd.Timestamp, row: dict[str, float]) -> None:
    base_dir = Path(base_dir)
    if region not in REGIONS:
        raise ValueError(f"Unknown region: {region}")

    ts = pd.Timestamp(ts, tz="UTC")
    day = ts.strftime("%Y-%m-%d")
    p = _region_day_path(base_dir, region, day)
    p.parent.mkdir(parents=True, exist_ok=True)

    new = pd.DataFrame([row], index=pd.DatetimeIndex([ts], tz="UTC", name="timestamp"))

    if p.exists():
        try:
            old = pd.read_parquet(p)
        except Exception:
            old = pd.DataFrame()
        if not old.empty:
            if "timestamp" in old.columns and not isinstance(old.index, pd.DatetimeIndex):
                old = old.set_index("timestamp")
            if isinstance(old.index, pd.DatetimeIndex) and old.index.tz is None:
                old.index = old.index.tz_localize("UTC")
        df = pd.concat([old, new], axis=0)
        df = df[~df.index.duplicated(keep="last")].sort_index()
    else:
        df = new

    # Derive features and persist (snappy)
    df_feat = _derive_features(df)
    df_feat.to_parquet(p, compression="snappy")


def _nan_feature_dict() -> Dict[str, float]:
    return {
        "flights_active": float("nan"),
        "flights_ground": float("nan"),
        "mean_speed": float("nan"),
        "activity_ratio": float("nan"),
        "traffic_momentum": float("nan"),
        "traffic_zscore": float("nan"),
        "congestion_index": float("nan"),
        "shock_flag": float("nan"),
    }


def get_aviation_features(timestamp: Any, region: str) -> Dict[str, float]:
    """Return aviation exogenous features for a given timestamp and region.

    - Forward-filled from the last available observation at or before timestamp.
    - If data is missing, stale (> 2x interval), empty, or below min aircraft count,
      returns NaNs (no estimation / no imputation beyond ffill).
    """
    cfg = AviationConfig()
    region = str(region).strip().upper()
    if region not in REGIONS:
        raise ValueError(f"region must be one of {REGIONS}")

    ts = pd.Timestamp(timestamp)
    ts = ts.tz_convert("UTC") if ts.tzinfo else ts.tz_localize("UTC")

    # Load the day's file; if timestamp is near midnight, also consider previous day for ffill.
    days = [ts.strftime("%Y-%m-%d"), (ts - pd.Timedelta(days=1)).strftime("%Y-%m-%d")]
    frames = []
    for day in days:
        p = _region_day_path(cfg.base_dir, region, day)
        if p.exists():
            try:
                df = pd.read_parquet(p)
                if "timestamp" in df.columns and not isinstance(df.index, pd.DatetimeIndex):
                    df = df.set_index("timestamp")
                if not isinstance(df.index, pd.DatetimeIndex):
                    continue
                if df.index.tz is None:
                    df.index = df.index.tz_localize("UTC")
                frames.append(df)
            except Exception:
                continue

    if not frames:
        return _nan_feature_dict()

    hist = pd.concat(frames, axis=0)
    hist = hist[~hist.index.duplicated(keep="last")].sort_index()
    hist = hist[~hist.index.isna()]
    if hist.empty:
        return _nan_feature_dict()

    # find last observation at or before ts
    pos = hist.index.searchsorted(ts, side="right") - 1
    if pos < 0:
        return _nan_feature_dict()

    last_ts = hist.index[pos]
    gap = (ts - last_ts).total_seconds()
    max_gap = cfg.max_gap_multiplier * cfg.interval_minutes * 60.0
    if gap > max_gap:
        return _nan_feature_dict()

    row = hist.iloc[pos]

    # quality gate: aircraft_count < threshold -> NaN
    try:
        active = float(row.get("flights_active"))
        if math.isfinite(active) and active < cfg.min_aircraft_count:
            return _nan_feature_dict()
    except Exception:
        pass

    out = {}
    for k in _nan_feature_dict().keys():
        out[k] = _safe_float(row.get(k))
    return out


def _collect_once(base_dir: Path, now_utc: Optional[pd.Timestamp] = None) -> None:
    """One polling cycle: fetch current snapshot and persist per-region aggregates."""
    cfg = AviationConfig(base_dir=Path(base_dir))
    now = now_utc or pd.Timestamp(datetime.now(timezone.utc))
    ts = _floor_to_interval(now, cfg.interval_minutes)

    try:
        payload = _fetch_opensky_states(timeout_s=10)
        frame = _states_to_frame(payload)
    except Exception:
        frame = pd.DataFrame()

    for region in REGIONS:
        if frame.empty:
            row = _nan_feature_dict()
            _append_row(cfg.base_dir, region, ts, row)
            continue
        base = _compute_region_features(frame, region)
        # If too few aircraft, write NaNs (no estimation)
        flights_active = as_float(base.get("flights_active"), default=float("nan"))
        if math.isfinite(flights_active) and flights_active < cfg.min_aircraft_count:
            row = _nan_feature_dict()
        else:
            row = {**_nan_feature_dict(), **base}
        _append_row(cfg.base_dir, region, ts, row)


def _run_collector_loop(base_dir: Path) -> None:
    """Run collector forever.

    Interval:
    - Target: every 5 minutes
    - Backoff: 10-15 minutes on errors

    Deterministic:
    - Timestamps are floored to the configured interval in UTC.
    """
    cfg = AviationConfig(base_dir=Path(base_dir))
    base_dir = cfg.base_dir

    sleep_ok = cfg.interval_minutes * 60
    sleep_backoff = 10 * 60

    while True:
        t0 = time.time()
        ok = True
        try:
            _collect_once(base_dir)
        except Exception:
            ok = False

        # next sleep
        dt_s = time.time() - t0
        if ok:
            time.sleep(max(1, sleep_ok - dt_s))
        else:
            # simple backoff, no estimation
            time.sleep(max(1, sleep_backoff - dt_s))
            # gradual backoff to 15min if repeated failures
            sleep_backoff = min(15 * 60, int(sleep_backoff * 1.25))


# Note: collector loop is intentionally not exposed as a public interface.
