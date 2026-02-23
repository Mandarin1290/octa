from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class SeriesHealthResult:
    ok: bool
    code: Optional[str]
    stats: Dict[str, Any]


def validate_price_series(
    df: pd.DataFrame,
    *,
    close_col: str = "close",
    min_rows: int = 2,
    max_nan_frac: float = 0.2,
) -> SeriesHealthResult:
    stats: Dict[str, Any] = {
        "rows_raw": int(len(df)) if isinstance(df, pd.DataFrame) else 0,
        "rows_clean": 0,
        "index_monotonic": None,
        "duplicate_timestamps": None,
        "close_nan_frac": None,
        "close_inf_frac": None,
        "ret_nan_frac": None,
        "ret_inf_frac": None,
        "close_variance": None,
        "ret_variance": None,
        "start_ts": None,
        "end_ts": None,
    }

    if not isinstance(df, pd.DataFrame) or len(df) == 0:
        return SeriesHealthResult(ok=False, code="data_empty_after_clean", stats=stats)
    if close_col not in df.columns:
        return SeriesHealthResult(ok=False, code="data_empty_after_clean", stats=stats)
    if not isinstance(df.index, pd.DatetimeIndex):
        return SeriesHealthResult(ok=False, code="data_non_monotonic_index", stats=stats)

    idx = df.index
    stats["index_monotonic"] = bool(idx.is_monotonic_increasing)
    stats["duplicate_timestamps"] = int(idx.duplicated().sum())
    if not bool(idx.is_monotonic_increasing):
        return SeriesHealthResult(ok=False, code="data_non_monotonic_index", stats=stats)
    if int(idx.duplicated().sum()) > 0:
        return SeriesHealthResult(ok=False, code="data_duplicate_timestamps", stats=stats)

    close = pd.to_numeric(df[close_col], errors="coerce")
    inf_mask = ~np.isfinite(close.to_numpy(dtype=float))
    stats["close_inf_frac"] = float(np.mean(inf_mask)) if len(close) else 1.0
    stats["close_nan_frac"] = float(close.isna().mean()) if len(close) else 1.0
    if float(stats["close_inf_frac"]) > 0.0:
        return SeriesHealthResult(ok=False, code="data_too_many_nans", stats=stats)
    if float(stats["close_nan_frac"]) > float(max_nan_frac):
        return SeriesHealthResult(ok=False, code="data_too_many_nans", stats=stats)

    w = pd.DataFrame({"close": close}, index=idx).dropna(subset=["close"])
    stats["rows_clean"] = int(len(w))
    if len(w) < int(min_rows):
        return SeriesHealthResult(ok=False, code="data_empty_after_clean", stats=stats)

    ret = w["close"].pct_change()
    ret_np = ret.to_numpy(dtype=float)
    stats["ret_nan_frac"] = float(np.mean(np.isnan(ret_np))) if len(ret_np) else 1.0
    stats["ret_inf_frac"] = float(np.mean(~np.isfinite(ret_np))) if len(ret_np) else 1.0
    if float(stats["ret_inf_frac"]) > 0.0 and float(stats["ret_nan_frac"]) > float(max_nan_frac):
        return SeriesHealthResult(ok=False, code="data_too_many_nans", stats=stats)

    close_var = float(w["close"].var(ddof=0)) if len(w) > 1 else 0.0
    ret_var = float(ret.dropna().var(ddof=0)) if ret.dropna().shape[0] > 1 else 0.0
    stats["close_variance"] = close_var
    stats["ret_variance"] = ret_var
    stats["start_ts"] = str(w.index[0]) if len(w) else None
    stats["end_ts"] = str(w.index[-1]) if len(w) else None

    if not np.isfinite(close_var) or not np.isfinite(ret_var):
        return SeriesHealthResult(ok=False, code="data_too_many_nans", stats=stats)
    if close_var <= 0.0:
        return SeriesHealthResult(ok=False, code="data_constant_close", stats=stats)

    return SeriesHealthResult(ok=True, code=None, stats=stats)
