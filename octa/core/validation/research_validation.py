from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


MAX_ABS_RETURN = 1.0
MIN_SIGNAL_DENSITY = 0.0
MAX_SIGNAL_DENSITY = 1.0


def _validate_frame(name: str, frame: pd.DataFrame) -> None:
    if not isinstance(frame, pd.DataFrame):
        raise TypeError(f"{name} must be a pandas DataFrame")
    if frame.empty:
        raise ValueError(f"{name} must not be empty")
    if not isinstance(frame.index, pd.DatetimeIndex):
        raise TypeError(f"{name} must use a DatetimeIndex")
    if not frame.index.is_monotonic_increasing:
        raise ValueError(f"{name} index must be monotonic increasing")
    if frame.index.has_duplicates:
        raise ValueError(f"{name} index must not contain duplicates")
    if frame.isna().any().any():
        raise ValueError(f"{name} contains NaNs")


def validate_research_payload(
    df_signals: pd.DataFrame,
    df_returns: pd.DataFrame,
    *,
    max_abs_return: float = MAX_ABS_RETURN,
) -> dict[str, Any]:
    _validate_frame("df_signals", df_signals)
    _validate_frame("df_returns", df_returns)

    if not {"long_signal", "short_signal"}.issubset(df_signals.columns):
        raise ValueError("df_signals must contain long_signal and short_signal columns")

    signal_active = (df_signals["long_signal"].astype(int) + df_signals["short_signal"].astype(int)) > 0
    density = float(signal_active.mean())
    if not (MIN_SIGNAL_DENSITY < density < MAX_SIGNAL_DENSITY):
        raise ValueError(f"signal density out of bounds: {density:.6f}")

    numeric_returns = df_returns.apply(pd.to_numeric, errors="raise")
    finite_mask = np.isfinite(numeric_returns.to_numpy(dtype=float))
    if not finite_mask.all():
        raise ValueError("df_returns contains non-finite values")

    max_observed_abs_return = float(np.abs(numeric_returns.to_numpy(dtype=float)).max())
    if max_observed_abs_return > max_abs_return:
        raise ValueError(
            f"return spike exceeds threshold: observed={max_observed_abs_return:.6f} threshold={max_abs_return:.6f}"
        )

    report = {
        "status": "ok",
        "rows": {
            "signals": int(len(df_signals)),
            "returns": int(len(df_returns)),
        },
        "signal_density": density,
        "max_abs_return": max_observed_abs_return,
        "index": {
            "signals_monotonic": bool(df_signals.index.is_monotonic_increasing),
            "returns_monotonic": bool(df_returns.index.is_monotonic_increasing),
        },
    }
    return report


def write_validation_report(report: dict[str, Any], out_path: str | Path) -> Path:
    path = Path(out_path)
    if path.exists():
        raise FileExistsError(f"refusing to overwrite existing artifact: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, sort_keys=True, indent=2), encoding="utf-8")
    return path


__all__ = ["MAX_ABS_RETURN", "validate_research_payload", "write_validation_report"]
