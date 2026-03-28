from __future__ import annotations

import pandas as pd


def _coerce_signal_frame(df_signals: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(df_signals, pd.DataFrame):
        raise TypeError("df_signals must be a pandas DataFrame")
    if df_signals.empty:
        raise ValueError("df_signals must not be empty")
    if not isinstance(df_signals.index, pd.DatetimeIndex):
        raise TypeError("df_signals must use a DatetimeIndex")
    if not df_signals.index.is_monotonic_increasing:
        raise ValueError("df_signals index must be monotonic increasing")
    return df_signals.sort_index()


def build_research_features(df_signals: pd.DataFrame) -> pd.DataFrame:
    signals = _coerce_signal_frame(df_signals)

    if "signal" in signals.columns:
        lagged_signal = pd.to_numeric(signals["signal"], errors="raise").shift(1).fillna(0.0)
        long_signal = (lagged_signal > 0).astype(int)
        short_signal = (lagged_signal < 0).astype(int)
        if "signal_strength" in signals.columns:
            signal_strength = pd.to_numeric(signals["signal_strength"], errors="raise").shift(1).fillna(0.0)
        else:
            signal_strength = lagged_signal.abs()
    else:
        available = {"long_signal", "short_signal"} & set(signals.columns)
        if not available:
            raise ValueError(
                "df_signals must contain either 'signal' or at least one of 'long_signal'/'short_signal'"
            )
        long_signal = (
            pd.to_numeric(signals.get("long_signal", 0), errors="raise")
            .shift(1)
            .fillna(0.0)
            .gt(0)
            .astype(int)
        )
        short_signal = (
            pd.to_numeric(signals.get("short_signal", 0), errors="raise")
            .shift(1)
            .fillna(0.0)
            .gt(0)
            .astype(int)
        )
        if "signal_strength" in signals.columns:
            signal_strength = pd.to_numeric(signals["signal_strength"], errors="raise").shift(1).fillna(0.0)
        else:
            signal_strength = pd.concat([long_signal, short_signal], axis=1).max(axis=1).astype(float)

    features = pd.DataFrame(
        {
            "long_signal": long_signal,
            "short_signal": short_signal,
            "signal_strength": signal_strength.astype(float),
        },
        index=signals.index,
    )
    if ((features["long_signal"] == 1) & (features["short_signal"] == 1)).any():
        raise ValueError("simultaneous long and short signals detected")
    return features


__all__ = ["build_research_features"]
