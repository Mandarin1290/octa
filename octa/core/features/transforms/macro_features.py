from __future__ import annotations

import pandas as pd


def build_macro_features(wide_macro: pd.DataFrame) -> pd.DataFrame:
    """Create simple, low-overfit macro features from wide macro levels.

    Input: columns like fred_FEDFUNDS, fred_DGS10, ... indexed by ts.
    Output: same index with derived features.
    """
    if wide_macro is None or wide_macro.empty:
        return pd.DataFrame()
    df = wide_macro.copy()
    out = pd.DataFrame(index=df.index)
    for c in df.columns:
        s = pd.to_numeric(df[c], errors="coerce").astype(float)
        out[f"{c}_lvl"] = s
        out[f"{c}_chg_1"] = s.diff(1)
        out[f"{c}_roc_20"] = s.pct_change(20)
        out[f"{c}_z_252"] = (s - s.rolling(252, min_periods=20).mean()) / (s.rolling(252, min_periods=20).std() + 1e-12)
    return out
