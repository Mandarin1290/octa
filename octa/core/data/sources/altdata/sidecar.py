from __future__ import annotations

from typing import Any, Dict, Tuple

import pandas as pd

from octa.core.data.sources.altdata.orchestrator import run_altdata


def try_run(
    *,
    bars_df: pd.DataFrame,
    settings: Any,
    asset_class: str,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Safe sidecar entry point for the existing feature pipeline.

    Never raises; returns (features_df, meta).
    Expects symbol/timezone optionally present on `settings`.
    """

    try:
        symbol = str(getattr(settings, "symbol", "unknown") or "unknown")
    except Exception:
        symbol = "unknown"
    try:
        tz = str(getattr(settings, "timezone", "UTC") or "UTC")
    except Exception:
        tz = "UTC"

    try:
        res = run_altdata(bars_df=bars_df, symbol=symbol, tz=tz)
        f = res.features_df
        # Align with existing feature convention (most core features are shifted by 1 bar).
        try:
            if isinstance(f, pd.DataFrame) and not f.empty:
                f = f.shift(1)
        except Exception:
            pass
        return f, res.meta
    except Exception as e:
        return pd.DataFrame(index=bars_df.index), {"enabled": False, "status": "ERROR", "error": str(e)}
