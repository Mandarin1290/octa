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

    raw_root = None
    for attr in ("raw_root", "raw_dir", "raw_data_root"):
        try:
            candidate = getattr(settings, attr)
        except Exception:
            candidate = None
        if candidate:
            raw_root = candidate
            break

    config_path = None
    for attr in ("altdata_config_path", "altdat_config_path"):
        try:
            candidate = getattr(settings, attr)
        except Exception:
            candidate = None
        if candidate:
            config_path = str(candidate)
            break

    try:
        res = run_altdata(
            bars_df=bars_df,
            symbol=symbol,
            tz=tz,
            config_path=config_path,
            asset_class=asset_class,
            raw_root=raw_root,
        )
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
