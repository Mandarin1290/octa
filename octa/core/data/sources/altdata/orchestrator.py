from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import yaml

from octa.core.data.sources.altdata.bootstrap_deps import ensure_deps
from octa.core.features.transforms.feature_builder import (
    AltDataBuildResult,
    build_altdata_features,
)


def load_altdat_config(path: Optional[str] = None) -> Dict[str, Any]:
    p = path or os.getenv("OKTA_ALTDATA_CONFIG") or str(Path("config") / "altdat.yaml")
    try:
        raw = Path(p).read_text()
        cfg = yaml.safe_load(raw) or {}
        if not isinstance(cfg, dict):
            return {}
        return cfg
    except Exception:
        return {}


def run_altdata(
    *,
    bars_df: pd.DataFrame,
    symbol: str,
    tz: str = "UTC",
    config_path: Optional[str] = None,
) -> AltDataBuildResult:
    cfg = load_altdat_config(config_path)
    enabled = bool(cfg.get("enabled", False)) or str(os.getenv("OKTA_ALTDATA_ENABLED", "")).strip() == "1"
    if not enabled:
        return AltDataBuildResult(features_df=pd.DataFrame(index=bars_df.index), meta={"enabled": False, "status": "DISABLED"})

    auto_install = bool(cfg.get("auto_install", False))
    deps = ensure_deps(auto_install=auto_install)
    if not deps.ok:
        return AltDataBuildResult(
            features_df=pd.DataFrame(index=bars_df.index),
            meta={
                "enabled": False,
                "status": "DEPS_MISSING",
                "missing": deps.missing,
                "attempted_install": deps.attempted_install,
                "errors": deps.errors,
            },
        )

    return build_altdata_features(bars_df=bars_df, symbol=symbol, altdat_cfg=cfg, tz=tz)
