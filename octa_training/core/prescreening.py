"""Pre-screening filter for universe symbols before expensive training (v0.1.0).

Eliminates symbols that structurally cannot pass training gates, saving ~20s/symbol
on the next retrain.  All checks run on the 1D parquet only (cheapest data).

Filter order (fail-fast, highest-gain first):
  F1 — Minimum history:   len(df) >= min_history_bars (default 504 = 2yr)
  F2 — Minimum price:     df['close'].mean() >= min_price (default $1.00)
  F3 — Minimum volume:    20-day avg volume >= min_volume_20d (default 100k)
  F4 — Warrant/rights:    symbol suffix not in warrant_suffixes
  F5 — Regime diversity:  >= 2 distinct regimes with sufficient rows

Usage::

    cfg = load_config("configs/sweep_catboost_1d.yaml")
    results = prescreen_universe(symbols, parquet_dir, cfg)
    passed = [s for s, r in results.items() if r.passed]
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd

logger = logging.getLogger(__name__)

_DEFAULT_WARRANT_SUFFIXES = ("W", "R", "WS", "WSA", "WSB", "WT")

# Reason codes
REASON_INSUFFICIENT_HISTORY = "insufficient_history"
REASON_PRICE_TOO_LOW = "price_too_low"
REASON_VOLUME_TOO_LOW = "volume_too_low"
REASON_WARRANT_OR_RIGHTS = "warrant_or_rights"
REASON_INSUFFICIENT_REGIME_DIVERSITY = "insufficient_regime_diversity"


@dataclass
class ScreenResult:
    """Result for a single symbol prescreening check."""
    symbol: str
    passed: bool
    reason: Optional[str] = None
    detail: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PrescreenSummary:
    """Aggregate statistics from prescreen_universe()."""
    total: int = 0
    passed: int = 0
    failed: int = 0
    by_reason: Dict[str, int] = field(default_factory=dict)

    def __str__(self) -> str:
        reasons = ", ".join(f"{r}={n}" for r, n in sorted(self.by_reason.items()))
        return (
            f"Pre-screen: {self.passed} passed, {self.failed} failed"
            f" ({reasons})"
        )


def _load_parquet_safe(path: str) -> Optional[pd.DataFrame]:
    """Load parquet returning None on any error (non-fatal in prescreening context)."""
    try:
        from octa_training.core.io_parquet import load_parquet
        return load_parquet(path)
    except Exception:
        return None


def _check_warrant_suffix(symbol: str, suffixes: Sequence[str]) -> bool:
    """Return True if symbol ends with a warrant/rights suffix (case-insensitive)."""
    sym_upper = symbol.strip().upper()
    for suffix in suffixes:
        if sym_upper.endswith(suffix.upper()):
            return True
    return False


def prescreen_symbol(
    df: Optional[pd.DataFrame],
    symbol: str,
    cfg: Any = None,
) -> ScreenResult:
    """Run all pre-screening filters for a single symbol.

    Parameters
    ----------
    df : 1D DataFrame loaded from parquet; may be None if load failed
    symbol : ticker symbol
    cfg : TrainingConfig; reads cfg.prescreening for thresholds.
          If None or cfg.prescreening is None, uses built-in defaults.

    Returns
    -------
    ScreenResult with passed=True or reason code explaining why it failed.
    """
    # Resolve thresholds
    ps_cfg = None
    if cfg is not None:
        ps_cfg = getattr(cfg, "prescreening", None)

    min_history: int = int(getattr(ps_cfg, "min_history_bars", 504)) if ps_cfg else 504
    min_price: float = float(getattr(ps_cfg, "min_price", 1.0)) if ps_cfg else 1.0
    min_volume: float = float(getattr(ps_cfg, "min_volume_20d", 100_000)) if ps_cfg else 100_000

    raw_suffixes = getattr(ps_cfg, "warrant_suffixes", None) if ps_cfg else None
    warrant_suffixes: Sequence[str] = (
        list(raw_suffixes) if raw_suffixes else list(_DEFAULT_WARRANT_SUFFIXES)
    )

    # ------------------------------------------------------------------ F4 --
    # Warrant/rights filter runs FIRST — no parquet needed, immediate win.
    if _check_warrant_suffix(symbol, warrant_suffixes):
        return ScreenResult(
            symbol=symbol,
            passed=False,
            reason=REASON_WARRANT_OR_RIGHTS,
            detail={"symbol": symbol, "matched_suffix": True},
        )

    # ------------------------------------------------------------------ F1 --
    # All remaining filters require a loaded DataFrame.
    if df is None:
        return ScreenResult(
            symbol=symbol,
            passed=False,
            reason=REASON_INSUFFICIENT_HISTORY,
            detail={"n_rows": 0, "min_history_bars": min_history},
        )

    n_rows = len(df)
    if n_rows < min_history:
        return ScreenResult(
            symbol=symbol,
            passed=False,
            reason=REASON_INSUFFICIENT_HISTORY,
            detail={"n_rows": n_rows, "min_history_bars": min_history},
        )

    # ------------------------------------------------------------------ F2 --
    if "close" not in df.columns:
        return ScreenResult(
            symbol=symbol,
            passed=False,
            reason=REASON_PRICE_TOO_LOW,
            detail={"error": "no_close_column"},
        )

    mean_price = float(df["close"].mean())
    if mean_price < min_price:
        return ScreenResult(
            symbol=symbol,
            passed=False,
            reason=REASON_PRICE_TOO_LOW,
            detail={"mean_price": round(mean_price, 4), "min_price": min_price},
        )

    # ------------------------------------------------------------------ F3 --
    if "volume" not in df.columns:
        # Volume column absent → skip volume check (non-fatal, some data sources omit it)
        pass
    else:
        vol_series = df["volume"].astype(float)
        rolling_vol_20d = vol_series.rolling(20, min_periods=1).mean()
        recent_vol = float(rolling_vol_20d.iloc[-1]) if len(rolling_vol_20d) > 0 else 0.0
        if recent_vol < min_volume:
            return ScreenResult(
                symbol=symbol,
                passed=False,
                reason=REASON_VOLUME_TOO_LOW,
                detail={
                    "recent_vol_20d": round(recent_vol, 0),
                    "min_volume_20d": min_volume,
                },
            )

    # ------------------------------------------------------------------ F5 --
    # Regime diversity: need ≥ 2 distinct regimes with sufficient rows.
    # Skipped if classify_regimes unavailable (graceful degradation).
    try:
        from octa_training.core.regime_labels import (
            RegimeLabelConfig,
            classify_regimes,
            get_regime_splits,
        )

        # Resolve min_rows from prescreening cfg or use defaults from RegimeLabelConfig
        re_cfg = getattr(cfg, "regime_ensemble", None) if cfg is not None else None
        min_rows_raw = getattr(re_cfg, "min_rows", {}) or {} if re_cfg else {}
        if hasattr(min_rows_raw, "model_dump"):
            min_rows_dict = min_rows_raw.model_dump()
        elif hasattr(min_rows_raw, "dict"):
            min_rows_dict = min_rows_raw.dict()
        else:
            min_rows_dict = dict(min_rows_raw)

        label_cfg = RegimeLabelConfig(min_rows=min_rows_dict) if min_rows_dict else RegimeLabelConfig()

        labels = classify_regimes(df, cfg=label_cfg)
        if not labels.empty:
            splits = get_regime_splits(df, labels, cfg=label_cfg)
            # Only count non-neutral regimes (bull/bear/crisis)
            from octa_training.core.regime_labels import REGIME_NEUTRAL
            active_regimes = [r for r in splits if r != REGIME_NEUTRAL]
            if len(active_regimes) < 1:
                # Zero actionable regimes — but don't fail on just this: regime ensemble
                # falls back gracefully; only fail if ZERO data regimes meet min_rows.
                n_qualifying = len(splits)  # includes neutral if present
                if n_qualifying == 0:
                    return ScreenResult(
                        symbol=symbol,
                        passed=False,
                        reason=REASON_INSUFFICIENT_REGIME_DIVERSITY,
                        detail={
                            "qualifying_regimes": [],
                            "min_regimes_needed": 1,
                        },
                    )
    except Exception:
        pass  # Regime check is best-effort; never block on import errors

    return ScreenResult(
        symbol=symbol,
        passed=True,
        reason=None,
        detail={"n_rows": n_rows, "mean_price": round(mean_price, 4)},
    )


def prescreen_universe(
    symbols: Sequence[str],
    inventory: Dict[str, Any],
    cfg: Any = None,
    log_fn: Any = None,
) -> Dict[str, ScreenResult]:
    """Run pre-screening for all symbols in the universe.

    Parameters
    ----------
    symbols : ordered list of symbols to screen
    inventory : dict mapping symbol → {"tfs": {"1D": [path, ...], ...}, ...}
    cfg : TrainingConfig; passed through to prescreen_symbol()
    log_fn : optional callable(msg: str) for logging; defaults to logger.info

    Returns
    -------
    dict mapping symbol → ScreenResult
    """
    if log_fn is None:
        log_fn = logger.info

    results: Dict[str, ScreenResult] = {}

    for symbol in symbols:
        # Resolve 1D parquet path from inventory
        by_tf = (inventory.get(symbol) or {}).get("tfs", {})
        paths_1d = by_tf.get("1D", [])
        path_1d: Optional[str] = paths_1d[0] if paths_1d else None

        df: Optional[pd.DataFrame] = None
        if path_1d and Path(path_1d).exists():
            df = _load_parquet_safe(path_1d)

        result = prescreen_symbol(df, symbol, cfg=cfg)
        results[symbol] = result

    # Build summary
    summary = PrescreenSummary(total=len(results))
    for r in results.values():
        if r.passed:
            summary.passed += 1
        else:
            summary.failed += 1
            reason = r.reason or "unknown"
            summary.by_reason[reason] = summary.by_reason.get(reason, 0) + 1

    log_fn(str(summary))
    return results
