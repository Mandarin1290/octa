"""Live Regime Detector for OCTA OS.

Determines current market regime from fresh price data.
Regime labels: low_vol / mid_vol / high_vol
(consistent with training regime labels)

Runs daily before the signal cycle.
Writes: octa/var/live_regime.json
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List, Optional

import numpy as np

log = logging.getLogger("octa.live_regime")

REGIME_LABELS = ["low_vol", "mid_vol", "high_vol"]


def detect_live_regime(
    symbol: str,
    prices: List[float],
    lookback: int = 20,
    low_vol_pct: float = 0.33,
    high_vol_pct: float = 0.67,
    hist_vol_path: Optional[str] = None,
) -> str:
    """Determine current regime from rolling volatility.

    Args:
        symbol: Symbol for logging.
        prices: Last N+1 close prices (minimum lookback+1).
        lookback: Rolling window for volatility computation.
        low_vol_pct: Annualised vol threshold for low_vol (naive mode).
        high_vol_pct: Annualised vol threshold for high_vol (naive mode).
        hist_vol_path: Path to historical vol percentiles from training.
            Expected JSON: {symbol: {vol_p33: float, vol_p67: float}}.
            If None, falls back to naive thresholds 0.10 / 0.25.

    Returns:
        'low_vol', 'mid_vol', or 'high_vol'.
    """
    if len(prices) < lookback + 1:
        log.warning(
            "[%s] Too few prices for regime detection (%d < %d) → mid_vol",
            symbol, len(prices), lookback + 1,
        )
        return "mid_vol"

    returns = np.diff(np.log(prices[-(lookback + 1):]))
    current_vol = float(np.std(returns) * np.sqrt(252))

    if hist_vol_path and Path(hist_vol_path).exists():
        hist = json.loads(Path(hist_vol_path).read_text(encoding="utf-8"))
        sym_hist = hist.get(symbol, {})
        p33 = float(sym_hist.get("vol_p33", 0.10))
        p67 = float(sym_hist.get("vol_p67", 0.25))
    else:
        p33 = 0.10
        p67 = 0.25

    if current_vol < p33:
        regime = "low_vol"
    elif current_vol > p67:
        regime = "high_vol"
    else:
        regime = "mid_vol"

    log.debug(
        "[%s] current_vol=%.3f p33=%.3f p67=%.3f → %s",
        symbol, current_vol, p33, p67, regime,
    )
    return regime


def update_live_regimes(
    symbols: List[str],
    price_getter: Callable[[str], List[float]],
    output_path: str = "octa/var/live_regime.json",
    hist_vol_path: Optional[str] = None,
    lookback: int = 20,
) -> Dict[str, str]:
    """Update regime labels for all symbols.

    Args:
        symbols: Symbols to evaluate.
        price_getter: Callable(symbol) → last N close prices.
        output_path: Destination JSON.
        hist_vol_path: Historical vol percentiles from training.
        lookback: Days for volatility window.

    Returns:
        {symbol: regime_label}
    """
    regimes: Dict[str, str] = {}
    errors: List[tuple] = []

    for sym in symbols:
        try:
            prices = price_getter(sym)
            if prices:
                regime = detect_live_regime(
                    symbol=sym,
                    prices=prices,
                    lookback=lookback,
                    hist_vol_path=hist_vol_path,
                )
                regimes[sym] = regime
            else:
                regimes[sym] = "mid_vol"
                log.warning("[%s] No prices available — mid_vol fallback", sym)
        except Exception as exc:
            errors.append((sym, str(exc)))
            regimes[sym] = "mid_vol"
            log.warning("[%s] Regime detection failed: %s", sym, exc)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    result = {
        "updated_at": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
        "lookback_days": lookback,
        "n_symbols": len(regimes),
        "n_errors": len(errors),
        "regimes": regimes,
        "distribution": {
            label: sum(1 for r in regimes.values() if r == label)
            for label in REGIME_LABELS
        },
    }
    out.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")

    log.info(
        "Live regimes updated: low=%d mid=%d high=%d",
        result["distribution"]["low_vol"],
        result["distribution"]["mid_vol"],
        result["distribution"]["high_vol"],
    )
    return regimes


def load_live_regime(
    symbol: str,
    regime_path: str = "octa/var/live_regime.json",
    max_age_hours: float = 26.0,
) -> Optional[str]:
    """Load current live regime for a symbol.

    Returns None if file is missing or stale (>26h = more than one trading day).
    """
    path = Path(regime_path)
    if not path.exists():
        return None

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

    updated_at = datetime.fromisoformat(data["updated_at"])
    age_hours = (datetime.utcnow() - updated_at).total_seconds() / 3600
    if age_hours > max_age_hours:
        log.warning("Live regime stale: %.1fh > %.1fh", age_hours, max_age_hours)
        return None

    return data.get("regimes", {}).get(symbol)
