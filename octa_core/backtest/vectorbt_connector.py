"""Lightweight connector/prototype for running fast local backtests with vectorbt.

This module intentionally imports `vectorbt` only inside the runtime functions so
the test/CI environment does not require the package unless the feature is used.

Functions:
- `run_vectorbt_backtest(price_df, entries, exits, **kwargs)` -> dict of basic results

The connector is a thin adapter — production integration should standardize
data shapes (pandas DataFrame of prices, boolean Series for entries/exits)
and error handling.
"""

from typing import Any, Dict


def run_vectorbt_backtest(
    price_df, entries, exits, **portfolio_kwargs
) -> Dict[str, Any]:
    try:
        import vectorbt as vbt
    except Exception as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "vectorbt is not installed. Install it with 'pip install vectorbt'."
        ) from exc

    # Expectation: price_df is a pandas Series or DataFrame index by DatetimeIndex,
    # entries/exits are boolean Series aligned to price_df.index.
    pf = vbt.Portfolio.from_signals(
        price_df, entries, exits, **(portfolio_kwargs or {})
    )

    # Provide a compact result payload; callers can introspect full `pf` if needed.
    try:
        stats = pf.stats()
    except Exception:
        stats = {}

    return {"portfolio": pf, "stats": stats}


__all__ = ["run_vectorbt_backtest"]
