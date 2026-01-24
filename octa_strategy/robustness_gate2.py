from __future__ import annotations

import math
from typing import Dict, Iterable, List, Sequence

import numpy as np


def _to_float_list(values: Iterable[float]) -> List[float]:
    out: List[float] = []
    for v in values:
        try:
            out.append(float(v))
        except Exception as e:
            raise TypeError(f"returns contains non-numeric value: {v!r}") from e
    return out


def _annualized_sharpe(returns: np.ndarray, periods_per_year: int = 252) -> float:
    if returns.size < 2:
        return float("nan")
    mu = float(np.mean(returns))
    sigma = float(np.std(returns, ddof=1))
    if not math.isfinite(sigma) or sigma <= 0:
        return float("nan")
    return (mu / sigma) * math.sqrt(float(periods_per_year))


def _max_drawdown_from_returns(returns: np.ndarray) -> float:
    if returns.size == 0:
        return float("nan")
    equity = np.cumprod(1.0 + returns)
    peak = np.maximum.accumulate(equity)
    dd = 1.0 - (equity / peak)
    return float(np.max(dd))


def _block_bootstrap_sample(rng: np.random.Generator, base: np.ndarray, block: int) -> np.ndarray:
    n = int(base.size)
    b = max(1, int(block))
    if n == 0:
        return base
    # number of blocks needed to reach length n
    k = int(math.ceil(n / b))
    starts = rng.integers(0, n, size=k, endpoint=False)
    chunks = [base[s : min(s + b, n)] for s in starts]
    sample = np.concatenate(chunks, axis=0)
    return sample[:n]


def evaluate_block_bootstrap(
    returns: Sequence[float],
    sharpe_floor: float,
    n: int = 2000,
    block: int = 5,
    seed: int = 1337,
) -> Dict[str, float]:
    """Deterministic block-bootstrap robustness for Gate2.

    Inputs:
      - returns: return series (e.g. daily), deterministic order.
      - sharpe_floor: floor used to compute tail probability.
      - n/block/seed: bootstrap controls.

    Outputs (all floats):
      - sharpe_p05: 5th percentile of bootstrap Sharpe
      - maxdd_p95: 95th percentile of bootstrap max drawdown
      - prob_sharpe_below_floor: P(Sharpe < sharpe_floor)

    Notes:
      - Fail-closed behavior is implemented by the caller (PaperGates) when returns are missing.
      - If returns are too short or degenerate, outputs may be NaN; Gate thresholds should reject.
    """

    r_list = _to_float_list(returns)
    base = np.asarray(r_list, dtype=float)
    if base.size < 30:
        # Too short for a meaningful promotion robustness check.
        return {
            "sharpe_p05": float("nan"),
            "maxdd_p95": float("nan"),
            "prob_sharpe_below_floor": 1.0,
        }

    rng = np.random.default_rng(int(seed))
    n_iter = max(1, int(n))

    sharpes = np.empty(n_iter, dtype=float)
    maxdds = np.empty(n_iter, dtype=float)
    for i in range(n_iter):
        sample = _block_bootstrap_sample(rng, base, block=int(block))
        sharpes[i] = _annualized_sharpe(sample)
        maxdds[i] = _max_drawdown_from_returns(sample)

    sharpe_p05 = float(np.nanpercentile(sharpes, 5))
    maxdd_p95 = float(np.nanpercentile(maxdds, 95))
    prob_below = float(np.mean(np.isfinite(sharpes) & (sharpes < float(sharpe_floor))))

    return {
        "sharpe_p05": sharpe_p05,
        "maxdd_p95": maxdd_p95,
        "prob_sharpe_below_floor": prob_below,
    }
