"""Feature selection utilities — reduce redundancy before model training.

Two-stage pipeline:
  1. Greedy correlation deduplication: drop features with |Pearson corr| > threshold
     with any earlier-accepted feature. Unsupervised, zero leakage risk.
  2. Target ranking (optional): if y is provided and remaining > max_features,
     rank by |target corr| and keep top max_features.

Usage in pipeline.py after leakage audit:
    from octa.core.features.selector import select_features
    selected = select_features(features_res.X, y=primary_y, ...)
    features_res.X = features_res.X[selected]
"""
from __future__ import annotations

import logging
from typing import List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def select_features(
    X: pd.DataFrame,
    y: Optional[pd.Series] = None,
    corr_threshold: float = 0.95,
    max_features: int = 35,
) -> List[str]:
    """Return selected feature column names.

    Parameters
    ----------
    X:               Feature matrix (rows=bars, cols=features).
    y:               Optional primary target for ranking after corr removal.
    corr_threshold:  Drop features whose |corr| with any already-accepted feature
                     exceeds this value. 1.0 disables deduplication.
    max_features:    Maximum columns to return. Applied as a final cap using
                     target correlation ranking when y is provided; otherwise a
                     simple head() cut is applied.
    """
    if X is None or X.empty or len(X.columns) == 0:
        return []

    cols: List[str] = list(X.columns)
    n = len(cols)

    if n == 0:
        return cols

    # Fast path: nothing to do
    if n <= max_features and corr_threshold >= 1.0:
        return cols

    # ------------------------------------------------------------------
    # Stage 1: greedy pairwise-correlation deduplication
    # ------------------------------------------------------------------
    accepted: List[str] = []
    corr_matrix: Optional[pd.DataFrame] = None

    if corr_threshold < 1.0:
        try:
            # Use only numeric columns; corr() handles NaN via pairwise complete obs
            X_num = X.select_dtypes(include=[np.number])
            corr_matrix = X_num.corr(method="pearson").abs()
        except Exception as exc:
            logger.debug("feature selector: corr matrix failed (%s), skipping dedup", exc)
            corr_matrix = None

    for col in cols:
        if corr_matrix is None or col not in corr_matrix.columns:
            accepted.append(col)
            continue
        if not accepted:
            accepted.append(col)
            continue
        # Check against all accepted features that exist in the corr matrix
        accepted_in_corr = [a for a in accepted if a in corr_matrix.index]
        if accepted_in_corr:
            max_corr = corr_matrix.loc[col, accepted_in_corr].max()
            if max_corr > corr_threshold:
                continue  # drop — too similar to an accepted feature
        accepted.append(col)

    n_dropped = n - len(accepted)
    if n_dropped > 0:
        logger.debug(
            "feature selector: corr dedup dropped %d/%d features (threshold=%.2f)",
            n_dropped, n, corr_threshold,
        )

    # ------------------------------------------------------------------
    # Stage 2: trim to max_features using target correlation ranking
    # ------------------------------------------------------------------
    if len(accepted) <= max_features:
        return accepted

    if y is not None:
        try:
            y_aligned = y.reindex(X.index)
            valid_mask = y_aligned.notna()
            # compute |Pearson corr| with target for each accepted feature
            target_corrs: dict = {}
            for col in accepted:
                col_vals = X[col].reindex(X.index)
                combined = pd.concat([col_vals, y_aligned], axis=1).dropna()
                if len(combined) < 20:
                    target_corrs[col] = 0.0
                    continue
                c = combined.iloc[:, 0].corr(combined.iloc[:, 1])
                target_corrs[col] = abs(c) if np.isfinite(c) else 0.0
            ranked = sorted(accepted, key=lambda c: target_corrs.get(c, 0.0), reverse=True)
            result = ranked[:max_features]
            logger.debug(
                "feature selector: target-rank trim %d→%d features",
                len(accepted), len(result),
            )
            return result
        except Exception as exc:
            logger.debug("feature selector: target ranking failed (%s), truncating", exc)
            return accepted[:max_features]
    else:
        # No target — just take the first max_features (deterministic)
        return accepted[:max_features]
