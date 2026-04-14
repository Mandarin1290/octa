"""RegimeDetector: rule-based regime classifier with pickle persistence (v0.0.0).

Wraps regime_labels.classify_regimes() behind a fit/predict/save/load interface
so that shadow execution can restore a frozen detector without re-fitting.

Persistence: <symbol>_<tf>_regime.pkl  (plain pickle, deterministic)
"""
from __future__ import annotations

import pickle
from pathlib import Path
from typing import Optional, Union

import pandas as pd

from octa_training.core.regime_labels import (
    RegimeLabelConfig,
    classify_regimes,
    regime_distribution,
    REGIME_NEUTRAL,
)


class RegimeDetector:
    """Rule-based regime detector.

    The detector is *stateless* after fit: it stores only the config used
    during fit so that predict() is fully deterministic given the same input.

    Usage::

        det = RegimeDetector()
        det.fit(train_df)                        # validates bar count
        labels = det.predict(df)                 # pd.Series of regime labels
        det.save(Path("artifacts/AAPL_1D_regime.pkl"))
        det2 = RegimeDetector.load(Path("artifacts/AAPL_1D_regime.pkl"))
    """

    def __init__(self, cfg: Optional[RegimeLabelConfig] = None) -> None:
        self.cfg = cfg or RegimeLabelConfig()
        self._fitted = False
        self._fit_n_rows: int = 0

    # ------------------------------------------------------------------
    # Training interface
    # ------------------------------------------------------------------

    def fit(self, df: pd.DataFrame, close_col: str = "close") -> "RegimeDetector":
        """Validate training data and mark detector as fitted.

        Parameters
        ----------
        df : training DataFrame; must have ≥252 rows and contain `close_col`
        close_col : close price column name

        Raises
        ------
        ValueError if df has insufficient rows or missing close column
        """
        if close_col not in df.columns:
            raise ValueError(
                f"RegimeDetector.fit: column '{close_col}' not found; "
                f"available: {list(df.columns)}"
            )
        if len(df) < 252:
            raise ValueError(
                f"RegimeDetector.fit: need ≥252 bars, got {len(df)}"
            )
        self._fitted = True
        self._fit_n_rows = len(df)
        return self

    # ------------------------------------------------------------------
    # Prediction interface
    # ------------------------------------------------------------------

    def predict(
        self,
        df: pd.DataFrame,
        close_col: str = "close",
    ) -> pd.Series:
        """Assign regime labels to df.

        Parameters
        ----------
        df : DataFrame with DatetimeIndex and `close_col`
        close_col : close price column name

        Returns
        -------
        pd.Series of str regime labels aligned to df.index
        Returns all-NEUTRAL series if df is too short (non-fatal).
        """
        if not self._fitted:
            raise RuntimeError(
                "RegimeDetector.predict called before fit(). Call fit() first."
            )

        if len(df) < 252:
            # Graceful degradation: return NEUTRAL for all bars
            return pd.Series(REGIME_NEUTRAL, index=df.index, dtype=str)

        return classify_regimes(df, cfg=self.cfg, close_col=close_col)

    def predict_distribution(
        self,
        df: pd.DataFrame,
        window: int = 20,
        close_col: str = "close",
    ) -> dict:
        """Return the regime distribution over the last `window` bars.

        Used by shadow execution to select the active submodel.

        Parameters
        ----------
        df : recent bars (e.g. last 20 bars)
        window : how many trailing bars to use
        close_col : close price column name

        Returns
        -------
        dict mapping regime → fraction in [0, 1]
        """
        labels = self.predict(df, close_col=close_col)
        if labels.empty:
            from octa_training.core.regime_labels import _REGIME_PRIORITY
            return {r: 0.0 for r in _REGIME_PRIORITY}
        recent = labels.iloc[-window:] if len(labels) > window else labels
        return regime_distribution(recent)

    def current_regime(
        self,
        df: pd.DataFrame,
        window: int = 20,
        close_col: str = "close",
    ) -> str:
        """Return the dominant regime over the last `window` bars.

        Priority: CRISIS > BEAR > BULL > NEUTRAL (first regime with any
        presence wins; NEUTRAL only if no other regime is present).

        Parameters
        ----------
        df : DataFrame containing recent bars
        window : trailing window for distribution
        close_col : close price column name

        Returns
        -------
        str: 'crisis' | 'bear' | 'bull' | 'neutral'
        """
        from octa_training.core.regime_labels import _REGIME_PRIORITY
        dist = self.predict_distribution(df, window=window, close_col=close_col)
        for regime in _REGIME_PRIORITY:
            if dist.get(regime, 0.0) > 0.0:
                return regime
        return REGIME_NEUTRAL

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self, path: Union[str, Path]) -> None:
        """Persist detector to pickle file.

        Parameters
        ----------
        path : file path; parent directory will be created if needed
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as f:
            pickle.dump(self, f, protocol=pickle.HIGHEST_PROTOCOL)

    @classmethod
    def load(cls, path: Union[str, Path]) -> "RegimeDetector":
        """Restore detector from pickle file.

        Parameters
        ----------
        path : file path written by save()

        Returns
        -------
        RegimeDetector instance (already fitted)

        Raises
        ------
        FileNotFoundError if path does not exist
        TypeError if file does not contain a RegimeDetector
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"RegimeDetector.load: {path} not found")
        with open(path, "rb") as f:
            obj = pickle.load(f)
        if not isinstance(obj, cls):
            raise TypeError(
                f"RegimeDetector.load: expected RegimeDetector, got {type(obj)}"
            )
        return obj
