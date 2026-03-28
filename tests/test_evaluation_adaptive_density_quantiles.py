from __future__ import annotations

import numpy as np
import pandas as pd

from octa_training.core.evaluation import EvalSettings, compute_equity_and_metrics


def test_adaptive_density_quantiles_relax_thresholds_when_signal_density_is_low() -> None:
    idx = pd.date_range("2024-01-01", periods=220, freq="1D", tz="UTC")
    prices = pd.Series(100.0 + np.cumsum(np.sin(np.linspace(0.0, 10.0, len(idx))) * 0.2 + 0.1), index=idx)
    preds = pd.Series(np.concatenate([np.full(180, 0.51), np.linspace(0.52, 0.95, 40)]), index=idx)

    base = compute_equity_and_metrics(
        prices,
        preds,
        EvalSettings(mode="cls", upper_q=0.8, lower_q=0.2, causal_quantiles=True, quantile_window=126),
    )
    relaxed = compute_equity_and_metrics(
        prices,
        preds,
        EvalSettings(
            mode="cls",
            upper_q=0.8,
            lower_q=0.2,
            causal_quantiles=True,
            quantile_window=126,
            adaptive_density_quantiles=True,
            density_target=0.12,
            density_window=63,
            density_relax_max=0.30,
        ),
    )

    base_df = base["df"]
    relaxed_df = relaxed["df"]
    assert "adaptive_density_pressure" in relaxed_df.columns
    assert float(relaxed_df["adaptive_density_pressure"].max()) > 0.0
    assert int((relaxed_df["raw_signal"] != 0.0).sum()) >= int((base_df["raw_signal"] != 0.0).sum())
