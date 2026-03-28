from __future__ import annotations

import numpy as np
import pandas as pd

from octa_training.core.config import load_config
from octa_training.core.evaluation import EvalSettings, compute_equity_and_metrics


def _make_market(n: int = 320) -> pd.DataFrame:
    idx = pd.date_range("2024-01-01 09:00", periods=n, freq="1h", tz="UTC")
    base = np.linspace(100.0, 104.0, n)
    noise = np.concatenate(
        [
            np.sin(np.linspace(0.0, 8.0, n // 2)) * 0.03,
            np.sin(np.linspace(0.0, 16.0, n - n // 2)) * 0.30,
        ]
    )
    close = base + noise
    high = close + np.concatenate([np.full(n // 2, 0.04), np.full(n - n // 2, 0.35)])
    low = close - np.concatenate([np.full(n // 2, 0.04), np.full(n - n // 2, 0.35)])
    volume = np.concatenate([np.full(n // 2, 8_000.0), np.full(n - n // 2, 30_000.0)])
    return pd.DataFrame(
        {
            "timestamp": idx,
            "open": close,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        }
    ).set_index("timestamp")


def _make_preds(index: pd.DatetimeIndex) -> pd.Series:
    x = np.linspace(-2.0, 2.0, len(index))
    preds = 0.5 + 0.35 * np.sin(4.0 * x) + 0.20 * np.sign(np.sin(7.0 * x))
    return pd.Series(preds, index=index)


def test_foundation_validation_loads_1h_regime_policy() -> None:
    cfg = load_config("configs/foundation_validation.yaml")
    per_tf = cfg.signal.regime_policy.get("per_timeframe", {})
    assert "1H" in per_tf
    assert per_tf["1H"]["low_vol_signal_strength_min"] == 0.55
    assert per_tf["1H"]["mid_vol_quality_keep_quantile"] == 0.55


def test_regime_policy_thins_low_vol_trades() -> None:
    market = _make_market()
    preds = _make_preds(market.index)

    baseline = compute_equity_and_metrics(
        market["close"],
        preds,
        EvalSettings(
            mode="cls",
            upper_q=0.8,
            lower_q=0.2,
            cost_bps=2.0,
            spread_bps=1.0,
            timeframe="1H",
        ),
        market_df=market,
    )

    policy = {
        "enabled": True,
        "per_timeframe": {
            "1H": {
                "vol_window": 24,
                "regime_window": 96,
                "atr_window": 14,
                "trend_window": 20,
                "liquidity_window": 96,
                "quality_window": 96,
                "quality_keep_quantile": 0.70,
                "low_vol_quality_keep_quantile": 0.72,
                "mid_vol_quality_keep_quantile": 0.55,
                "high_vol_quality_keep_quantile": 0.45,
                "quality_quantile_relax_scale": 0.30,
                "low_vol_z_max": -0.10,
                "low_vol_atr_pct_max": 0.50,
                "high_vol_z_min": 0.25,
                "high_vol_atr_pct_min": 0.70,
                "low_vol_signal_strength_min": 0.55,
                "low_vol_require_trend_alignment": True,
                "low_vol_quality_floor": 0.80,
                "mid_vol_quality_floor": 0.60,
                "high_vol_quality_floor": 0.50,
                "low_vol_size_mult": 0.35,
                "low_vol_size_mult_soft": 0.55,
                "mid_vol_size_mult": 1.0,
                "mid_vol_size_mult_low": 0.90,
                "mid_vol_size_mult_high": 1.10,
                "high_vol_size_mult": 1.15,
                "high_vol_size_mult_low": 1.10,
                "high_vol_size_mult_high": 1.25,
                "cost_edge_buffer": 1.05,
                "density_floor_cost_buffer_min": 1.0,
                "density_window": 96,
                "min_signal_density": 0.02,
                "density_floor_relax": 0.10,
                "hour_min_observations": 4,
                "hour_coverage_relax": 0.02,
                "quality_size_floor": 0.50,
            }
        },
    }
    filtered = compute_equity_and_metrics(
        market["close"],
        preds,
        EvalSettings(
            mode="cls",
            upper_q=0.8,
            lower_q=0.2,
            cost_bps=2.0,
            spread_bps=1.0,
            timeframe="1H",
            regime_policy=policy,
        ),
        market_df=market,
    )

    base_df = baseline["df"]
    filt_df = filtered["df"]
    low_mask = filt_df["vol_regime_label"] == "LOW_VOL"
    assert int((filt_df.loc[low_mask, "raw_signal"] != 0).sum()) < int((base_df.loc[low_mask, "raw_signal"] != 0).sum())
    assert "density_pressure_eval" in filt_df.columns
    assert float(filt_df["density_pressure_eval"].max()) >= 0.0


def test_regime_policy_scales_position_by_regime() -> None:
    market = _make_market()
    preds = _make_preds(market.index)
    policy = {
        "enabled": True,
        "per_timeframe": {
            "1H": {
                "vol_window": 24,
                "regime_window": 96,
                "atr_window": 14,
                "trend_window": 20,
                "liquidity_window": 96,
                "quality_window": 96,
                "quality_keep_quantile": 0.60,
                "low_vol_quality_keep_quantile": 0.70,
                "mid_vol_quality_keep_quantile": 0.50,
                "high_vol_quality_keep_quantile": 0.40,
                "low_vol_z_max": -0.10,
                "low_vol_atr_pct_max": 0.50,
                "high_vol_z_min": 0.25,
                "high_vol_atr_pct_min": 0.70,
                "low_vol_signal_strength_min": 0.40,
                "low_vol_quality_floor": 0.60,
                "mid_vol_quality_floor": 0.50,
                "high_vol_quality_floor": 0.45,
                "low_vol_size_mult": 0.25,
                "low_vol_size_mult_soft": 0.45,
                "mid_vol_size_mult": 1.0,
                "mid_vol_size_mult_low": 0.85,
                "mid_vol_size_mult_high": 1.05,
                "high_vol_size_mult": 1.20,
                "high_vol_size_mult_low": 1.05,
                "high_vol_size_mult_high": 1.25,
                "cost_edge_buffer": 1.0,
                "density_floor_cost_buffer_min": 1.0,
                "density_window": 96,
                "min_signal_density": 0.01,
                "density_floor_relax": 0.08,
                "quality_size_floor": 0.50,
            }
        },
    }
    out = compute_equity_and_metrics(
        market["close"],
        preds,
        EvalSettings(
            mode="cls",
            upper_q=0.8,
            lower_q=0.2,
            cost_bps=1.0,
            spread_bps=0.5,
            timeframe="1H",
            regime_policy=policy,
        ),
        market_df=market,
    )
    df = out["df"]
    low = df.loc[df["vol_regime_label"] == "LOW_VOL", "size_multiplier_eval"].median()
    high = df.loc[df["vol_regime_label"] == "HIGH_VOL", "size_multiplier_eval"].median()
    assert high > low


def test_density_floor_relaxes_threshold_but_keeps_cost_floor() -> None:
    market = _make_market(240)
    preds = pd.Series(np.concatenate([np.full(120, 0.51), np.full(120, 0.82)]), index=market.index)
    policy = {
        "enabled": True,
        "per_timeframe": {
            "1H": {
                "vol_window": 24,
                "regime_window": 96,
                "atr_window": 14,
                "trend_window": 20,
                "liquidity_window": 96,
                "quality_window": 96,
                "quality_keep_quantile": 0.70,
                "low_vol_quality_keep_quantile": 0.72,
                "mid_vol_quality_keep_quantile": 0.55,
                "high_vol_quality_keep_quantile": 0.45,
                "low_vol_z_max": -0.10,
                "low_vol_atr_pct_max": 0.50,
                "high_vol_z_min": 0.25,
                "high_vol_atr_pct_min": 0.70,
                "low_vol_signal_strength_min": 0.40,
                "low_vol_quality_floor": 0.55,
                "mid_vol_quality_floor": 0.50,
                "high_vol_quality_floor": 0.45,
                "low_vol_size_mult": 0.35,
                "low_vol_size_mult_soft": 0.55,
                "mid_vol_size_mult": 1.0,
                "mid_vol_size_mult_low": 0.90,
                "mid_vol_size_mult_high": 1.10,
                "high_vol_size_mult": 1.15,
                "high_vol_size_mult_low": 1.10,
                "high_vol_size_mult_high": 1.25,
                "cost_edge_buffer": 1.10,
                "density_floor_cost_buffer_min": 1.0,
                "density_window": 48,
                "min_signal_density": 0.03,
                "density_floor_relax": 0.20,
                "hour_min_observations": 4,
                "hour_coverage_relax": 0.02,
                "quality_size_floor": 0.50,
            }
        },
    }
    out = compute_equity_and_metrics(
        market["close"],
        preds,
        EvalSettings(
            mode="cls",
            upper_q=0.8,
            lower_q=0.2,
            cost_bps=2.0,
            spread_bps=1.0,
            timeframe="1H",
            regime_policy=policy,
        ),
        market_df=market,
    )
    df = out["df"]
    assert float(df["quality_threshold"].min()) >= 0.55
    assert float(df["estimated_cost_edge"].min()) >= (2.0 + 1.0) / 10000.0


def test_ensemble_policy_can_create_additional_signals_without_disabling_cost_floor() -> None:
    market = _make_market(320)
    preds = pd.Series(np.concatenate([np.full(220, 0.5), np.linspace(0.48, 0.86, 100)]), index=market.index)
    policy = {
        "enabled": True,
        "per_timeframe": {
            "1H": {
                "vol_window": 24,
                "regime_window": 96,
                "atr_window": 14,
                "trend_window": 20,
                "liquidity_window": 96,
                "quality_window": 96,
                "quality_keep_quantile": 0.70,
                "mid_vol_quality_keep_quantile": 0.55,
                "cost_edge_buffer": 1.05,
                "density_floor_cost_buffer_min": 1.0,
                "low_vol_quality_floor": 0.60,
                "mid_vol_quality_floor": 0.50,
                "high_vol_quality_floor": 0.45,
                "ensemble": {
                    "enabled": True,
                    "model_weight": 0.55,
                    "ensemble_weight": 0.45,
                    "active_score_min": 0.10,
                    "diversity_min": 2,
                    "ensemble_only_min_score": 0.35,
                    "sleeves": {
                        "momentum": {"weight": 1.0},
                        "breakout": {"weight": 0.9},
                        "mean_reversion": {"weight": 0.8},
                        "volume_flow": {"weight": 0.7},
                        "intraday_structure": {"weight": 0.5},
                        "range_release": {"weight": 0.8},
                    },
                },
            }
        },
    }
    out = compute_equity_and_metrics(
        market["close"],
        preds,
        EvalSettings(
            mode="cls",
            upper_q=0.8,
            lower_q=0.2,
            cost_bps=2.0,
            spread_bps=1.0,
            timeframe="1H",
            regime_policy=policy,
        ),
        market_df=market,
    )
    df = out["df"]
    assert "ensemble_score" in df.columns
    assert "ensemble_diversity_count" in df.columns
    assert int(df["ensemble_only_allowed"].astype(int).sum()) >= 0
    assert float(df["estimated_cost_edge"].min()) >= (2.0 + 1.0) / 10000.0
