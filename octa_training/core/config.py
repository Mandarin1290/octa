from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from octa.core.utils.typing_safe import as_float

try:
    from pydantic.v1 import BaseModel, Field, validator
except Exception:  # pragma: no cover
    from pydantic.v1 import BaseModel, Field, validator


class PathsConfig(BaseModel):
    raw_dir: Path = Path("/home/n-b/Octa/raw")
    # Dedicated FX parquet directory (preferred for FX intraday, esp. 1H).
    fx_parquet_dir: Path = Path("/home/n-b/Octa/raw/FX_parquet")
    pkl_dir: Path = Path("/home/n-b/Octa/raw/PKL")
    logs_dir: Path = Path("/home/n-b/Octa/logs")
    state_dir: Path = Path("/home/n-b/Octa/state")
    reports_dir: Path = Path("/home/n-b/Octa/reports")

    @validator("raw_dir", "fx_parquet_dir", "pkl_dir", "logs_dir", "state_dir", "reports_dir", pre=True)
    def ensure_path(cls, v):
        return Path(v)


class GatingConfig(BaseModel):
    norm_mse_threshold: float = 0.02
    abs_mse_threshold: Optional[float] = None
    min_train_samples: int = 1000
    min_backtest_samples: int = 2000
    max_cv_rel_std: float = 0.2
    max_cv_backtest_delta: float = 0.15
    min_sharpe: float = 1.0
    max_drawdown: float = 0.2


class TuningConfig(BaseModel):
    optuna_trials: int = 50
    cv_folds: int = 5
    early_stop_rounds: int = 50
    timeout_sec: Optional[int] = None
    max_concurrent_trials: int = 1
    enabled: bool = False
    objective_cls: str = "auc"
    objective_reg: str = "rmse"
    models_order: List[str] = Field(default_factory=lambda: ["lightgbm", "xgboost", "catboost", "logreg", "ridge"])
    # Optional bounded search space hints for Optuna to reduce overfitting risk.
    # Structure: {model_name: {param_name: {type, low, high}}}
    search_space: Dict[str, Any] = Field(default_factory=dict)


class RetrainConfig(BaseModel):
    default_cadence_days: int = 7
    max_retries: int = 3
    skip_window_days: int = 3


class CostConfig(BaseModel):
    compute_cost_per_hour_usd: float = 1.5
    storage_cost_per_gb_month_usd: float = 0.02
    # Safety guard: FX evaluations must not silently run with zero trading costs.
    # When True (default), FX gates fail-closed if the effective cost model is zero.
    require_nonzero_for_fx: bool = True


class SignalConfig(BaseModel):
    mode: str = "cls"
    upper_q: float = 0.9
    lower_q: float = 0.1
    # If True, compute quantile thresholds in a leakage-safe way using only past predictions.
    # When False, thresholds are computed over the full sample (legacy behavior).
    causal_quantiles: bool = False
    # Rolling lookback window for causal quantiles. If None, uses an expanding window.
    quantile_window: Optional[int] = 252
    leverage_cap: float = 3.0
    vol_target: float = 0.1
    realized_vol_window: int = 20
    cost_bps: float = 1.0
    spread_bps: float = 0.5
    stress_cost_multiplier: float = 3.0


class BrokerConfig(BaseModel):
    """Execution/cost model configuration.

    Project constraint: only IBKR is supported.
    """

    name: str = "ibkr"
    # conservative, all-in assumptions in basis points
    cost_bps: float = 2.0
    spread_bps: float = 1.0
    stress_cost_multiplier: float = 3.0

    @validator("name")
    def broker_must_be_ibkr(cls, v: str):
        if v is None:
            return "ibkr"
        vv = str(v).strip().lower()
        if vv not in {"ibkr"}:
            raise ValueError("Only IBKR is supported (broker.name must be 'ibkr')")
        return vv


class SessionConfig(BaseModel):
    """Optional trading session filter (primarily for intraday equities/ETFs/index).

    If enabled, evaluation disables signals outside session to avoid unrealistic
    overnight trading assumptions.
    """

    enabled: bool = False
    timezone: str = "UTC"
    open: str = "00:00"  # HH:MM
    close: str = "23:59"  # HH:MM
    weekdays: Optional[List[int]] = None  # 0=Mon..6=Sun


class LiquidityConfig(BaseModel):
    enabled: bool = False
    adv_lookback_days: int = 20
    min_adv_usd: Optional[float] = None
    min_adv_shares: Optional[float] = None
    min_history_days: int = 10


class TelegramConfig(BaseModel):
    enabled: bool = False
    token_env: str = "OCTA_TELEGRAM_BOT_TOKEN"
    chat_id_env: str = "OCTA_TELEGRAM_CHAT_ID"


class NotificationsConfig(BaseModel):
    telegram: TelegramConfig = Field(default_factory=TelegramConfig)


class PortfolioGateConfig(BaseModel):
    """Portfolio-level packaging gate proxies.

    Since we train per-symbol, this uses per-symbol proxies (turnover/exposure)
    to prevent packaging pathological artifacts.
    """

    enabled: bool = False
    max_turnover_ann: Optional[float] = None
    max_avg_gross_exposure: Optional[float] = None


class ParquetConfig(BaseModel):
    nan_threshold: float = 0.2
    allow_negative_prices: bool = False
    resample_enabled: bool = False
    resample_bar_size: str = "1D"


class DataConfig(BaseModel):
    # Per-timeframe parquet requirements. Default is fail-closed (True when missing).
    # Keys should match cascade timeframes (e.g., "1D", "1H", "30m", "5m", "1m").
    require_parquet_for_tf: Dict[str, bool] = Field(default_factory=dict)


class RegexRule(BaseModel):
    pattern: str
    asset_class: str
    priority: int = 0


class AssetClassOverrides(BaseModel):
    regex_rules: List[RegexRule] = Field(default_factory=list)
    direct_map: dict = Field(default_factory=dict)


class ComputeConfig(BaseModel):
    """Optional runtime compute policy.

    Default is disabled to preserve legacy behavior.
    When enabled, the system aims to use N-1 physical cores (leave one core free)
    and caps BLAS/OpenMP threads per worker to avoid oversubscription.
    """

    enabled: bool = False
    reserve_cores: int = 1
    prefer_physical_cores: bool = True
    # If True, daemon-style parallelism is limited to available cores.
    auto_cap_max_workers: bool = True
    # Threads per worker for BLAS/OpenMP libraries (MKL/OpenBLAS/OMP/NumExpr).
    # Use 1 when running multiple symbols in parallel for stability.
    blas_threads_per_worker: int = 1
    # Optional: for single-symbol runs, allow BLAS threads >1 if desired.
    single_job_blas_threads: Optional[int] = None


class KvpConfig(BaseModel):
    """Continuous improvement (KVP) registry.

    When enabled, writes a small JSON state file with aggregate metrics by asset_class.
    It does NOT alter training behavior automatically.
    """

    enabled: bool = False
    filename: str = "kvp_summary.json"




class TrainingConfig(BaseModel):
    paths: PathsConfig = Field(default_factory=PathsConfig)
    gating: GatingConfig = Field(default_factory=GatingConfig)
    tuning: TuningConfig = Field(default_factory=TuningConfig)
    parquet: ParquetConfig = Field(default_factory=ParquetConfig)
    data: DataConfig = Field(default_factory=DataConfig)
    asset_class_overrides: AssetClassOverrides = Field(default_factory=AssetClassOverrides)
    compute: ComputeConfig = Field(default_factory=ComputeConfig)
    kvp: KvpConfig = Field(default_factory=KvpConfig)
    # runtime / hardware tuning
    max_workers: int = 4
    min_ram_mb: int = 2048
    prefer_gpu: bool = True
    # Gate configuration
    gates: dict = Field(default_factory=dict)

    # Asset-specific routing/profiles for Global Gate.
    # Optional and fully backwards-compatible.
    asset_defaults: Dict[str, Any] = Field(default_factory=dict)
    asset_profiles: Dict[str, Any] = Field(default_factory=dict)
    # Feature engineering settings
    features: dict = Field(default_factory=lambda: {
        "window_short": 5,
        "window_med": 20,
        "window_long": 60,
        "vol_window": 20,
        "horizons": [1, 3, 5],
        # Leakage audit tolerance for deterministic numeric drift checks.
        "leakage_audit_rtol": 2e-2,
        "leakage_audit_atol": 1e-5,
        # Optional macro features (FRED) - disabled by default.
        # Enable by setting features.macro.enabled=true and providing FRED_API_KEY env var.
        "macro": {
            "enabled": False,
            # auto: prefer cached parquet, else fetch via API if key available
            # parquet: only use cached parquet
            # api: fetch from API (and cache)
            "source": "auto",
            "cache_filename": "fred_macro.parquet",
            "key_env": "FRED_API_KEY",
            # Daily series commonly used as macro proxies
            "series": ["FEDFUNDS", "DGS10", "DGS2", "UNRATE"],
            # shift macro signals by 1 bar after alignment (leakage-safe)
            "shift_bars": 1,
        },
    })
    # Signal / evaluation settings
    signal: SignalConfig = Field(default_factory=SignalConfig)
    # Broker/cost model (IBKR-only)
    broker: BrokerConfig = Field(default_factory=BrokerConfig)
    # Optional session filter (intraday)
    session: SessionConfig = Field(default_factory=SessionConfig)
    # Liquidity filter
    liquidity: LiquidityConfig = Field(default_factory=LiquidityConfig)
    # Notifications
    notifications: NotificationsConfig = Field(default_factory=NotificationsConfig)
    # Portfolio-level packaging gate
    portfolio_gate: PortfolioGateConfig = Field(default_factory=PortfolioGateConfig)
    # Splitting settings
    splits: dict = Field(default_factory=lambda: {
        "n_folds": 5,
        "train_window": 1000,
        "test_window": 200,
        "step": 200,
        "purge_size": 10,
        "embargo_size": 5,
        "min_train_size": 500,
        "min_test_size": 100,
        "min_folds_required": 1,
        "expanding": True
    })
    # Per-timeframe split overrides (keys: 1D, 1H, 30M, 5M, 1M).
    # Each entry merges on top of `splits` for that timeframe only.
    # Required so the OOF window (n_folds * test_window) meets the
    # institutional gate minimum (train_bars + 2*oos_bars) per TF.
    splits_by_timeframe: Dict[str, Any] = Field(default_factory=dict)
    # Model training settings
    seed: int = 42
    scale_linear: bool = True
    models_order: List[str] = Field(default_factory=lambda: ["lightgbm", "xgboost", "catboost", "logreg", "ridge"])
    lgbm_params: Dict[str, Any] = Field(default_factory=lambda: {"objective": "binary", "metric": "auc"})
    xgb_params: Dict[str, Any] = Field(default_factory=lambda: {"objective": "binary:logistic", "eval_metric": "auc"})
    cat_params: Dict[str, Any] = Field(default_factory=lambda: {"loss_function": "Logloss"})
    early_stopping_rounds: int = 50
    num_boost_round: int = 1000
    retrain: RetrainConfig = Field(default_factory=RetrainConfig)
    costs: CostConfig = Field(default_factory=CostConfig)
    training_command: str = ""
    symbol_overrides: Dict[str, Any] = Field(default_factory=dict)
    # Packaging policy
    class PackagingPolicy(BaseModel):
        compare_metric_name: str = "sharpe"
        min_improvement: float = 0.01
        atomic_tmp_dir: Optional[str] = None
        quarantine_on_smoke_fail: bool = True
        quarantine_dir: Optional[str] = None
        max_age_days: Optional[int] = 30

        # Optional audit mode: write a clearly-labeled debug artifact even on gate FAIL.
        # Does NOT update symbol PASS state and should not be treated as tradeable.
        save_debug_on_fail: bool = False
        debug_dir: Optional[str] = None

    packaging: PackagingPolicy = Field(default_factory=PackagingPolicy)

    # Assurance / audit / governance integrations
    class AssuranceConfig(BaseModel):
        """Tier-1 assurance hooks.

        Purpose: persist run-level evidence (audit snapshot + optional compliance attestation
        and governance review trigger) without changing the PASS/FAIL gating semantics.
        """

        enabled: bool = True
        # If True, pipeline will fail the run if assurance emission fails.
        # Default is tolerant to avoid blocking training due to audit plumbing.
        fail_closed: bool = False
        # Subdirectory under cfg.paths.reports_dir where assurance JSON is written.
        report_subdir: str = "assurance"
        # Include a compact config snapshot inside the evidence.
        include_config: bool = True

        # Governance: on tradeable artifact creation, emit a governance review record.
        governance_review_on_tradeable: bool = True
        governance_cycle: str = "weekly_strategy"  # daily_risk|weekly_strategy|monthly_committee

        # Compliance: emit a textual attestation record via ContinuousAudit.
        compliance_attestation: bool = True

        # Optional: wire in RegulatoryAdaptation if rules are provided.
        regulatory_enabled: bool = False
        regulatory_compatibility_mode: str = "strict"  # strict|lenient
        regulatory_rules: List[Dict[str, Any]] = Field(default_factory=list)

    assurance: AssuranceConfig = Field(default_factory=AssuranceConfig)
    # smoke test defaults
    smoke_test_last_n: int = 50
    # Robustness defaults
    class RobustnessDefaults(BaseModel):
        permutation_auc_max: float = 0.55
        subwindow_min_sharpe_ratio: float = 0.5
        subwindow_abs_sharpe_min: float = 0.5
        stress_min_sharpe: float = 0.5
        regime_top_quantile: float = 0.8
        regime_max_drawdown: float = 0.5

    robustness: RobustnessDefaults = Field(default_factory=RobustnessDefaults)


# Pydantic v1 + postponed annotations can require explicit forward-ref resolution
# for nested models. Since the annotation is `PackagingPolicy` (a nested class),
# we must provide it in `localns`.
try:  # pragma: no cover
    TrainingConfig.update_forward_refs(
        PackagingPolicy=TrainingConfig.PackagingPolicy,
        AssuranceConfig=TrainingConfig.AssuranceConfig,
        RobustnessDefaults=TrainingConfig.RobustnessDefaults,
    )
except Exception:
    pass


def load_config(path: Optional[str] = None) -> TrainingConfig:
    """Load YAML config and validate with pydantic.

    Args:
        path: optional yaml path; if None, use package default `config/training.yaml`.
    Returns:
        TrainingConfig instance with validated values.
    """
    if path:
        p = Path(path)
    else:
        p = Path(__file__).resolve().parents[1] / "config" / "training.yaml"
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {p}")
    with p.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}

    # Tier-1 HF defaults (global gates, bounded tuning ranges, etc.).
    # Merge order: hf_defaults < (optional overlay) < user config file.
    try:
        hf_path = Path(__file__).resolve().parents[2] / 'configs' / 'hf_defaults.yaml'
        if hf_path.exists():
            hf_raw = yaml.safe_load(hf_path.read_text()) or {}
        else:
            hf_raw = {}
    except Exception:
        hf_raw = {}

    # Optional non-destructive overlay to relax some model-quality thresholds.
    # Enabled only via wrapper by setting env var OCTA_GATE_OVERLAY_PATH.
    overlay_path = os.getenv("OCTA_GATE_OVERLAY_PATH")
    if overlay_path:
        try:
            op = Path(str(overlay_path))
            if op.exists():
                overlay_raw = yaml.safe_load(op.read_text(encoding="utf-8")) or {}
            else:
                overlay_raw = {}
        except Exception:
            overlay_raw = {}

        def _apply_overlay(hf: dict, overlay: dict) -> None:
            if not isinstance(hf, dict) or not isinstance(overlay, dict):
                return
            ov_raw = overlay.get("overlay")
            ov: dict[str, Any] = dict(ov_raw) if isinstance(ov_raw, dict) else {}
            relax_raw = ov.get("relax")
            relax: dict[str, Any] = dict(relax_raw) if isinstance(relax_raw, dict) else {}
            gate_v = str(ov.get("gate_version") or "")
            sharpe_factor = as_float(relax.get("sharpe_min_factor", 1.0), default=1.0)
            if sharpe_factor <= 0:
                sharpe_factor = 1.0

            gates = hf.get("gates") if isinstance(hf.get("gates"), dict) else None
            if gates is None:
                return
            gbt = gates.get("global_by_timeframe") if isinstance(gates.get("global_by_timeframe"), dict) else {}

            # Only relax model-quality MIN thresholds. Do not relax drawdown/cvar/turnover MAX constraints.
            relax_keys = {
                "sharpe_min",
                "sortino_min",
                "profit_factor_min",
                "sharpe_oos_over_is_min",
                "net_to_gross_min",
                "avg_net_trade_return_min",
            }

            for _tf, conf in gbt.items():
                if not isinstance(conf, dict):
                    continue
                for k in list(conf.keys()):
                    if k not in relax_keys:
                        continue
                    raw_value = conf.get(k)
                    if raw_value is None:
                        continue
                    try:
                        v = float(raw_value)
                    except Exception:
                        continue
                    conf[k] = v * sharpe_factor

            gates["overlay_applied"] = {
                "path": str(overlay_path),
                "gate_version": gate_v,
                "sharpe_min_factor": sharpe_factor,
            }

        try:
            _apply_overlay(hf_raw, overlay_raw)
        except Exception:
            # Fail-closed is handled by wrapper; training can proceed without overlay.
            pass

    def _deep_merge(dst: dict, src: dict) -> None:
        if not isinstance(dst, dict) or not isinstance(src, dict):
            return
        for k, v in src.items():
            if k in dst and isinstance(dst[k], dict) and isinstance(v, dict):
                _deep_merge(dst[k], v)
            else:
                dst[k] = v

    merged_raw = dict(hf_raw) if isinstance(hf_raw, dict) else {}
    if isinstance(raw, dict):
        _deep_merge(merged_raw, raw)

    cfg = TrainingConfig(**(merged_raw or {}))
    # ensure directories exist
    for d in [cfg.paths.raw_dir, cfg.paths.pkl_dir, cfg.paths.logs_dir, cfg.paths.state_dir, cfg.paths.reports_dir]:
        d.mkdir(parents=True, exist_ok=True)
    return cfg
