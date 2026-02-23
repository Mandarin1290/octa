"""StageResult — canonical output contract for a single cascade training stage.

Every call to train_evaluate_package() for (symbol, timeframe) must produce
a StageResult. The cascade orchestrator validates conformance before recording
promotion decisions.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Literal, Optional


@dataclass
class StageMandatoryMetrics:
    """Required metrics for each stage. All numeric fields are finite float or None."""

    # Walk-forward
    sharpe_ratio: Optional[float] = None           # annualised
    sortino_ratio: Optional[float] = None
    max_drawdown: Optional[float] = None            # negative fraction, e.g. -0.15
    cvar_95: Optional[float] = None                 # 95th pct CVaR
    cvar_95_over_daily_vol: Optional[float] = None  # CVaR / daily vol ratio

    # OOS evaluation
    oos_accuracy: Optional[float] = None
    oos_auc: Optional[float] = None
    regime_stability: Optional[float] = None        # fraction of folds consistent
    walk_forward_passed: Optional[bool] = None
    n_folds_completed: Optional[int] = None

    # Data health
    bars_available: Optional[int] = None
    bars_used_train: Optional[int] = None
    bars_used_test: Optional[int] = None
    nan_fraction: Optional[float] = None

    # Cost / liquidity / leakage
    cost_stress_passed: Optional[bool] = None
    liquidity_passed: Optional[bool] = None
    leakage_detected: Optional[bool] = None         # must be False for performance_pass=True

    def validate(self) -> list[str]:
        """Return list of violation messages; empty = OK."""
        issues = []
        floats = {
            "sharpe_ratio": self.sharpe_ratio,
            "sortino_ratio": self.sortino_ratio,
            "max_drawdown": self.max_drawdown,
            "cvar_95": self.cvar_95,
            "cvar_95_over_daily_vol": self.cvar_95_over_daily_vol,
            "oos_accuracy": self.oos_accuracy,
            "oos_auc": self.oos_auc,
            "regime_stability": self.regime_stability,
            "nan_fraction": self.nan_fraction,
        }
        for name, val in floats.items():
            if val is not None and (math.isnan(val) or math.isinf(val)):
                issues.append(f"{name} is not finite: {val}")
        return issues


@dataclass
class StageResult:
    """Canonical output of a single cascade training stage."""

    # Identity
    symbol: str
    timeframe: str          # normalised: "1D", "1H", "30M", "5M", "1M"
    asset_class: str
    run_id: str
    stage_index: int        # 0-based position in cascade order

    # Promotion flags
    structural_pass: bool
    performance_pass: bool

    # Required metrics
    metrics: StageMandatoryMetrics = field(default_factory=StageMandatoryMetrics)

    # Artifacts
    model_path: Optional[str] = None
    model_hash: Optional[str] = None
    gate_report_path: Optional[str] = None

    # Audit hashes
    altdata_pack_hash: Optional[str] = None
    feature_weighting_hash: Optional[str] = None
    profile_hash: Optional[str] = None
    elapsed_sec: float = 0.0

    # Failure info
    fail_status: Literal["PASS", "GATE_FAIL", "TRAIN_ERROR", "SKIP", "DATA_INVALID", "MISSING_PARQUET"] = "PASS"
    fail_reason: Optional[str] = None
    error_traceback: Optional[str] = None

    def validate(self) -> list[str]:
        """Return list of violation messages; empty = OK."""
        issues = list(self.metrics.validate())
        if self.performance_pass and self.metrics.leakage_detected:
            issues.append("performance_pass=True but leakage_detected=True")
        if self.structural_pass and self.model_path is None:
            # structural pass should have produced a model (not enforced for SKIP)
            if self.fail_status not in ("SKIP", "GATE_FAIL"):
                issues.append("structural_pass=True but model_path is None")
        if self.asset_class == "unknown" and self.model_path is not None:
            issues.append("model_path set but asset_class='unknown' — collision risk")
        return issues

    def to_details_dict(self) -> dict:
        """Return the details dict required by GateDecision.details."""
        return {
            "structural_pass": self.structural_pass,
            "performance_pass": self.performance_pass,
            "fail_status": self.fail_status,
            "fail_reason": self.fail_reason,
            "model_path": self.model_path,
            "model_hash": self.model_hash,
            "profile_hash": self.profile_hash,
            "altdata_pack_hash": self.altdata_pack_hash,
            "feature_weighting_hash": self.feature_weighting_hash,
            "leakage_detected": self.metrics.leakage_detected,
            "n_folds_completed": self.metrics.n_folds_completed,
            "walk_forward_passed": self.metrics.walk_forward_passed,
        }
