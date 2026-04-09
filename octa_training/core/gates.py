from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple, Union

try:
    from pydantic.v1 import BaseModel, Field, validator
except Exception:  # pragma: no cover
    from pydantic.v1 import BaseModel, Field, validator

from octa_training.core.metrics_contract import MetricsSummary


class GateSpec(BaseModel):
    # Governance/versioning
    gate_version: str = "v0"

    sharpe_min: float = 0.0
    sortino_min: float = 0.0
    max_drawdown_max: float = 1.0
    profit_factor_min: float = 0.0
    min_trades: int = 0

    # Stability / degradation
    sharpe_wf_std_max: float = 1e9
    sharpe_oos_over_is_min: float = 0.0

    # Cost coverage
    net_to_gross_min: float = 0.0

    # Trade economics
    avg_net_trade_return_min: float = float('-inf')

    # Turnover / activity controls
    turnover_per_day_max: float = 1e9
    avg_gross_exposure_max: float = 1e9
    # Tail-risk proxy control (frequency-agnostic)
    cvar_99_sigma_max: float = 1e9

    # Hard kill-switch: CVaR95 must not exceed X * daily vol
    cvar_95_over_daily_vol_max: float = 2.5
    required_folds_pass_ratio: float = 0.7
    overfit_gap_ratio: float = 0.4
    stress_cost_multiplier: float = 1.0
    # robustness thresholds
    robustness_permutation_auc_max: float = 0.55
    # Optional robustness test intensity controls (do not change thresholds)
    robustness_permutation_max_folds: Optional[int] = None
    robustness_permutation_n_shuffles: Optional[int] = None
    robustness_subwindow_min_sharpe_ratio: float = 0.5
    robustness_subwindow_abs_sharpe_min: float = 0.5
    robustness_stress_min_sharpe: float = 0.5
    robustness_regime_top_quantile: float = 0.8
    robustness_regime_max_drawdown: float = 0.5

    # Optional deterministic block-bootstrap "MC" robustness (enabled only if any threshold is set)
    bootstrap_sharpe_floor: Optional[float] = None
    bootstrap_sharpe_p05_min: Optional[float] = None
    bootstrap_maxdd_p95_max: Optional[float] = None
    bootstrap_prob_sharpe_below_max: Optional[float] = None
    bootstrap_n: Optional[int] = None
    bootstrap_block: Optional[int] = None
    bootstrap_seed: Optional[int] = None

    # Mandatory per-stage Monte Carlo gate (deterministic, net-of-cost).
    monte_carlo_n: int = 600
    monte_carlo_seed: int = 1337
    monte_carlo_pf_p05_min: float = 1.05
    monte_carlo_sharpe_p05_min: float = 0.40
    monte_carlo_maxdd_mult: float = 1.5
    monte_carlo_maxdd_floor: float = 0.0
    monte_carlo_prob_loss_max: float = 0.40

    # Mandatory walk-forward OOS gate
    walkforward_oos_pf_scale: float = 0.95
    walkforward_oos_sharpe_scale: float = 0.90
    walkforward_oos_dd_scale: float = 1.0
    walkforward_min_fold_pass_ratio: float = 1.0
    walkforward_oos_pf_min: Optional[float] = None
    walkforward_oos_sharpe_min: Optional[float] = None
    walkforward_oos_maxdd_max: Optional[float] = None

    # Mandatory regime-stability gate
    regime_dd_limit: Optional[float] = None
    regime_pf_min: float = 1.1
    regime_pf_min_worst: float = 1.0
    regime_sharpe_collapse_ratio: float = 0.35
    # When True: skip pf/dd check for the low-volatility regime and exclude it
    # from the cross-regime sharpe-collapse calculation.  Use for assets that are
    # structurally regime-selective (e.g. REITs, carry trades) where low-vol
    # underperformance is expected and not a signal of model failure.
    regime_stability_skip_low: bool = False
    # When True: skip pf/dd check for the high-volatility regime and exclude it
    # from the cross-regime sharpe-collapse calculation.  Use for intraday (1H)
    # strategies that are regime-selective: they trade low/mid-vol windows
    # where spread_bps is tight; during VIX-spike high-vol, bid-ask widens 3-5×
    # and signal quality naturally degrades.  Not a model-quality failure for
    # research screening — monitor and disable during stress in production.
    regime_stability_skip_high: bool = False

    # Mandatory cost-stress gate
    stress_pf_min: float = 1.05
    stress_dd_mult: float = 1.25
    stress_dd_limit: Optional[float] = None

    # Mandatory liquidity gate
    liquidity_percentile_min: float = 40.0

    # Statistical sufficiency / Phase-1 survival semantics
    # If evidence is insufficient, emit PASS_LIMITED_STATISTICAL_CONFIDENCE instead of FAIL.
    folds_min_evaluable: int = 3
    fold_min_trades: Optional[int] = None
    sharpe_wf_std_min_folds: int = 3
    sharpe_oos_over_is_min_folds: int = 1

    @validator("max_drawdown_max")
    def md_range(cls, v):
        if not (0 <= v <= 10):
            raise ValueError("max_drawdown_max must be in [0,10]")
        return v


class GateResult(BaseModel):
    passed: bool
    status: str = "PASS_FULL"
    gate_version: Optional[str] = None
    reasons: List[str] = Field(default_factory=list)
    passed_checks: List[str] = Field(default_factory=list)
    insufficient_evidence: List[str] = Field(default_factory=list)
    robustness: Optional[dict] = None
    diagnostics: Optional[List[Dict[str, Any]]] = None


def _status_from_reasons(reasons: List[str]) -> str:
    """Classify failures in a fail-closed, audit-first manner."""
    rs = [str(r or "") for r in (reasons or [])]
    if any("data_load_failed" in r for r in rs):
        return "FAIL_DATA"
    if any("missing_walk_forward" in r for r in rs):
        return "FAIL_STRUCTURAL"
    risk_markers = (
        "tail_kill_switch",
        "tail_risk",
        "cvar_",
        "max_drawdown",
        "avg_gross_exposure",
        "regime_stress_failed",
    )
    if any(any(m in r for m in risk_markers) for r in rs):
        return "FAIL_RISK"
    return "FAIL_STRUCTURAL"


def finalize_gate_decision(
    raw_gate: Union[None, "GateResult", Dict[str, Any]],
    error: Optional[str] = None,
    hard_flags: Optional[Dict[str, Any]] = None,
) -> Tuple[GateResult, bool]:
    """Finalize (gate, passed) with strict invariants.

    Invariants enforced:
    - If status is PASS_* -> gate.passed and returned passed are True.
    - If error exists or reasons include data_load_failed/missing_walk_forward -> FAIL_* and passed False.

    This is intended to be the single, shared decision point for writers and pipelines.
    """

    hard_flags = hard_flags or {}

    if raw_gate is None:
        reasons = []
        if error:
            reasons = [str(error)]
        g = GateResult(
            passed=False,
            status="FAIL_DATA" if error else "FAIL_STRUCTURAL",
            gate_version=None,
            reasons=reasons,
            passed_checks=[],
            robustness=None,
            diagnostics=None,
        )
        return g, False

    if isinstance(raw_gate, GateResult):
        g = raw_gate
    else:
        # dict-like gate from serialization
        try:
            g = GateResult(**raw_gate)
        except Exception:
            # fail-closed: unknown shape
            reasons = []
            if error:
                reasons = [str(error)]
            g = GateResult(passed=False, status="FAIL_DATA" if error else "FAIL_STRUCTURAL", reasons=reasons)

    # Normalize reasons and insufficiency
    reasons = list(getattr(g, "reasons", None) or [])
    insufficient = list(getattr(g, "insufficient_evidence", None) or [])

    # Hard error or explicit hard flags -> FAIL_DATA
    if error or hard_flags.get("error"):
        if error:
            if str(error) not in reasons:
                reasons.insert(0, str(error))
        g.passed = False
        g.status = "FAIL_DATA"
        g.reasons = reasons
        g.insufficient_evidence = insufficient
        return g, False

    # If any reason indicates data/wf failure, fail-closed
    if any("data_load_failed" in str(r or "") for r in reasons):
        g.passed = False
        g.status = "FAIL_DATA"
        g.reasons = reasons
        g.insufficient_evidence = insufficient
        return g, False
    if any("missing_walk_forward" in str(r or "") for r in reasons):
        g.passed = False
        g.status = "FAIL_STRUCTURAL"
        g.reasons = reasons
        g.insufficient_evidence = insufficient
        return g, False

    # If reasons exist, it is a fail (unless reasons are empty)
    if reasons:
        g.passed = False
        g.status = _status_from_reasons(reasons)
        g.reasons = reasons
        g.insufficient_evidence = insufficient
        return g, False

    # No reasons: it is a pass; choose full vs limited
    g.passed = True
    g.status = "PASS_FULL" if len(insufficient) == 0 else "PASS_LIMITED_STATISTICAL_CONFIDENCE"
    g.reasons = reasons
    g.insufficient_evidence = insufficient
    return g, True


def _fold_pass_count(metrics: MetricsSummary, gate: GateSpec) -> Tuple[int, int]:
    if not metrics.fold_metrics:
        return (0, 0)
    passed = 0
    total = 0
    for fm in metrics.fold_metrics:
        # Only count folds that have the metrics needed for gating.
        if fm.sharpe is None or fm.max_drawdown is None or fm.n_trades is None:
            continue
        total += 1
        if fm.sharpe >= gate.sharpe_min and fm.max_drawdown <= gate.max_drawdown_max and fm.n_trades >= gate.min_trades:
            passed += 1
    return passed, total


def gate_evaluate(metrics: MetricsSummary, gate: GateSpec) -> GateResult:
    reasons: List[str] = []
    passed_checks: List[str] = []
    diagnostics: List[Dict[str, Any]] = []

    insufficient: List[str] = []
    risk_failures: List[str] = []
    structural_failures: List[str] = []

    def _conf_margin(value: Optional[float], threshold: Optional[float], op: str) -> float:
        """Return a bounded confidence-like score in [0,1] based on margin.

        This is not a statistical CI; it's a normalized distance from the threshold.
        - For ">=": margin = value - threshold
        - For "<=": margin = threshold - value
        Values <=0 map to 0, large positive margins asymptote to 1.
        """
        try:
            if value is None or threshold is None:
                return 0.0
            v = float(value)
            t = float(threshold)
            if op == ">=":
                margin = v - t
            elif op == "<=":
                margin = t - v
            else:
                return 0.0
            if not (margin == margin):
                return 0.0
            if margin <= 0:
                return 0.0
            denom = abs(t) if abs(t) > 1e-12 else 1.0
            scaled = margin / denom
            # smooth clamp
            return float(max(0.0, min(1.0, scaled / (1.0 + scaled))))
        except Exception:
            return 0.0

    def _add_check(
        name: str,
        value: Any,
        threshold: Any,
        op: str,
        passed: bool,
        reason: Optional[str] = None,
        evaluable: bool = True,
    ) -> None:
        try:
            v = float(value) if value is not None else None
        except Exception:
            v = None
        try:
            t = float(threshold) if threshold is not None else None
        except Exception:
            t = None
        diagnostics.append(
            {
                "name": name,
                "value": v,
                "threshold": t,
                "op": op,
                "passed": bool(passed),
                "evaluable": bool(evaluable),
                "confidence": _conf_margin(v, t, op),
                "reason": reason,
            }
        )

    # Global hard kill-switch: tail risk (CVaR95 vs daily vol)
    tr = getattr(metrics, 'cvar_95_over_daily_vol', None)
    if tr is None:
        msg = 'missing_cvar_95_over_daily_vol'
        reasons.append(msg)
        risk_failures.append(msg)
        _add_check('cvar_95_over_daily_vol', None, getattr(gate, 'cvar_95_over_daily_vol_max', 2.5), '<=', False, msg)
    else:
        try:
            tr_v = float(tr)
            thr = float(getattr(gate, 'cvar_95_over_daily_vol_max', 2.5))
            ok = tr_v <= thr
            if not ok:
                msg = f"tail_kill_switch: cvar_95_over_daily_vol {tr} > {gate.cvar_95_over_daily_vol_max}"
                reasons.append(msg)
                risk_failures.append(msg)
            else:
                passed_checks.append('tail_kill_switch')
            _add_check('cvar_95_over_daily_vol', tr_v, thr, '<=', ok, None if ok else 'tail_kill_switch')
        except Exception:
            msg = 'tail_kill_switch_parse_error'
            reasons.append(msg)
            risk_failures.append(msg)
            _add_check('cvar_95_over_daily_vol', tr, getattr(gate, 'cvar_95_over_daily_vol_max', 2.5), '<=', False, msg)

    # sharpe
    if metrics.sharpe is None:
        msg = f"sharpe too low: {metrics.sharpe} < {gate.sharpe_min}"
        reasons.append(msg)
        structural_failures.append(msg)
        _add_check('sharpe', None, gate.sharpe_min, '>=', False, 'sharpe')
    else:
        ok = float(metrics.sharpe) >= float(gate.sharpe_min)
        if not ok:
            msg = f"sharpe too low: {metrics.sharpe} < {gate.sharpe_min}"
            reasons.append(msg)
            structural_failures.append(msg)
        else:
            passed_checks.append("sharpe")
        _add_check('sharpe', float(metrics.sharpe), float(gate.sharpe_min), '>=', ok, None if ok else 'sharpe')

    # sortino
    if metrics.sortino is None:
        msg = f"sortino too low: {metrics.sortino} < {gate.sortino_min}"
        reasons.append(msg)
        structural_failures.append(msg)
        _add_check('sortino', None, gate.sortino_min, '>=', False, 'sortino')
    else:
        ok = float(metrics.sortino) >= float(gate.sortino_min)
        if not ok:
            msg = f"sortino too low: {metrics.sortino} < {gate.sortino_min}"
            reasons.append(msg)
            structural_failures.append(msg)
        else:
            passed_checks.append("sortino")
        _add_check('sortino', float(metrics.sortino), float(gate.sortino_min), '>=', ok, None if ok else 'sortino')

    # max drawdown
    if metrics.max_drawdown is None:
        msg = "missing_max_drawdown"
        reasons.append(msg)
        risk_failures.append(msg)
        _add_check('max_drawdown', None, gate.max_drawdown_max, '<=', False, msg)
    elif metrics.max_drawdown > gate.max_drawdown_max:
        msg = f"max_drawdown too large: {metrics.max_drawdown} > {gate.max_drawdown_max}"
        reasons.append(msg)
        risk_failures.append(msg)
        _add_check('max_drawdown', float(metrics.max_drawdown), float(gate.max_drawdown_max), '<=', False, 'max_drawdown')
    else:
        passed_checks.append("max_drawdown")
        _add_check('max_drawdown', float(metrics.max_drawdown), float(gate.max_drawdown_max), '<=', True, None)

    # profit factor
    if metrics.profit_factor is None:
        msg = f"profit_factor too low: {metrics.profit_factor} < {gate.profit_factor_min}"
        reasons.append(msg)
        structural_failures.append(msg)
        _add_check('profit_factor', None, gate.profit_factor_min, '>=', False, 'profit_factor')
    else:
        ok = float(metrics.profit_factor) >= float(gate.profit_factor_min)
        if not ok:
            msg = f"profit_factor too low: {metrics.profit_factor} < {gate.profit_factor_min}"
            reasons.append(msg)
            structural_failures.append(msg)
        else:
            passed_checks.append("profit_factor")
        _add_check('profit_factor', float(metrics.profit_factor), float(gate.profit_factor_min), '>=', ok, None if ok else 'profit_factor')

    # trades
    try:
        ntr = int(metrics.n_trades)
    except Exception:
        ntr = 0
    min_tr = int(gate.min_trades)
    ok = ntr >= min_tr
    if not ok:
        insufficient.append('n_trades')
        passed_checks.append('n_trades_insufficient')
        _add_check('n_trades', ntr, min_tr, '>=', True, 'insufficient_evidence_n_trades', evaluable=False)
    else:
        passed_checks.append("n_trades")
        _add_check('n_trades', ntr, min_tr, '>=', True, None)

    # Sharpe stability across WF splits (std of OOS fold sharpes)
    try:
        sh = []
        if getattr(metrics, 'fold_metrics', None):
            for fm in metrics.fold_metrics:
                if fm is None:
                    continue
                if fm.sharpe is None:
                    continue
                sh.append(float(fm.sharpe))
        min_folds = int(getattr(gate, 'sharpe_wf_std_min_folds', 3) or 3)
        if len(sh) >= min_folds:
            import numpy as np

            stdv = float(np.std(np.asarray(sh), ddof=0))
            metrics.sharpe_wf_std = stdv
            thr = float(gate.sharpe_wf_std_max)
            ok = stdv <= thr
            if not ok:
                # Phase-1 semantics: instability reduces confidence but is not an auto-kill.
                insufficient.append('sharpe_wf_std_high')
                passed_checks.append('sharpe_wf_std_soft_fail')
                _add_check('sharpe_wf_std', stdv, thr, '<=', False, 'soft_fail_sharpe_wf_std', evaluable=True)
            else:
                passed_checks.append('sharpe_wf_std')
                _add_check('sharpe_wf_std', stdv, thr, '<=', True, None)
        else:
            insufficient.append('sharpe_wf_std')
            passed_checks.append('sharpe_wf_std_insufficient')
            _add_check('sharpe_wf_std', None, float(gate.sharpe_wf_std_max), '<=', True, 'insufficient_evidence_folds', evaluable=False)
    except Exception:
        msg = 'sharpe_wf_std_error'
        reasons.append(msg)
        structural_failures.append(msg)
        _add_check('sharpe_wf_std', None, float(gate.sharpe_wf_std_max), '<=', False, msg)

    # IS/OOS degradation proxy: Sharpe_OOS / mean(Sharpe_IS)
    try:
        is_sh = []
        if getattr(metrics, 'fold_metrics', None):
            for fm in metrics.fold_metrics:
                if fm is None:
                    continue
                v = getattr(fm, 'sharpe_is', None)
                if v is None:
                    continue
                is_sh.append(float(v))
        min_is = int(getattr(gate, 'sharpe_oos_over_is_min_folds', 1) or 1)
        if len(is_sh) >= min_is and metrics.sharpe is not None:
            import numpy as np

            is_mean = float(np.mean(np.asarray(is_sh)))
            metrics.sharpe_is_mean = is_mean
            ratio = float(metrics.sharpe / (is_mean + 1e-12)) if is_mean != 0 else 0.0
            metrics.sharpe_oos_over_is = ratio
            thr = float(gate.sharpe_oos_over_is_min)
            ok = ratio >= thr
            if not ok:
                msg = f"is_oos_degradation: sharpe_oos_over_is {ratio} < {gate.sharpe_oos_over_is_min}"
                reasons.append(msg)
                structural_failures.append(msg)
            else:
                passed_checks.append('is_oos_degradation')
            _add_check('sharpe_oos_over_is', ratio, thr, '>=', ok, None if ok else 'is_oos_degradation')
        else:
            insufficient.append('is_oos_degradation')
            passed_checks.append('is_oos_degradation_insufficient')
            _add_check('sharpe_oos_over_is', None, float(gate.sharpe_oos_over_is_min), '>=', True, 'insufficient_evidence_missing_is_or_oos', evaluable=False)
    except Exception:
        msg = 'is_oos_degradation_error'
        reasons.append(msg)
        structural_failures.append(msg)
        _add_check('sharpe_oos_over_is', None, float(gate.sharpe_oos_over_is_min), '>=', False, msg)

    # Cost coverage: Net / Gross
    ntg = getattr(metrics, 'net_to_gross', None)
    if ntg is None:
        msg = 'missing_net_to_gross'
        reasons.append(msg)
        structural_failures.append(msg)
        _add_check('net_to_gross', None, gate.net_to_gross_min, '>=', False, msg)
    else:
        try:
            v = float(ntg)
            thr = float(gate.net_to_gross_min)
            ok = v >= thr
            if not ok:
                msg = f"net_to_gross too low: {ntg} < {gate.net_to_gross_min}"
                reasons.append(msg)
                structural_failures.append(msg)
            else:
                passed_checks.append('net_to_gross')
            _add_check('net_to_gross', v, thr, '>=', ok, None if ok else 'net_to_gross')
        except Exception:
            msg = 'net_to_gross_parse_error'
            reasons.append(msg)
            structural_failures.append(msg)
            _add_check('net_to_gross', ntg, gate.net_to_gross_min, '>=', False, msg)

    # Avg net return per trade
    antr = getattr(metrics, 'avg_net_trade_return', None)
    if antr is None:
        msg = 'missing_avg_net_trade_return'
        reasons.append(msg)
        structural_failures.append(msg)
        _add_check('avg_net_trade_return', None, gate.avg_net_trade_return_min, '>=', False, msg)
    else:
        try:
            v = float(antr)
            thr = float(gate.avg_net_trade_return_min)
            ok = v >= thr
            if not ok:
                msg = f"avg_net_trade_return too low: {antr} < {gate.avg_net_trade_return_min}"
                reasons.append(msg)
                structural_failures.append(msg)
            else:
                passed_checks.append('avg_net_trade_return')
            _add_check('avg_net_trade_return', v, thr, '>=', ok, None if ok else 'avg_net_trade_return')
        except Exception:
            msg = 'avg_net_trade_return_parse_error'
            reasons.append(msg)
            structural_failures.append(msg)
            _add_check('avg_net_trade_return', antr, gate.avg_net_trade_return_min, '>=', False, msg)

    # turnover per day
    tpd = getattr(metrics, 'turnover_per_day', None)
    if tpd is None:
        msg = "missing_turnover_per_day"
        reasons.append(msg)
        structural_failures.append(msg)
        _add_check('turnover_per_day', None, gate.turnover_per_day_max, '<=', False, msg)
    else:
        try:
            v = float(tpd)
            thr = float(gate.turnover_per_day_max)
            ok = v <= thr
            if not ok:
                msg = f"turnover_per_day too high: {tpd} > {gate.turnover_per_day_max}"
                reasons.append(msg)
                structural_failures.append(msg)
            else:
                passed_checks.append("turnover_per_day")
            _add_check('turnover_per_day', v, thr, '<=', ok, None if ok else 'turnover_per_day')
        except Exception:
            msg = "turnover_per_day_parse_error"
            reasons.append(msg)
            structural_failures.append(msg)
            _add_check('turnover_per_day', tpd, gate.turnover_per_day_max, '<=', False, msg)

    # average gross exposure
    age = getattr(metrics, 'avg_gross_exposure', None)
    if age is None:
        msg = "missing_avg_gross_exposure"
        reasons.append(msg)
        risk_failures.append(msg)
        _add_check('avg_gross_exposure', None, gate.avg_gross_exposure_max, '<=', False, msg)
    else:
        try:
            v = float(age)
            thr = float(gate.avg_gross_exposure_max)
            ok = v <= thr
            if not ok:
                msg = f"avg_gross_exposure too high: {age} > {gate.avg_gross_exposure_max}"
                reasons.append(msg)
                risk_failures.append(msg)
            else:
                passed_checks.append("avg_gross_exposure")
            _add_check('avg_gross_exposure', v, thr, '<=', ok, None if ok else 'avg_gross_exposure')
        except Exception:
            msg = 'avg_gross_exposure_parse_error'
            reasons.append(msg)
            risk_failures.append(msg)
            _add_check('avg_gross_exposure', age, gate.avg_gross_exposure_max, '<=', False, msg)

    # tail risk (cvar sigma)
    cvar_sig = getattr(metrics, 'cvar_99_sigma', None)
    if cvar_sig is None:
        msg = "missing_cvar_99_sigma"
        reasons.append(msg)
        risk_failures.append(msg)
        _add_check('cvar_99_sigma', None, gate.cvar_99_sigma_max, '<=', False, msg)
    else:
        try:
            v = float(cvar_sig)
            thr = float(gate.cvar_99_sigma_max)
            ok = v <= thr
            if not ok:
                msg = f"tail_risk too high: cvar_99_sigma {cvar_sig} > {gate.cvar_99_sigma_max}"
                reasons.append(msg)
                risk_failures.append(msg)
            else:
                passed_checks.append("tail_risk")
            _add_check('cvar_99_sigma', v, thr, '<=', ok, None if ok else 'tail_risk')
        except Exception:
            msg = 'tail_risk_parse_error'
            reasons.append(msg)
            risk_failures.append(msg)
            _add_check('cvar_99_sigma', cvar_sig, gate.cvar_99_sigma_max, '<=', False, msg)

    # fold pass ratio
    # Phase-1 semantics: only hard-fail on systematic failure; otherwise downgrade confidence.
    eval_total = 0
    eval_good = 0
    fold_min_trades = int(getattr(gate, 'fold_min_trades', None) or int(gate.min_trades))
    if getattr(metrics, 'fold_metrics', None):
        for fm in metrics.fold_metrics:
            if fm is None or fm.sharpe is None or fm.max_drawdown is None or fm.n_trades is None:
                continue
            try:
                fm_trades = int(fm.n_trades)
            except Exception:
                fm_trades = 0
            if fm_trades < fold_min_trades:
                continue
            eval_total += 1
            # Fold is considered "good" if it is not negative AND respects drawdown.
            if float(fm.sharpe) >= 0.0 and float(fm.max_drawdown) <= float(gate.max_drawdown_max):
                eval_good += 1

    min_eval_folds = int(getattr(gate, 'folds_min_evaluable', 3) or 3)
    if eval_total < min_eval_folds:
        insufficient.append('folds_pass_ratio')
        passed_checks.append('folds_pass_ratio_insufficient')
        _add_check('folds_pass_ratio', None, gate.required_folds_pass_ratio, '>=', True, 'insufficient_evidence_folds', evaluable=False)
    else:
        ratio = eval_good / eval_total
        thr = float(gate.required_folds_pass_ratio)
        ok = float(ratio) >= thr
        if ratio <= 0.0:
            msg = f"folds systematic failure: good_folds 0/{eval_total}"
            reasons.append(msg)
            structural_failures.append(msg)
            _add_check('folds_pass_ratio', float(ratio), thr, '>=', False, 'folds_pass_ratio_systematic')
        elif not ok:
            insufficient.append('folds_pass_ratio_soft_fail')
            passed_checks.append('folds_pass_ratio_soft_fail')
            _add_check('folds_pass_ratio', float(ratio), thr, '>=', False, 'soft_fail_folds_pass_ratio', evaluable=True)
        else:
            passed_checks.append('folds_pass_ratio')
            _add_check('folds_pass_ratio', float(ratio), thr, '>=', True, None)

    overall = len(reasons) == 0
    if overall:
        status = "PASS_FULL" if len(insufficient) == 0 else "PASS_LIMITED_STATISTICAL_CONFIDENCE"
    else:
        status = "FAIL_RISK" if len(risk_failures) > 0 else "FAIL_STRUCTURAL"
    return GateResult(
        passed=overall,
        status=status,
        gate_version=getattr(gate, 'gate_version', None),
        reasons=reasons,
        passed_checks=passed_checks,
        insufficient_evidence=insufficient,
        diagnostics=diagnostics,
    )
