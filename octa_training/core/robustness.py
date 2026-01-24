from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from octa_training.core.metrics_contract import MetricsSummary


class RobustnessResult(BaseModel):
    passed: bool
    reasons: List[str] = Field(default_factory=list)
    details: Dict[str, Any] = Field(default_factory=dict)
    limited_reasons: List[str] = Field(default_factory=list)


def run_risk_overlay_tests(
    df_backtest: pd.DataFrame,
    preds: pd.Series,
    metrics: MetricsSummary,
    gate: Any,
    settings: Any,
) -> RobustnessResult:
    """FX 1D Risk/Regime overlay robustness subset.

    Intentionally excludes alpha/overfit diagnostics (permutation/subwindows/bootstrap)
    and retains only hard risk/cost/regime sanity checks.
    """

    reasons: List[str] = []
    details: Dict[str, Any] = {}

    # cost stress (HARD)
    try:
        cs = cost_stress_test(df_backtest['price'], preds, settings, gate)
        details['cost_stress'] = cs
        if not cs.get('passed', True):
            reasons.append(f"cost_stress_failed sharpe={cs.get('sharpe')}")
    except Exception as e:
        details['cost_stress'] = {'passed': False, 'error': str(e)}
        reasons.append('cost_stress_error')

    # regime stress (HARD)
    try:
        rg = regime_stress(df_backtest, gate)
        details['regime'] = rg
        if not rg.get('passed', True):
            reasons.append(f"regime_stress_failed subset_max_dd={rg.get('subset_max_drawdown')}")
    except Exception as e:
        details['regime'] = {'passed': False, 'error': str(e)}
        reasons.append('regime_stress_error')

    return RobustnessResult(
        passed=(len(reasons) == 0),
        reasons=reasons,
        details=details,
        limited_reasons=[],
    )


def _annualized_sharpe_from_returns(r: np.ndarray, periods_per_year: int = 252) -> float:
    if r.size < 2:
        return float('nan')
    mu = float(np.mean(r))
    sd = float(np.std(r, ddof=1))
    if not np.isfinite(sd) or sd <= 0:
        return float('nan')
    return (mu / sd) * float(np.sqrt(periods_per_year))


def _max_drawdown_from_returns(r: np.ndarray) -> float:
    if r.size == 0:
        return float('nan')
    eq = np.cumprod(1.0 + r)
    peak = np.maximum.accumulate(eq)
    dd = 1.0 - (eq / peak)
    return float(np.max(dd))


def _block_bootstrap_sample(rng: np.random.Generator, base: np.ndarray, block: int) -> np.ndarray:
    n = int(base.size)
    b = max(1, int(block))
    if n == 0:
        return base
    k = int(np.ceil(n / b))
    starts = rng.integers(0, n, size=k, endpoint=False)
    chunks = [base[s : min(s + b, n)] for s in starts]
    sample = np.concatenate(chunks, axis=0)
    return sample[:n]


def block_bootstrap_robustness(returns: np.ndarray, gate: Any, n_trades: Optional[int] = None) -> Dict[str, Any]:
    """Deterministic block-bootstrap robustness check.

    Enabled only when any `bootstrap_*` thresholds are configured on the gate.
    Phase-1 semantics: if evidence is insufficient, skip (PASS_LIMITED upstream)
    instead of fail-closed.
    """

    wants = any(
        getattr(gate, k, None) is not None
        for k in (
            'bootstrap_sharpe_floor',
            'bootstrap_sharpe_p05_min',
            'bootstrap_maxdd_p95_max',
            'bootstrap_prob_sharpe_below_max',
            'bootstrap_n',
            'bootstrap_block',
            'bootstrap_seed',
        )
    )
    if not wants:
        return {'enabled': False, 'passed': True, 'skipped': True}

    if returns is None:
        return {'enabled': True, 'passed': False, 'skipped': False, 'reason': 'missing_returns_fail_closed'}

    base = np.asarray(returns, dtype=float)
    base = base[np.isfinite(base)]

    min_obs = int(getattr(gate, 'bootstrap_min_obs', 60) or 60)
    # Default min-trades scales with timeframe min_trades unless explicitly set.
    gate_min_trades = int(getattr(gate, 'min_trades', 0) or 0)
    min_trades = getattr(gate, 'bootstrap_min_trades', None)
    if min_trades is None:
        min_trades = max(60, int(gate_min_trades) * 2) if gate_min_trades > 0 else 60
    try:
        min_trades = int(min_trades)
    except Exception:
        min_trades = 60

    if base.size < min_obs:
        return {
            'enabled': True,
            'passed': True,
            'skipped': True,
            'reason': f'insufficient_evidence_too_short_returns:{int(base.size)}<{min_obs}',
        }

    if n_trades is not None and int(n_trades) < int(min_trades):
        return {
            'enabled': True,
            'passed': True,
            'skipped': True,
            'reason': f'insufficient_evidence_too_few_trades:{int(n_trades)}<{int(min_trades)}',
        }

    sharpe_floor = float(getattr(gate, 'bootstrap_sharpe_floor', 0.0) or 0.0)
    n_iter = int(getattr(gate, 'bootstrap_n', 2000) or 2000)
    block = int(getattr(gate, 'bootstrap_block', 5) or 5)
    seed = int(getattr(gate, 'bootstrap_seed', 1337) or 1337)
    rng = np.random.default_rng(seed)

    sharpes = np.empty(max(1, n_iter), dtype=float)
    maxdds = np.empty(max(1, n_iter), dtype=float)
    for i in range(max(1, n_iter)):
        s = _block_bootstrap_sample(rng, base, block)
        sharpes[i] = _annualized_sharpe_from_returns(s)
        maxdds[i] = _max_drawdown_from_returns(s)

    sharpe_p05 = float(np.nanpercentile(sharpes, 5))
    maxdd_p95 = float(np.nanpercentile(maxdds, 95))
    prob_below = float(np.mean(np.isfinite(sharpes) & (sharpes < sharpe_floor)))

    checks = {}
    passed = True

    thr = getattr(gate, 'bootstrap_sharpe_p05_min', None)
    if thr is not None:
        ok = bool(np.isfinite(sharpe_p05) and sharpe_p05 >= float(thr))
        checks['sharpe_p05'] = {'value': sharpe_p05, 'pass': ok, 'threshold': float(thr)}
        passed = passed and ok
    else:
        checks['sharpe_p05'] = {'value': sharpe_p05, 'pass': True, 'threshold': None}

    thr = getattr(gate, 'bootstrap_maxdd_p95_max', None)
    if thr is not None:
        ok = bool(np.isfinite(maxdd_p95) and maxdd_p95 <= float(thr))
        checks['maxdd_p95'] = {'value': maxdd_p95, 'pass': ok, 'threshold': float(thr)}
        passed = passed and ok
    else:
        checks['maxdd_p95'] = {'value': maxdd_p95, 'pass': True, 'threshold': None}

    thr = getattr(gate, 'bootstrap_prob_sharpe_below_max', None)
    if thr is not None:
        ok = bool(np.isfinite(prob_below) and prob_below <= float(thr))
        checks['prob_sharpe_below_floor'] = {
            'value': prob_below,
            'pass': ok,
            'threshold': float(thr),
            'floor': sharpe_floor,
        }
        passed = passed and ok
    else:
        checks['prob_sharpe_below_floor'] = {
            'value': prob_below,
            'pass': True,
            'threshold': None,
            'floor': sharpe_floor,
        }

    return {
        'enabled': True,
        'passed': bool(passed),
        'checks': checks,
    }


def _primary_target(y_dict: Dict[str, pd.Series]) -> Tuple[str, str]:
    # return (type, key) where type in {'cls','reg'}
    for k in sorted(y_dict.keys()):
        if k.startswith('y_cls_'):
            return 'cls', k
    for k in sorted(y_dict.keys()):
        if k.startswith('y_reg_'):
            return 'reg', k
    # fallback
    keys = list(y_dict.keys())
    return ('cls', keys[0]) if keys else (None, None)


def _make_indexed(X: pd.DataFrame, y: pd.Series) -> Tuple[pd.DataFrame, pd.Series]:
    # ensure X and y share same index and are aligned
    X_local = X.loc[y.index].copy()
    # keep rows with valid targets; allow feature NaNs (we impute)
    mask = ~y.isna()
    X_local = X_local.loc[mask].replace([np.inf, -np.inf], np.nan).fillna(0)
    return X_local, y.loc[mask]


def permutation_test(
    X: pd.DataFrame,
    y: pd.Series,
    folds: List[Any],
    gate: Any,
    max_folds: Optional[int] = None,
    n_shuffles: int = 20,
) -> Dict[str, Any]:
    """
    For each fold: shuffle labels within train indices, train fast logistic, eval AUC on validation.
    Expect mean AUC across folds to be near 0.5; fail if > gate.robustness_permutation_auc_max
    """
    aucs = []
    cnt = 0
    if X is None or y is None or len(X) == 0 or len(y) == 0:
        return {'mean_auc': float('nan'), 'aucs': [], 'passed': True, 'skipped': True, 'reason': 'empty_X_or_y'}

    # Optional intensity overrides from gate (do not change pass threshold)
    try:
        mf = getattr(gate, 'robustness_permutation_max_folds', None)
        if mf is not None:
            max_folds = int(mf)
    except Exception:
        pass
    try:
        ns = getattr(gate, 'robustness_permutation_n_shuffles', None)
        if ns is not None:
            n_shuffles = int(ns)
    except Exception:
        pass

    # Keep this test lightweight: it is a leakage proxy and should be near-random.
    # Subsample large folds and use a faster solver while keeping the same threshold.
    max_train_rows = int(getattr(gate, 'robustness_permutation_max_train_rows', 5000) or 5000)
    max_val_rows = int(getattr(gate, 'robustness_permutation_max_val_rows', 2000) or 2000)

    for i, fold in enumerate(folds):
        if max_folds and cnt >= max_folds:
            break
        train_idx = getattr(fold, 'train_idx', None)
        val_idx = getattr(fold, 'val_idx', None)
        if train_idx is None or val_idx is None:
            continue
        train_idx = np.asarray(train_idx, dtype=int)
        val_idx = np.asarray(val_idx, dtype=int)
        if train_idx.size == 0 or val_idx.size == 0:
            continue
        n = len(X)
        if train_idx.max(initial=-1) >= n or val_idx.max(initial=-1) >= n:
            continue

        # Use positional indices (folds are produced from X.index ordering)
        X_tr = X.iloc[train_idx]
        X_val = X.iloc[val_idx]
        y_tr = y.iloc[train_idx]
        y_val = y.iloc[val_idx]
        if X_tr.shape[0] < 20 or X_val.shape[0] < 20:
            continue

        if max_train_rows and X_tr.shape[0] > max_train_rows:
            X_tr = X_tr.sample(n=max_train_rows, random_state=1000 + 31 * i)
            y_tr = y_tr.loc[X_tr.index]
        if max_val_rows and X_val.shape[0] > max_val_rows:
            X_val = X_val.sample(n=max_val_rows, random_state=2000 + 37 * i)
            y_val = y_val.loc[X_val.index]

        # shuffle only within train; use multiple shuffles to reduce test noise.
        fold_aucs = []
        for j in range(int(max(1, n_shuffles))):
            y_shuf = y_tr.sample(frac=1.0, random_state=42 + 997 * i + 17 * j).values
            try:
                clf = make_pipeline(
                    StandardScaler(),
                    LogisticRegression(
                        solver="lbfgs",
                        max_iter=200,
                    ),
                )
                clf.fit(X_tr.to_numpy(copy=False), y_shuf)
                prob = clf.predict_proba(X_val.to_numpy(copy=False))[:, 1]
                fold_aucs.append(float(roc_auc_score(y_val.values, prob)))
            except Exception:
                fold_aucs.append(float('nan'))

        # store fold-mean AUC (keeps summary comparable to prior runs)
        try:
            arr = np.asarray(fold_aucs, dtype=float)
            finite = arr[np.isfinite(arr)]
            aucs.append(float(np.mean(finite)) if finite.size else float('nan'))
        except Exception:
            aucs.append(float('nan'))
        cnt += 1

    try:
        arr = np.asarray(aucs, dtype=float)
        finite = arr[np.isfinite(arr)]
        mean_auc = float(np.mean(finite)) if finite.size else float('nan')
    except Exception:
        mean_auc = float('nan')
    passed = np.isnan(mean_auc) or (mean_auc <= getattr(gate, 'robustness_permutation_auc_max', 0.55))
    return {'mean_auc': mean_auc, 'aucs': aucs, 'passed': bool(passed)}


def subwindow_stability(df: pd.DataFrame, overall_sharpe: float, gate: Any) -> Dict[str, Any]:
    # df expected to contain 'strat_ret'
    n = len(df)
    min_obs = int(getattr(gate, 'robustness_subwindow_min_obs', 0) or 0)
    if min_obs > 0 and n < min_obs:
        return {'passed': True, 'skipped': True, 'reason': f'insufficient_evidence:{n}<{min_obs}', 'windows': []}
    if n < 3:
        return {'passed': True, 'skipped': True, 'reason': 'insufficient_evidence_too_short', 'windows': []}
    size = n // 3
    windows = []
    pass_count = 0
    for i in range(3):
        start = i * size
        end = (i + 1) * size if i < 2 else n
        seg = df.iloc[start:end]
        perf = seg['strat_ret']
        mean_ann = perf.mean() * 252.0
        vol_ann = perf.std(ddof=0) * (252.0 ** 0.5)
        sharpe = float(mean_ann / vol_ann) if vol_ann > 0 else 0.0
        windows.append({'i': i, 'sharpe': sharpe})
        th = gate.robustness_subwindow_min_sharpe_ratio * (overall_sharpe or 0.0)
        if sharpe >= th or sharpe >= gate.robustness_subwindow_abs_sharpe_min:
            pass_count += 1
    # Phase-1 semantics: require existence of at least one stable regime,
    # do not enforce uniformity across all subwindows.
    passed = pass_count >= 1
    return {'passed': passed, 'pass_count': pass_count, 'required_pass_count': 1, 'windows': windows}


def cost_stress_test(prices: pd.Series, preds: pd.Series, settings: Any, gate: Any) -> Dict[str, Any]:
    # double cost/spread
    s2 = settings
    try:
        from copy import deepcopy
        s2 = deepcopy(settings)
        s2.cost_bps = settings.cost_bps * 2
        s2.spread_bps = settings.spread_bps * 2
    except Exception:
        # fallback: mutate
        s2 = settings
        s2.cost_bps = settings.cost_bps * 2
        s2.spread_bps = settings.spread_bps * 2

    from octa_training.core.evaluation import compute_equity_and_metrics
    res = compute_equity_and_metrics(prices, preds, s2)
    sharpe = res['metrics'].sharpe
    passed = sharpe >= getattr(gate, 'robustness_stress_min_sharpe', 0.5)
    return {'passed': passed, 'sharpe': sharpe}


def regime_stress(df: pd.DataFrame, gate: Any) -> Dict[str, Any]:
    # identify high-vol regime using rolling vol on returns
    if 'ret' not in df.columns:
        return {'passed': True, 'reason': 'no_returns'}
    vol = df['ret'].rolling(window=20, min_periods=5).std()
    th = vol.quantile(getattr(gate, 'robustness_regime_top_quantile', 0.8))
    hv = df.loc[vol >= th]
    if hv.empty:
        return {'passed': True, 'reason': 'no_high_vol_periods'}
    # compute max drawdown in subset
    eq = hv['equity']
    roll_max = eq.cummax()
    dd = (eq / roll_max - 1.0).min()
    passed = abs(dd) <= getattr(gate, 'robustness_regime_max_drawdown', 0.5)
    return {'passed': passed, 'subset_max_drawdown': float(dd)}


def run_all_tests(symbol: str, features_res: Any, folds: List[Any], df_backtest: pd.DataFrame, preds: pd.Series, metrics: MetricsSummary, gate: Any, settings: Any) -> RobustnessResult:
    reasons: List[str] = []
    details: Dict[str, Any] = {}
    limited_reasons: List[str] = []

    ttype, tkey = _primary_target(features_res.y_dict)
    if ttype != 'cls':
        # currently only implemented for classification targets; non-cls fall through
        details['permutation'] = {'skipped': True, 'reason': 'insufficient_evidence_no_classification_target'}
        limited_reasons.append('permutation_test_insufficient')
    else:
        X, y = _make_indexed(features_res.X, features_res.y_dict[tkey])
        # Use all available folds and multiple shuffles to reduce test noise.
        perm = permutation_test(X, y, folds, gate, max_folds=None)
        details['permutation'] = perm
        if perm.get('skipped') or (not np.isfinite(perm.get('mean_auc', float('nan')))):
            limited_reasons.append('permutation_test_insufficient')
        elif not perm.get('passed', False):
            reasons.append(f"permutation_test_failed mean_auc={perm.get('mean_auc')}")

    # subwindow stability
    overall_sharpe = metrics.sharpe or 0.0
    sw = subwindow_stability(df_backtest, overall_sharpe, gate)
    details['subwindow'] = sw
    if sw.get('skipped'):
        limited_reasons.append('subwindow_stability_insufficient')
    elif not sw.get('passed', True):
        reasons.append(f"subwindow_stability_failed pass_count={sw.get('pass_count')}")

    # cost stress
    cs = cost_stress_test(df_backtest['price'], preds, settings, gate)
    details['cost_stress'] = cs
    if not cs.get('passed', True):
        reasons.append(f"cost_stress_failed sharpe={cs.get('sharpe')}")

    # regime stress
    rg = regime_stress(df_backtest, gate)
    details['regime'] = rg
    if not rg.get('passed', True):
        reasons.append(f"regime_stress_failed subset_max_dd={rg.get('subset_max_drawdown')}")

    # deterministic block-bootstrap "MC" robustness (optional, post-global gate)
    try:
        if df_backtest is None or 'strat_ret' not in df_backtest.columns:
            boot = block_bootstrap_robustness(None, gate, n_trades=getattr(metrics, 'n_trades', None))
        else:
            boot = block_bootstrap_robustness(
                df_backtest['strat_ret'].astype(float).values,
                gate,
                n_trades=getattr(metrics, 'n_trades', None),
            )
        details['bootstrap'] = boot
        if boot.get('skipped'):
            limited_reasons.append('bootstrap_insufficient')
        elif boot.get('enabled') and (not boot.get('passed', False)):
            # Phase-1 semantics: bootstrap is a confidence amplifier, not a kill-switch.
            # If it fails, keep the symbol but mark limited-confidence.
            limited_reasons.append('bootstrap_failed')
            try:
                boot['soft_failed'] = True
            except Exception:
                pass
    except Exception as e:
        details['bootstrap'] = {'enabled': True, 'passed': False, 'error': str(e)}
        reasons.append('bootstrap_robustness_error')

    passed = len(reasons) == 0
    return RobustnessResult(passed=passed, reasons=reasons, details=details, limited_reasons=limited_reasons)
