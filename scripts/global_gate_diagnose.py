#!/usr/bin/env python3
"""Global Gate Diagnose Sweep (Indices + FX).

Requirements from user:
- "Diagnose Modus": no automatic discard; log and continue per symbol.
- List per symbol which thresholds failed, including confidence-like scores and fail reasons.
- Output immediately after each filter stage and as each symbol finishes.

This script:
- Discovers parquets under provided directories.
- Applies only a name-based 1D filter by default (configurable).
- Enables cfg.gates.diagnose_mode so hard-kill switches do not abort runs.
- Runs train_evaluate_package(..., safe_mode=True, parquet_path=...).
- Emits NDJSON records as it goes (stdout + file) so users can tail results.

FX two-pass + hourly G1 recheck loop (audit-first example):

        /home/n-b/Octa/.venv/bin/python scripts/global_gate_diagnose.py --config configs/dev.yaml --fx-only \
            --fx-1d-risk-overlay-and-1h-alpha --fx-g1-recheck-after \
            --fx-g1-recheck-loop --fx-g1-recheck-interval-secs 3600 \
            --fx-dir raw/FX_parquet --out reports/fx_two_pass.jsonl

Loop outputs land in --fx-g1-recheck-loop-dir (default: reports/fx_g1_recheck):
    - <base>.<UTC_TS>.g1_recheck.jsonl + .summary.json per iteration
    - latest.g1_recheck.jsonl + latest.summary.json (copied each iteration)

Note: "confidence" here is a normalized margin-to-threshold score in [0,1]
computed inside octa_training.core.gates.gate_evaluate (GateResult.diagnostics).
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Keep stdout NDJSON clean (no tqdm progressbars).
os.environ.setdefault("OCTA_DISABLE_TQDM", "1")

from octa_training.core.asset_profiles import AssetProfileMismatchError
from octa_training.core.config import load_config
from octa_training.core.gates import finalize_gate_decision
from octa_training.core.io_parquet import load_parquet, sanitize_symbol
from octa_training.core.pipeline import (
    evaluate_fx_g0_risk_overlay_1d,
    evaluate_fx_g1_alpha_1h,
    train_evaluate_package,
)
from octa_training.core.state import StateRegistry


def _now_tag() -> str:
    return datetime.now(timezone.utc).isoformat().replace(":", "-")


def _json_default(o: Any):
    try:
        item = getattr(o, "item", None)
        if callable(item):
            return item()
    except Exception:
        pass
    try:
        from pathlib import Path as _Path

        if isinstance(o, _Path):
            return str(o)
    except Exception:
        pass
    try:
        import pandas as _pd

        if isinstance(o, (_pd.Timestamp, _pd.Timedelta)):
            return o.isoformat()
    except Exception:
        pass
    return str(o)


def _print_stage(stage: str, payload: Dict[str, Any]) -> None:
    msg = {"type": "stage", "stage": stage, **payload}
    print(json.dumps(msg, default=_json_default), flush=True)


def _write_text_lines(path: Path, lines: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = "\n".join(lines) + ("\n" if lines else "")
    path.write_text(payload, encoding="utf-8")


def _read_text_lines(path: Path) -> List[str]:
    try:
        if not path.exists():
            return []
        rows = [ln.strip() for ln in path.read_text(encoding="utf-8").splitlines()]
        return [r for r in rows if r]
    except Exception:
        return []


def _append_ndjson_event(out_fh, event: Dict[str, Any], quiet_symbols: bool) -> None:
    line = json.dumps(event, default=_json_default)
    if not quiet_symbols:
        print(line, flush=True)
    out_fh.write(line + "\n")
    out_fh.flush()


def _diag_failed_checks(gate: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return failed check diagnostics with computed margins where possible."""

    failed: List[Dict[str, Any]] = []
    diags = (gate or {}).get('diagnostics')
    if not isinstance(diags, list):
        return failed
    for d in diags:
        if not isinstance(d, dict):
            continue
        if d.get('evaluable') is False:
            continue
        if bool(d.get('passed')):
            continue
        op = d.get('op')
        if op not in {">=", "<="}:
            continue
        try:
            v = d.get('value')
            t = d.get('threshold')
            if v is None or t is None:
                margin = None
            else:
                v = float(v)
                t = float(t)
                margin = (v - t) if op == ">=" else (t - v)
        except Exception:
            margin = None
        failed.append(
            {
                'name': d.get('name'),
                'value': d.get('value'),
                'threshold': d.get('threshold'),
                'op': op,
                'margin': margin,
                'reason': d.get('reason'),
            }
        )
    return failed


def _is_near_miss(failed_checks: List[Dict[str, Any]]) -> bool:
    """Near-miss heuristic: exactly one failed check with small absolute margin."""

    if len(failed_checks) != 1:
        return False
    fc = failed_checks[0]
    margin = fc.get('margin')
    thr = fc.get('threshold')
    if margin is None or thr is None:
        return False
    try:
        m = float(margin)
        t = float(thr)
        # m < 0 indicates fail for our margin convention
        if m >= 0:
            return False
        denom = abs(t) if abs(t) > 1e-9 else 1.0
        return abs(m) <= 0.05 * denom
    except Exception:
        return False


def _fx_cost_assumptions(cfg: Any) -> Dict[str, Any]:
    try:
        broker = getattr(cfg, 'broker', None)
        signal = getattr(cfg, 'signal', None)
        return {
            'broker': {
                'cost_bps': getattr(broker, 'cost_bps', None) if broker is not None else None,
                'spread_bps': getattr(broker, 'spread_bps', None) if broker is not None else None,
                'stress_cost_multiplier': getattr(broker, 'stress_cost_multiplier', None) if broker is not None else None,
                'name': getattr(broker, 'name', None) if broker is not None else None,
            },
            'signal_fallback': {
                'cost_bps': getattr(signal, 'cost_bps', None) if signal is not None else None,
                'spread_bps': getattr(signal, 'spread_bps', None) if signal is not None else None,
                'stress_cost_multiplier': getattr(signal, 'stress_cost_multiplier', None) if signal is not None else None,
            },
        }
    except Exception:
        return {}


def _extract_fx_cost_guard(cfg: Any, gate: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Best-effort snapshot of FX cost-model guard inputs and whether it blocks.

    The pipeline has a safety guard that fails closed when both cost and spread
    are zero (and require_nonzero_for_fx is True). We expose the same inputs
    in NDJSON for auditability.
    """

    def _get_attr_chain(obj: Any, *names: str) -> Any:
        cur = obj
        for n in names:
            if cur is None:
                return None
            cur = getattr(cur, n, None)
        return cur

    require_nonzero = True
    try:
        require_nonzero = bool(_get_attr_chain(cfg, 'costs', 'require_nonzero_for_fx'))
    except Exception:
        require_nonzero = True

    broker = getattr(cfg, 'broker', None)
    signal = getattr(cfg, 'signal', None)
    cost_bps = getattr(broker, 'cost_bps', None) if broker is not None else None
    spread_bps = getattr(broker, 'spread_bps', None) if broker is not None else None
    stress_mult = getattr(broker, 'stress_cost_multiplier', None) if broker is not None else None
    try:
        if cost_bps is None and signal is not None:
            cost_bps = getattr(signal, 'cost_bps', None)
        if spread_bps is None and signal is not None:
            spread_bps = getattr(signal, 'spread_bps', None)
        if stress_mult is None and signal is not None:
            stress_mult = getattr(signal, 'stress_cost_multiplier', None)
    except Exception:
        pass

    # If gate diagnostics include cost fields, prefer those values (they represent
    # what the pipeline actually saw after any overlays).
    try:
        diags = (gate or {}).get('diagnostics')
        if isinstance(diags, list):
            by_name = {}
            for d in diags:
                if isinstance(d, dict) and d.get('name'):
                    by_name[str(d.get('name'))] = d
            if 'fx_cost_bps' in by_name:
                cost_bps = by_name['fx_cost_bps'].get('value')
            if 'fx_spread_bps' in by_name:
                spread_bps = by_name['fx_spread_bps'].get('value')
            if 'fx_stress_cost_multiplier' in by_name:
                stress_mult = by_name['fx_stress_cost_multiplier'].get('value')
    except Exception:
        pass

    try:
        cost_f = float(cost_bps or 0.0)
    except Exception:
        cost_f = 0.0
    try:
        spread_f = float(spread_bps or 0.0)
    except Exception:
        spread_f = 0.0

    blocked = bool(require_nonzero and cost_f <= 0.0 and spread_f <= 0.0)
    payload: Dict[str, Any] = {
        'require_nonzero_for_fx': bool(require_nonzero),
        'fx_cost_bps': cost_bps,
        'fx_spread_bps': spread_bps,
        'fx_stress_cost_multiplier': stress_mult,
        'blocked': bool(blocked),
        'blocked_reason': 'fx_cost_model_missing_or_zero' if blocked else None,
    }
    return payload


def _synthetic_failed_check(name: str, reason: str, value: Any = None) -> Dict[str, Any]:
    return {
        'name': name,
        'value': value,
        'threshold': None,
        'op': None,
        'margin': None,
        'reason': reason,
    }


def _debug_time_index(df: Any) -> Dict[str, Any]:
    """Extract debug info about a dataframe's index, fail-closed and exception-safe."""

    out: Dict[str, Any] = {}
    try:
        idx = getattr(df, 'index', None)
    except Exception:
        idx = None

    if idx is None:
        return out

    def _ts_to_iso(x: Any) -> Optional[str]:
        try:
            import pandas as pd

            if x is None:
                return None
            if isinstance(x, pd.Timestamp):
                if pd.isna(x):
                    return None
                return x.isoformat()
        except Exception:
            pass
        try:
            return x.isoformat()  # datetime-like
        except Exception:
            try:
                return str(x)
            except Exception:
                return None

    try:
        out['g1_index_dtype'] = str(getattr(idx, 'dtype', type(idx)))
    except Exception:
        out['g1_index_dtype'] = str(type(idx))

    try:
        # DatetimeIndex has tz; for others keep None.
        tz = getattr(idx, 'tz', None)
        out['g1_index_tz'] = None if tz is None else str(tz)
    except Exception:
        out['g1_index_tz'] = None

    try:
        out['g1_index_n'] = int(len(idx))
    except Exception:
        pass

    try:
        # Index.nunique exists for pandas Index
        nun = getattr(idx, 'nunique', None)
        out['g1_index_unique_n'] = int(nun()) if callable(nun) else None
    except Exception:
        pass

    try:
        out['g1_index_min'] = _ts_to_iso(idx.min())
    except Exception:
        pass

    try:
        out['g1_index_max'] = _ts_to_iso(idx.max())
    except Exception:
        pass

    # head/tail timestamps (first/last 3)
    try:
        head = list(idx[:3])
        out['g1_head_ts'] = [_ts_to_iso(x) for x in head]
    except Exception:
        pass
    try:
        tail = list(idx[-3:])
        out['g1_tail_ts'] = [_ts_to_iso(x) for x in tail]
    except Exception:
        pass

    # pandas.infer_freq (only meaningful for DatetimeIndex)
    try:
        import pandas as pd

        if isinstance(idx, pd.DatetimeIndex) and len(idx) >= 3:
            try:
                out['g1_inferred_freq'] = pd.infer_freq(idx)
            except Exception:
                out['g1_inferred_freq'] = None
    except Exception:
        pass

    return out


def _iter_parquets(root: Path) -> List[Path]:
    if not root.exists():
        return []
    if root.is_file() and root.suffix.lower() == ".parquet":
        return [root]
    return sorted([p for p in root.rglob("*.parquet") if p.is_file()])


def _filter_1d(paths: List[Path], dataset: str) -> Tuple[List[Path], Counter]:
    """Name-based 1D filter; logs what would be filtered but does not discard when disabled."""
    c = Counter()
    kept: List[Path] = []
    for p in paths:
        name = p.stem.upper()
        is_1d = False
        if dataset == "indices":
            is_1d = ("1DAY" in name) or name.endswith("_1D") or name.endswith("_DAILY")
        else:
            is_1d = name.endswith("_1D")
        if is_1d:
            kept.append(p)
            c["kept_1d"] += 1
        else:
            c["non_1d"] += 1
    return kept, c


def _fx_pair_path(p_1d: Path, tf: str) -> Path:
    """Map <SYMBOL>_1D.parquet -> <SYMBOL>_<tf>.parquet within same directory."""

    name = p_1d.name
    if name.upper().endswith('_1D.PARQUET'):
        return p_1d.with_name(name[:-len('_1D.parquet')] + f'_{tf}.parquet')
    # fallback: replace last token
    stem = p_1d.stem
    parts = stem.split('_')
    if parts:
        parts[-1] = tf
    return p_1d.with_name('_'.join(parts) + p_1d.suffix)


def _append_fx_stage_diagnostic(gate: Dict[str, Any], fx_stage_results: Dict[str, Any]) -> None:
    try:
        diags = gate.get('diagnostics')
        if not isinstance(diags, list):
            diags = []
        diags.append(
            {
                'name': 'fx_stage_results',
                'value': fx_stage_results,
                'threshold': None,
                'op': None,
                'passed': bool(fx_stage_results.get('g0_1d', {}).get('passed', False))
                and bool(fx_stage_results.get('g1_1h', {}).get('passed', False)),
                'evaluable': True,
                'confidence': 0.0,
                'reason': None,
            }
        )
        gate['diagnostics'] = diags
    except Exception:
        pass


def _fx_strict_1h_lookup(discovered: List[Path], p_1d: Path, cfg: Any = None) -> Optional[Path]:
    """Return the explicit 1H parquet path for a given 1D parquet.

    Hard rules for FX 2-stage sweep:
    - NEVER fall back from 1H -> 1D (or any other file).
    - If cfg.paths.fx_parquet_dir is configured, ONLY look there (no fallback to generic raw discovery).
    """

    try:
        expected_same_dir = _fx_pair_path(p_1d, '1H')
    except Exception:
        return None

    fx_dir = None
    try:
        fx_dir = Path(getattr(cfg, 'paths', None).fx_parquet_dir)
    except Exception:
        fx_dir = None

    if fx_dir is not None:
        expected = fx_dir / expected_same_dir.name
        return expected if expected.exists() else None

    # Back-compat fallback when cfg has no fx_parquet_dir: strict match within discovered.
    try:
        by_str = {str(p): p for p in (discovered or [])}
        return by_str.get(str(expected_same_dir))
    except Exception:
        return None


def _bar_spacing_seconds(df) -> Tuple[Optional[float], Optional[float]]:
    """Compute robust bar spacing stats from a UTC DatetimeIndex.

    Returns (median_seconds, p90_seconds) or (None, None) if not computable.
    """

    try:
        import pandas as pd

        idx = getattr(df, 'index', None)
        if idx is None or len(idx) < 2:
            return None, None
        if not isinstance(idx, pd.DatetimeIndex):
            return None, None
        d = idx.to_series().diff().dropna()
        if d.empty:
            return None, None
        secs = d.dt.total_seconds()
        secs = secs.replace([pd.NA], float('nan')).dropna()
        if secs.empty:
            return None, None
        med = float(secs.median())
        p90 = float(secs.quantile(0.9))
        return med, p90
    except Exception:
        return None, None


def _merge_fx_g0_tail_diagnostics(final_gate: Dict[str, Any], g0_gate: Dict[str, Any]) -> None:
    """Merge selected FX-G0 tail diagnostics into the final gate record.

    Rationale: when a symbol passes G0 and reaches G1, the final gate is G1,
    but we still want audit visibility into the G0 tail inputs/series.
    This is additive-only and schema-compatible (same diagnostics list shape).
    """

    try:
        if not isinstance(final_gate, dict) or not isinstance(g0_gate, dict):
            return
        g0_diags = g0_gate.get('diagnostics')
        if not isinstance(g0_diags, list) or not g0_diags:
            return
        keep_names = {
            'cvar_95_over_daily_vol',
            'fx_g0_tail_series',
            'fx_g0_cvar95_mkt',
            'fx_g0_vol_mkt',
            'fx_g0_tail_ratio_mkt',
        }
        keep = [d for d in g0_diags if isinstance(d, dict) and d.get('name') in keep_names]
        if not keep:
            return
        diags = final_gate.get('diagnostics')
        if not isinstance(diags, list):
            diags = []
        existing = {d.get('name') for d in diags if isinstance(d, dict)}
        for d in keep:
            nm = d.get('name')
            if nm not in existing:
                diags.append(d)
        final_gate['diagnostics'] = diags
    except Exception:
        return


def _fx_select_universe_1d(root: Path, limit: int = 0) -> Tuple[List[Path], List[Path]]:
    """Select FX universe deterministically from *_1D.parquet under root.

    Returns (selected_1d_paths, all_discovered_parquets).
    """

    all_paths = _iter_parquets(root)
    selected = [p for p in all_paths if p.name.upper().endswith('_1D.PARQUET')]
    if limit and limit > 0:
        selected = selected[: int(limit)]
    return selected, all_paths


def _run_fx_two_stage(
    root: Path,
    cfg: Any,
    state: StateRegistry,
    run_id: str,
    out_fh,
    limit: int,
    quiet_symbols: bool,
    append_quarantine: bool = False,
    quarantined_symbols: Optional[set[str]] = None,
    horizons_tag: Optional[List[int]] = None,
    emit_pass_candidates: bool = False,
    pass_candidates_out: Optional[set[str]] = None,
    near_miss_out: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """FX two-stage runner: G0 1D risk overlay -> (if pass) G1 1H alpha."""

    selected, all_paths = _fx_select_universe_1d(root, limit=limit)
    _print_stage(
        "fx:discover",
        {"root": str(root), "discovered": len(all_paths)},
    )
    _print_stage(
        "fx:fx_two_stage_select_1d",
        {"selected": len(selected), "note": "using *_1D.parquet as universe"},
    )
    if limit and limit > 0:
        _print_stage(
            "fx:limit",
            {"limit": int(limit), "selected": len(selected)},
        )

    stats = Counter()
    reasons_counter = Counter()
    status_counter = Counter()

    quarantine: Dict[str, Dict[str, Any]] = {}
    reports_dir = None
    try:
        reports_dir = Path(getattr(cfg, 'paths', None).reports_dir)
    except Exception:
        reports_dir = None

    for i, p1d in enumerate(selected, 1):
        base = p1d.stem
        if base.upper().endswith('_1D'):
            base = base[:-3]
        sym = sanitize_symbol(base)

        # -------- FX-G0 (1D risk overlay) --------
        rec_error: Optional[str] = None
        g0 = None
        g1 = None

        # G1 metadata for audit (added as top-level fields on the record)
        g1_attempted = False
        g1_status = None
        g1_reason = None
        g1_path = None
        g1_median_spacing = None
        g1_p90_spacing = None
        g1_index_debug: Dict[str, Any] = {}
        g1_index_debug: Dict[str, Any] = {}

        fx_stage_results: Dict[str, Any] = {
            'g0_1d': {'passed': False, 'reasons': []},
            'g1_1h': {'passed': False, 'reasons': []},
        }

        try:
            res0 = evaluate_fx_g0_risk_overlay_1d(sym, cfg, state, run_id=run_id, parquet_path=str(p1d), safe_mode=True)
            g0 = _gate_to_dict(getattr(res0, 'gate_result', None))
            rec_error = getattr(res0, 'error', None)
        except Exception as e:
            rec_error = f"exception_g0:{e}"
            g0 = None

        g0_final, g0_pass = finalize_gate_decision(g0, error=rec_error, hard_flags={'error': bool(rec_error)})
        g0d = _gate_to_dict(g0_final)
        g0_reasons = list((g0d or {}).get('reasons') or [])
        fx_stage_results['g0_1d'] = {'passed': bool(g0_pass), 'reasons': [f"fx_g0:{r}" for r in g0_reasons]}

        g0_failed_checks: List[Dict[str, Any]] = []
        try:
            g0_failed_checks = _diag_failed_checks(g0d if isinstance(g0d, dict) else {})
        except Exception:
            g0_failed_checks = []
        if rec_error:
            g0_failed_checks.append(_synthetic_failed_check('fx_g0_error', str(rec_error)))

        g1_failed_checks: List[Dict[str, Any]] = []

        final_gate: Dict[str, Any] | None = None
        final_error: Optional[str] = None

        g1_failed_checks: List[Dict[str, Any]] = []

        if not g0_pass:
            # Final fail is G0; prefix reasons.
            if g0d is None:
                g0d = {'passed': False, 'status': 'FAIL_DATA', 'reasons': []}
            g0d['reasons'] = [f"fx_g0:{r}" for r in (g0d.get('reasons') or [])]
            final_gate = g0d
            final_error = None
        else:
            # -------- FX-G1 (1H alpha) --------
            if quarantined_symbols and (sym in quarantined_symbols):
                # Pass-finder behavior: do not attempt G1 for symbols known to have bad 1H data.
                p1h_q = _fx_strict_1h_lookup(all_paths, p1d, cfg=cfg)
                g1_attempted = False
                g1_status = 'SKIP'
                g1_reason = 'quarantine_listed'
                g1_path = str(p1h_q) if p1h_q is not None else None
                final_error = None
                final_gate = {
                    'passed': False,
                    'status': 'FAIL_DATA',
                    'reasons': ["data_load_failed: fx_g1:quarantine_listed"],
                    'passed_checks': [],
                    'insufficient_evidence': [],
                    'robustness': None,
                    'diagnostics': [],
                }
                fx_stage_results['g1_1h'] = {'passed': False, 'reasons': ["fx_g1:quarantine_listed"]}
            else:
                p1h = _fx_strict_1h_lookup(all_paths, p1d, cfg=cfg)
                if p1h is None:
                    # Strict rule: if explicit 1H parquet is missing, SKIP G1 (do not attempt evaluation).
                    g1_attempted = False
                    g1_status = 'SKIP'
                    g1_reason = 'missing_1h_parquet'
                    g1_path = None
                    final_error = None
                    final_gate = {
                        'passed': False,
                        'status': 'FAIL_DATA',
                        'reasons': ["data_load_failed: fx_g1:missing_1h_parquet"],
                        'passed_checks': [],
                        'insufficient_evidence': [],
                        'robustness': None,
                        'diagnostics': [],
                    }
                    fx_stage_results['g1_1h'] = {'passed': False, 'reasons': ["fx_g1:missing_1h_parquet"]}
                else:
                    g1_path = str(p1h)
                    # Tier-1 data-truth gate for the sweep: ensure the 1H parquet is actually hourly-like.
                    try:
                        df_1h = load_parquet(Path(p1h))
                        try:
                            g1_index_debug = _debug_time_index(df_1h)
                        except Exception:
                            g1_index_debug = {}
                        med_s, p90_s = _bar_spacing_seconds(df_1h)
                        g1_median_spacing = med_s
                        g1_p90_spacing = p90_s
                        hourly_like = (med_s is not None and p90_s is not None and med_s <= 2 * 3600 and p90_s <= 4 * 3600)
                    except Exception as e:
                        hourly_like = False
                        g1_reason = f"spacing_check_error:{e}"
                        g1_status = 'SKIP'
                        final_error = None
                        final_gate = {
                            'passed': False,
                            'status': 'FAIL_DATA',
                            'reasons': [f"data_load_failed: fx_g1:spacing_check_error: {str(e).splitlines()[0]}"] if str(e) else ["data_load_failed: fx_g1:spacing_check_error"],
                            'passed_checks': [],
                            'insufficient_evidence': [],
                            'robustness': None,
                            'diagnostics': [],
                        }
                        fx_stage_results['g1_1h'] = {'passed': False, 'reasons': ["fx_g1:spacing_check_error"]}

                    if final_gate is None:
                        if not hourly_like:
                            g1_attempted = False
                            g1_status = 'SKIP'
                            g1_reason = 'non_hourly_1h_parquet'
                            final_error = None
                            final_gate = {
                                'passed': False,
                                'status': 'FAIL_DATA',
                                'reasons': [
                                    f"data_load_failed: fx_g1:non_hourly_1h_parquet: median_spacing={g1_median_spacing} p90_spacing={g1_p90_spacing} path={g1_path}"
                                ],
                                'passed_checks': [],
                                'insufficient_evidence': [],
                                'robustness': None,
                                'diagnostics': [],
                            }
                            fx_stage_results['g1_1h'] = {'passed': False, 'reasons': ["fx_g1:non_hourly_1h_parquet"]}

                            quarantine[sym] = {
                                'symbol': sym,
                                'reason': g1_reason,
                                'path': g1_path,
                                'median_spacing': g1_median_spacing,
                                'p90_spacing': g1_p90_spacing,
                                'run_id': run_id,
                                'timestamp_utc': datetime.now(timezone.utc).isoformat(),
                            }
                            _append_ndjson_event(
                                out_fh,
                                {
                                    'type': 'quarantine',
                                    'dataset': 'fx',
                                    'layer': 'g1',
                                    'symbol': sym,
                                    'reason': g1_reason,
                                    'path': g1_path,
                                    'median_spacing': g1_median_spacing,
                                    'p90_spacing': g1_p90_spacing,
                                    'run_id': run_id,
                                },
                                quiet_symbols,
                            )
                        else:
                            # Hourly-like: attempt real G1 evaluation.
                            g1_attempted = True
                            g1_status = 'RUN'
                            try:
                                res1 = evaluate_fx_g1_alpha_1h(sym, cfg, state, run_id=run_id, parquet_path=str(p1h), safe_mode=True)
                                g1 = _gate_to_dict(getattr(res1, 'gate_result', None))
                                final_error = getattr(res1, 'error', None)
                            except Exception as e:
                                final_error = f"exception_g1:{e}"
                                g1 = None

                            g1_final, g1_pass = finalize_gate_decision(g1, error=final_error, hard_flags={'error': bool(final_error)})
                            g1d = _gate_to_dict(g1_final)
                            g1_reasons = list((g1d or {}).get('reasons') or [])
                            fx_stage_results['g1_1h'] = {'passed': bool(g1_pass), 'reasons': [f"fx_g1:{r}" for r in g1_reasons]}

                            try:
                                g1_failed_checks = _diag_failed_checks(g1d if isinstance(g1d, dict) else {})
                            except Exception:
                                g1_failed_checks = []

                            if final_error:
                                g1_failed_checks.append(_synthetic_failed_check('fx_g1_error', str(final_error)))

                            # If deeper validation flags invalid 1H data, quarantine it for future runs.
                            try:
                                if any('invalid_1h_data' in str(r) for r in g1_reasons):
                                    quarantine[sym] = {
                                        'symbol': sym,
                                        'reason': 'invalid_1h_data',
                                        'path': g1_path,
                                        'median_spacing': g1_median_spacing,
                                        'p90_spacing': g1_p90_spacing,
                                        'run_id': run_id,
                                        'timestamp_utc': datetime.now(timezone.utc).isoformat(),
                                    }
                                    _append_ndjson_event(
                                        out_fh,
                                        {
                                            'type': 'quarantine',
                                            'dataset': 'fx',
                                            'layer': 'g1',
                                            'symbol': sym,
                                            'reason': 'invalid_1h_data',
                                            'path': g1_path,
                                            'median_spacing': g1_median_spacing,
                                            'p90_spacing': g1_p90_spacing,
                                            'run_id': run_id,
                                        },
                                        quiet_symbols,
                                    )
                            except Exception:
                                pass

                            # Prefix G1 reasons in final gate
                            if g1d is None:
                                g1d = {'passed': False, 'status': 'FAIL_DATA', 'reasons': []}
                            g1d['reasons'] = [f"fx_g1:{r}" for r in (g1d.get('reasons') or [])]
                            final_gate = g1d

            # Preserve key FX-G0 tail diagnostics in final record for auditability.
            try:
                if isinstance(final_gate, dict) and isinstance(g0d, dict):
                    _merge_fx_g0_tail_diagnostics(final_gate, g0d)
            except Exception:
                pass

        # Add additive fx stage diagnostics (non-breaking)
        if isinstance(final_gate, dict):
            _append_fx_stage_diagnostic(final_gate, fx_stage_results)

        rec: Dict[str, Any] = {
            'type': 'symbol',
            'dataset': 'fx',
            'pass_id': 'two_stage',
            'i': i,
            'n': len(selected),
            'symbol': sym,
            'parquet': str(p1d),
            'run_id': run_id,
            'passed': False,
            'error': final_error,
            'gate': final_gate,
            'g0_passed': bool(g0_pass),
            'g0_reasons': list(fx_stage_results.get('g0_1d', {}).get('reasons') or []),
            'g0_failed_checks': g0_failed_checks,
            'g1_failed_checks': g1_failed_checks,
            'cost_guard': _extract_fx_cost_guard(cfg, gate=(g0d if not bool(g0_pass) else (final_gate if isinstance(final_gate, dict) else None))),
            'g1_attempted': bool(g1_attempted),
            'g1_status': g1_status,
            'g1_reason': g1_reason,
            'g1_path': g1_path,
            'g1_median_spacing': g1_median_spacing,
            'g1_p90_spacing': g1_p90_spacing,
            'horizons': horizons_tag,
        }

        # Data-truth debug fields (only when df_1h was loaded successfully and we SKIP for non-hourly).
        try:
            if g1_status == 'SKIP' and g1_reason == 'non_hourly_1h_parquet' and isinstance(g1_index_debug, dict) and g1_index_debug:
                rec.update(g1_index_debug)
        except Exception:
            pass

        # Ensure SKIP reasons show up as explicit blockers in the failed_checks stream.
        try:
            if not bool(rec.get('passed')) and rec.get('g1_status') == 'SKIP' and rec.get('g1_reason'):
                rec['g1_failed_checks'] = list(rec.get('g1_failed_checks') or []) + [
                    _synthetic_failed_check('fx_g1_prereq', str(rec.get('g1_reason')))
                ]
        except Exception:
            pass

        rec = _finalize_record(rec)

        # Structural-fail lightweight diagnostic bundle (FX-only)
        try:
            g = rec.get('gate') or {}
            st = g.get('status') if isinstance(g, dict) else None
            if reports_dir is not None and st == 'FAIL_STRUCTURAL':
                stage = 'g0' if not bool(g0_pass) else 'g1'
                horizons_eff = horizons_tag
                if horizons_eff is None:
                    try:
                        horizons_eff = (getattr(cfg, 'features', {}) or {}).get('horizons')
                    except Exception:
                        horizons_eff = None
                diag_dir = reports_dir / 'fx_structural_fail_diag' / str(run_id)
                diag_dir.mkdir(parents=True, exist_ok=True)
                payload = {
                    'symbol': sym,
                    'run_id': run_id,
                    'timestamp_utc': datetime.now(timezone.utc).isoformat(),
                    'stage': stage,
                    'status': st,
                    'reasons': list((g.get('reasons') or []) if isinstance(g, dict) else []),
                    'failed_checks': _diag_failed_checks(g if isinstance(g, dict) else {}),
                    'cost_assumptions': _fx_cost_assumptions(cfg),
                    'horizons': horizons_eff,
                }
                (diag_dir / f"{sym}.json").write_text(json.dumps(payload, indent=2, default=_json_default) + "\n", encoding='utf-8')
        except Exception:
            pass

        # Pass-candidate event (opt-in)
        try:
            if emit_pass_candidates and bool(rec.get('passed')):
                if isinstance(pass_candidates_out, set):
                    pass_candidates_out.add(sym)
                _append_ndjson_event(
                    out_fh,
                    {
                        'type': 'pass_candidate',
                        'dataset': 'fx',
                        'symbol': sym,
                        'horizons': horizons_tag,
                        'gate': 'g0+g1',
                        'run_id': run_id,
                    },
                    quiet_symbols,
                )
        except Exception:
            pass

        # Near-miss capture (opt-in)
        try:
            if isinstance(near_miss_out, list):
                g = rec.get('gate') or {}
                st = (g.get('status') if isinstance(g, dict) else None) or None
                if st and str(st).startswith('FAIL'):
                    failed = _diag_failed_checks(g if isinstance(g, dict) else {})
                    if _is_near_miss(failed):
                        near_miss_out.append(
                            {
                                'symbol': sym,
                                'horizons': horizons_tag,
                                'status': st,
                                'failed_check': failed[0],
                                'reasons': list((g.get('reasons') or []) if isinstance(g, dict) else []),
                            }
                        )
        except Exception:
            pass

        # aggregate stats
        if rec.get('passed'):
            stats['pass'] += 1
        else:
            stats['fail'] += 1
        if rec.get('error'):
            stats['error'] += 1

        g = rec.get('gate') or {}
        st = g.get('status')
        if st:
            status_counter[str(st)] += 1
        rr = g.get('reasons') if isinstance(g, dict) else None
        if isinstance(rr, list):
            for r in rr:
                if r:
                    reasons_counter[str(r)] += 1

        line = json.dumps(rec, default=_json_default)
        if not quiet_symbols:
            print(line, flush=True)
        out_fh.write(line + "\n")
        out_fh.flush()

    # Deterministic quarantine registry files (overwrite by default; merge when append_quarantine)
    try:
        if reports_dir is not None and quarantine:
            txt_p = reports_dir / 'fx_g1_quarantine_symbols.txt'
            json_p = reports_dir / 'fx_g1_quarantine_symbols.json'

            existing: Dict[str, Dict[str, Any]] = {}
            if append_quarantine:
                try:
                    if json_p.exists():
                        existing_list = json.loads(json_p.read_text(encoding='utf-8'))
                        if isinstance(existing_list, list):
                            for item in existing_list:
                                if isinstance(item, dict) and item.get('symbol'):
                                    existing[str(item['symbol'])] = item
                except Exception:
                    existing = {}

            merged = {**existing, **quarantine}
            symbols_sorted = sorted(set(merged.keys()))
            _write_text_lines(txt_p, symbols_sorted)

            payload_list = [merged[s] for s in symbols_sorted]
            json_p.parent.mkdir(parents=True, exist_ok=True)
            json_p.write_text(json.dumps(payload_list, indent=2, default=_json_default) + "\n", encoding='utf-8')
    except Exception:
        pass

    selected_symbols: List[str] = []
    try:
        for p1d in selected:
            base = p1d.stem
            if base.upper().endswith('_1D'):
                base = base[:-3]
            selected_symbols.append(sanitize_symbol(base))
    except Exception:
        selected_symbols = []

    summary = {
        'dataset': 'fx',
        'pass_id': 'two_stage',
        'root': str(root),
        'discovered': len(all_paths),
        'selected': len(selected),
        'selected_symbols': selected_symbols,
        'selected_1d_paths': [str(p) for p in selected],
        'stats': dict(stats),
        'status_counts': dict(status_counter),
        'top_reasons': reasons_counter.most_common(25),
    }
    _print_stage('fx:done', summary)
    return summary


def _run_fx_g1_recheck(
    root: Path,
    cfg: Any,
    state: StateRegistry,
    run_id: str,
    out_fh,
    selected_1d_paths: List[Path],
    quiet_symbols: bool,
    append_quarantine: bool = False,
) -> Dict[str, Any]:
    """FX Pass 2: recheck ONLY G1 (1H) for the full Pass-1 universe.

    Strict: 1H resolution uses ONLY cfg.paths.fx_parquet_dir (no fallback).
    """

    selected = list(selected_1d_paths or [])
    _print_stage(
        'fx_g1_recheck:start',
        {
            'root': str(root),
            'selected': len(selected),
            'note': 'recheck G1 only for full Pass-1 universe',
        },
    )

    stats = Counter()
    reasons_counter = Counter()
    status_counter = Counter()

    quarantine: Dict[str, Dict[str, Any]] = {}
    reports_dir = None
    try:
        reports_dir = Path(getattr(cfg, 'paths', None).reports_dir)
    except Exception:
        reports_dir = None

    for i, p1d in enumerate(selected, 1):
        base = p1d.stem
        if base.upper().endswith('_1D'):
            base = base[:-3]
        sym = sanitize_symbol(base)

        g1_attempted = False
        g1_status = None
        g1_reason = None
        g1_path = None
        g1_median_spacing = None
        g1_p90_spacing = None

        final_gate: Dict[str, Any] | None = None
        final_error: Optional[str] = None

        g1_failed_checks: List[Dict[str, Any]] = []

        try:
            p1h = _fx_strict_1h_lookup([], p1d, cfg=cfg)
            if p1h is None:
                g1_attempted = False
                g1_status = 'SKIP'
                g1_reason = 'missing_1h_parquet'
                g1_path = None
                final_gate = {
                    'passed': False,
                    'status': 'FAIL_DATA',
                    'reasons': ['data_load_failed: fx_g1:missing_1h_parquet'],
                    'passed_checks': [],
                    'insufficient_evidence': [],
                    'robustness': None,
                    'diagnostics': [],
                }
            else:
                g1_path = str(p1h)
                # Tier-1 data-truth gate: hourly-like spacing.
                try:
                    df_1h = load_parquet(Path(p1h))
                    try:
                        g1_index_debug = _debug_time_index(df_1h)
                    except Exception:
                        g1_index_debug = {}
                    med_s, p90_s = _bar_spacing_seconds(df_1h)
                    g1_median_spacing = med_s
                    g1_p90_spacing = p90_s
                    hourly_like = (
                        med_s is not None
                        and p90_s is not None
                        and med_s <= 2 * 3600
                        and p90_s <= 4 * 3600
                    )
                except Exception as e:
                    hourly_like = False
                    g1_reason = f"spacing_check_error:{e}"
                    g1_status = 'SKIP'
                    final_gate = {
                        'passed': False,
                        'status': 'FAIL_DATA',
                        'reasons': [
                            f"data_load_failed: fx_g1:spacing_check_error: {str(e).splitlines()[0]}"
                        ]
                        if str(e)
                        else ['data_load_failed: fx_g1:spacing_check_error'],
                        'passed_checks': [],
                        'insufficient_evidence': [],
                        'robustness': None,
                        'diagnostics': [],
                    }

                if final_gate is None:
                    if not hourly_like:
                        g1_attempted = False
                        g1_status = 'SKIP'
                        g1_reason = 'non_hourly_1h_parquet'
                        final_gate = {
                            'passed': False,
                            'status': 'FAIL_DATA',
                            'reasons': [
                                f"data_load_failed: fx_g1:non_hourly_1h_parquet: median_spacing={g1_median_spacing} p90_spacing={g1_p90_spacing} path={g1_path}"
                            ],
                            'passed_checks': [],
                            'insufficient_evidence': [],
                            'robustness': None,
                            'diagnostics': [],
                        }

                        quarantine[sym] = {
                            'symbol': sym,
                            'reason': g1_reason,
                            'path': g1_path,
                            'median_spacing': g1_median_spacing,
                            'p90_spacing': g1_p90_spacing,
                            'run_id': run_id,
                            'timestamp_utc': datetime.now(timezone.utc).isoformat(),
                        }
                        _append_ndjson_event(
                            out_fh,
                            {
                                'type': 'quarantine',
                                'dataset': 'fx',
                                'layer': 'g1_recheck',
                                'symbol': sym,
                                'reason': g1_reason,
                                'path': g1_path,
                                'median_spacing': g1_median_spacing,
                                'p90_spacing': g1_p90_spacing,
                                'run_id': run_id,
                            },
                            quiet_symbols,
                        )
                    else:
                        g1_attempted = True
                        g1_status = 'RUN'
                        try:
                            res1 = evaluate_fx_g1_alpha_1h(sym, cfg, state, run_id=run_id, parquet_path=str(p1h), safe_mode=True)
                            g1 = _gate_to_dict(getattr(res1, 'gate_result', None))
                            final_error = getattr(res1, 'error', None)
                        except Exception as e:
                            final_error = f"exception_g1:{e}"
                            g1 = None

                        g1_final, _g1_pass = finalize_gate_decision(g1, error=final_error, hard_flags={'error': bool(final_error)})
                        final_gate = _gate_to_dict(g1_final)

                        # Prefix G1 reasons (consistent with FX two-stage output)
                        if isinstance(final_gate, dict):
                            final_gate['reasons'] = [f"fx_g1:{r}" for r in (final_gate.get('reasons') or [])]

                        try:
                            g1_failed_checks = _diag_failed_checks(final_gate if isinstance(final_gate, dict) else {})
                        except Exception:
                            g1_failed_checks = []
                        if final_error:
                            g1_failed_checks.append(_synthetic_failed_check('fx_g1_error', str(final_error)))
        except Exception as e:
            g1_attempted = False
            g1_status = 'SKIP'
            g1_reason = f"g1_recheck_error:{e}"
            final_gate = {
                'passed': False,
                'status': 'FAIL_DATA',
                'reasons': [f"data_load_failed: fx_g1:g1_recheck_error: {str(e).splitlines()[0]}"],
                'passed_checks': [],
                'insufficient_evidence': [],
                'robustness': None,
                'diagnostics': [],
            }

        # Ensure SKIP reasons show up as explicit blockers in the failed_checks stream.
        try:
            if g1_status == 'SKIP' and g1_reason:
                g1_failed_checks = list(g1_failed_checks or []) + [_synthetic_failed_check('fx_g1_prereq', str(g1_reason))]
        except Exception:
            pass

        rec: Dict[str, Any] = {
            'type': 'symbol',
            'dataset': 'fx',
            'pass_id': 'g1_recheck',
            'i': i,
            'n': len(selected),
            'symbol': sym,
            'parquet': str(p1d),
            'run_id': run_id,
            'passed': False,
            'error': final_error,
            'gate': final_gate,
            'g0_passed': None,
            'g0_reasons': [],
            'g0_failed_checks': [],
            'g1_failed_checks': g1_failed_checks,
            'cost_guard': _extract_fx_cost_guard(cfg, gate=(final_gate if isinstance(final_gate, dict) else None)),
            'g1_attempted': bool(g1_attempted),
            'g1_status': g1_status,
            'g1_reason': g1_reason,
            'g1_path': g1_path,
            'g1_median_spacing': g1_median_spacing,
            'g1_p90_spacing': g1_p90_spacing,
        }

        # Data-truth debug fields (only when df_1h was loaded successfully and we SKIP for non-hourly).
        try:
            if g1_status == 'SKIP' and g1_reason == 'non_hourly_1h_parquet' and isinstance(g1_index_debug, dict) and g1_index_debug:
                rec.update(g1_index_debug)
        except Exception:
            pass

        rec = _finalize_record(rec)

        if rec.get('passed'):
            stats['pass'] += 1
        else:
            stats['fail'] += 1
        if rec.get('error'):
            stats['error'] += 1

        g = rec.get('gate') or {}
        st = g.get('status')
        if st:
            status_counter[str(st)] += 1
        rr = g.get('reasons') if isinstance(g, dict) else None
        if isinstance(rr, list):
            for r in rr:
                if r:
                    reasons_counter[str(r)] += 1

        line = json.dumps(rec, default=_json_default)
        if not quiet_symbols:
            print(line, flush=True)
        out_fh.write(line + "\n")
        out_fh.flush()

    # Deterministic quarantine registry update (optional)
    try:
        if reports_dir is not None and quarantine:
            txt_p = reports_dir / 'fx_g1_quarantine_symbols.txt'
            json_p = reports_dir / 'fx_g1_quarantine_symbols.json'

            existing: Dict[str, Dict[str, Any]] = {}
            if append_quarantine:
                try:
                    if json_p.exists():
                        existing_list = json.loads(json_p.read_text(encoding='utf-8'))
                        if isinstance(existing_list, list):
                            for item in existing_list:
                                if isinstance(item, dict) and item.get('symbol'):
                                    existing[str(item['symbol'])] = item
                except Exception:
                    existing = {}

            merged = {**existing, **quarantine}
            symbols_sorted = sorted(set(merged.keys()))
            _write_text_lines(txt_p, symbols_sorted)

            payload_list = [merged[s] for s in symbols_sorted]
            json_p.parent.mkdir(parents=True, exist_ok=True)
            json_p.write_text(json.dumps(payload_list, indent=2, default=_json_default) + "\n", encoding='utf-8')
    except Exception:
        pass

    selected_symbols: List[str] = []
    try:
        for p1d in selected:
            base = p1d.stem
            if base.upper().endswith('_1D'):
                base = base[:-3]
            selected_symbols.append(sanitize_symbol(base))
    except Exception:
        selected_symbols = []

    summary = {
        'dataset': 'fx',
        'pass_id': 'g1_recheck',
        'root': str(root),
        'selected': len(selected),
        'selected_symbols': selected_symbols,
        'selected_1d_paths': [str(p) for p in selected],
        'stats': dict(stats),
        'status_counts': dict(status_counter),
        'top_reasons': reasons_counter.most_common(25),
    }
    _print_stage('fx_g1_recheck:done', summary)
    return summary


def _load_fx_pass1_universe_from_ndjson(pass1_out_path: Path) -> List[Path]:
    """Derive Pass-1 selected FX 1D universe from the Pass-1 NDJSON output.

    Hard rule: fail-closed if file is missing or contains no FX two-stage symbol records.
    """
    pass1_out_path = Path(pass1_out_path)
    if not pass1_out_path.exists():
        raise FileNotFoundError(f"Pass-1 NDJSON not found: {pass1_out_path}")

    selected: List[Path] = []
    with pass1_out_path.open('r', encoding='utf-8') as fh:
        for ln in fh:
            ln = (ln or '').strip()
            if not ln:
                continue
            try:
                obj = json.loads(ln)
            except Exception:
                continue
            if not isinstance(obj, dict):
                continue
            if obj.get('type') != 'symbol':
                continue
            if str(obj.get('dataset')) != 'fx':
                continue
            if str(obj.get('pass_id')) != 'two_stage':
                continue
            p = obj.get('parquet')
            if isinstance(p, str) and p:
                selected.append(Path(p))

    uniq = sorted({str(p): p for p in selected}.values(), key=lambda p: str(p))
    if not uniq:
        raise RuntimeError(f"Pass-1 NDJSON has no FX two_stage universe records: {pass1_out_path}")
    return uniq


def _write_latest_copy(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    try:
        shutil.copyfile(str(src), str(dst))
    except Exception:
        pass


def _run_fx_g1_recheck_loop(
    *,
    fx_root: Path,
    cfg: Any,
    state: StateRegistry,
    run_id: str,
    pass1_out_path: Path,
    loop_dir: Path,
    interval_secs: int = 3600,
    max_iters: int = 0,
    quiet_symbols: bool = True,
    append_quarantine: bool = False,
    refresh_universe: bool = False,
) -> None:
    """Run an hourly (or custom interval) G1-only recheck loop.

    Hard rules:
    - Universe comes ONLY from Pass-1 NDJSON (fail-closed if missing/empty).
    - Always calls the existing _run_fx_g1_recheck codepath.
    """
    loop_dir = Path(loop_dir)
    loop_dir.mkdir(parents=True, exist_ok=True)

    base = Path(pass1_out_path).stem
    selected_1d_paths = _load_fx_pass1_universe_from_ndjson(Path(pass1_out_path))

    it = 0
    while True:
        if max_iters and it >= int(max_iters):
            break
        it += 1

        # Spec: sleep, then recheck.
        try:
            time.sleep(max(0, int(interval_secs)))
        except Exception:
            pass

        if refresh_universe:
            selected_1d_paths = _load_fx_pass1_universe_from_ndjson(Path(pass1_out_path))

        created_utc = datetime.now(timezone.utc).isoformat()
        ts = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        out_path = loop_dir / f"{base}.{ts}.g1_recheck.jsonl"
        summary_path = out_path.with_suffix('.summary.json')
        latest_jsonl = loop_dir / 'latest.g1_recheck.jsonl'
        latest_summary = loop_dir / 'latest.summary.json'

        _print_stage(
            'g1_recheck_loop:tick',
            {
                'run_id': run_id,
                'created_utc': created_utc,
                'iteration': it,
                'pass1_out': str(pass1_out_path),
                'selected': len(selected_1d_paths),
                'out': str(out_path),
                'interval_secs': int(interval_secs),
                'max_iters': int(max_iters),
                'refresh_universe': bool(refresh_universe),
            },
        )

        summary: Dict[str, Any] = {}
        err: Optional[str] = None
        try:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with out_path.open('w', encoding='utf-8') as out_fh:
                _append_ndjson_event(
                    out_fh,
                    {
                        'type': 'iteration',
                        'dataset': 'fx',
                        'pass_id': 'g1_recheck',
                        'run_id': run_id,
                        'created_utc': created_utc,
                        'iteration': it,
                    },
                    quiet_symbols=True,
                )
                summary = _run_fx_g1_recheck(
                    fx_root,
                    cfg,
                    state,
                    run_id,
                    out_fh,
                    selected_1d_paths=selected_1d_paths,
                    quiet_symbols=quiet_symbols,
                    append_quarantine=append_quarantine,
                )
        except Exception as e:
            err = str(e)
            summary = {
                'dataset': 'fx',
                'pass_id': 'g1_recheck',
                'root': str(fx_root),
                'selected': len(selected_1d_paths),
                'error': f"loop_iteration_error:{e}",
            }

            # Ensure the output NDJSON is not empty/ambiguous even on iteration-level errors.
            try:
                out_path.parent.mkdir(parents=True, exist_ok=True)
                with out_path.open('a', encoding='utf-8') as out_fh:
                    _append_ndjson_event(
                        out_fh,
                        {
                            'type': 'stage',
                            'stage': 'g1_recheck_loop:error',
                            'dataset': 'fx',
                            'pass_id': 'g1_recheck',
                            'run_id': run_id,
                            'created_utc': created_utc,
                            'iteration': it,
                            'error': f"loop_iteration_error:{e}",
                        },
                        quiet_symbols=True,
                    )
            except Exception:
                pass

        # Persist summary and update latest copies.
        try:
            if isinstance(summary, dict):
                summary['run_id'] = run_id
                summary['created_utc'] = created_utc
                summary['iteration'] = it
            summary_path.write_text(json.dumps(summary, indent=2, default=_json_default))
            _write_latest_copy(out_path, latest_jsonl)
            _write_latest_copy(summary_path, latest_summary)
        except Exception:
            pass

        _print_stage(
            'g1_recheck_loop:done',
            {
                'run_id': run_id,
                'created_utc': created_utc,
                'iteration': it,
                'out': str(out_path),
                'summary': str(summary_path),
                'error': err,
            },
        )


def _run_fx_pass_finder(
    cfg: Any,
    state: StateRegistry,
    run_id: str,
    fx_root: Path,
    quiet_symbols: bool,
    sweep_fast: bool,
) -> Dict[str, Any]:
    """Run FX-only two-stage across multiple horizon sets to discover any PASS candidates."""

    tag = _now_tag()
    reports_dir = Path(getattr(getattr(cfg, 'paths', None), 'reports_dir', 'reports'))
    quarantine_txt = reports_dir / 'fx_g1_quarantine_symbols.txt'
    quarantined = set(_read_text_lines(quarantine_txt))

    horizon_sets: List[List[int]] = [[1], [3], [5], [1, 3], [1, 3, 5]]

    per_set: List[Dict[str, Any]] = []
    pass_candidates: set[str] = set()
    near_misses: List[Dict[str, Any]] = []

    for hs in horizon_sets:
        out_path = reports_dir / f"global_gate_diagnose_fx_pass_finder_{tag}_horizons={'-'.join(map(str, hs))}.jsonl"
        # Override horizons for this run only
        try:
            if not hasattr(cfg, 'features') or not isinstance(cfg.features, dict):
                cfg.features = {}
            cfg.features['horizons'] = list(hs)
        except Exception:
            pass

        with out_path.open('w', encoding='utf-8') as out_fh:
            s = _run_fx_two_stage(
                fx_root,
                cfg,
                state,
                run_id,
                out_fh,
                limit=0,
                quiet_symbols=quiet_symbols,
                append_quarantine=False,
                quarantined_symbols=quarantined,
                horizons_tag=list(hs),
                emit_pass_candidates=True,
                pass_candidates_out=pass_candidates,
                near_miss_out=near_misses,
            )
        s['out'] = str(out_path)
        s['horizons'] = list(hs)
        per_set.append(s)

    # Write pass-candidate file
    try:
        cand_p = reports_dir / 'fx_pass_candidates.txt'
        _write_text_lines(cand_p, sorted(pass_candidates))
    except Exception:
        pass

    summary = {
        'type': 'pass_finder_summary',
        'run_id': run_id,
        'created_utc': datetime.now(timezone.utc).isoformat(),
        'fx_root': str(fx_root),
        'quarantine_file': str(quarantine_txt),
        'horizon_sets': horizon_sets,
        'per_horizon_set': [
            {
                'horizons': s.get('horizons'),
                'status_counts': s.get('status_counts'),
                'top_reasons': s.get('top_reasons'),
                'out': s.get('out'),
            }
            for s in per_set
        ],
        'pass_candidates': sorted(pass_candidates),
        'near_misses': near_misses[:200],
    }

    out_summary = reports_dir / f"global_gate_diagnose_fx_pass_finder_{tag}.summary.json"
    out_summary.write_text(json.dumps(summary, indent=2, default=_json_default) + "\n", encoding='utf-8')
    return {'dataset': 'fx_pass_finder', 'summary_path': str(out_summary), 'pass_candidates': len(pass_candidates)}


def _gate_to_dict(gate: Any) -> Optional[Dict[str, Any]]:
    if gate is None:
        return None
    try:
        if hasattr(gate, "model_dump"):
            return gate.model_dump()
        if hasattr(gate, "dict"):
            return gate.dict()
    except Exception:
        pass
    try:
        return {
            "passed": getattr(gate, "passed", None),
            "status": getattr(gate, "status", None),
            "gate_version": getattr(gate, "gate_version", None),
            "reasons": getattr(gate, "reasons", None),
            "passed_checks": getattr(gate, "passed_checks", None),
            "insufficient_evidence": getattr(gate, "insufficient_evidence", None),
            "robustness": getattr(gate, "robustness", None),
            "diagnostics": getattr(gate, "diagnostics", None),
        }
    except Exception:
        return None


def _finalize_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    """Fail-closed normalization for a NDJSON symbol record.

    Enforces invariants:
    - PASS_* => rec.passed True and gate.passed True
    - any error / data_load_failed / missing_walk_forward => FAIL_* and passed False
    """

    g_obj, passed = finalize_gate_decision(rec.get("gate"), error=rec.get("error"), hard_flags={"error": bool(rec.get("error"))})
    rec["gate"] = _gate_to_dict(g_obj)
    rec["passed"] = bool(passed)

    # FX audit invariant: if a record fails, ensure we have at least one explicit blocker.
    # Blockers are either the FX cost guard, or a non-empty failed_checks list.
    try:
        if str(rec.get('dataset') or '').lower() == 'fx' and not bool(rec.get('passed')):
            cost_guard = rec.get('cost_guard') if isinstance(rec.get('cost_guard'), dict) else {}
            blocked = bool(cost_guard.get('blocked'))
            g0_fc = rec.get('g0_failed_checks') if isinstance(rec.get('g0_failed_checks'), list) else []
            g1_fc = rec.get('g1_failed_checks') if isinstance(rec.get('g1_failed_checks'), list) else []
            if not blocked and (not g0_fc) and (not g1_fc):
                g = rec.get('gate')
                if not isinstance(g, dict):
                    g = {'passed': False, 'status': 'FAIL_STRUCTURAL', 'reasons': []}
                rr = g.get('reasons')
                if not isinstance(rr, list):
                    rr = []
                rr.append('diagnose_invariant_violation:no_explicit_blocker')
                g['reasons'] = rr
                rec['gate'] = g
                # Also attach a synthetic failed check so downstream aggregations are consistent.
                rec['g1_failed_checks'] = list(g1_fc) + [_synthetic_failed_check('diagnose_no_blocker', 'no_explicit_blocker_detected')]
    except Exception:
        pass
    return rec


def _rebuild_datasets_from_jsonl(jsonl_path: Path) -> List[Dict[str, Any]]:
    """Deterministically rebuild datasets[] from a jsonl file (fallback)."""

    from collections import Counter

    by_ds: Dict[str, Dict[str, Any]] = {}
    with jsonl_path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
            except Exception:
                continue
            if rec.get("type") != "symbol":
                continue
            ds = rec.get("dataset") or "unknown"
            d = by_ds.setdefault(
                ds,
                {
                    "dataset": ds,
                    "discovered": None,
                    "selected": 0,
                    "stats": Counter(),
                    "status_counts": Counter(),
                    "reasons": Counter(),
                },
            )
            d["selected"] += 1
            g = rec.get("gate") or {}
            status = g.get("status") or ("PASS_FULL" if rec.get("passed") else "FAIL_STRUCTURAL")
            d["status_counts"][str(status)] += 1
            if rec.get("passed"):
                d["stats"]["pass"] += 1
            else:
                d["stats"]["fail"] += 1
            rr = g.get("reasons")
            if isinstance(rr, list):
                for r in rr:
                    if r:
                        d["reasons"][str(r)] += 1

    datasets: List[Dict[str, Any]] = []
    for ds in sorted(by_ds.keys()):
        d = by_ds[ds]
        datasets.append(
            {
                "dataset": d["dataset"],
                "discovered": d.get("discovered"),
                "selected": int(d.get("selected") or 0),
                "stats": dict(d["stats"]),
                "status_counts": dict(d["status_counts"]),
                "top_reasons": d["reasons"].most_common(25),
            }
        )
    return datasets


def _run_dataset(
    dataset: str,
    root: Path,
    cfg: Any,
    state: StateRegistry,
    run_id: str,
    out_fh,
    only_1d: bool,
    limit: int,
    quiet_symbols: bool,
) -> Dict[str, Any]:
    all_paths = _iter_parquets(root)
    _print_stage(
        f"{dataset}:discover",
        {"root": str(root), "discovered": len(all_paths)},
    )

    selected = all_paths
    filt_counts = Counter()
    if only_1d:
        selected, filt_counts = _filter_1d(all_paths, dataset)
        _print_stage(
            f"{dataset}:filter_1d",
            {"selected": len(selected), "filtered": dict(filt_counts)},
        )

    if limit and limit > 0:
        selected = selected[: int(limit)]
        _print_stage(
            f"{dataset}:limit",
            {"limit": int(limit), "selected": len(selected)},
        )

    stats = Counter()
    reasons_counter = Counter()
    status_counter = Counter()

    for i, p in enumerate(selected, 1):
        sym = sanitize_symbol(p.stem)
        rec: Dict[str, Any] = {
            "type": "symbol",
            "dataset": dataset,
            "i": i,
            "n": len(selected),
            "symbol": sym,
            "parquet": str(p),
            "run_id": run_id,
            "passed": False,
            "error": None,
            "gate": None,
            "asset_profile": None,
            "asset_profile_hash": None,
            "applied_thresholds": None,
        }
        try:
            res = train_evaluate_package(
                sym,
                cfg,
                state,
                run_id=run_id,
                safe_mode=True,
                parquet_path=str(p),
                dataset=str(dataset),
            )
            rec["error"] = getattr(res, "error", None)
            rec["gate"] = _gate_to_dict(getattr(res, "gate_result", None))
        except AssetProfileMismatchError as e:
            # Fail-closed audit record for profile mismatch (stocks only).
            # Attach asset_profile metadata and a structural FAIL gate so downstream
            # aggregations see the blocker explicitly.
            prof = getattr(e, 'profile_name', None)
            prof_hash = getattr(e, 'profile_hash', None)
            gv = getattr(e, 'gate_version', None)

            # Stage header explaining the abort.
            try:
                _append_ndjson_event(
                    out_fh,
                    {
                        'type': 'stage',
                        'stage': 'profile_mismatch',
                        'dataset': dataset,
                        'pass_id': 'g1_recheck' if str(dataset).lower() == 'stocks' else None,
                        'run_id': run_id,
                        'asset_profile': prof,
                        'asset_profile_hash': prof_hash,
                        'gate_version': gv,
                        'error': str(e),
                    },
                    quiet_symbols,
                )
            except Exception:
                pass

            rec['asset_profile'] = prof
            rec['asset_profile_hash'] = prof_hash
            rec['applied_thresholds'] = None
            rec['gate'] = {
                'passed': False,
                'status': 'FAIL_STRUCTURAL',
                'gate_version': gv,
                'reasons': [str(e)],
                'passed_checks': [],
                'insufficient_evidence': [],
                'robustness': None,
                'diagnostics': [
                    {'name': 'asset_profile', 'value': prof, 'threshold': None, 'op': None, 'passed': True, 'evaluable': True, 'confidence': 0.0, 'reason': None},
                    {'name': 'asset_profile_hash', 'value': prof_hash, 'threshold': None, 'op': None, 'passed': True, 'evaluable': True, 'confidence': 0.0, 'reason': None},
                ],
            }
            rec['error'] = f"STOCKS_PROFILE_MISMATCH:{str(e)}"
        except Exception as e:
            rec["error"] = f"exception:{e}"

        # Pull profile metadata from gate diagnostics if present.
        try:
            g = rec.get('gate')
            diags = None
            if isinstance(g, dict):
                diags = g.get('diagnostics')
            if isinstance(diags, list):
                for d in diags:
                    if not isinstance(d, dict):
                        continue
                    if d.get('name') == 'asset_profile' and rec.get('asset_profile') is None:
                        rec['asset_profile'] = d.get('value')
                    if d.get('name') == 'asset_profile_hash' and rec.get('asset_profile_hash') is None:
                        rec['asset_profile_hash'] = d.get('value')
                    if d.get('name') == 'applied_thresholds' and rec.get('applied_thresholds') is None:
                        rec['applied_thresholds'] = d.get('value')
        except Exception:
            pass

        rec = _finalize_record(rec)

        # aggregate stats (do not discard)
        if rec.get("passed"):
            stats["pass"] += 1
        else:
            stats["fail"] += 1
        if rec.get("error"):
            stats["error"] += 1

        g = rec.get("gate") or {}
        st = g.get("status")
        if st:
            status_counter[str(st)] += 1

        rr = g.get("reasons") if isinstance(g, dict) else None
        if isinstance(rr, list):
            for r in rr:
                if r:
                    reasons_counter[str(r)] += 1

        line = json.dumps(rec, default=_json_default)
        if not quiet_symbols:
            print(line, flush=True)
        out_fh.write(line + "\n")
        out_fh.flush()

    summary = {
        "dataset": dataset,
        "root": str(root),
        "discovered": len(all_paths),
        "selected": len(selected),
        "stats": dict(stats),
        "status_counts": dict(status_counter),
        "top_reasons": reasons_counter.most_common(25),
    }
    _print_stage(f"{dataset}:done", summary)
    return summary


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="configs/dev.yaml")
    ap.add_argument("--indices-dir", default="raw/Indices_parquet")
    ap.add_argument("--stocks-dir", default="raw/Stock_parquet")
    ap.add_argument("--fx-dir", default="raw/FX_parquet")
    ap.add_argument(
        "--stocks",
        action="store_true",
        help="Run stocks dataset (equities) from --stocks-dir in addition to indices (unless --stocks-only).",
    )
    ap.add_argument("--stocks-only", action="store_true", help="Run only stocks dataset (skip indices + FX)")
    ap.add_argument("--fx-only", action="store_true", help="Run only FX dataset (skip indices)")
    ap.add_argument(
        "--fx-1d-risk-overlay-and-1h-alpha",
        action="store_true",
        help="FX only: run two-stage gating (G0 1D risk overlay, then G1 1H alpha)",
    )
    ap.add_argument(
        "--fx-g1-recheck-after",
        action="store_true",
        help="FX only: after two-stage (G0->G1), re-run G1-only for the full Pass-1 selected universe",
    )
    ap.add_argument(
        "--fx-g1-recheck-out",
        default="",
        help="FX only: output NDJSON path for G1-only recheck (default: <out>.g1_recheck.jsonl)",
    )
    ap.add_argument(
        "--fx-g1-recheck-loop",
        action="store_true",
        help="FX only: after initial two-pass run, keep re-running G1-only at a fixed interval using the Pass-1 universe",
    )
    ap.add_argument(
        "--fx-g1-recheck-interval-secs",
        type=int,
        default=3600,
        help="FX only: sleep interval between G1-only recheck iterations (default: 3600)",
    )
    ap.add_argument(
        "--fx-g1-recheck-max-iters",
        type=int,
        default=0,
        help="FX only: stop after N iterations (0 => infinite)",
    )
    ap.add_argument(
        "--fx-g1-recheck-loop-dir",
        default="reports/fx_g1_recheck",
        help="FX only: directory for timestamped G1-only recheck outputs (default: reports/fx_g1_recheck)",
    )
    ap.add_argument(
        "--fx-g1-recheck-refresh-universe",
        action="store_true",
        help="FX only: refresh the Pass-1 universe snapshot each loop iteration by re-reading Pass-1 NDJSON (no discover-all)",
    )
    ap.add_argument(
        "--fx-pass-finder",
        action="store_true",
        help="FX only: run two-stage across multiple horizon sets and emit PASS candidates (does not weaken gates; opt-in).",
    )
    ap.add_argument(
        "--fx-dump-1h-index",
        default="",
        help="FX only: dump time-index diagnostics for raw/FX_parquet/<SYMBOL>_1H.parquet (load+inspect only).",
    )
    ap.add_argument("--only-1d", action="store_true", help="Only run name-matched 1D parquets")
    ap.add_argument("--all-timeframes", action="store_true", help="Override --only-1d and run all parquets")
    ap.add_argument("--limit", type=int, default=0, help="If >0, only run first N per dataset")
    ap.add_argument("--out", default="", help="Output NDJSON path (default: reports/global_gate_diagnose_<tag>.jsonl)")
    ap.add_argument(
        "--sweep-fast",
        action="store_true",
        help="Speed-focused overrides for large sweeps (keeps HF gates; disables tuning, drops catboost, reduces horizons/rounds)",
    )
    ap.add_argument(
        "--horizons",
        default="",
        help="Override cfg.features.horizons for this run (comma-separated ints, e.g. '1' or '1,3,5')",
    )
    ap.add_argument(
        "--quiet-symbols",
        action="store_true",
        help="Do not print per-symbol NDJSON to stdout (still written to --out file).",
    )
    ap.add_argument(
        "--append-quarantine",
        action="store_true",
        help="When writing FX G1 quarantine registry files, append/merge with existing ones instead of overwriting.",
    )
    args = ap.parse_args()

    cfg = load_config(args.config)

    # Mini-repro tool: dump raw 1H index diagnostics and exit.
    if bool(getattr(args, 'fx_dump_1h_index', '')):
        sym = sanitize_symbol(str(args.fx_dump_1h_index).strip())
        p = Path(args.fx_dir) / f"{sym}_1H.parquet"
        _print_stage('fx_dump_1h_index:start', {'symbol': sym, 'path': str(p)})
        if not p.exists():
            _print_stage('fx_dump_1h_index:error', {'symbol': sym, 'path': str(p), 'error': 'missing_1h_parquet'})
            return 2
        try:
            df = load_parquet(Path(p))
            payload = _debug_time_index(df)
            print(json.dumps(payload, indent=2, default=_json_default), flush=True)
            _print_stage('fx_dump_1h_index:done', {'symbol': sym, 'path': str(p)})
            return 0
        except Exception as e:
            _print_stage('fx_dump_1h_index:error', {'symbol': sym, 'path': str(p), 'error': f"load_failed:{e}"})
            return 2

    # Optional sweep overrides (do not change HF gate thresholds)
    if bool(args.horizons):
        try:
            hs = [int(x.strip()) for x in str(args.horizons).split(",") if x.strip()]
            if hs:
                if not hasattr(cfg, "features") or not isinstance(cfg.features, dict):
                    cfg.features = {}
                cfg.features["horizons"] = hs
        except Exception:
            pass

    if bool(args.sweep_fast):
        # Disable tuning (Optuna) for runtime tractability.
        try:
            if hasattr(cfg, "tuning"):
                cfg.tuning.enabled = False
                cfg.tuning.optuna_trials = min(int(getattr(cfg.tuning, "optuna_trials", 50)), 5)
        except Exception:
            pass

        # Drop CatBoost (often the slowest) for bulk sweeps.
        try:
            cfg.models_order = [m for m in list(getattr(cfg, "models_order", [])) if str(m).lower() != "catboost"]
        except Exception:
            pass
        try:
            if hasattr(cfg, "tuning") and hasattr(cfg.tuning, "models_order"):
                cfg.tuning.models_order = [m for m in list(cfg.tuning.models_order) if str(m).lower() != "catboost"]
        except Exception:
            pass

        # Reduce boosting rounds / early stopping for quicker fits.
        try:
            cfg.num_boost_round = min(int(getattr(cfg, "num_boost_round", 1000)), 300)
            cfg.early_stopping_rounds = min(int(getattr(cfg, "early_stopping_rounds", 50)), 30)
        except Exception:
            pass

        # Reduce horizons by default if not explicitly set.
        try:
            if not bool(args.horizons):
                if not hasattr(cfg, "features") or not isinstance(cfg.features, dict):
                    cfg.features = {}
                cfg.features["horizons"] = [1]
        except Exception:
            pass

        # Prefer CPU for consistent bulk runs.
        try:
            cfg.prefer_gpu = False
        except Exception:
            pass

    # Enable diagnose mode without mutating committed config files.
    try:
        if not hasattr(cfg, "gates") or not isinstance(cfg.gates, dict):
            cfg.gates = {}
        cfg.gates["diagnose_mode"] = True
    except Exception:
        pass

    state = StateRegistry(str(cfg.paths.state_dir))

    run_id = f"global_gate_diagnose_{_now_tag()}"

    # Pass-finder is opt-in and has its own outputs under reports/.
    if bool(getattr(args, 'fx_pass_finder', False)):
        res = _run_fx_pass_finder(
            cfg=cfg,
            state=state,
            run_id=run_id,
            fx_root=Path(args.fx_dir),
            quiet_symbols=bool(args.quiet_symbols),
            sweep_fast=bool(args.sweep_fast),
        )
        _print_stage('fx:pass_finder_done', res)
        return 0

    out_path = Path(args.out) if args.out else (Path("reports") / f"global_gate_diagnose_{_now_tag()}.jsonl")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fx_g1_recheck_out = Path(str(args.fx_g1_recheck_out)) if bool(args.fx_g1_recheck_out) else out_path.with_name(
        out_path.stem + ".g1_recheck" + out_path.suffix
    )

    only_1d = bool(args.only_1d) and (not bool(args.all_timeframes))

    _print_stage(
        "run:start",
        {
            "run_id": run_id,
            "config": args.config,
            "only_1d": only_1d,
            "limit": int(args.limit),
            "out": str(out_path),
            "stocks": bool(getattr(args, 'stocks', False)),
            "stocks_only": bool(getattr(args, 'stocks_only', False)),
            "fx_g1_recheck_after": bool(args.fx_g1_recheck_after),
            "fx_g1_recheck_out": str(fx_g1_recheck_out),
            "sweep_fast": bool(args.sweep_fast),
            "horizons": (getattr(cfg, "features", {}) or {}).get("horizons"),
            "models_order": getattr(cfg, "models_order", None),
            "tuning_enabled": getattr(getattr(cfg, "tuning", None), "enabled", None),
        },
    )

    with out_path.open("w", encoding="utf-8") as out_fh:
        idx_summary = None
        stocks_summary = None
        fx_summary = None
        fx_g1_recheck_summary = None

        if bool(getattr(args, 'stocks_only', False)):
            stocks_summary = _run_dataset(
                "stocks",
                Path(args.stocks_dir),
                cfg,
                state,
                run_id,
                out_fh,
                only_1d=only_1d,
                limit=int(args.limit),
                quiet_symbols=bool(args.quiet_symbols),
            )

        if (not bool(args.fx_only)) and (not bool(getattr(args, 'stocks_only', False))):
            idx_summary = _run_dataset(
                "indices",
                Path(args.indices_dir),
                cfg,
                state,
                run_id,
                out_fh,
                only_1d=only_1d,
                limit=int(args.limit),
                quiet_symbols=bool(args.quiet_symbols),
            )

            if bool(getattr(args, 'stocks', False)):
                stocks_summary = _run_dataset(
                    "stocks",
                    Path(args.stocks_dir),
                    cfg,
                    state,
                    run_id,
                    out_fh,
                    only_1d=only_1d,
                    limit=int(args.limit),
                    quiet_symbols=bool(args.quiet_symbols),
                )

        if (not bool(getattr(args, 'stocks_only', False))) and bool(args.fx_1d_risk_overlay_and_1h_alpha):
            fx_summary = _run_fx_two_stage(
                Path(args.fx_dir),
                cfg,
                state,
                run_id,
                out_fh,
                limit=int(args.limit),
                quiet_symbols=bool(args.quiet_symbols),
                append_quarantine=bool(args.append_quarantine),
            )

            if bool(args.fx_g1_recheck_after):
                selected_1d_paths: List[Path] = []
                if isinstance(fx_summary, dict):
                    raw_paths = fx_summary.get('selected_1d_paths')
                    if isinstance(raw_paths, list):
                        selected_1d_paths = [Path(x) for x in raw_paths if isinstance(x, str)]

                fx_g1_recheck_out.parent.mkdir(parents=True, exist_ok=True)
                with fx_g1_recheck_out.open('w', encoding='utf-8') as out2_fh:
                    _append_ndjson_event(
                        out2_fh,
                        {
                            'type': 'iteration',
                            'dataset': 'fx',
                            'pass_id': 'g1_recheck',
                            'run_id': run_id,
                            'created_utc': datetime.now(timezone.utc).isoformat(),
                            'iteration': 0,
                            'note': 'initial_g1_recheck_after',
                        },
                        quiet_symbols=True,
                    )
                    fx_g1_recheck_summary = _run_fx_g1_recheck(
                        Path(args.fx_dir),
                        cfg,
                        state,
                        run_id,
                        out2_fh,
                        selected_1d_paths=selected_1d_paths,
                        quiet_symbols=bool(args.quiet_symbols),
                        append_quarantine=bool(args.append_quarantine),
                    )
        else:
            if not bool(getattr(args, 'stocks_only', False)):
                fx_summary = _run_dataset(
                    "fx",
                    Path(args.fx_dir),
                    cfg,
                    state,
                    run_id,
                    out_fh,
                    only_1d=only_1d,
                    limit=int(args.limit),
                    quiet_symbols=bool(args.quiet_symbols),
                )

        # Attach pass-2 summary if present (kept separate from fx_summary).
        if fx_g1_recheck_summary is not None:
            try:
                out2_summary_path = fx_g1_recheck_out.with_suffix('.summary.json')
                out2_summary_path.write_text(json.dumps(fx_g1_recheck_summary, indent=2, default=_json_default))
            except Exception:
                pass

    summary_path = out_path.with_suffix(".summary.json")
    datasets = []
    if idx_summary is not None:
        datasets.append(idx_summary)
    if stocks_summary is not None:
        datasets.append(stocks_summary)
    if fx_summary is not None:
        datasets.append(fx_summary)
    if fx_g1_recheck_summary is not None:
        datasets.append(fx_g1_recheck_summary)
    summary = {
        "run_id": run_id,
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "config": str(args.config),
        "only_1d": bool(only_1d),
        "sweep_fast": bool(args.sweep_fast),
        "datasets": datasets,
    }
    # Fallback: if in-memory accumulation is missing/corrupt, rebuild from JSONL deterministically.
    try:
        if not isinstance(summary.get("datasets"), list) or len(summary.get("datasets")) == 0:
            summary["datasets"] = _rebuild_datasets_from_jsonl(out_path)
    except Exception:
        pass
    summary_path.write_text(json.dumps(summary, indent=2, default=_json_default))
    _print_stage("run:done", {"summary": str(summary_path)})

    # Optional: hourly (or custom interval) G1-only recheck loop.
    if bool(args.fx_g1_recheck_loop):
        if not bool(args.fx_1d_risk_overlay_and_1h_alpha):
            _print_stage(
                'g1_recheck_loop:abort',
                {
                    'reason': 'must_run_pass1_two_stage_first',
                    'hint': 'Use --fx-1d-risk-overlay-and-1h-alpha (and usually --fx-g1-recheck-after) before --fx-g1-recheck-loop',
                },
            )
            return 2
        try:
            _run_fx_g1_recheck_loop(
                fx_root=Path(args.fx_dir),
                cfg=cfg,
                state=state,
                run_id=run_id,
                pass1_out_path=out_path,
                loop_dir=Path(str(args.fx_g1_recheck_loop_dir)),
                interval_secs=int(args.fx_g1_recheck_interval_secs),
                max_iters=int(args.fx_g1_recheck_max_iters),
                quiet_symbols=bool(args.quiet_symbols),
                append_quarantine=bool(args.append_quarantine),
                refresh_universe=bool(args.fx_g1_recheck_refresh_universe),
            )
        except Exception as e:
            _print_stage(
                'g1_recheck_loop:abort',
                {
                    'reason': 'loop_error',
                    'error': str(e),
                    'pass1_out': str(out_path),
                },
            )
            return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
