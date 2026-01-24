#!/usr/bin/env python3
"""Batch-run strict multi-timeframe (global gate) cascade on index parquets.

Key properties:
- Per-symbol execution (no per-TF universe expansion).
- Strict cascade: 1D -> 1H -> 30m -> 5m -> 1m, stop on FAIL/ERR.
- 5m/1m disabled by default; enable via config `enable_micro_timeframes: true`.
- Writes per-run audit artifacts under `reports/cascade/<run_id>/...` via
  scripts/train_multiframe_symbol.run_sequence.

This script does NOT modify gate thresholds.

Usage:
  python scripts/batch_gate_check_indices_multitf.py \
    --config configs/tmp_train_djr_multitf_hf.yaml \
    --raw-dir raw/Indices_parquet
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import yaml
except Exception:
    yaml = None


# Allow running as a script from the repo root without installing the package
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from octa_training.core.config import load_config
from octa_training.core.io_parquet import discover_parquets, sanitize_symbol
from octa_training.core.state import StateRegistry


def _now_tag() -> str:
    return datetime.now(timezone.utc).isoformat().replace(":", "-")


def _base_symbol_root(sym: str) -> str:
    # Indices naming is typically BASE_FULL_1DAY, BASE_FULL_1HOUR, etc.
    s = sanitize_symbol(sym)
    if not s:
        return ""
    return s.split("_", 1)[0]


def _load_yaml(path: Optional[str]) -> Optional[dict]:
    if not path:
        return None
    if not yaml:
        return None
    p = Path(path)
    if not p.exists():
        return None
    try:
        return yaml.safe_load(p.read_text()) or {}
    except Exception:
        return None


def _deep_merge(dst: dict, src: dict) -> None:
    if not isinstance(dst, dict) or not isinstance(src, dict):
        return
    for k, v in src.items():
        if k in dst and isinstance(dst[k], dict) and isinstance(v, dict):
            _deep_merge(dst[k], v)
        else:
            dst[k] = v


def _apply_batch_overrides(cfg, cfg_raw: Optional[dict]) -> Optional[dict]:
    """Disable tuning/optional altdata for batch sweeps (compute + determinism).

    This does NOT modify gate thresholds.
    """
    # Best-effort pydantic overrides.
    try:
        if getattr(cfg, "tuning", None) is not None:
            cfg.tuning.enabled = False
            try:
                cfg.tuning.models_order = ["logreg"]
            except Exception:
                pass
    except Exception:
        pass
    try:
        if getattr(cfg, "features", None) is not None:
            # Keep simple daily horizon for gating smoke; avoids heavy multi-horizon.
            cfg.features["horizons"] = [1] if isinstance(cfg.features, dict) else cfg.features
    except Exception:
        pass
    try:
        if getattr(cfg, "features", None) is not None and isinstance(cfg.features, dict):
            avi = cfg.features.get("aviation")
            if isinstance(avi, dict):
                avi["enabled"] = False
    except Exception:
        pass

    # Raw config overrides (so run_sequence layer merges see it).
    raw = cfg_raw.copy() if isinstance(cfg_raw, dict) else {}
    overrides = {
        "tuning": {"enabled": False, "models_order": ["logreg"], "optuna_trials": 0, "timeout_sec": 0},
        "features": {"horizons": [1], "aviation": {"enabled": False}},
        "layers": {
            "struct_1h": {"tuning": {"enabled": False, "optuna_trials": 0, "timeout_sec": 0}},
            "struct_30m": {"tuning": {"enabled": False, "optuna_trials": 0, "timeout_sec": 0}},
        },
    }
    _deep_merge(raw, overrides)
    return raw


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=None, help="Training config YAML")
    ap.add_argument("--raw-dir", default=None, help="Raw data directory (e.g. raw/Indices_parquet)")
    ap.add_argument("--out", default=None, help="Output summary report path (json)")
    ap.add_argument("--max-symbols", type=int, default=0, help="Limit number of base symbols (0 = all)")
    ap.add_argument("--exclude-symbols-file", default=None, help="Newline-delimited symbols to exclude")
    ap.add_argument("--run-id", default=None, help="Optional run id override")
    args = ap.parse_args()

    cfg = load_config(args.config)
    state = StateRegistry(str(cfg.paths.state_dir))

    raw_dir = Path(args.raw_dir) if args.raw_dir else Path(cfg.paths.raw_dir)
    # Critical: run_sequence discovers parquets from cfg.paths.raw_dir.
    cfg.paths.raw_dir = str(raw_dir)

    cfg_raw = _load_yaml(args.config)
    cfg_raw = _apply_batch_overrides(cfg, cfg_raw)

    discovered = discover_parquets(raw_dir, state=state)
    print(f"Discovered parquets: {len(discovered)} under {raw_dir}")

    # Build unique base universe (root token).
    by_root: dict[str, str] = {}
    for d in discovered:
        root = _base_symbol_root(d.symbol)
        if not root:
            continue
        # keep first seen; run_sequence will resolve variants.
        by_root.setdefault(root, root)

    bases = sorted(by_root.values())

    excluded: set[str] = set()
    if args.exclude_symbols_file:
        p = Path(str(args.exclude_symbols_file))
        if not p.exists():
            raise SystemExit(f"exclude-symbols-file not found: {p}")
        for line in p.read_text().splitlines():
            s = sanitize_symbol(line.strip())
            if s:
                excluded.add(_base_symbol_root(s) or s)
        if excluded:
            print(f"Excluding {len(excluded)} base symbols from exclude list")

    if excluded:
        bases = [b for b in bases if b not in excluded]

    if args.max_symbols and args.max_symbols > 0:
        bases = bases[: args.max_symbols]

    run_id = args.run_id or f"index_global_gate_{_now_tag()}"

    from scripts.train_multiframe_symbol import run_sequence

    status_counts_by_tf: dict[str, Counter[str]] = {"1D": Counter(), "1H": Counter(), "30m": Counter(), "5m": Counter(), "1m": Counter()}
    stop_reasons = Counter()

    results: List[Dict[str, Any]] = []

    for i, base in enumerate(bases, 1):
        print(f"[{i}/{len(bases)}] {base} -> strict cascade")
        # Per-symbol, sequential cascade.
        _ = run_sequence(
            base,
            cfg,
            state,
            run_id=run_id,
            force=False,
            include_5m=False,
            include_1m=False,
            continue_on_fail=False,
            train_daily_regime=True,
            mode="live",
            config_raw=cfg_raw,
            layers=None,
        )

        # Summarize statuses from cascade artifacts (authoritative).
        cascade_dir = Path(cfg.paths.reports_dir) / "cascade" / str(run_id) / sanitize_symbol(base)
        per_tf: Dict[str, str] = {}
        gate_version: str | None = None
        for tf in ["1D", "1H", "30m", "5m", "1m"]:
            dec = cascade_dir / tf / "decision.json"
            if dec.exists():
                payload = json.loads(dec.read_text())
                st = str(payload.get("status") or "")
                per_tf[tf] = st
                status_counts_by_tf[tf][st] += 1
                gate_version = gate_version or payload.get("gate_version")
            else:
                per_tf[tf] = "SKIPPED"
                status_counts_by_tf[tf]["SKIPPED"] += 1

        # Find stop reason from the run report (best-effort).
        stop_reason = None
        rep_files = sorted((Path(cfg.paths.reports_dir)).glob(f"{sanitize_symbol(base)}_multitf_{run_id}_*.json"))
        if rep_files:
            try:
                rep = json.loads(rep_files[-1].read_text())
                stop_reason = rep.get("stop_reason")
            except Exception:
                stop_reason = None
        if stop_reason:
            stop_reasons[str(stop_reason)] += 1

        results.append({"symbol": base, "run_id": run_id, "gate_version": gate_version, "per_tf": per_tf, "stop_reason": stop_reason})

    out_path = Path(args.out) if args.out else (Path(cfg.paths.reports_dir) / f"index_global_gate_multitf_{run_id}.json")
    payload = {
        "run_id": run_id,
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "config": {
            "config_path": args.config,
            "raw_dir": str(raw_dir),
            "exclude_symbols_file": str(args.exclude_symbols_file) if args.exclude_symbols_file else None,
        },
        "summary": {
            "n_symbols": len(bases),
            "status_counts_by_tf": {k: dict(v) for k, v in status_counts_by_tf.items()},
            "top_stop_reasons": stop_reasons.most_common(25),
        },
        "results": results,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n")
    print(f"Wrote report: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
