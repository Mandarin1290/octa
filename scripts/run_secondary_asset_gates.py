#!/usr/bin/env python3
"""Run secondary per-asset gates based on a primary gate manifest.

For each symbol listed in the primary pass file (1D), run a safe-mode
evaluation (`train_evaluate_package` with `safe_mode=True`) to produce
per-asset gate artifacts under `artifacts_dir/assets/<symbol>/` and write
a consolidated `assets_pass.txt` for the run.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.training_safety_lock import load_latest_gate_run
from octa_training.core.config import load_config
from octa_training.core.pipeline import train_evaluate_package
from octa_training.core.state import StateRegistry


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", default=None, help="Path to gate_manifest.json or gates directory")
    ap.add_argument("--tf", default="1D", help="Timeframe to use from primary pass file")
    ap.add_argument("--config", default=None, help="Optional config path to use for per-asset checks")
    ap.add_argument("--max-symbols", type=int, default=None, help="Limit number of symbols to evaluate (for testing)")
    args = ap.parse_args()

    manifest = load_latest_gate_run(args.manifest or (Path('reports') / 'gates'))
    artifacts_dir = Path(manifest.get('artifacts_dir'))
    tf = args.tf
    pass_file = artifacts_dir / manifest.get('pass_files', {}).get(tf, f'pass_symbols_{tf}.txt')
    if not pass_file.exists():
        print(f"No primary pass file found: {pass_file}")
        raise SystemExit(2)

    symbols = [s.strip() for s in pass_file.read_text().splitlines() if s.strip()]
    if args.max_symbols:
        symbols = symbols[: args.max_symbols]

    # load config (allow override)
    cfg = load_config(args.config) if args.config else load_config()
    state = StateRegistry(cfg.paths.state_dir)

    assets_base = artifacts_dir / 'assets'
    assets_base.mkdir(parents=True, exist_ok=True)
    assets_pass = []

    for sym in symbols:
        run_id = f"secondary_asset_gate:{manifest.get('run_id')}:{sym}"
        outdir = assets_base / sym
        outdir.mkdir(parents=True, exist_ok=True)
        print(f"Running secondary gate for {sym} (safe-mode evaluation)")
        try:
            res = train_evaluate_package(sym, cfg, state, run_id=run_id, safe_mode=True, smoke_test=False)
        except Exception as e:
            print(f"  ERROR running eval for {sym}: {e}")
            (outdir / 'asset_gate_manifest.json').write_text(json.dumps({'symbol': sym, 'status': 'ERR', 'error': str(e), 'ts': datetime.utcnow().isoformat()}))
            continue

        passed = bool(getattr(res, 'passed', False))
        manifest_out = {
            'symbol': sym,
            'passed': passed,
            'gate_result': getattr(res, 'gate_result', None),
            'metrics': getattr(res, 'metrics', None),
            'run_id': run_id,
            'ts': datetime.utcnow().isoformat(),
        }
        (outdir / 'asset_gate_manifest.json').write_text(json.dumps(manifest_out, default=str, indent=2))
        if passed:
            assets_pass.append(sym)

    # write consolidated pass list
    pass_list_path = assets_base / 'assets_pass.txt'
    pass_list_path.write_text("\n".join(sorted(assets_pass)))
    print(f"Wrote secondary asset pass list: {pass_list_path} (passed={len(assets_pass)})")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
