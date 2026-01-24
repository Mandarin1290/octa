#!/usr/bin/env python3
"""Train all symbols that passed the primary gate run.

Reads the latest `reports/gates/<run_id>/gate_manifest.json`, loads the
pass file for the configured timeframe (default `1D`) and runs the
multi-timeframe training sequence for each passed symbol.

Strict cascade semantics apply:
- 1D is trained first; only if 1D PASS do we train 1H, etc.
- 5m/1m are optional and only run if explicitly enabled.

This script expects the gate run to be ARMed (ARMED.ok present). It will
skip symbols that fail during the sequence and write per-symbol manifests
under the original artifacts cascade dir.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

try:
    import yaml
except Exception:
    yaml = None

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.training_safety_lock import load_latest_gate_run, verify_config_alignment
from octa_training.core.config import load_config
from octa_training.core.state import StateRegistry
from scripts.train_multiframe_symbol import run_sequence


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", default=None, help="Path to gate_manifest.json or gates directory")
    ap.add_argument("--tf", default="1D", help="Timeframe to use from primary pass file")
    ap.add_argument("--config", default=None, help="Optional config path")
    ap.add_argument("--include-5m", action="store_true", help="Include 5m layer in strict cascade (research/exit-only)")
    ap.add_argument("--include-1m", action="store_true", help="Include 1m layer in strict cascade (research/exit-only)")
    ap.add_argument("--max-symbols", type=int, default=None, help="Limit number of symbols to run (for testing)")
    args = ap.parse_args()

    manifest = load_latest_gate_run(args.manifest or (Path('reports') / 'gates'))
    artifacts_dir = Path(manifest.get('artifacts_dir'))
    tf = args.tf
    pass_file = artifacts_dir / manifest.get('pass_files', {}).get(tf, f'pass_symbols_{tf}.txt')
    if not pass_file.exists():
        print(f"No pass file found: {pass_file}")
        raise SystemExit(2)

    symbols = [s.strip() for s in pass_file.read_text().splitlines() if s.strip()]
    if args.max_symbols:
        symbols = symbols[: args.max_symbols]

    # Load a base config (may be replaced below to match manifest fingerprint).
    cfg = load_config(args.config) if args.config else load_config()

    cfg_raw = None
    if args.config and yaml:
        try:
            cfg_raw = yaml.safe_load(Path(args.config).read_text()) or {}
        except Exception:
            cfg_raw = None

    # Ensure the runtime cfg matches the manifest fingerprint (fail-closed).
    # If it doesn't, attempt to recover the exact config used by the sweep.
    try:
        verify_config_alignment(manifest, cfg)
    except Exception:
        manifest_cfg_err = None

        # Candidate config files to try (deterministic order).
        cfg_paths: list[str | None] = []
        if args.config:
            cfg_paths.append(args.config)
        # Standard workspace configs
        cfg_paths.extend([
            str(Path('configs') / 'base.yaml'),
            str(Path('configs') / 'dev.yaml'),
            str(Path('configs') / 'paper.yaml'),
            str(Path('configs') / 'live.yaml'),
            None,
        ])

        # Some sweeps apply batch overrides (indices) and override raw_dir at runtime.
        raw_dir_candidates = [
            Path('raw') / 'Indices_parquet',
            Path('raw') / 'INDICES_PARQUET',
            Path('raw') / 'Indices',
            Path('raw') / 'INDICES',
            None,
        ]

        for cpath in cfg_paths:
            try:
                cfg_try = load_config(cpath) if cpath else load_config()
            except Exception as e:
                manifest_cfg_err = e
                continue

            # Keep YAML raw for enable_micro_timeframes and layer overrides.
            cfg_try_raw = None
            if cpath and yaml:
                try:
                    cfg_try_raw = yaml.safe_load(Path(cpath).read_text()) or {}
                except Exception:
                    cfg_try_raw = None

            # Apply sweep overrides if the helper exists.
            cfg_eff = cfg_try
            try:
                from scripts.batch_gate_check_indices_1d import _apply_batch_overrides

                cfg_eff = _apply_batch_overrides(cfg_try)
            except Exception:
                cfg_eff = cfg_try

            for rd in raw_dir_candidates:
                if rd is not None:
                    try:
                        rd_path = Path(rd)
                        if rd_path.exists() and rd_path.is_dir():
                            cfg_eff.paths.raw_dir = rd_path
                    except Exception:
                        pass

                try:
                    verify_config_alignment(manifest, cfg_eff)
                    cfg = cfg_eff
                    cfg_raw = cfg_try_raw
                    print(
                        "Config adjusted to match manifest fingerprint "
                        f"(config={cpath or 'DEFAULT'}, raw_dir={getattr(cfg.paths, 'raw_dir', None)})"
                    )
                    manifest_cfg_err = None
                    break
                except Exception as e:
                    manifest_cfg_err = e
                    continue

            if manifest_cfg_err is None:
                break

        if manifest_cfg_err is not None:
            print(f"ERROR: training config does not match gate manifest config: {manifest_cfg_err}")
            raise SystemExit(7) from manifest_cfg_err

    state = StateRegistry(cfg.paths.state_dir)

    run_id_base = f"train_passed:{manifest.get('run_id')}"
    for sym in symbols:
        run_id = f"{run_id_base}:{sym}"
        print(f"Training symbol: {sym} (run_id={run_id})")
        try:
            out = run_sequence(
                sym,
                cfg,
                state,
                run_id=run_id,
                gate_only=False,
                force=False,
                include_5m=bool(getattr(args, 'include_5m', False)),
                include_1m=bool(getattr(args, 'include_1m', False)),
                config_raw=cfg_raw,
            )
            # write per-symbol manifest
            per = Path(artifacts_dir) / 'trained' / sym
            per.mkdir(parents=True, exist_ok=True)
            (per / 'train_manifest.json').write_text(json.dumps({'symbol': sym, 'run_id': run_id, 'result': out, 'ts': datetime.utcnow().isoformat()}, default=str, indent=2))
        except Exception as e:
            print(f"  ERROR training {sym}: {e}")
            per = Path(artifacts_dir) / 'trained' / sym
            per.mkdir(parents=True, exist_ok=True)
            (per / 'train_manifest.json').write_text(json.dumps({'symbol': sym, 'run_id': run_id, 'error': str(e), 'ts': datetime.utcnow().isoformat()}, default=str, indent=2))

    print("Finished training passed symbols")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
