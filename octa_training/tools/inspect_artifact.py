"""CLI to inspect packaged artifacts and run a smoke inference test.

Usage:
  python -m octa_training.tools.inspect_artifact --symbol <SYM>
  python -m octa_training.tools.inspect_artifact --path <PKL_PATH>
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from octa_training.core.artifact_io import (
    load_tradeable_artifact,
    read_meta,
    smoke_test_artifact,
)
from octa_training.core.config import load_config


def parse_args(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", type=str, help="Symbol to inspect (looks in cfg.paths.pkl_dir)")
    p.add_argument("--path", type=str, help="Path to PKL artifact")
    p.add_argument("--last-n", type=int, default=50, help="Rows to use for smoke predict")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    cfg = load_config()
    pkl_path = None
    if args.path:
        pkl_path = Path(args.path)
    elif args.symbol:
        pkl_path = Path(cfg.paths.pkl_dir) / f"{args.symbol}.pkl"
    else:
        raise SystemExit("Provide --symbol or --path")

    if not pkl_path.exists():
        raise SystemExit(f"Artifact not found: {pkl_path}")

    # load meta and artifact
    meta_path = pkl_path.with_suffix('.meta.json')
    sha_path = pkl_path.with_suffix('.sha256')
    meta = None
    try:
        meta = read_meta(str(meta_path))
    except Exception as e:
        print("WARN: failed to read meta:", e)

    try:
        art = load_tradeable_artifact(str(pkl_path), str(sha_path) if sha_path.exists() else None)
    except Exception as e:
        raise SystemExit(f"Failed to load artifact: {e}") from e

    out = {
        'path': str(pkl_path),
        'meta': meta.dict() if meta is not None else None,
        'model': art.get('model_bundle'),
        'feature_count': len(art.get('feature_spec', {}).get('features', [])),
    }
    print(json.dumps(out, indent=2, default=str))

    # run smoke test
    try:
        st = smoke_test_artifact(str(pkl_path), str(cfg.paths.raw_dir), last_n=args.last_n)
        print('SMOKE_TEST:', json.dumps(st, indent=2, default=str))
    except Exception as e:
        print('SMOKE_TEST_FAILED:', e)


if __name__ == '__main__':
    main()
