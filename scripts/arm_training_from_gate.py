#!/usr/bin/env python3
"""Arm training from a gate manifest.

Validates a gate manifest and artifacts and writes ARMED.ok plus audit log.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.training_safety_lock import (
    emit_audit_log,
    load_latest_gate_run,
    verify_gate_artifacts,
)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", default=None, help="Path to gate_manifest.json or gates directory")
    ap.add_argument("--max-age-days", type=int, default=14)
    ap.add_argument("--require-secondary", dest='require_secondary', action="store_true", help="Require secondary per-asset gates to have passed before arming")
    ap.add_argument("--no-require-secondary", dest='require_secondary', action='store_false', help="Do not require secondary per-asset gates before arming")
    ap.set_defaults(require_secondary=True)
    args = ap.parse_args()

    try:
        manifest = load_latest_gate_run(args.manifest or (Path('reports') / 'gates'))
    except Exception as e:
        print(f"ERROR: could not load manifest: {e}")
        raise SystemExit(2) from e

    manifest_dir = Path(manifest.get("manifest_dir") or (Path('reports') / 'gates' / manifest.get('run_id', '')))
    artifacts_dir = Path(manifest.get('artifacts_dir') or (Path('reports') / 'cascade' / manifest.get('run_id', '')))
    dataset = manifest.get('dataset') or manifest.get('asset_class') or 'unknown'
    reports_dir = Path(manifest.get('reports_dir') or Path('reports'))
    canonical_dir = reports_dir / 'assurance' / 'armed' / str(dataset) / str(manifest.get('run_id', ''))

    try:
        # basic artifact verification for 1D
        verify_gate_artifacts(manifest, '1D')
    except Exception as e:
        print(f"ERROR: gate artifacts invalid: {e}")
        raise SystemExit(3) from e

    # optional: verify secondary per-asset gates
    if bool(args.require_secondary):
        assets_dir = Path(manifest.get('artifacts_dir')) / 'assets'
        pass_list = assets_dir / 'assets_pass.txt'
        primary_pass_file = Path(manifest.get('artifacts_dir')) / manifest.get('pass_files', {}).get('1D', 'pass_symbols_1D.txt')
        primary_syms = {s.strip().upper() for s in primary_pass_file.read_text().splitlines() if s.strip()} if primary_pass_file.exists() else set()
        if not pass_list.exists():
            print(f"ERROR: secondary asset pass list missing: {pass_list}")
            raise SystemExit(5)
        secondary_syms = {s.strip().upper() for s in pass_list.read_text().splitlines() if s.strip()}
        missing = primary_syms - secondary_syms
        if missing:
            print(f"ERROR: some primary-pass symbols did not pass secondary gate: {sorted(missing)[:10]} (showing up to 10)")
            raise SystemExit(6)

    # write legacy ARMED.ok (for backward compatibility) and canonical ARMED.ok + metadata
    armed_legacy = manifest_dir / 'ARMED.ok'
    armed_canonical = canonical_dir / 'ARMED.ok'
    try:
        manifest_dir.mkdir(parents=True, exist_ok=True)
        armed_legacy.write_text('armed\n')
    except Exception as e:
        print(f"ERROR: failed to write legacy ARMED.ok: {e}")
        raise SystemExit(4) from e

    try:
        canonical_dir.mkdir(parents=True, exist_ok=True)
        # write a simple marker and a structured metadata file for auditing
        armed_canonical.write_text('armed\n')
        meta = {
            'action': 'arm_training',
            'run_id': manifest.get('run_id'),
            'dataset': dataset,
            'manifest': str(manifest_dir / 'gate_manifest.json'),
            'artifacts_dir': str(artifacts_dir),
        }
        (canonical_dir / 'ARMED.meta.json').write_text(json.dumps(meta, sort_keys=True, indent=2) + '\n')
    except Exception as e:
        print(f"ERROR: failed to write canonical ARMED.ok or metadata: {e}")
        raise SystemExit(4) from e

    # audit event
    audit_p = manifest_dir / 'audit.jsonl'
    event = {
        'action': 'arm_training',
        'run_id': manifest.get('run_id'),
        'dataset': dataset,
        'manifest': str(manifest_dir / 'gate_manifest.json'),
        'artifacts_dir': str(artifacts_dir),
    }
    emit_audit_log(event, audit_p)
    # also add audit under canonical dir
    try:
        emit_audit_log(event, canonical_dir / 'audit.jsonl')
    except Exception:
        pass
    print(f"ARMED ok written to {armed_legacy} and {armed_canonical}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
