#!/usr/bin/env python3

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Tuple


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding='utf-8'))


def _iter_diag_files(root: Path) -> List[Path]:
    if not root.exists():
        return []
    return sorted([p for p in root.rglob('*.json') if p.is_file()])


def _top_reasons(diags: List[Dict[str, Any]]) -> List[Tuple[str, int]]:
    c: Counter[str] = Counter()
    for d in diags:
        for r in (d.get('reasons') or []):
            c[str(r)] += 1
    return c.most_common(25)


def _top_failed_checks(diags: List[Dict[str, Any]]) -> List[Tuple[str, int]]:
    c: Counter[str] = Counter()
    for d in diags:
        for chk in (d.get('failed_checks') or []):
            name = chk.get('name')
            if name:
                c[str(name)] += 1
    return c.most_common(25)


def main() -> int:
    ap = argparse.ArgumentParser(description='Summarize FX structural-fail diagnostic bundles produced by global_gate_diagnose.')
    ap.add_argument('--reports-dir', type=str, default='reports', help='Reports directory (default: reports)')
    ap.add_argument('--run-id', type=str, default=None, help='Specific run_id to summarize (subdir under fx_structural_fail_diag/)')
    args = ap.parse_args()

    reports_dir = Path(args.reports_dir)
    base = reports_dir / 'fx_structural_fail_diag'
    if args.run_id:
        roots = [base / args.run_id]
    else:
        roots = sorted([p for p in base.iterdir() if p.is_dir()]) if base.exists() else []

    all_files: List[Path] = []
    for r in roots:
        all_files.extend(_iter_diag_files(r))

    if not all_files:
        print(f'No diagnostic bundles found under {base}')
        return 0

    diags = [_load_json(p) for p in all_files]

    by_stage: Counter[str] = Counter(str(d.get('stage') or 'unknown') for d in diags)
    by_status: Counter[str] = Counter(str(d.get('status') or 'unknown') for d in diags)

    print(f'FX structural-fail diagnostic bundles: {len(diags)}')
    if args.run_id:
        print(f'run_id: {args.run_id}')

    print('\nStages:')
    for k, v in by_stage.most_common():
        print(f'  {k}: {v}')

    print('\nStatuses:')
    for k, v in by_status.most_common():
        print(f'  {k}: {v}')

    print('\nTop reasons:')
    for r, n in _top_reasons(diags):
        print(f'  {n:4d}  {r}')

    print('\nTop failed checks:')
    for n, c in _top_failed_checks(diags):
        print(f'  {c:4d}  {n}')

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
