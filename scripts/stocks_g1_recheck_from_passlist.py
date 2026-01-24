#!/usr/bin/env python3
"""Run 1H recheck for stocks from a pass-list and emit NDJSON + summary.

Usage example:
  python scripts/stocks_g1_recheck_from_passlist.py --pass-list reports/pass_symbols_stock.txt --out reports/stocks_g1_recheck_$(date -u +%Y%m%dT%H%M%SZ).jsonl
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
import sys

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from octa_training.core.config import load_config
from octa_training.core.pipeline import train_evaluate_package
from octa_training.core.state import StateRegistry


def _json_default(o: Any):
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


def _read_pass_list(p: Path) -> List[str]:
    try:
        return [l.strip() for l in p.read_text(encoding="utf-8").splitlines() if l.strip()]
    except Exception:
        return []


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="configs/dev.yaml")
    ap.add_argument("--pass-list", default="reports/pass_symbols_stock.txt")
    ap.add_argument("--stocks-dir", default="raw/Stock_parquet")
    ap.add_argument("--out", default="")
    ap.add_argument("--quiet-symbols", action="store_true")
    args = ap.parse_args()

    cfg = load_config(args.config)
    state = StateRegistry(str(cfg.paths.state_dir))

    now_tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = Path(args.out) if args.out else (Path("reports") / f"stocks_g1_recheck_{now_tag}.jsonl")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path = out_path.with_suffix('.summary.json')

    symbols = _read_pass_list(Path(args.pass_list))
    if not symbols:
        print(json.dumps({"error": "empty_pass_list", "path": str(args.pass_list)}))
        return 2

    stats = {"pass": 0, "fail": 0, "error": 0}

    run_id = f"stocks_g1_recheck_{now_tag}"

    with out_path.open('w', encoding='utf-8') as out_fh:
        for i, sym in enumerate(symbols, 1):
            sym = str(sym).strip()
            rec: Dict[str, Any] = {
                'type': 'symbol',
                'dataset': 'stocks',
                'pass_id': 'g1_recheck',
                'i': i,
                'n': len(symbols),
                'symbol': sym,
                'parquet': None,
                'run_id': run_id,
                'passed': False,
                'error': None,
                'gate': None,
            }
            p = Path(args.stocks_dir) / f"{sym}_1H.parquet"
            rec['parquet'] = str(p)
            try:
                if not p.exists():
                    rec['error'] = 'missing_1h_parquet'
                    rec['gate'] = {
                        'passed': False,
                        'status': 'FAIL_DATA',
                        'reasons': ['data_load_failed: stocks_g1:missing_1h_parquet'],
                        'passed_checks': [],
                        'insufficient_evidence': [],
                        'robustness': None,
                        'diagnostics': [],
                    }
                else:
                    res = train_evaluate_package(sym, cfg, state, run_id=run_id, safe_mode=True, parquet_path=str(p), dataset='stocks')
                    rec['error'] = getattr(res, 'error', None)
                    rec['gate'] = _gate_to_dict(getattr(res, 'gate_result', None))
            except Exception as e:
                rec['error'] = str(e)
            # finalize passed flag
            try:
                g = rec.get('gate')
                passed = False
                if isinstance(g, dict):
                    passed = bool(g.get('passed'))
                rec['passed'] = bool(passed)
            except Exception:
                rec['passed'] = False

            if rec.get('passed'):
                stats['pass'] += 1
            else:
                stats['fail'] += 1
            if rec.get('error'):
                stats['error'] += 1

            line = json.dumps(rec, default=_json_default)
            if not args.quiet_symbols:
                print(line, flush=True)
            out_fh.write(line + "\n")
            out_fh.flush()

    summary = {
        'run_id': run_id,
        'created_utc': datetime.now(timezone.utc).isoformat(),
        'pass_list': str(args.pass_list),
        'out': str(out_path),
        'selected': len(symbols),
        'stats': stats,
    }
    try:
        summary_path.write_text(json.dumps(summary, indent=2, default=_json_default), encoding='utf-8')
    except Exception:
        pass

    print(json.dumps({'summary': str(summary_path)}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
