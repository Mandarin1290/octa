#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--meta-dir", required=True, help="Directory containing *.meta.json")
    ap.add_argument("--out", default="", help="Optional path to write summary JSON")
    args = ap.parse_args()

    meta_dir = Path(args.meta_dir)
    metas = sorted(meta_dir.glob("*.meta.json"))

    counts_1d = {"PASS": 0, "FAIL": 0, "ERROR": 0, "OTHER": 0}
    counts_1h = {"PASS": 0, "FAIL": 0, "ERROR": 0, "SKIP_H1_NOT_ELIGIBLE": 0, "OTHER": 0}

    armed = 0
    pkl_1d = 0
    pkl_1h = 0
    by_run: Dict[str, int] = {}
    armed_syms: list[str] = []

    for p in metas:
        meta = _load_json(p)
        run_id = str(meta.get("run_id") or "")
        by_run[run_id] = by_run.get(run_id, 0) + 1

        tf = meta.get("timeframe_status") or {}
        s1d = str(tf.get("1D") or "").upper()
        s1h = str(tf.get("1H") or "").upper()

        if s1d in counts_1d:
            counts_1d[s1d] += 1
        else:
            counts_1d["OTHER"] += 1

        if s1h in counts_1h:
            counts_1h[s1h] += 1
        else:
            counts_1h["OTHER"] += 1

        if bool(meta.get("armed")):
            armed += 1
            armed_syms.append(str(meta.get("symbol") or "").upper())

        models = meta.get("models") or {}
        m1d = models.get("1D")
        if isinstance(m1d, dict) and m1d.get("pkl"):
            pkl_1d += 1
        m1h = models.get("1H")
        if isinstance(m1h, dict) and m1h.get("pkl"):
            pkl_1h += 1

    summary = {
        "meta_total": len(metas),
        "counts": {"1D": counts_1d, "1H": counts_1h},
        "pkls_in_meta": {"1D": pkl_1d, "1H": pkl_1h},
        "armed": armed,
        "armed_symbols_sample": armed_syms[:30],
        "by_run_id": dict(sorted(by_run.items(), key=lambda kv: (-kv[1], kv[0]))),
    }

    txt = json.dumps(summary, indent=2) + "\n"
    if str(args.out).strip():
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(txt, encoding="utf-8")

    print(txt, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
