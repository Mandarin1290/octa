from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Iterable

# Allow running this file directly: ensure repo root is on sys.path.
_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from octa_ops.autopilot.global_gate import (
    GlobalGatePolicy,
    evaluate_global_gate,
    write_global_outputs,
)
from octa_ops.autopilot.types import GateDecision


def _now_tag() -> str:
    # No external deps; stable enough for filenames.
    import datetime as _dt

    s = _dt.datetime.now(_dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return s.replace(":", "").replace("-", "")


def _load_symbols_from_gate_report(path: Path) -> list[str]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    symbols_obj = payload.get("symbols")
    if isinstance(symbols_obj, dict):
        return sorted({str(k).strip() for k in symbols_obj.keys() if str(k).strip()})
    # fall back: maybe list of dicts
    if isinstance(symbols_obj, list):
        out: set[str] = set()
        for item in symbols_obj:
            if isinstance(item, dict) and item.get("symbol"):
                out.add(str(item["symbol"]).strip())
        return sorted(out)
    raise SystemExit(f"gate_report.json has unexpected 'symbols' shape: {type(symbols_obj)}")


def _symbols_from_file(path: Path) -> list[str]:
    out: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        out.append(s)
    return sorted(set(out))


def _symbols_from_stock_dir(stock_dir: Path) -> list[str]:
    out: set[str] = set()
    for p in stock_dir.glob("*_1D.parquet"):
        name = p.name
        if not name.endswith("_1D.parquet"):
            continue
        sym = name[: -len("_1D.parquet")].strip()
        if sym:
            out.add(sym)
    return sorted(out)


def _scan_raw_root_for_1d(raw_root: Path) -> dict[str, Path]:
    out: dict[str, Path] = {}
    if not raw_root.exists():
        return out
    # Support both standard and index naming.
    patterns = ["*_1D.parquet", "*_full_1day.parquet", "*_full_1DAY.parquet"]
    candidates: list[Path] = []
    for pat in patterns:
        candidates.extend(raw_root.rglob(pat))
    for p in sorted(set(candidates)):
        name = p.name
        if name.endswith("_1D.parquet"):
            sym = name[: -len("_1D.parquet")].strip()
        elif name.lower().endswith("_full_1day.parquet"):
            sym = name[: -len("_full_1day.parquet")].strip()
        else:
            continue
        if not sym:
            continue
        # Deterministic: keep first (paths are sorted)
        out.setdefault(sym, p)
    return out


def _write_lines(path: Path, lines: Iterable[str]) -> None:
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Run OCTA Global Gate for available 1D parquets (risk-only by default).")
    ap.add_argument("--stock-dir", default="raw/Stock_parquet", help="Directory containing *_1D.parquet files (legacy default)")
    ap.add_argument("--raw-root", default="", help="If set, scan recursively for *_1D.parquet under this folder")
    ap.add_argument("--from-gate-report", default="", help="Path to reports/gate_reports/<run_id>/gate_report.json")
    ap.add_argument("--symbols-file", default="", help="Text file with one symbol per line")
    ap.add_argument("--out-dir", default="", help="Output dir (default: reports/global_gate_runs/<tag>)")
    ap.add_argument("--limit", type=int, default=0, help="Limit number of symbols (0 = no limit)")

    # Policy knobs
    ap.add_argument("--min-history-days", type=int, default=252)
    ap.add_argument("--max-drawdown-max", type=float, default=0.6)
    ap.add_argument("--max-vol-annual", type=float, default=2.5)

    # Enrichers: OFF by default to avoid network/rate-limit cost.
    ap.add_argument("--fred", action="store_true", help="Enable FRED enrichment (requires FRED_API_KEY)")
    ap.add_argument("--edgar", action="store_true", help="Enable EDGAR enrichment (network + rate-limit)")
    ap.add_argument("--edgar-user-agent", default="OCTA/1.0 (research; contact=ops@example.com)")
    ap.add_argument("--edgar-rpm", type=int, default=8)

    args = ap.parse_args()

    raw_root = Path(args.raw_root) if str(args.raw_root).strip() else None
    stock_dir = Path(args.stock_dir)
    scanned_1d: dict[str, Path] = {}
    if raw_root is not None:
        scanned_1d = _scan_raw_root_for_1d(raw_root)
        if not scanned_1d:
            raise SystemExit(f"No *_1D.parquet found under raw_root: {raw_root}")
    else:
        if not stock_dir.exists():
            raise SystemExit(f"stock_dir does not exist: {stock_dir}")

    if args.from_gate_report:
        symbols = _load_symbols_from_gate_report(Path(args.from_gate_report))
        source = f"gate_report:{args.from_gate_report}"
    elif args.symbols_file:
        symbols = _symbols_from_file(Path(args.symbols_file))
        source = f"symbols_file:{args.symbols_file}"
    else:
        if raw_root is not None:
            symbols = sorted(scanned_1d.keys())
            source = f"scan_raw:{raw_root}"
        else:
            symbols = _symbols_from_stock_dir(stock_dir)
            source = f"scan:{stock_dir}"

    if args.limit and args.limit > 0:
        symbols = symbols[: int(args.limit)]

    out_dir = Path(args.out_dir) if args.out_dir else (Path("reports") / "global_gate_runs" / f"stock_1d_{_now_tag()}")
    out_dir.mkdir(parents=True, exist_ok=True)

    policy = GlobalGatePolicy(
        min_history_days=int(args.min_history_days),
        max_drawdown_max=float(args.max_drawdown_max),
        max_vol_annual=float(args.max_vol_annual),
        fred_enabled=bool(args.fred),
        edgar_enabled=bool(args.edgar),
        edgar_user_agent=str(args.edgar_user_agent),
        edgar_rate_limit_per_minute=int(args.edgar_rpm),
    )

    decisions: dict[str, GateDecision] = {}
    cache_dir = str(out_dir / "global_features_store")

    for sym in symbols:
        if raw_root is not None:
            p1d = scanned_1d.get(sym)
            if p1d is None or not p1d.exists():
                decisions[sym] = GateDecision(sym, "1D", "global", "SKIP", "missing_1d", {"expected": "scan_raw"})
                continue
        else:
            p1d = stock_dir / f"{sym}_1D.parquet"
            if not p1d.exists():
                decisions[sym] = GateDecision(sym, "1D", "global", "SKIP", "missing_1d", {"expected": str(p1d)})
                continue
        decisions[sym] = evaluate_global_gate(symbol=sym, parquet_1d_path=str(p1d), policy=policy, cache_dir=cache_dir)

    status_path = write_global_outputs(run_dir=str(out_dir), decisions=decisions)

    passed = sorted([s for s, d in decisions.items() if d.status == "PASS"])
    failed = sorted([s for s, d in decisions.items() if d.status == "FAIL"])
    skipped = sorted([s for s, d in decisions.items() if d.status == "SKIP"])

    _write_lines(out_dir / "pass_symbols_1d.txt", passed)
    _write_lines(out_dir / "fail_symbols_1d.txt", failed)
    _write_lines(out_dir / "skip_symbols_1d.txt", skipped)

    (out_dir / "run_summary.json").write_text(
        json.dumps(
            {
                "source": source,
                "stock_dir": str(stock_dir),
                "raw_root": str(raw_root) if raw_root is not None else "",
                "policy": asdict(policy),
                "counts": {"total": len(symbols), "PASS": len(passed), "FAIL": len(failed), "SKIP": len(skipped)},
                "outputs": {
                    "global_gate_status": str(status_path),
                    "pass_symbols": str(out_dir / "pass_symbols_1d.txt"),
                    "fail_symbols": str(out_dir / "fail_symbols_1d.txt"),
                    "skip_symbols": str(out_dir / "skip_symbols_1d.txt"),
                },
            },
            ensure_ascii=False,
            indent=2,
            default=str,
        )
        + "\n",
        encoding="utf-8",
    )

    print(str(out_dir))


if __name__ == "__main__":
    main()
