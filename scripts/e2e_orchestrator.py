#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

TIMEFRAMES = ["1H", "30m", "5m", "1m"]
ALL_TFS_FOR_SCAN = ["1D"] + TIMEFRAMES


def _passlist_name(timeframe: str) -> str:
    tf = timeframe.strip()
    if tf == "1H":
        return "pass_1h.txt"
    if tf == "30m":
        return "pass_30m.txt"
    if tf == "5m":
        return "pass_5m.txt"
    if tf == "1m":
        return "pass_1m.txt"
    return f"pass_{tf}.txt"


def read_lines(p: Path) -> List[str]:
    if not p.exists():
        return []
    return [l.strip() for l in p.read_text(encoding="utf-8").splitlines() if l.strip()]


def write_lines(p: Path, lines: List[str]) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def _now_tag() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y%m%dT%H%M%SZ")


def _run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, capture_output=True, text=True)


def _extract_passed(output: str) -> Optional[bool]:
    # scripts/train_and_save.py prints: "Pipeline result symbol=XYZ passed=True"
    marker = "Pipeline result symbol="
    if marker not in output:
        return None
    for line in output.splitlines()[::-1]:
        if line.startswith(marker) and "passed=" in line:
            tail = line.split("passed=", 1)[1].strip()
            if tail.startswith("True"):
                return True
            if tail.startswith("False"):
                return False
    return None


def _scan_raw_for_timeframe(raw_root: Path, timeframe: str) -> dict[str, Path]:
    out: dict[str, Path] = {}
    if not raw_root.exists():
        return out
    # Support both standard convention (SYMBOL_{TF}.parquet) and index convention
    # (SYMBOL_full_1hour.parquet etc.).
    tf = str(timeframe)
    idx_bar = {
        "1D": "1day",
        "1H": "1hour",
        "30m": "30min",
        "5m": "5min",
        "1m": "1min",
    }.get(tf)
    suffixes = [
        f"_{tf}.parquet",
        f"_{tf.lower()}.parquet",
        f"_{tf.upper()}.parquet",
    ]
    if idx_bar:
        suffixes.extend([
            f"_full_{idx_bar}.parquet",
            f"_full_{idx_bar.lower()}.parquet",
        ])
    candidates: list[Path] = []
    for suf in suffixes:
        candidates.extend(raw_root.rglob(f"*{suf}"))
    for p in sorted(set(candidates)):
        name = p.name
        suf = next((s for s in suffixes if name.endswith(s)), "")
        if not suf:
            continue
        sym = name[: -len(suf)].strip()
        if not sym:
            continue
        out.setdefault(sym, p)
    return out


def _build_raw_index(raw_root: Path) -> dict[str, dict[str, Path]]:
    idx: dict[str, dict[str, Path]] = {}
    for tf in ALL_TFS_FOR_SCAN:
        idx[tf] = _scan_raw_for_timeframe(raw_root, tf)
    return idx


def train_symbol(
    symbol: str,
    timeframe: str,
    parquet_dir: Path,
    parquet_index: Optional[dict[str, dict[str, Path]]],
    run_id: str,
    config_path: Optional[str],
    safe_mode: bool,
    fast: bool,
) -> bool:
    parquet: Optional[Path] = None
    if parquet_index is not None:
        parquet = parquet_index.get(timeframe, {}).get(symbol)
    if parquet is None:
        # Fallback: search in parquet_dir
        tf_suffix = timeframe if timeframe != "1H" else "1H"
        search_patterns = [
            parquet_dir / f"{symbol}_{tf_suffix}.parquet",
            parquet_dir / f"{symbol}_{tf_suffix.lower()}.parquet",
        ]
        parquet = next((p for p in search_patterns if p.exists()), None)
    if parquet is None:
        print(f"[{symbol}/{timeframe}] parquet not found")
        return False

    cmd = [
        "python3",
        "scripts/train_and_save.py",
        "--symbol",
        symbol,
        "--parquet",
        str(parquet),
        "--version",
        f"cascade-{timeframe}",
        "--seed",
        "42",
        "--cv-folds",
        "1",
        "--run-id",
        f"{run_id}__{timeframe}",
    ]
    if config_path:
        cmd += ["--config", config_path]
    if safe_mode:
        cmd.append("--safe-mode")
    if fast:
        cmd.append("--fast")

    print("Running:", " ".join(cmd))
    proc = _run(cmd)
    out = (proc.stdout or "") + "\n" + (proc.stderr or "")
    print(out)
    passed = _extract_passed(out)
    return bool(passed)


def _diagnose_stage(
    *,
    passlist: Path,
    timeframe: str,
    config_path: Optional[str],
    parquet_dir: Path,
    raw_root: Optional[Path],
    out_dir: Path,
    limit: int,
) -> None:
    diag_dir = out_dir / "diagnostics" / "fast_reason_report"
    diag_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        "python3",
        "scripts/collect_1h_gate_reasons_fast.py",
        "--passlist",
        str(passlist),
        "--timeframe",
        timeframe,
        "--out-dir",
        str(diag_dir),
    ]
    if raw_root is not None:
        cmd += ["--raw-root", str(raw_root)]
    else:
        cmd += ["--parquet-dir", str(parquet_dir)]
    if config_path:
        cmd += ["--config", config_path]
    if limit > 0:
        cmd += ["--limit", str(limit)]
    print("Auto-diagnose:", " ".join(cmd))
    proc = _run(cmd)
    print((proc.stdout or "") + "\n" + (proc.stderr or ""))
    if proc.returncode != 0:
        print(f"Auto-diagnose failed (exit {proc.returncode}).")


def run_stage(
    *,
    input_list: Path,
    timeframe: str,
    stage_dir: Path,
    parquet_dir: Path,
    parquet_index: Optional[dict[str, dict[str, Path]]],
    raw_root: Optional[Path],
    run_id: str,
    config_path: Optional[str],
    safe_mode: bool,
    fast: bool,
    diagnose_on_zero_pass: bool,
) -> Path:
    symbols = read_lines(input_list)
    missing_symbols: List[str] = []

    if parquet_index is not None:
        available = parquet_index.get(timeframe, {})
        present = [s for s in symbols if s in available]
        missing_symbols = [s for s in symbols if s not in available]
        symbols = present
    passed_symbols: List[str] = []
    start = time.time()

    out_list = stage_dir / _passlist_name(timeframe)
    missing_list = stage_dir / "missing_parquet.txt"
    stage_dir.mkdir(parents=True, exist_ok=True)

    if missing_symbols:
        write_lines(missing_list, missing_symbols)

    for i, s in enumerate(symbols, 1):
        print(f"[{timeframe}] ({i}/{len(symbols)}) {s}")
        ok = train_symbol(
            s,
            timeframe,
            parquet_dir=parquet_dir,
            parquet_index=parquet_index,
            run_id=run_id,
            config_path=config_path,
            safe_mode=safe_mode,
            fast=fast,
        )
        if ok:
            passed_symbols.append(s)
        time.sleep(0.2)

    write_lines(out_list, passed_symbols)
    summary = {
        "timeframe": timeframe,
        "input_list": str(input_list),
        "n_input": len(symbols) + len(missing_symbols),
        "n_missing_parquet": len(missing_symbols),
        "n_pass": len(passed_symbols),
        "passlist": str(out_list),
        "missing_parquet_list": str(missing_list) if missing_symbols else "",
        "safe_mode": bool(safe_mode),
        "fast": bool(fast),
        "seconds": round(time.time() - start, 3),
    }
    (stage_dir / "stage_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Stage {timeframe} done: {len(passed_symbols)}/{len(symbols)} passed in {time.time()-start:.1f}s")

    if diagnose_on_zero_pass and len(symbols) > 0 and len(passed_symbols) == 0:
        _diagnose_stage(
            passlist=input_list,
            timeframe=timeframe,
            config_path=config_path,
            parquet_dir=parquet_dir,
            raw_root=raw_root,
            out_dir=stage_dir,
            limit=0,
        )

    return out_list


def _run_global_gate(
    run_dir: Path,
    stock_dir: Path,
    raw_root: Optional[Path],
    limit: int,
    fred: bool,
    edgar: bool,
    edgar_user_agent: str,
    edgar_rpm: int,
) -> Path:
    cmd = [
        "python3",
        "scripts/run_global_gate_1d.py",
        "--out-dir",
        str(run_dir),
    ]
    if raw_root is not None:
        cmd += ["--raw-root", str(raw_root)]
    else:
        cmd += ["--stock-dir", str(stock_dir)]
    if limit and limit > 0:
        cmd += ["--limit", str(limit)]
    if fred:
        cmd.append("--fred")
    if edgar:
        cmd.append("--edgar")
        if str(edgar_user_agent).strip():
            cmd += ["--edgar-user-agent", str(edgar_user_agent).strip()]
        if int(edgar_rpm) > 0:
            cmd += ["--edgar-rpm", str(int(edgar_rpm))]
    print("Global Gate:", " ".join(cmd))
    proc = _run(cmd)
    print((proc.stdout or "") + "\n" + (proc.stderr or ""))
    if proc.returncode != 0:
        raise SystemExit(f"Global Gate failed (exit {proc.returncode}).")
    passlist = run_dir / "pass_symbols_1d.txt"
    if not passlist.exists():
        raise SystemExit(f"Global Gate did not produce passlist: {passlist}")
    return passlist


def _materialize_1d_stage(*, base_out: Path, passlist_1d: Path, source: str) -> Path:
    stage_dir = base_out / "1D"
    stage_dir.mkdir(parents=True, exist_ok=True)
    out_list = stage_dir / "pass_1d.txt"
    # Copy (not symlink) to keep artifacts portable.
    out_list.write_text(passlist_1d.read_text(encoding="utf-8"), encoding="utf-8")
    symbols = read_lines(out_list)
    summary = {
        "timeframe": "1D",
        "source": source,
        "input_passlist": str(passlist_1d),
        "n_pass": len(symbols),
        "passlist": str(out_list),
    }
    (stage_dir / "stage_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return out_list


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", default="", help="Run id; default is UTC timestamp")
    ap.add_argument("--out-dir", default="reports/cascade", help="Base output directory")
    ap.add_argument("--parquet-dir", default="raw/Stock_parquet", help="Legacy fallback parquet dir (used if --no-scan-raw)")
    ap.add_argument("--raw-root", default="raw", help="Root folder to scan for *_{TF}.parquet")
    ap.add_argument("--no-scan-raw", action="store_false", dest="scan_raw", help="Disable raw scan; use only --parquet-dir")
    ap.set_defaults(scan_raw=True)
    ap.add_argument("--config", default=None, help="Training config YAML (optional)")
    ap.add_argument(
        "--hf",
        action="store_true",
        help="Use HF cascade preset config (defaults to configs/cascade_hf.yaml if --config not set)",
    )
    ap.add_argument("--safe-mode", action="store_true", default=True)
    ap.add_argument("--no-safe-mode", action="store_false", dest="safe_mode", help="Disable safe-mode (not recommended)")
    ap.add_argument("--fast", action="store_true", help="Use fast pipeline mode for stages")
    ap.add_argument("--diagnose-on-zero-pass", action="store_true", default=True)
    ap.add_argument("--pass-1d", default="", help="Existing 1D passlist (skip global gate)")
    ap.add_argument("--run-global-gate", action="store_true", help="Run 1D Global Gate first")
    ap.add_argument("--stock-dir", default="raw/Stock_parquet")
    ap.add_argument("--gate-limit", type=int, default=0, help="Limit symbols in global gate (0=no limit)")
    ap.add_argument("--gate-fred", action="store_true", help="Enable FRED enrichment in global gate")
    ap.add_argument("--gate-edgar", action="store_true", help="Enable EDGAR enrichment in global gate")
    ap.add_argument(
        "--gate-edgar-user-agent",
        default="OCTA/1.0 (research; contact=ops@example.com)",
        help="EDGAR User-Agent string (required by SEC)",
    )
    ap.add_argument("--gate-edgar-rpm", type=int, default=8, help="EDGAR requests per minute")
    args = ap.parse_args()

    run_id = str(args.run_id).strip() or _now_tag()
    base_out = Path(args.out_dir) / run_id
    base_out.mkdir(parents=True, exist_ok=True)
    parquet_dir = Path(args.parquet_dir)

    # Config selection policy: explicit --config wins; otherwise --hf chooses the HF preset.
    config_path = args.config
    if (not config_path) and bool(args.hf):
        config_path = "configs/cascade_hf.yaml"

    raw_root = Path(args.raw_root)
    parquet_index = _build_raw_index(raw_root) if bool(args.scan_raw) else None

    if args.run_global_gate:
        gate_dir = base_out / "global_gate_1d"
        pass_1d = _run_global_gate(
            run_dir=gate_dir,
            stock_dir=Path(args.stock_dir),
            raw_root=raw_root if bool(args.scan_raw) else None,
            limit=int(args.gate_limit),
            fred=bool(args.gate_fred),
            edgar=bool(args.gate_edgar),
            edgar_user_agent=str(args.gate_edgar_user_agent),
            edgar_rpm=int(args.gate_edgar_rpm),
        )
        current_input = _materialize_1d_stage(base_out=base_out, passlist_1d=pass_1d, source="global_gate_1d")
        # Filter the materialized 1D passlist to only symbols that have all required TFs
        if parquet_index is not None:
            p = current_input
            syms = read_lines(p)
            kept: list[str] = []
            missing: list[str] = []
            for s in syms:
                ok = True
                for tf in TIMEFRAMES:
                    if s not in (parquet_index.get(tf, {}) or {}):
                        ok = False
                        break
                if ok:
                    kept.append(s)
                else:
                    missing.append(s)
            if missing:
                # overwrite missing_parquet in 1D stage dir
                stage_dir = base_out / "1D"
                stage_dir.mkdir(parents=True, exist_ok=True)
                (stage_dir / "missing_parquet.txt").write_text("\n".join(missing) + "\n", encoding="utf-8")
            # overwrite pass_1d with filtered list
            (base_out / "1D" / "pass_1d.txt").write_text("\n".join(kept) + ("\n" if kept else ""), encoding="utf-8")
            # update stage summary
            summary = {
                "timeframe": "1D",
                "source": "global_gate_1d",
                "input_passlist": str(pass_1d),
                "n_pass": len(kept),
                "passlist": str(base_out / "1D" / "pass_1d.txt"),
            }
            (base_out / "1D" / "stage_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    else:
        if not args.pass_1d:
            raise SystemExit("Provide --pass-1d or use --run-global-gate")
        pass_1d = Path(args.pass_1d)

        current_input = _materialize_1d_stage(base_out=base_out, passlist_1d=pass_1d, source="external_passlist")
        # If scan_raw enabled, filter external passlist to only symbols with complete TFs
        if parquet_index is not None:
            p = current_input
            syms = read_lines(p)
            kept: list[str] = []
            missing: list[str] = []
            for s in syms:
                ok = True
                for tf in TIMEFRAMES:
                    if s not in (parquet_index.get(tf, {}) or {}):
                        ok = False
                        break
                if ok:
                    kept.append(s)
                else:
                    missing.append(s)
            if missing:
                stage_dir = base_out / "1D"
                stage_dir.mkdir(parents=True, exist_ok=True)
                (stage_dir / "missing_parquet.txt").write_text("\n".join(missing) + "\n", encoding="utf-8")
            (base_out / "1D" / "pass_1d.txt").write_text("\n".join(kept) + ("\n" if kept else ""), encoding="utf-8")
            summary = {
                "timeframe": "1D",
                "source": "external_passlist",
                "input_passlist": str(pass_1d),
                "n_pass": len(kept),
                "passlist": str(base_out / "1D" / "pass_1d.txt"),
            }
            (base_out / "1D" / "stage_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    for tf in TIMEFRAMES:
        stage_dir = base_out / tf
        print(f"Starting stage {tf}; input={current_input}; out={stage_dir}")
        current_input = run_stage(
            input_list=current_input,
            timeframe=tf,
            stage_dir=stage_dir,
            parquet_dir=parquet_dir,
            parquet_index=parquet_index,
            raw_root=raw_root if bool(args.scan_raw) else None,
            run_id=run_id,
            config_path=config_path,
            safe_mode=bool(args.safe_mode),
            fast=bool(args.fast),
            diagnose_on_zero_pass=bool(args.diagnose_on_zero_pass),
        )

    # Top-level run summary for operational observability.
    try:
        stage_summaries: list[dict] = []
        for tf in ["1D"] + TIMEFRAMES:
            p = base_out / tf / "stage_summary.json"
            if p.exists():
                stage_summaries.append(json.loads(p.read_text(encoding="utf-8")))
        run_summary = {
            "run_id": run_id,
            "out_dir": str(base_out),
            "raw_root": str(raw_root),
            "scan_raw": bool(args.scan_raw),
            "config": str(config_path or ""),
            "safe_mode": bool(args.safe_mode),
            "fast": bool(args.fast),
            "stages": stage_summaries,
        }
        (base_out / "run_summary.json").write_text(
            json.dumps(run_summary, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
    except Exception:
        pass


if __name__ == '__main__':
    main()
