#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from octa_training.core.config import load_config
from octa_training.core.pipeline import train_evaluate_package
from octa_training.core.state import StateRegistry


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "model_dump"):
        return value.model_dump()
    if hasattr(value, "dict"):
        return value.dict()
    return str(value)


def _load_symbols(path: Path) -> list[str]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise SystemExit(f"symbols file must be a JSON list: {path}")
    out = []
    for item in raw:
        sym = str(item or "").strip().upper()
        if sym:
            out.append(sym)
    return sorted(dict.fromkeys(out))


def _config_fingerprint(cfg: Any) -> str:
    raw = cfg.model_dump() if hasattr(cfg, "model_dump") else (cfg.dict() if hasattr(cfg, "dict") else {})
    return hashlib.sha256(json.dumps(raw, sort_keys=True, default=_json_default).encode("utf-8")).hexdigest()


def _dataset_fingerprint(paths: list[Path]) -> str:
    payload: list[list[Any]] = []
    for path in sorted(paths):
        st = path.stat()
        payload.append([str(path.resolve()), int(st.st_size), int(st.st_mtime_ns)])
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def _result_to_dict(symbol: str, res: Any, parquet_path: Path) -> dict[str, Any]:
    gate = None
    metrics = None
    if getattr(res, "gate_result", None) is not None:
        gate_obj = res.gate_result
        gate = gate_obj.model_dump() if hasattr(gate_obj, "model_dump") else (gate_obj.dict() if hasattr(gate_obj, "dict") else gate_obj)
    if getattr(res, "metrics", None) is not None:
        metrics_obj = res.metrics
        metrics = metrics_obj.model_dump() if hasattr(metrics_obj, "model_dump") else (metrics_obj.dict() if hasattr(metrics_obj, "dict") else metrics_obj)
    return {
        "symbol": symbol,
        "parquet_path": str(parquet_path),
        "passed": bool(getattr(res, "passed", False)),
        "error": getattr(res, "error", None),
        "gate": gate,
        "metrics": metrics,
        "run_id": getattr(res, "run_id", None),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Run a stock 1D gate sweep on the repaired runtime feature regime.")
    ap.add_argument("--config", default="octa_training/config/training.yaml")
    ap.add_argument("--symbols-json", required=True, help="JSON file with a list of 1D symbols")
    ap.add_argument("--raw-dir", default="raw/Stock_parquet")
    ap.add_argument("--run-id", default=None)
    args = ap.parse_args()

    cfg = load_config(args.config)
    state = StateRegistry(str(cfg.paths.state_dir))
    run_id = str(args.run_id).strip() if args.run_id else datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    symbols = _load_symbols(Path(args.symbols_json))
    raw_dir = Path(args.raw_dir)
    if not raw_dir.exists():
        raise SystemExit(f"raw-dir not found: {raw_dir}")

    artifacts_dir = Path(cfg.paths.reports_dir) / "cascade" / run_id / "global_gate_1d"
    manifest_dir = Path(cfg.paths.reports_dir) / "gates" / run_id
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    manifest_dir.mkdir(parents=True, exist_ok=True)

    results: list[dict[str, Any]] = []
    parquet_paths: list[Path] = []
    pass_syms: list[str] = []
    fail_syms: list[str] = []
    err_syms: list[str] = []

    for idx, symbol in enumerate(symbols, start=1):
        parquet_path = raw_dir / f"{symbol}_1D.parquet"
        print(f"[{idx}/{len(symbols)}] {symbol} -> {parquet_path}")
        if not parquet_path.exists():
            err_syms.append(symbol)
            results.append({"symbol": symbol, "parquet_path": str(parquet_path), "passed": False, "error": "missing_parquet", "gate": None, "metrics": None, "run_id": run_id})
            continue
        parquet_paths.append(parquet_path)
        try:
            res = train_evaluate_package(symbol, cfg, state, run_id=run_id, safe_mode=True, parquet_path=str(parquet_path), dataset="stocks")
            item = _result_to_dict(symbol, res, parquet_path)
            results.append(item)
            if item["error"]:
                err_syms.append(symbol)
            elif item["passed"]:
                pass_syms.append(symbol)
            else:
                fail_syms.append(symbol)
        except Exception as exc:
            err_syms.append(symbol)
            results.append({"symbol": symbol, "parquet_path": str(parquet_path), "passed": False, "error": f"{type(exc).__name__}: {exc}", "gate": None, "metrics": None, "run_id": run_id})

    pass_syms = sorted(dict.fromkeys(pass_syms))
    fail_syms = sorted(dict.fromkeys(fail_syms))
    err_syms = sorted(dict.fromkeys(err_syms))

    (artifacts_dir / "pass_symbols_1D.txt").write_text(("\n".join(pass_syms) + "\n") if pass_syms else "", encoding="utf-8")
    (artifacts_dir / "fail_symbols_1D.txt").write_text(("\n".join(fail_syms) + "\n") if fail_syms else "", encoding="utf-8")
    (artifacts_dir / "err_symbols_1D.txt").write_text(("\n".join(err_syms) + "\n") if err_syms else "", encoding="utf-8")

    summary = {
        "run_id": run_id,
        "created_utc": _utc_now_iso(),
        "dataset": "stocks",
        "config_path": str(args.config),
        "symbols_json": str(Path(args.symbols_json).resolve()),
        "raw_dir": str(raw_dir.resolve()),
        "counts": {
            "requested": len(symbols),
            "pass": len(pass_syms),
            "fail": len(fail_syms),
            "err": len(err_syms),
        },
        "results_path": str(artifacts_dir / "results.json"),
    }
    (artifacts_dir / "results.json").write_text(json.dumps(results, ensure_ascii=False, indent=2, default=_json_default) + "\n", encoding="utf-8")
    (artifacts_dir / "run_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=_json_default) + "\n", encoding="utf-8")

    manifest = {
        "run_id": run_id,
        "created_utc": _utc_now_iso(),
        "reports_dir": str(Path(cfg.paths.reports_dir).resolve()),
        "dataset": "stocks",
        "asset_class": "stocks",
        "timeframes": ["1D"],
        "artifacts_dir": str(artifacts_dir.resolve()),
        "pass_files": {"1D": "pass_symbols_1D.txt"},
        "fail_files": {"1D": "fail_symbols_1D.txt"},
        "err_files": {"1D": "err_symbols_1D.txt"},
        "config_fingerprint": _config_fingerprint(cfg),
        "dataset_fingerprint": _dataset_fingerprint(parquet_paths),
        "symbol_universe_fingerprint": hashlib.sha256(json.dumps(symbols, sort_keys=True).encode("utf-8")).hexdigest(),
        "manifest_dir": str(manifest_dir.resolve()),
        "notes": "safe_mode gate sweep on repaired stock 1D feature regime",
    }
    (manifest_dir / "gate_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=_json_default) + "\n", encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"Wrote gate manifest: {manifest_dir / 'gate_manifest.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
