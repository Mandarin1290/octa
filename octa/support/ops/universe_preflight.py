from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

DEFAULT_REQUIRED_TFS: Tuple[str, ...] = ("1D", "1H", "30M", "5M", "1M")


@dataclass(frozen=True)
class PreflightResult:
    root: str
    required_tfs: Tuple[str, ...]
    strict: bool
    follow_symlinks: bool
    scanned_files: int
    inventory: Dict[str, Dict[str, List[str]]]
    all_paths_by_symbol: Dict[str, List[str]]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _normalize_required_tfs(required_tfs: Iterable[str]) -> Tuple[str, ...]:
    out: List[str] = []
    seen = set()
    for tf in required_tfs:
        norm = str(tf).strip().upper()
        if norm and norm not in seen:
            out.append(norm)
            seen.add(norm)
    return tuple(out)


def _detect_root() -> Path:
    env_root = os.getenv("OCTA_PARQUET_ROOT") or os.getenv("PARQUET_ROOT")
    if env_root:
        return Path(env_root)
    candidates = [Path("raw"), Path("data"), Path("datasets"), Path("data/raw"), Path("datasets/raw")]
    for c in candidates:
        if c.exists():
            return c
    raise FileNotFoundError("No parquet root found (set --root or OCTA_PARQUET_ROOT)")


def _parse_symbol_tf(path: Path, required: Sequence[str]) -> Optional[Tuple[str, str]]:
    if path.suffix.lower() != ".parquet":
        return None
    stem = path.stem
    idx = stem.rfind("_")
    if idx <= 0:
        return None
    symbol = stem[:idx].strip().upper()
    tf = stem[idx + 1 :].strip().upper()
    if not symbol:
        return None
    if tf not in required:
        return None
    return symbol, tf


def scan_inventory(
    root: Path,
    required_tfs: Iterable[str],
    strict: bool,
    follow_symlinks: bool = False,
) -> PreflightResult:
    required = _normalize_required_tfs(required_tfs)
    inventory: Dict[str, Dict[str, List[str]]] = {}
    all_paths_by_symbol: Dict[str, List[str]] = {}
    scanned = 0

    # Always scan recursively; strict controls grouping/acceptance, not discovery.
    for dirpath, dirnames, filenames in os.walk(root, followlinks=bool(follow_symlinks)):
        dirnames.sort()
        for filename in sorted(filenames):
            if not str(filename).lower().endswith(".parquet"):
                continue
            path = Path(dirpath) / filename
            scanned += 1
            parsed = _parse_symbol_tf(path, required)
            if not parsed:
                continue
            symbol, tf = parsed
            s_path = str(path)
            inventory.setdefault(symbol, {}).setdefault(tf, []).append(s_path)
            all_paths_by_symbol.setdefault(symbol, []).append(s_path)

    for sym in inventory:
        for tf in inventory[sym]:
            inventory[sym][tf] = sorted(inventory[sym][tf])
    for sym in all_paths_by_symbol:
        all_paths_by_symbol[sym] = sorted(dict.fromkeys(all_paths_by_symbol[sym]))

    if strict:
        # Enforce per-symbol root dir: only count TFs that live in the same directory
        # as the selected 1D file (no cross-directory upgrades).
        for sym, by_tf in list(inventory.items()):
            root_dir: Optional[Path] = None
            tf_1d = by_tf.get("1D", [])
            if tf_1d:
                root_path = Path(sorted(tf_1d, key=lambda p: (len(p), p))[0])
                root_dir = root_path.parent
            if root_dir is None:
                continue
            filtered: Dict[str, List[str]] = {}
            for tf, paths in by_tf.items():
                kept = [p for p in paths if Path(p).parent == root_dir]
                if kept:
                    filtered[tf] = kept
            inventory[sym] = filtered

    return PreflightResult(
        root=str(root),
        required_tfs=required,
        strict=bool(strict),
        follow_symlinks=bool(follow_symlinks),
        scanned_files=scanned,
        inventory=inventory,
        all_paths_by_symbol=all_paths_by_symbol,
    )


def _write_json(path: Path, obj: object) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _write_jsonl(path: Path, rows: Sequence[Dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
            fh.write("\n")


def _pick_representative(paths: Sequence[str]) -> str:
    return sorted(paths, key=lambda p: (len(p), p))[0]


KNOWN_ASSET_CLASSES: Tuple[str, ...] = ("equities", "futures", "fx", "crypto", "etfs", "options", "rates", "indices")
ASSET_CLASS_ALIASES: Dict[str, str] = {
    "equity": "equities",
    "stock": "equities",
    "stocks": "equities",
    "stock_parquet": "equities",
    "future": "futures",
    "forex": "fx",
    "etf": "etfs",
    "index": "indices",
}


def _derive_asset_class_from_path(path: Path) -> Optional[str]:
    parts = [str(p).strip().lower() for p in path.parts if str(p).strip()]
    for part in parts:
        alias = ASSET_CLASS_ALIASES.get(part)
        if alias:
            return alias
        if part in KNOWN_ASSET_CLASSES:
            return part
    return None


def _has_recognizable_time_axis(path: Path) -> bool:
    """True when parquet exposes a known time column or datetime index."""
    time_candidates = {"timestamp", "datetime", "date", "time"}
    try:
        import pyarrow.parquet as pq
        import pyarrow.types as pat

        pf = pq.ParquetFile(str(path))
        arrow_schema = pf.schema_arrow
        fields = {str(f.name): f for f in arrow_schema}
        # Accept explicit time-like columns only when dtype is temporal.
        for name, field in fields.items():
            if str(name).lower() in time_candidates and (pat.is_timestamp(field.type) or pat.is_date(field.type)):
                return True

        # Accept datetime/date index persisted by pandas parquet metadata.
        md = arrow_schema.metadata or {}
        pandas_meta_raw = md.get(b"pandas")
        if pandas_meta_raw:
            pandas_meta = json.loads(pandas_meta_raw.decode("utf-8"))
            for idx_col in pandas_meta.get("index_columns") or []:
                if isinstance(idx_col, str):
                    field = fields.get(idx_col)
                    if field is not None and (pat.is_timestamp(field.type) or pat.is_date(field.type)):
                        return True
                elif isinstance(idx_col, dict):
                    # RangeIndex metadata is non-temporal and should not count.
                    continue
    except Exception:
        pass

    # Fallback for engines/files without parseable pyarrow metadata.
    try:
        import pandas as pd

        df = pd.read_parquet(path)
        if isinstance(df.index, pd.DatetimeIndex):
            return True
    except Exception:
        pass
    return False


def write_outputs(result: PreflightResult, outdir: Path) -> Dict[str, object]:
    outdir.mkdir(parents=True, exist_ok=True)
    timestamp = _utc_now_iso()

    symbols = sorted(result.inventory.keys())
    required = result.required_tfs

    trainable: List[str] = []
    excluded_rows: List[Dict[str, object]] = []
    inventory_rows: List[Dict[str, object]] = []
    exclusion_reasons: Dict[str, int] = {}
    invalid_time_by_symbol: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    asset_info_by_symbol: Dict[str, Dict[str, object]] = {}

    for sym in symbols:
        observed = result.inventory.get(sym, {})
        all_paths = sorted(result.all_paths_by_symbol.get(sym, []))
        path_assets: List[Dict[str, Optional[str]]] = []
        for p in all_paths:
            path_assets.append({"path": p, "asset_class": _derive_asset_class_from_path(Path(p))})
        first_path = all_paths[0] if all_paths else None
        first_asset = _derive_asset_class_from_path(Path(first_path)) if first_path else None
        unique_assets = sorted({str(x["asset_class"]) for x in path_assets if x["asset_class"]})
        unknown_paths = sorted([str(x["path"]) for x in path_assets if not x["asset_class"]])
        offending_paths = sorted([str(x["path"]) for x in path_assets if x["asset_class"] != first_asset])
        asset_info_by_symbol[sym] = {
            "all_paths": all_paths,
            "first_path": first_path,
            "first_asset": first_asset,
            "unique_assets": unique_assets,
            "unknown_paths": unknown_paths,
            "offending_paths": offending_paths,
        }
        for tf, paths in observed.items():
            if not paths:
                continue
            rep = _pick_representative(paths)
            if not _has_recognizable_time_axis(Path(rep)):
                invalid_time_by_symbol[sym].append(
                    {
                        "timeframe": str(tf),
                        "offending_path": str(rep),
                    }
                )

    for sym in symbols:
        observed = result.inventory.get(sym, {})
        asset_info = asset_info_by_symbol.get(sym, {})
        first_asset = asset_info.get("first_asset")
        unique_assets = list(asset_info.get("unique_assets") or [])
        unknown_paths = list(asset_info.get("unknown_paths") or [])
        offending_paths = list(asset_info.get("offending_paths") or [])
        first_path = asset_info.get("first_path")
        observed_tfs = sorted(observed.keys())
        missing = [tf for tf in required if tf not in observed]
        invalid_time = sorted(invalid_time_by_symbol.get(sym, []), key=lambda x: (x["timeframe"], x["offending_path"]))
        if not first_asset or unknown_paths:
            exclusion_reasons["undetermined_asset_class"] = exclusion_reasons.get("undetermined_asset_class", 0) + 1
            excluded_rows.append(
                {
                    "symbol": sym,
                    "reason": "undetermined_asset_class",
                    "first_path": first_path,
                    "observed_tfs": observed_tfs,
                    "unknown_asset_paths": unknown_paths,
                    "sample_paths": {tf: _pick_representative(paths) for tf, paths in observed.items()},
                }
            )
        elif len(unique_assets) > 1 or offending_paths:
            exclusion_reasons["mixed_asset_class"] = exclusion_reasons.get("mixed_asset_class", 0) + 1
            excluded_rows.append(
                {
                    "symbol": sym,
                    "reason": "mixed_asset_class",
                    "first_path": first_path,
                    "asset_classes": unique_assets,
                    "offending_paths": offending_paths,
                    "observed_tfs": observed_tfs,
                    "sample_paths": {tf: _pick_representative(paths) for tf, paths in observed.items()},
                }
            )
        elif result.strict and invalid_time:
            exclusion_reasons["missing_time_column"] = exclusion_reasons.get("missing_time_column", 0) + 1
            excluded_rows.append(
                {
                    "symbol": sym,
                    "reason": "missing_time_column",
                    "observed_tfs": observed_tfs,
                    "sample_paths": {tf: _pick_representative(paths) for tf, paths in observed.items()},
                    "invalid_time_axes": invalid_time,
                }
            )
        elif result.strict and missing:
            key = ",".join(missing)
            exclusion_reasons[key] = exclusion_reasons.get(key, 0) + 1
            excluded_rows.append(
                {
                    "symbol": sym,
                    "reason": "missing_required_timeframes",
                    "missing_tfs": missing,
                    "observed_tfs": observed_tfs,
                    "sample_paths": {tf: _pick_representative(paths) for tf, paths in observed.items()},
                }
            )
        else:
            trainable.append(sym)

        inventory_rows.append(
            {
                "symbol": sym,
                "asset_class": first_asset if first_asset else "unknown",
                "tfs": {tf: list(paths) for tf, paths in sorted(observed.items())},
            }
        )

    summary = {
        "timestamp": timestamp,
        "root": result.root,
        "required_tfs": list(required),
        "strict": result.strict,
        "follow_symlinks": bool(result.follow_symlinks),
        "scanned_files": result.scanned_files,
        "total_symbols": len(symbols),
        "trainable_count": len(trainable),
        "excluded_count": len(excluded_rows),
        "top_exclusion_reasons": [{"reason": k, "count": v} for k, v in sorted(exclusion_reasons.items(), key=lambda x: (-x[1], x[0]))],
    }

    summary_path = outdir / "summary.json"
    trainable_path = outdir / "trainable_symbols.txt"
    excluded_path = outdir / "excluded.jsonl"
    inventory_path = outdir / "inventory.jsonl"

    _write_json(summary_path, summary)
    trainable_path.write_text("\n".join(trainable) + ("\n" if trainable else ""), encoding="utf-8")
    _write_jsonl(excluded_path, excluded_rows)
    _write_jsonl(inventory_path, inventory_rows)

    return {
        "summary": str(summary_path),
        "trainable_symbols": str(trainable_path),
        "excluded": str(excluded_path),
        "inventory": str(inventory_path),
    }


def _write_runtime_info(outdir: Path) -> None:
    py_path = outdir / "python_version.txt"
    freeze_path = outdir / "pip_freeze.txt"
    py = subprocess.run([sys.executable, "-V"], capture_output=True, text=True)
    py_path.write_text((py.stdout or "") + (py.stderr or ""), encoding="utf-8")
    pip = subprocess.run([sys.executable, "-m", "pip", "freeze"], capture_output=True, text=True)
    freeze_path.write_text(pip.stdout or "", encoding="utf-8")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--root", default=None, help="Parquet root")
    p.add_argument("--required-tfs", default=",".join(DEFAULT_REQUIRED_TFS))
    p.add_argument("--outdir", default=None)
    p.add_argument("--out", dest="outdir", default=None)
    strict_group = p.add_mutually_exclusive_group()
    strict_group.add_argument("--strict", dest="strict", action="store_true", default=True)
    strict_group.add_argument("--no-strict", dest="strict", action="store_false")
    p.add_argument("--follow-symlinks", action="store_true", default=False)
    args = p.parse_args()

    outdir = Path(args.outdir) if args.outdir else Path("octa") / "var" / "evidence" / f"universe_preflight_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"

    try:
        root = Path(args.root) if args.root else _detect_root()
        if not root.exists():
            raise FileNotFoundError(f"Parquet root not found: {root}")
        print(f"Using root: {root}")
        required = [t.strip().upper() for t in str(args.required_tfs).split(",") if t.strip()]
        result = scan_inventory(root, required, strict=args.strict, follow_symlinks=bool(args.follow_symlinks))
        paths = write_outputs(result, outdir)
        _write_runtime_info(outdir)

        print("Universe preflight complete")
        print(f"- total_symbols: {len(result.inventory)}")
        trainable_count = len(Path(paths["trainable_symbols"]).read_text(encoding="utf-8").splitlines())
        excluded_count = sum(1 for _ in Path(paths["excluded"]).read_text(encoding="utf-8").splitlines())
        print(f"- trainable_count: {trainable_count}")
        print(f"- excluded_count: {excluded_count}")
        print(f"- summary: {paths['summary']}")
        print(f"- trainable_symbols: {paths['trainable_symbols']}")
        print(f"- excluded: {paths['excluded']}")
        print(f"- inventory: {paths['inventory']}")
    except Exception as exc:
        outdir.mkdir(parents=True, exist_ok=True)
        failed = outdir / "FAILED.txt"
        failed.write_text(f"{type(exc).__name__}: {exc}\n", encoding="utf-8")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
