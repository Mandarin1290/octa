from __future__ import annotations

import argparse
import errno
import hashlib
import json
import os
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

FILENAME_RE = r"^(?P<symbol>[A-Z0-9._-]+)_(?P<tf>1D|1H|30M|5M|1M)\.parquet$"
DEFAULT_REQUIRED_TFS = ("1D", "1H", "30M", "5M", "1M")
ASSET_SCAN_ORDER = ("equities", "etfs", "fx", "futures", "crypto")


@dataclass(frozen=True)
class ParsedName:
    symbol: str
    timeframe: str


@dataclass(frozen=True)
class SourceRecord:
    asset_class: str
    src_dir: Path
    src_path: Path
    parsed: Optional[ParsedName]
    option_filtered: bool
    parse_error: Optional[str]


def parse_filename_strict(name: str) -> Optional[ParsedName]:
    m = re.match(FILENAME_RE, str(name))
    if not m:
        return None
    symbol = str(m.group("symbol")).upper()
    tf = str(m.group("tf")).upper()
    if tf not in DEFAULT_REQUIRED_TFS:
        return None
    return ParsedName(symbol=symbol, timeframe=tf)


def _norm_dir_name(name: str) -> str:
    return "".join(ch for ch in str(name).lower() if ch.isalnum())


def _is_option_like_folder(name: str) -> bool:
    return "option" in str(name).lower()


def _is_option_like_file(name: str) -> bool:
    u = str(name).upper()
    markers = ("OPTION", "OPTIONS", "CHAIN", "OPT", "_C_", "_P_", " C_", " P_")
    return any(m in u for m in markers)


def detect_asset_folders(source_root: Path, ignore_options: bool = True) -> Dict[str, Path]:
    canonical_to_source_keys = {
        "equities": ("Stock_parquet",),
        "etfs": ("ETF_parquet",),
        "fx": ("FX_parquet",),
        "futures": ("Futures_parquet",),
        "crypto": ("Crypto_parquet",),
    }
    available = [p for p in source_root.iterdir() if p.is_dir()]
    if ignore_options:
        available = [p for p in available if not _is_option_like_folder(p.name)]
    by_norm = {_norm_dir_name(p.name): p for p in available}
    out: Dict[str, Path] = {}
    for canonical in ASSET_SCAN_ORDER:
        found: Optional[Path] = None
        for key in canonical_to_source_keys[canonical]:
            p = by_norm.get(_norm_dir_name(key))
            if p is not None:
                found = p
                break
        if found is not None:
            out[canonical] = found
    return out


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _write_json(path: Path, obj: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _append_jsonl(path: Path, row: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
        fh.write("\n")


def _inventory_snapshot(source_root: Path, dest_root: Path, source_folders: Dict[str, Path]) -> Dict[str, object]:
    src: Dict[str, object] = {}
    for asset in ASSET_SCAN_ORDER:
        d = source_folders.get(asset)
        if d is None:
            src[asset] = {"present": False, "dir": None, "parquet_files": 0}
            continue
        count = sum(1 for p in d.iterdir() if p.is_file() and p.suffix.lower() == ".parquet")
        src[asset] = {"present": True, "dir": str(d), "parquet_files": int(count)}

    dest: Dict[str, object] = {}
    for asset in ASSET_SCAN_ORDER:
        d = dest_root / asset
        if not d.exists():
            dest[asset] = {"present": False, "symbol_dirs": 0, "parquet_files": 0}
            continue
        sym_dirs = [p for p in d.iterdir() if p.is_dir()]
        pq = 0
        for sdir in sym_dirs:
            pq += sum(1 for p in sdir.iterdir() if p.is_file() and p.suffix.lower() == ".parquet")
        dest[asset] = {"present": True, "symbol_dirs": int(len(sym_dirs)), "parquet_files": int(pq)}
    return {"source": src, "dest": dest, "source_root": str(source_root), "dest_root": str(dest_root)}


def _scan_records(
    source_folders: Dict[str, Path],
    *,
    ignore_options: bool,
    max_files: Optional[int],
) -> List[SourceRecord]:
    records: List[SourceRecord] = []
    seen = 0
    for asset in ASSET_SCAN_ORDER:
        src_dir = source_folders.get(asset)
        if src_dir is None:
            continue
        files = sorted([p for p in src_dir.iterdir() if p.is_file() and p.suffix.lower() == ".parquet"], key=lambda p: p.name)
        for src in files:
            if max_files is not None and seen >= int(max_files):
                return records
            seen += 1
            option_filtered = bool(ignore_options and _is_option_like_file(src.name))
            parsed = None if option_filtered else parse_filename_strict(src.name)
            parse_error = None
            if not option_filtered and parsed is None:
                parse_error = "invalid_filename_format"
            records.append(
                SourceRecord(
                    asset_class=asset,
                    src_dir=src_dir,
                    src_path=src,
                    parsed=parsed,
                    option_filtered=option_filtered,
                    parse_error=parse_error,
                )
            )
    return records


def _group_records(records: Sequence[SourceRecord]) -> Dict[Tuple[str, str], Dict[str, Path]]:
    out: Dict[Tuple[str, str], Dict[str, Path]] = {}
    for r in records:
        if r.option_filtered or r.parsed is None:
            continue
        key = (r.asset_class, r.parsed.symbol)
        out.setdefault(key, {})[r.parsed.timeframe] = r.src_path
    return out


def _verify_existing(dst: Path, src: Path, mode_requested: str) -> Tuple[bool, Optional[str], str]:
    try:
        if mode_requested == "hardlink":
            if dst.is_symlink():
                target = os.readlink(dst)
                if target == str(src):
                    return True, None, "symlink_fallback"
                return False, "integrity_mismatch_symlink_target", "symlink_fallback"
            sst = src.stat()
            dstst = dst.stat()
            if int(sst.st_ino) == int(dstst.st_ino):
                return True, None, "hardlink"
            return False, "integrity_mismatch_inode", "hardlink"
        if mode_requested == "symlink":
            if not dst.is_symlink():
                return False, "integrity_mismatch_not_symlink", "symlink"
            target = os.readlink(dst)
            if target == str(src):
                return True, None, "symlink"
            return False, "integrity_mismatch_symlink_target", "symlink"
        sst = src.stat()
        dstst = dst.stat()
        if int(sst.st_size) == int(dstst.st_size):
            return True, None, "copy"
        return False, "integrity_mismatch_size", "copy"
    except Exception as exc:
        return False, f"integrity_verify_error:{type(exc).__name__}:{exc}", mode_requested


def _create_link_or_copy(src: Path, dst: Path, mode: str) -> Tuple[str, int]:
    if mode == "hardlink":
        try:
            os.link(src, dst)
            return "hardlink", 0
        except OSError as exc:
            if exc.errno == errno.EXDEV:
                os.symlink(src, dst)
                return "symlink_fallback", 1
            raise
    if mode == "symlink":
        os.symlink(src, dst)
        return "symlink", 0
    shutil.copy2(src, dst)
    return "copy", 0


def _required_tfs(raw: str) -> Tuple[str, ...]:
    out: List[str] = []
    seen = set()
    for tf in str(raw).split(","):
        t = tf.strip().upper()
        if not t:
            continue
        if t not in DEFAULT_REQUIRED_TFS:
            raise ValueError(f"unsupported timeframe: {t}")
        if t not in seen:
            seen.add(t)
            out.append(t)
    if not out:
        raise ValueError("required_tfs cannot be empty")
    return tuple(out)


def build_raw_tree(
    *,
    source_root: Path,
    dest_root: Path,
    mode: str,
    required_tfs: Tuple[str, ...] = DEFAULT_REQUIRED_TFS,
    dry_run: bool = False,
    max_files: Optional[int] = None,
    ignore_options: bool = True,
    evidence_dir: Optional[Path] = None,
) -> Dict[str, object]:
    source_root = source_root.resolve()
    dest_root = dest_root.resolve()
    if evidence_dir is None:
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        evidence_dir = dest_root / "_manifests" / f"build_raw_tree_{run_id}"
    evidence_dir.mkdir(parents=True, exist_ok=True)

    plan_path = evidence_dir / "plan.json"
    completeness_path = evidence_dir / "completeness_report.json"
    manifest_path = evidence_dir / "manifest.jsonl"
    summary_path = evidence_dir / "summary.json"
    hashes_path = evidence_dir / "hashes.sha256"
    if manifest_path.exists():
        manifest_path.unlink()

    source_folders = detect_asset_folders(source_root, ignore_options=ignore_options)
    inventory = _inventory_snapshot(source_root, dest_root, source_folders)
    records = _scan_records(source_folders, ignore_options=ignore_options, max_files=max_files)
    grouped = _group_records(records)

    eligible_symbols: Dict[str, List[str]] = {a: [] for a in ASSET_SCAN_ORDER}
    skipped_incomplete: List[Dict[str, object]] = []
    unparseable_rows: List[Dict[str, object]] = []
    option_filtered_count = 0
    symbols_seen: Dict[str, set[str]] = {a: set() for a in ASSET_SCAN_ORDER}
    tf_distribution_seen: Dict[str, int] = {tf: 0 for tf in required_tfs}

    for r in records:
        if r.option_filtered:
            option_filtered_count += 1
            continue
        if r.parsed is None:
            unparseable_rows.append(
                {
                    "asset_class": r.asset_class,
                    "src": str(r.src_path),
                    "status": "ERROR",
                    "reason": "UNPARSEABLE",
                }
            )
            continue
        symbols_seen[r.asset_class].add(r.parsed.symbol)
        if r.parsed.timeframe in tf_distribution_seen:
            tf_distribution_seen[r.parsed.timeframe] += 1

    for asset in ASSET_SCAN_ORDER:
        syms = sorted(symbols_seen[asset])
        for sym in syms:
            present = set((grouped.get((asset, sym)) or {}).keys())
            missing = [tf for tf in required_tfs if tf not in present]
            if not missing:
                eligible_symbols[asset].append(sym)
            else:
                skipped_incomplete.append(
                    {
                        "asset_class": asset,
                        "symbol": sym,
                        "missing_tfs": missing,
                        "present_tfs": sorted(present),
                        "status": "SKIP_INCOMPLETE",
                    }
                )

    plan_rows: List[Dict[str, object]] = []
    for asset in ASSET_SCAN_ORDER:
        for sym in eligible_symbols[asset]:
            files = grouped[(asset, sym)]
            for tf in required_tfs:
                src = files[tf]
                dst = dest_root / asset / sym / f"{sym}_{tf}.parquet"
                plan_rows.append(
                    {
                        "asset_class": asset,
                        "symbol": sym,
                        "timeframe": tf,
                        "src": str(src),
                        "dst": str(dst),
                        "mode_requested": mode,
                    }
                )
    plan_rows = sorted(plan_rows, key=lambda r: (str(r["asset_class"]), str(r["symbol"]), str(r["timeframe"]), str(r["src"])))
    plan = {
        "source_root": str(source_root),
        "dest_root": str(dest_root),
        "mode_requested": mode,
        "dry_run": bool(dry_run),
        "required_tfs": list(required_tfs),
        "ignore_options": bool(ignore_options),
        "max_files": int(max_files) if max_files is not None else None,
        "inventory": inventory,
        "actions": plan_rows,
    }
    completeness = {
        "required_tfs": list(required_tfs),
        "eligible_symbols": eligible_symbols,
        "skipped_symbols": sorted(skipped_incomplete, key=lambda r: (str(r["asset_class"]), str(r["symbol"]))),
        "unparseable_rows": sorted(unparseable_rows, key=lambda r: (str(r["asset_class"]), str(r["src"]))),
        "totals": {
            "symbols_seen": int(sum(len(v) for v in symbols_seen.values())),
            "eligible_count": int(sum(len(v) for v in eligible_symbols.values())),
            "skipped_incomplete_count": int(len(skipped_incomplete)),
            "unparseable_count": int(len(unparseable_rows)),
            "option_filtered_count": int(option_filtered_count),
        },
    }

    # Planning artifacts written before execution (evidence-first).
    _write_json(plan_path, plan)
    _write_json(completeness_path, completeness)

    counts_by_asset = {a: 0 for a in ASSET_SCAN_ORDER}
    fallback_count = 0
    error_count = 0

    for row in unparseable_rows:
        _append_jsonl(
            manifest_path,
            {
                "src": row["src"],
                "dst": None,
                "asset_class": row["asset_class"],
                "symbol": None,
                "timeframe": None,
                "mode_used": None,
                "status": "ERROR",
                "error": "UNPARSEABLE",
            },
        )
        error_count += 1

    for row in sorted(skipped_incomplete, key=lambda r: (str(r["asset_class"]), str(r["symbol"]))):
        _append_jsonl(
            manifest_path,
            {
                "src": None,
                "dst": None,
                "asset_class": row["asset_class"],
                "symbol": row["symbol"],
                "timeframe": None,
                "mode_used": None,
                "status": "SKIP_INCOMPLETE",
                "missing_tfs": row["missing_tfs"],
            },
        )

    for row in plan_rows:
        src = Path(str(row["src"]))
        dst = Path(str(row["dst"]))
        asset = str(row["asset_class"])
        sym = str(row["symbol"])
        tf = str(row["timeframe"])
        mode_used: str = str(mode)
        status = "DRY_RUN" if dry_run else "OK"
        err: Optional[str] = None
        try:
            if not dry_run:
                dst.parent.mkdir(parents=True, exist_ok=True)
                if dst.exists() or dst.is_symlink():
                    ok, verr, resolved_mode = _verify_existing(dst, src, mode_requested=mode)
                    if not ok:
                        raise RuntimeError(verr or "integrity_mismatch_existing_destination")
                    mode_used = resolved_mode
                    status = "EXISTS_MATCH"
                else:
                    mode_used, fb = _create_link_or_copy(src, dst, mode)
                    fallback_count += int(fb)
                    ok, verr, _ = _verify_existing(dst, src, mode_requested="hardlink" if mode_used == "symlink_fallback" else mode_used)
                    if not ok:
                        raise RuntimeError(verr or "integrity_mismatch_after_write")
            else:
                mode_used = mode
        except Exception as exc:
            status = "ERROR"
            err = f"{type(exc).__name__}:{exc}"
            error_count += 1

        rec = {
            "src": str(src),
            "dst": str(dst),
            "asset_class": asset,
            "symbol": sym,
            "timeframe": tf,
            "mode_used": mode_used,
            "status": status,
        }
        if err:
            rec["error"] = err
        _append_jsonl(manifest_path, rec)
        if status in {"OK", "EXISTS_MATCH", "DRY_RUN"}:
            counts_by_asset[asset] += 1

    summary = {
        "source_root": str(source_root),
        "dest_root": str(dest_root),
        "mode_requested": mode,
        "dry_run": bool(dry_run),
        "required_tfs": list(required_tfs),
        "ignore_options": bool(ignore_options),
        "max_files": int(max_files) if max_files is not None else None,
        "counts_by_asset_class": counts_by_asset,
        "eligible_count": int(completeness["totals"]["eligible_count"]),
        "skipped_incomplete_count": int(completeness["totals"]["skipped_incomplete_count"]),
        "unparseable_count": int(completeness["totals"]["unparseable_count"]),
        "option_filtered_count": int(completeness["totals"]["option_filtered_count"]),
        "symbols_seen": int(completeness["totals"]["symbols_seen"]),
        "timeframe_distribution_seen": tf_distribution_seen,
        "fallback_count": int(fallback_count),
        "error_count": int(error_count),
    }
    _write_json(summary_path, summary)

    hash_targets = [plan_path, completeness_path, manifest_path, summary_path]
    lines = [f"{_sha256_file(p)}  {p}" for p in hash_targets]
    hashes_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {
        "plan": str(plan_path),
        "completeness_report": str(completeness_path),
        "manifest": str(manifest_path),
        "summary": str(summary_path),
        "hashes": str(hashes_path),
        "evidence": str(evidence_dir),
    }


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--source-root", default="/media/n-b/INTENSO")
    p.add_argument("--dest-root", default="/media/n-b/INTENSO/raw")
    p.add_argument("--required-tfs", default="1D,1H,30M,5M,1M")
    p.add_argument("--mode", choices=("hardlink", "symlink", "copy"), default="hardlink")
    p.add_argument("--dry-run", action="store_true", default=False)
    p.add_argument("--max-files", type=int, default=None)
    p.add_argument("--ignore-options", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--evidence-dir", default=None)
    return p.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    source_root = Path(args.source_root)
    dest_root = Path(args.dest_root)
    if not source_root.exists():
        raise SystemExit(f"source root not found: {source_root}")
    req = _required_tfs(str(args.required_tfs))
    evidence_dir = Path(args.evidence_dir) if args.evidence_dir else None
    out = build_raw_tree(
        source_root=source_root,
        dest_root=dest_root,
        mode=str(args.mode),
        required_tfs=req,
        dry_run=bool(args.dry_run),
        max_files=args.max_files,
        ignore_options=bool(args.ignore_options),
        evidence_dir=evidence_dir,
    )
    print(f"DONE dest_root={dest_root} evidence={out['evidence']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
