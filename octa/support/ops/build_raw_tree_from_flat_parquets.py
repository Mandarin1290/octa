from __future__ import annotations

import argparse
import errno
import hashlib
import json
import os
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

FILENAME_RE = r"^(?P<symbol>[A-Z0-9._-]+)_(?P<tf>1D|1H|30M|5M|1M)\.parquet$"
TIMEFRAMES = ("1D", "1H", "30M", "5M", "1M")
ASSET_SCAN_ORDER = ("equities", "etfs", "fx", "futures", "crypto")


@dataclass(frozen=True)
class ParsedName:
    symbol: str
    timeframe: str


def parse_filename_strict(name: str) -> Optional[ParsedName]:
    import re

    m = re.match(FILENAME_RE, str(name))
    if not m:
        return None
    symbol = str(m.group("symbol")).upper()
    tf = str(m.group("tf")).upper()
    if tf not in TIMEFRAMES:
        return None
    return ParsedName(symbol=symbol, timeframe=tf)


def _norm_dir_name(name: str) -> str:
    return "".join(ch for ch in str(name).lower() if ch.isalnum())


def detect_asset_folders(source_root: Path) -> Dict[str, Path]:
    canonical_to_source_keys = {
        "equities": ("Stock_parquet",),
        "etfs": ("ETF_parquet",),
        "fx": ("FX_parquet",),
        "futures": ("Futures_parquet",),
        "crypto": ("Crypto_parquet",),
    }
    available = [p for p in source_root.iterdir() if p.is_dir()]
    by_norm = {_norm_dir_name(p.name): p for p in available}
    out: Dict[str, Path] = {}
    for canonical in ASSET_SCAN_ORDER:
        keys = canonical_to_source_keys[canonical]
        found: Optional[Path] = None
        for k in keys:
            kp = by_norm.get(_norm_dir_name(k))
            if kp is not None:
                found = kp
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


def _verify_existing(dst: Path, src: Path, mode_used: str) -> Tuple[bool, Optional[str]]:
    try:
        if mode_used == "hardlink":
            sst = src.stat()
            dstst = dst.stat()
            if int(sst.st_ino) != int(dstst.st_ino):
                return False, "integrity_mismatch_inode"
            return True, None
        if mode_used == "symlink":
            if not dst.is_symlink():
                return False, "integrity_mismatch_not_symlink"
            target = os.readlink(dst)
            expected = str(src)
            if target != expected:
                return False, "integrity_mismatch_symlink_target"
            return True, None
        if mode_used == "copy":
            sst = src.stat()
            dstst = dst.stat()
            if int(sst.st_size) != int(dstst.st_size):
                return False, "integrity_mismatch_size"
            return True, None
        return False, "integrity_mismatch_unknown_mode"
    except Exception as exc:
        return False, f"integrity_verify_error:{type(exc).__name__}:{exc}"


def _link_or_copy(src: Path, dst: Path, mode: str) -> Tuple[str, int]:
    fallback_count = 0
    if mode == "hardlink":
        try:
            os.link(src, dst)
            return "hardlink", fallback_count
        except OSError as exc:
            if exc.errno == errno.EXDEV:
                os.symlink(src, dst)
                fallback_count += 1
                return "symlink", fallback_count
            raise
    if mode == "symlink":
        os.symlink(src, dst)
        return "symlink", fallback_count
    shutil.copy2(src, dst)
    return "copy", fallback_count


def build_raw_tree(
    *,
    source_root: Path,
    dest_root: Path,
    mode: str,
    dry_run: bool = False,
    max_files: Optional[int] = None,
    evidence_dir: Optional[Path] = None,
) -> Dict[str, object]:
    source_root = source_root.resolve()
    dest_root = dest_root.resolve()

    if evidence_dir is None:
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        evidence_dir = dest_root / "_manifests" / f"build_raw_tree_{run_id}"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = evidence_dir / "manifest.jsonl"
    summary_path = evidence_dir / "summary.json"
    hashes_path = evidence_dir / "hashes.sha256"
    if manifest_path.exists():
        manifest_path.unlink()

    folders = detect_asset_folders(source_root)
    counts_by_asset = {k: 0 for k in ASSET_SCAN_ORDER}
    timeframe_dist = {k: 0 for k in TIMEFRAMES}
    symbol_set = set()
    processed = 0
    errors = 0
    fallback_count = 0

    for asset in ASSET_SCAN_ORDER:
        src_dir = folders.get(asset)
        if src_dir is None:
            continue
        files = sorted([p for p in src_dir.iterdir() if p.is_file() and p.suffix.lower() == ".parquet"], key=lambda p: p.name)
        for src in files:
            if max_files is not None and processed >= int(max_files):
                break
            processed += 1
            parsed = parse_filename_strict(src.name)
            if parsed is None:
                errors += 1
                _append_jsonl(
                    manifest_path,
                    {
                        "src": str(src),
                        "dst": None,
                        "asset_class": asset,
                        "symbol": None,
                        "timeframe": None,
                        "mode_used": None,
                        "status": "ERROR",
                        "error": "invalid_filename_format",
                    },
                )
                continue

            symbol = parsed.symbol
            tf = parsed.timeframe
            dst_dir = dest_root / asset / symbol
            dst = dst_dir / src.name
            symbol_set.add((asset, symbol))
            timeframe_dist[tf] += 1

            mode_used = mode
            status = "OK"
            err_text = None
            try:
                if not dry_run:
                    dst_dir.mkdir(parents=True, exist_ok=True)
                    if dst.exists() or dst.is_symlink():
                        verify_ok, verr = _verify_existing(dst, src, "hardlink" if mode == "hardlink" and not dst.is_symlink() else ("symlink" if dst.is_symlink() else mode))
                        if not verify_ok:
                            raise RuntimeError(verr or "integrity_mismatch_existing_destination")
                        status = "EXISTS_MATCH"
                        if dst.is_symlink():
                            mode_used = "symlink"
                        elif mode == "copy":
                            mode_used = "copy"
                        else:
                            mode_used = "hardlink"
                    else:
                        mode_used, fb = _link_or_copy(src, dst, mode)
                        fallback_count += int(fb)
                        verify_ok, verr = _verify_existing(dst, src, mode_used)
                        if not verify_ok:
                            raise RuntimeError(verr or "integrity_mismatch_after_write")
                        status = "LINKED"
                else:
                    status = "DRY_RUN"
                    if mode == "hardlink":
                        mode_used = "hardlink"
            except Exception as exc:
                errors += 1
                status = "ERROR"
                err_text = f"{type(exc).__name__}:{exc}"

            rec = {
                "src": str(src),
                "dst": str(dst),
                "asset_class": asset,
                "symbol": symbol,
                "timeframe": tf,
                "mode_used": mode_used,
                "status": status,
            }
            if err_text:
                rec["error"] = err_text
            _append_jsonl(manifest_path, rec)
            if status in {"LINKED", "EXISTS_MATCH", "DRY_RUN"}:
                counts_by_asset[asset] += 1
        if max_files is not None and processed >= int(max_files):
            break

    summary = {
        "source_root": str(source_root),
        "dest_root": str(dest_root),
        "mode_requested": mode,
        "dry_run": bool(dry_run),
        "max_files": int(max_files) if max_files is not None else None,
        "asset_folders": {k: str(v) for k, v in folders.items()},
        "counts_by_asset_class": counts_by_asset,
        "unique_symbols": int(len(symbol_set)),
        "timeframe_distribution": timeframe_dist,
        "processed_files": int(processed),
        "error_count": int(errors),
        "fallback_count": int(fallback_count),
    }
    _write_json(summary_path, summary)
    h_summary = _sha256_file(summary_path)
    h_manifest = _sha256_file(manifest_path) if manifest_path.exists() else None
    lines = [f"{h_summary}  {summary_path}"]
    if h_manifest:
        lines.append(f"{h_manifest}  {manifest_path}")
    hashes_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"manifest": str(manifest_path), "summary": str(summary_path), "hashes": str(hashes_path), "evidence": str(evidence_dir)}


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--source-root", default="/media/n-b/INTENSO")
    p.add_argument("--dest-root", default="/media/n-b/INTENSO/raw")
    p.add_argument("--mode", choices=("hardlink", "symlink", "copy"), default="hardlink")
    p.add_argument("--dry-run", action="store_true", default=False)
    p.add_argument("--max-files", type=int, default=None)
    p.add_argument("--evidence-dir", default=None)
    return p.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    source_root = Path(args.source_root)
    dest_root = Path(args.dest_root)
    if not source_root.exists():
        raise SystemExit(f"source root not found: {source_root}")
    evidence_dir = Path(args.evidence_dir) if args.evidence_dir else None
    out = build_raw_tree(
        source_root=source_root,
        dest_root=dest_root,
        mode=str(args.mode),
        dry_run=bool(args.dry_run),
        max_files=args.max_files,
        evidence_dir=evidence_dir,
    )
    print(f"DONE dest_root={dest_root} evidence={out['evidence']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
