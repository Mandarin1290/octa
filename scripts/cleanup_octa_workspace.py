#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class Candidate:
    path: Path
    kind: str
    approx_bytes: int


def _now() -> float:
    return time.time()


def _human_gb(n: int) -> float:
    return float(n) / (1024.0 ** 3)


def _dir_size_bytes(path: Path) -> int:
    total = 0
    if not path.exists():
        return 0
    if path.is_file():
        try:
            return int(path.stat().st_size)
        except OSError:
            return 0
    for p in path.rglob("*"):
        try:
            if p.is_file():
                total += int(p.stat().st_size)
        except OSError:
            continue
    return total


def _iter_files_older_than(root: Path, days: int, exts: tuple[str, ...] | None = None) -> Iterable[Path]:
    if not root.exists():
        return
    cutoff = _now() - float(days) * 86400.0
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        if exts and p.suffix.lower() not in exts:
            continue
        try:
            if p.stat().st_mtime < cutoff:
                yield p
        except OSError:
            continue


def _rm_tree(path: Path, apply: bool) -> None:
    if not path.exists():
        return
    if not apply:
        return
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    else:
        path.unlink(missing_ok=True)


def _rm_files(paths: Iterable[Path], apply: bool) -> int:
    removed = 0
    for p in paths:
        if not p.exists() or not p.is_file():
            continue
        if apply:
            try:
                p.unlink()
            except OSError:
                continue
        removed += 1
    return removed


def _candidates(repo_root: Path, include_venv: bool, include_raw_fx_txt: bool, include_raw_pkl: bool) -> list[Candidate]:
    cands: list[Candidate] = []

    # Always safe-ish: caches (rebuildable)
    for rel in (".pytest_cache", ".mypy_cache", ".ruff_cache", "__pycache__"):
        p = repo_root / rel
        if p.exists():
            cands.append(Candidate(p, kind="cache", approx_bytes=_dir_size_bytes(p)))

    # Generated outputs (often reproducible): reports, artifacts, mlruns
    for rel, kind in (("reports", "reports"), ("artifacts", "artifacts"), ("mlruns", "mlruns")):
        p = repo_root / rel
        if p.exists():
            cands.append(Candidate(p, kind=kind, approx_bytes=_dir_size_bytes(p)))

    # Environment: not part of repo truth, but large.
    if include_venv:
        p = repo_root / ".venv"
        if p.exists():
            cands.append(Candidate(p, kind="venv", approx_bytes=_dir_size_bytes(p)))

    # Raw data: big, and potentially source-of-truth. Only include when explicitly requested.
    if include_raw_fx_txt:
        p = repo_root / "raw" / "FX_txt"
        if p.exists():
            cands.append(Candidate(p, kind="raw_fx_txt", approx_bytes=_dir_size_bytes(p)))

    if include_raw_pkl:
        p = repo_root / "raw" / "PKL"
        if p.exists():
            cands.append(Candidate(p, kind="raw_pkl", approx_bytes=_dir_size_bytes(p)))

    return sorted(cands, key=lambda c: c.approx_bytes, reverse=True)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="OCTA workspace cleanup (safe-by-default)")
    ap.add_argument("--repo-root", default=".", help="Repository root (default: .)")
    ap.add_argument("--apply", action="store_true", help="Actually delete files (default: dry-run)")

    ap.add_argument("--include-venv", action="store_true", help="Allow deleting .venv (saves a lot, but requires reinstall)")
    ap.add_argument("--include-raw-fx-txt", action="store_true", help="Allow deleting raw/FX_txt (very large; only if not needed)")
    ap.add_argument("--include-raw-pkl", action="store_true", help="Allow deleting raw/PKL (derived artifacts)")

    ap.add_argument("--prune-artifacts-logs-days", type=int, default=0, help="Delete files older than N days under artifacts/logs (0 disables)")
    ap.add_argument("--prune-reports-days", type=int, default=0, help="Delete report files older than N days under reports (0 disables)")
    ap.add_argument("--prune-mlruns-days", type=int, default=0, help="Delete mlflow run files older than N days under mlruns (0 disables)")

    args = ap.parse_args(argv)

    repo_root = Path(args.repo_root).resolve()
    apply = bool(args.apply)

    print(
        {
            "event": "cleanup:start",
            "repo_root": str(repo_root),
            "apply": apply,
            "include_venv": bool(args.include_venv),
            "include_raw_fx_txt": bool(args.include_raw_fx_txt),
            "include_raw_pkl": bool(args.include_raw_pkl),
        }
    )

    cands = _candidates(
        repo_root,
        include_venv=bool(args.include_venv),
        include_raw_fx_txt=bool(args.include_raw_fx_txt),
        include_raw_pkl=bool(args.include_raw_pkl),
    )

    print("\nTop cleanup candidates (dry-run sizes):")
    for c in cands:
        print(f"- {c.kind:12s} {_human_gb(c.approx_bytes):8.2f} GB  {c.path}")

    # Prune by age (safe-ish, targeted)
    total_removed_files = 0
    if args.prune_artifacts_logs_days and args.prune_artifacts_logs_days > 0:
        root = repo_root / "artifacts" / "logs"
        files = list(_iter_files_older_than(root, days=int(args.prune_artifacts_logs_days)))
        total_removed_files += _rm_files(files, apply=apply)
        print(f"\nprune: artifacts/logs older than {args.prune_artifacts_logs_days}d -> {len(files)} files ({'deleted' if apply else 'would delete'})")

    if args.prune_reports_days and args.prune_reports_days > 0:
        root = repo_root / "reports"
        exts = (".json", ".csv", ".parquet")
        files = list(_iter_files_older_than(root, days=int(args.prune_reports_days), exts=exts))
        total_removed_files += _rm_files(files, apply=apply)
        print(f"prune: reports older than {args.prune_reports_days}d -> {len(files)} files ({'deleted' if apply else 'would delete'})")

    if args.prune_mlruns_days and args.prune_mlruns_days > 0:
        root = repo_root / "mlruns"
        files = list(_iter_files_older_than(root, days=int(args.prune_mlruns_days)))
        total_removed_files += _rm_files(files, apply=apply)
        print(f"prune: mlruns older than {args.prune_mlruns_days}d -> {len(files)} files ({'deleted' if apply else 'would delete'})")

    # Full directory removals
    if apply:
        for c in cands:
            # Do not auto-delete broad categories by default; only delete if explicitly included.
            if c.kind in {"cache"}:
                _rm_tree(c.path, apply=True)
            if c.kind in {"venv", "raw_fx_txt", "raw_pkl"}:
                _rm_tree(c.path, apply=True)

    print(f"\nRemoved files count (prune-by-age): {total_removed_files}")
    print({"event": "cleanup:done", "apply": apply})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
