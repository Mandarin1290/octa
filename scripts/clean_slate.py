#!/usr/bin/env python3
"""Remove generated run artifacts ("Altlasten") safely.

This script deletes ONLY generated outputs, not raw data.
It requires explicit confirmation via --yes.

Targets (by default):
- reports/*
- state/state.db, state/locks, state/_e2e_real
- artifacts/* (except artifacts/dvc_remote by default)

Optional:
- --include-mlruns: delete mlruns/
- --include-catboost: delete catboost_info/

Usage:
  python3 scripts/clean_slate.py --yes
"""

from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path


def _on_rmtree_error(func, path_str, exc_info):
    # Try to make the file writable and retry once.
    try:
        os.chmod(path_str, 0o700)
        func(path_str)
    except Exception:
        # Let caller decide how to surface the error.
        raise


def _rm_tree(path: Path) -> None:
    if not path.exists():
        return
    if path.is_symlink() or path.is_file():
        try:
            path.unlink(missing_ok=True)
        except PermissionError:
            os.chmod(path, 0o600)
            path.unlink(missing_ok=True)
        return
    shutil.rmtree(path, onerror=_on_rmtree_error)


def _rm_contents(dir_path: Path) -> None:
    if not dir_path.exists():
        return
    for child in dir_path.iterdir():
        _rm_tree(child)


def main() -> int:
    ap = argparse.ArgumentParser(description="Delete generated run artifacts (safe clean-slate).")
    ap.add_argument("--yes", action="store_true", help="Actually delete. Without this, it's a dry-run.")
    ap.add_argument("--include-mlruns", action="store_true", help="Also delete mlruns/ (MLflow tracking).")
    ap.add_argument("--include-catboost", action="store_true", help="Also delete catboost_info/.")
    ap.add_argument(
        "--keep-artifacts-dvc-remote",
        action="store_true",
        default=True,
        help="Keep artifacts/dvc_remote (default: keep).",
    )
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[1]

    delete_paths: list[Path] = []

    # reports: generated summaries, gates, cascade outputs
    delete_paths.append(repo_root / "reports")

    # state: generated state DB + locks (not raw data)
    delete_paths.append(repo_root / "state" / "state.db")
    delete_paths.append(repo_root / "state" / "locks")
    delete_paths.append(repo_root / "state" / "_e2e_real")

    # artifacts: generated models/logs/datasets/features
    artifacts = repo_root / "artifacts"
    if artifacts.exists():
        for child in artifacts.iterdir():
            if args.keep_artifacts_dvc_remote and child.name == "dvc_remote":
                continue
            delete_paths.append(child)

    if args.include_mlruns:
        delete_paths.append(repo_root / "mlruns")

    if args.include_catboost:
        delete_paths.append(repo_root / "catboost_info")

    # Dry-run listing
    print("Planned deletions:")
    for p in delete_paths:
        rel = p.relative_to(repo_root)
        print(f"  - {rel}")

    if not args.yes:
        print("\nDry-run only. Re-run with --yes to delete.")
        return 0

    # Execute deletions.
    errors: list[str] = []
    not_owned: list[str] = []
    for p in delete_paths:
        try:
            if p.name == "reports" and p.is_dir():
                _rm_contents(p)
                p.mkdir(parents=True, exist_ok=True)
                continue
            _rm_tree(p)
        except Exception as e:
            # Common case: directory is owned by user but contains root-owned subdirs
            # created by containers (EPERM on unlink). Treat as "not deletable".
            msg = str(e)
            if "Operation not permitted" in msg and "artifacts" in str(p):
                not_owned.append(f"{p}: {e}")
                continue

            try:
                st = os.stat(p)
                if st.st_uid != os.getuid():
                    not_owned.append(f"{p} (uid={st.st_uid} gid={st.st_gid}): {e}")
                else:
                    errors.append(f"{p}: {e}")
            except Exception:
                errors.append(f"{p}: {e}")

    # Ensure key dirs exist
    (repo_root / "reports").mkdir(parents=True, exist_ok=True)
    (repo_root / "artifacts").mkdir(parents=True, exist_ok=True)
    (repo_root / "state").mkdir(parents=True, exist_ok=True)

    if not_owned:
        print("Leftovers not deletable as current user (likely created by root/container):")
        for msg in not_owned:
            rel = None
            try:
                rel = str(Path(msg.split(" (uid=", 1)[0]).relative_to(repo_root))
            except Exception:
                rel = msg
            print(f"  - {msg}")
        print("To delete them, run (manually):")
        print("  sudo rm -rf artifacts/models/**/container-run artifacts/canary/**")

    if errors:
        print("Done with errors:")
        for msg in errors:
            print(f"  - {msg}")
        return 2

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
