#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BRAND_STRINGS = ["O C T Λ", "Institutional Risk-First Quant Architecture"]
ALLOWED_HARDCODE = {
    "octa/support/branding.py",
    "scripts/brand_guard.py",
}

FORBIDDEN_PREFIXES = [
    "octa/execution/",
    "octa_training/",
    "octa/core/cascade/",
]
FORBIDDEN_CONTAINS = ["/risk", "risk_"]


def _is_text(path: Path) -> bool:
    return path.suffix.lower() in {
        ".py",
        ".sh",
        ".md",
        ".txt",
        ".yaml",
        ".yml",
        ".toml",
        ".json",
    }


def _iter_text_files() -> list[Path]:
    files: list[Path] = []
    cp = subprocess.run(
        ["git", "ls-files"],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    if cp.returncode != 0:
        return files
    for line in (cp.stdout or "").splitlines():
        rel = line.strip()
        if not rel:
            continue
        path = ROOT / rel
        if not path.exists() or not path.is_file():
            continue
        if not _is_text(path):
            continue
        files.append(path)
    return files


def _is_forbidden(rel: str) -> bool:
    if any(rel.startswith(pfx) for pfx in FORBIDDEN_PREFIXES):
        return True
    rel_slash = f"/{rel}"
    return any(tok in rel_slash for tok in FORBIDDEN_CONTAINS)


def _changed_files() -> list[str]:
    out: list[str] = []
    cp = subprocess.run(["git", "status", "--porcelain"], cwd=str(ROOT), capture_output=True, text=True, check=False)
    if cp.returncode != 0:
        return out
    for line in (cp.stdout or "").splitlines():
        if not line.strip():
            continue
        rel = line[3:].strip()
        if rel:
            out.append(rel)
    return out


def main() -> int:
    violations: list[str] = []

    for path in _iter_text_files():
        rel = path.relative_to(ROOT).as_posix()
        if rel.startswith(".git/") or "/.git/" in rel:
            continue

        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue

        has_brand = any(s in text for s in BRAND_STRINGS)
        if not has_brand:
            continue

        is_doc = rel.startswith("docs/") or rel.startswith("README")
        if not is_doc and rel not in ALLOWED_HARDCODE:
            violations.append(f"hardcoded_brand_outside_allowed:{rel}")

        if _is_forbidden(rel):
            violations.append(f"brand_in_forbidden_zone:{rel}")

    changed = _changed_files()
    for rel in changed:
        if not _is_forbidden(rel):
            continue
        p = ROOT / rel
        if not p.exists() or not p.is_file() or not _is_text(p):
            continue
        text = p.read_text(encoding="utf-8", errors="ignore")
        if any(s in text for s in BRAND_STRINGS):
            violations.append(f"changed_forbidden_file_contains_brand:{rel}")

    if violations:
        print("BRAND_GUARD:FAIL")
        for row in sorted(set(violations)):
            print(row)
        return 1

    print("BRAND_GUARD:PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
