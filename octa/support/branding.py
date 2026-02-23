from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import TextIO

BRAND_NAME = "O C T Λ"
PLATFORM_NAME = "OCTA"
TAGLINE = "Institutional Risk-First Quant Architecture"
COPYRIGHT = "© O C T Λ"
ASCII_BANNER = (
    "=============================\n"
    "        O C T Λ\n"
    "  Institutional Risk-First\n"
    "   Quant Architecture\n"
    "============================="
)

_PRINTED = False


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _read_git_commit() -> str | None:
    repo = _repo_root()
    try:
        cp = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(repo),
            capture_output=True,
            text=True,
            check=False,
        )
        if cp.returncode == 0:
            commit = (cp.stdout or "").strip()
            if len(commit) == 40:
                return commit
    except Exception:
        pass

    git_dir = repo / ".git"
    if not git_dir.exists():
        return None

    head_path = git_dir / "HEAD"
    if not head_path.exists():
        return None

    try:
        head_val = head_path.read_text(encoding="utf-8").strip()
    except Exception:
        return None

    if head_val.startswith("ref:"):
        ref = head_val.split(":", 1)[1].strip()
        ref_path = git_dir / ref
        if ref_path.exists():
            try:
                commit = ref_path.read_text(encoding="utf-8").strip()
                return commit if len(commit) == 40 else None
            except Exception:
                return None

        packed = git_dir / "packed-refs"
        if packed.exists():
            try:
                for line in packed.read_text(encoding="utf-8").splitlines():
                    row = line.strip()
                    if not row or row.startswith("#") or row.startswith("^"):
                        continue
                    parts = row.split()
                    if len(parts) == 2 and parts[1] == ref and len(parts[0]) == 40:
                        return parts[0]
            except Exception:
                return None
        return None

    return head_val if len(head_val) == 40 else None


def run_identity_payload() -> dict[str, object]:
    return {
        "platform": PLATFORM_NAME,
        "brand": BRAND_NAME,
        "tagline": TAGLINE,
        "generated_by": "branding_identity_layer",
        "git_commit": _read_git_commit(),
        "identity_schema_version": 1,
    }


def print_banner_once(stream: TextIO | None = None, enabled: bool = True) -> None:
    global _PRINTED
    if not enabled or _PRINTED:
        return
    out = stream if stream is not None else sys.stdout
    out.write(ASCII_BANNER + "\n")
    out.flush()
    _PRINTED = True
