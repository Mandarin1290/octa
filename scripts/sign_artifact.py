from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path
from shutil import which


def compute_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def write_integrity(path: str) -> str:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)
    digest = compute_sha256(p)
    out = p.parent / "integrity.sha256"
    out.write_text(f"{digest}  {p.name}\n")
    return str(out)


def gpg_sign(path: str, key: str | None = None) -> str | None:
    # optional: gpg --detach-sign
    if not key:
        return None
    if which("gpg") is None:
        return None
    p = Path(path)
    sig = p.with_suffix(p.suffix + ".asc")
    key_args = ["--default-key", key] if key else []
    cmd = ["gpg", "--batch", "--yes", *key_args, "--detach-sign", "--armor", str(p)]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return str(sig)
    except Exception:
        return None


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("path", help="file to sign")
    p.add_argument("--gpg-key", default=None)
    args = p.parse_args()
    print("writing integrity:", write_integrity(args.path))
    sig = gpg_sign(args.path, args.gpg_key)
    if sig:
        print("gpg signature written:", sig)
    else:
        print("gpg signing not available or failed")
