from __future__ import annotations

import hashlib
import json
from typing import Any


def canonicalize(obj: Any) -> str:
    """Return stable canonical JSON representation for fingerprinting."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_hexdigest(obj: Any) -> str:
    j = canonicalize(obj)
    h = hashlib.sha256(j.encode("utf-8")).hexdigest()
    return h
