from __future__ import annotations

import uuid

from .types import Identifier


def generate_id(prefix: str | None = None) -> Identifier:
    raw = uuid.uuid4().hex
    if prefix:
        return Identifier(f"{prefix}-{raw}")
    return Identifier(raw)
