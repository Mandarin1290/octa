from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

from .registry import ArtifactRegistry


@dataclass
class PromotionPolicy:
    live_enable: bool = False


def build_live_ready_manifest(*, registry_root: str, out_path: str, policy: PromotionPolicy) -> str:
    reg = ArtifactRegistry(root=registry_root)
    promoted = reg.get_promoted_artifacts(level="paper")

    manifest: Dict[str, Any] = {
        "schema_version": 1,
        "live_enable": bool(policy.live_enable),
        "note": "Fail-closed: live_enable must be explicitly true; broker adapter requires multi-approval.",
        "artifacts": promoted,
    }

    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return str(p)
