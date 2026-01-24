from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _atomic_write(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    os.replace(tmp_path, path)


@dataclass
class RunState:
    path: Path
    data: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path, *, run_id: Optional[str] = None) -> "RunState":
        if path.exists():
            raw = json.loads(path.read_text(encoding="utf-8"))
        else:
            raw = {
                "run_id": run_id or "",
                "created_at": _now_utc_iso(),
                "layers": {},
            }
            _atomic_write(path, raw)
        return cls(path=path, data=raw)

    def save(self) -> None:
        _atomic_write(self.path, self.data)

    def mark_symbol(
        self,
        *,
        layer: str,
        symbol: str,
        status: str,
        decision: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> None:
        layers = self.data.setdefault("layers", {})
        layer_state = layers.setdefault(layer, {})
        layer_state[symbol] = {
            "status": status,
            "decision": decision,
            "reason": reason,
            "ts": _now_utc_iso(),
        }
        self.save()

    def status_for(self, layer: str, symbol: str) -> Optional[str]:
        layers = self.data.get("layers", {})
        return layers.get(layer, {}).get(symbol, {}).get("status")

    def is_complete(self, layer: str, symbol: str) -> bool:
        status = self.status_for(layer, symbol)
        return status in {"PASS", "FAIL", "SKIP"}

