from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol

import requests

from octa_core.security.secrets import get_secret


class SafeDexConnector(Protocol):
    def push_ledger(self, entries: List[Dict[str, Any]]) -> None:  # pragma: no cover
        ...

    def pull_chart_of_accounts(self) -> Dict[str, str]:  # pragma: no cover
        ...

    def push_reports(self, nav: Dict[str, Any], pnl: Dict[str, Any], risk: Dict[str, Any]) -> None:  # pragma: no cover
        ...


@dataclass(frozen=True)
class SafeDexConfig:
    base_url: str
    api_key_env: str = "SAFEDEX_API_KEY"
    queue_dir: str = "artifacts/accounting/safedex_queue"
    timeout_s: float = 10.0


class HttpSafeDexConnector:
    """HTTP connector with fail-closed queueing.

    - Never hardcodes secrets.
    - If SafeDex is unreachable, writes payloads to local queue for later replay.
    """

    def __init__(self, cfg: SafeDexConfig, *, security_cfg: Optional[dict] = None):
        self.cfg = cfg
        self.security_cfg = security_cfg or {}
        Path(self.cfg.queue_dir).mkdir(parents=True, exist_ok=True)

    def _headers(self) -> Dict[str, str]:
        k = get_secret(self.cfg.api_key_env, cfg=self.security_cfg)
        if not k:
            raise RuntimeError("safedex_api_key_missing")
        return {"Authorization": f"Bearer {k}", "Content-Type": "application/json"}

    def _queue(self, name: str, payload: Dict[str, Any]) -> None:
        p = Path(self.cfg.queue_dir) / f"{name}.json"
        p.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    def push_ledger(self, entries: List[Dict[str, Any]]) -> None:
        payload = {"entries": entries}
        try:
            r = requests.post(self.cfg.base_url.rstrip("/") + "/ledger", headers=self._headers(), json=payload, timeout=self.cfg.timeout_s)
            r.raise_for_status()
        except Exception:
            self._queue("ledger", payload)

    def pull_chart_of_accounts(self) -> Dict[str, str]:
        try:
            r = requests.get(self.cfg.base_url.rstrip("/") + "/coa", headers=self._headers(), timeout=self.cfg.timeout_s)
            r.raise_for_status()
            obj = r.json()
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}

    def push_reports(self, nav: Dict[str, Any], pnl: Dict[str, Any], risk: Dict[str, Any]) -> None:
        payload = {"nav": nav, "pnl": pnl, "risk": risk}
        try:
            r = requests.post(self.cfg.base_url.rstrip("/") + "/reports", headers=self._headers(), json=payload, timeout=self.cfg.timeout_s)
            r.raise_for_status()
        except Exception:
            self._queue("reports", payload)


__all__ = ["SafeDexConnector", "SafeDexConfig", "HttpSafeDexConnector"]
