from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True)
class TelegramEnv:
    enabled: bool
    token: Optional[str]
    chat_id: Optional[str]


def _telegram_env() -> TelegramEnv:
    enabled_raw = str(os.getenv("OCTA_TELEGRAM_ENABLED", "false")).strip().lower()
    enabled = enabled_raw in {"1", "true", "yes", "on"}
    token = os.getenv("OCTA_TELEGRAM_BOT_TOKEN") or os.getenv("OCTA_TELEGRAM_BOT_TOKEN".replace("OCTA_", ""))
    chat_id = os.getenv("OCTA_TELEGRAM_CHAT_ID") or os.getenv("OCTA_TELEGRAM_CHAT_ID".replace("OCTA_", ""))
    return TelegramEnv(enabled=enabled, token=token, chat_id=chat_id)


class ExecutionNotifier:
    def __init__(self, evidence_dir: Path, rate_limit_seconds: int = 5) -> None:
        self.evidence_dir = evidence_dir
        self.evidence_dir.mkdir(parents=True, exist_ok=True)
        self.notifications_path = self.evidence_dir / "notifications.jsonl"
        self.rate_limit_seconds = max(0, int(rate_limit_seconds))
        self._last_event_ts: Dict[str, float] = {}

    def emit(self, event_type: str, payload: Dict[str, Any]) -> bool:
        key = f"{event_type}:{json.dumps(payload, sort_keys=True, default=str)}"
        now = time.time()
        last = self._last_event_ts.get(key)
        if last is not None and (now - last) < float(self.rate_limit_seconds):
            self._write_row(
                event_type,
                payload,
                telegram_success=False,
                error="rate_limited",
            )
            return False

        self._last_event_ts[key] = now
        success, error = self._send_telegram(event_type=event_type, payload=payload)
        self._write_row(event_type, payload, telegram_success=success, error=error)
        return success

    def _write_row(self, event_type: str, payload: Dict[str, Any], telegram_success: bool, error: Optional[str]) -> None:
        row = {
            "timestamp_utc": _utc_now_iso(),
            "type": str(event_type),
            "payload": payload,
            "telegram_success": bool(telegram_success),
            "error": error,
        }
        with self.notifications_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, sort_keys=True, default=str))
            fh.write("\n")

    def _send_telegram(self, *, event_type: str, payload: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        env = _telegram_env()
        if not env.enabled:
            return False, "telegram_disabled"
        if not env.token or not env.chat_id:
            return False, "telegram_env_missing"

        if "message" in payload:
            text = str(payload["message"])[:3000]
        else:
            text = f"[{event_type}] {json.dumps(payload, sort_keys=True, default=str)[:3000]}"
        url = f"https://api.telegram.org/bot{env.token}/sendMessage"
        body = urllib.parse.urlencode(
            {
                "chat_id": env.chat_id,
                "text": text,
                "disable_web_page_preview": True,
            }
        ).encode("utf-8")
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/x-www-form-urlencoded"})

        last_error: Optional[str] = None
        for _attempt in range(2):
            try:
                with urllib.request.urlopen(req, timeout=5) as resp:
                    raw = resp.read().decode("utf-8", errors="ignore")
                parsed = json.loads(raw)
                if bool(parsed.get("ok", False)):
                    return True, None
                last_error = f"telegram_not_ok:{parsed}"
            except (urllib.error.URLError, TimeoutError, Exception) as exc:
                last_error = f"telegram_send_error:{exc}"
        return False, last_error
