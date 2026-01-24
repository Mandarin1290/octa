from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class TelegramSettings:
    enabled: bool
    token_env: str
    chat_id_env: str


def _get_telegram_settings(cfg: Any) -> TelegramSettings:
    tg = getattr(getattr(cfg, "notifications", None), "telegram", None)
    enabled = bool(getattr(tg, "enabled", False)) if tg is not None else False
    token_env = str(getattr(tg, "token_env", "OCTA_TELEGRAM_BOT_TOKEN")) if tg is not None else "OCTA_TELEGRAM_BOT_TOKEN"
    chat_id_env = str(getattr(tg, "chat_id_env", "OCTA_TELEGRAM_CHAT_ID")) if tg is not None else "OCTA_TELEGRAM_CHAT_ID"
    return TelegramSettings(enabled=enabled, token_env=token_env, chat_id_env=chat_id_env)


def send_telegram(cfg: Any, text: str, logger: Optional[Any] = None) -> bool:
    """Best-effort Telegram send.

    Reads credentials from env vars defined in config.
    Never raises.
    """

    try:
        settings = _get_telegram_settings(cfg)
        if not settings.enabled:
            return False

        token = os.getenv(settings.token_env)
        chat_id = os.getenv(settings.chat_id_env)
        if not token or not chat_id:
            if logger:
                logger.warning("Telegram enabled but missing env vars: %s/%s", settings.token_env, settings.chat_id_env)
            return False

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        data = urllib.parse.urlencode(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
            try:
                parsed = json.loads(raw)
                return bool(parsed.get("ok"))
            except Exception:
                return resp.status == 200
    except (urllib.error.URLError, Exception) as e:
        if logger:
            logger.warning("Telegram send failed: %s", e)
        return False
