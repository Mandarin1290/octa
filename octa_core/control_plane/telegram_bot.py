from __future__ import annotations

from dataclasses import dataclass

import requests

from octa_core.security.audit import AuditLog
from octa_core.security.secrets import get_secret


@dataclass(frozen=True)
class TelegramBotConfig:
    enabled: bool
    api_base_url: str
    token_env: str = "TELEGRAM_BOT_TOKEN"
    audit_path: str = "artifacts/security/audit.jsonl"


def run_bot(*, cfg: TelegramBotConfig, security_cfg: dict) -> None:
    if not cfg.enabled:
        raise RuntimeError("telegram_bot_disabled")

    token = get_secret(cfg.token_env, cfg=security_cfg)
    if not token:
        raise RuntimeError("telegram_token_missing")

    # Import lazily to avoid heavy deps unless enabled.
    try:
        from telegram import Update  # type: ignore
        from telegram.ext import (  # type: ignore
            Application,
            CommandHandler,
            ContextTypes,
        )
    except Exception as e:
        raise RuntimeError("python_telegram_bot_not_available") from e

    alog = AuditLog(path=cfg.audit_path)

    async def _call_api(path: str, method: str = "GET") -> str:
        url = cfg.api_base_url.rstrip("/") + path
        try:
            if method == "POST":
                r = requests.post(url, timeout=10)
            else:
                r = requests.get(url, timeout=10)
            return f"{r.status_code}: {r.text[:1500]}"
        except Exception as e:
            return f"api_error:{e}"

    async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        alog.append(event_type="telegram.status", payload={"user": str(update.effective_user.id if update.effective_user else "")})
        txt = await _call_api("/status", "GET")
        await update.message.reply_text(txt)  # type: ignore

    async def start_paper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        alog.append(event_type="telegram.start_paper", payload={"user": str(update.effective_user.id if update.effective_user else "")})
        txt = await _call_api("/start?mode=paper", "POST")
        await update.message.reply_text(txt)  # type: ignore

    async def start_live(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        alog.append(event_type="telegram.start_live", payload={"user": str(update.effective_user.id if update.effective_user else "")})
        txt = await _call_api("/start?mode=live", "POST")
        await update.message.reply_text(txt)  # type: ignore

    async def stop_safe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        alog.append(event_type="telegram.stop_safe", payload={"user": str(update.effective_user.id if update.effective_user else "")})
        txt = await _call_api("/stop?mode=SAFE", "POST")
        await update.message.reply_text(txt)  # type: ignore

    async def stop_now(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        alog.append(event_type="telegram.stop_now", payload={"user": str(update.effective_user.id if update.effective_user else "")})
        txt = await _call_api("/stop?mode=IMMEDIATE", "POST")
        await update.message.reply_text(txt)  # type: ignore

    async def train_global(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        alog.append(event_type="telegram.train_global", payload={"user": str(update.effective_user.id if update.effective_user else "")})
        txt = await _call_api("/train?scope=global", "POST")
        await update.message.reply_text(txt)  # type: ignore

    async def lockdown(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        alog.append(event_type="telegram.lockdown", payload={"user": str(update.effective_user.id if update.effective_user else "")})
        txt = await _call_api("/lockdown", "POST")
        await update.message.reply_text(txt)  # type: ignore

    app = Application.builder().token(token).build()
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("start_paper", start_paper))
    app.add_handler(CommandHandler("start_live", start_live))
    app.add_handler(CommandHandler("stop_safe", stop_safe))
    app.add_handler(CommandHandler("stop_now", stop_now))
    app.add_handler(CommandHandler("train_global", train_global))
    app.add_handler(CommandHandler("lockdown", lockdown))

    app.run_polling()


__all__ = ["TelegramBotConfig", "run_bot"]
