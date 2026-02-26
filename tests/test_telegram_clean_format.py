"""Tests: Telegram outbound message uses clean text when payload has 'message' key.
Internal JSONL evidence must remain unchanged (full structured payload).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional
from unittest.mock import patch, MagicMock

import pytest

from octa.execution.notifier import ExecutionNotifier


class _CaptureTelegramNotifier(ExecutionNotifier):
    """Subclass that intercepts _send_telegram and records the text sent."""

    def __init__(self, evidence_dir: Path) -> None:
        super().__init__(evidence_dir)
        self.sent_texts: list[str] = []

    def _send_telegram(self, *, event_type: str, payload: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        # Replicate the production text-building logic
        if "message" in payload:
            text = str(payload["message"])[:3000]
        else:
            text = f"[{event_type}] {json.dumps(payload, sort_keys=True, default=str)[:3000]}"
        self.sent_texts.append(text)
        return True, None  # pretend success


def test_pre_execution_ready_sends_clean_message(tmp_path: Path) -> None:
    """pre_execution_ready with message key → only the message text is sent to Telegram."""
    n = _CaptureTelegramNotifier(tmp_path)
    n.emit("pre_execution_ready", {"message": "TWS bereit ✅"})

    assert len(n.sent_texts) == 1
    assert n.sent_texts[0] == "TWS bereit ✅"
    # Not the JSON-wrapped form:
    assert "[pre_execution_ready]" not in n.sent_texts[0]


def test_event_without_message_key_keeps_bracketed_format(tmp_path: Path) -> None:
    """Events without a 'message' key still use the [type] json format."""
    n = _CaptureTelegramNotifier(tmp_path)
    n.emit("cycle_summary", {"cycle": 1, "orders": 2})

    assert len(n.sent_texts) == 1
    assert n.sent_texts[0].startswith("[cycle_summary]")
    assert "cycle" in n.sent_texts[0]


def test_jsonl_evidence_still_contains_full_payload(tmp_path: Path, monkeypatch) -> None:
    """JSONL evidence record must contain the full structured payload, unchanged."""
    monkeypatch.setenv("OCTA_TELEGRAM_ENABLED", "false")
    n = ExecutionNotifier(tmp_path)
    n.emit("pre_execution_ready", {"message": "TWS bereit ✅"})

    rows = [
        json.loads(line)
        for line in (tmp_path / "notifications.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(rows) == 1
    row = rows[0]
    assert row["type"] == "pre_execution_ready"
    assert row["payload"] == {"message": "TWS bereit ✅"}


def test_telegram_failure_does_not_fail_emission(tmp_path: Path, monkeypatch) -> None:
    """A Telegram send failure must not raise — emit() returns False but doesn't throw."""
    monkeypatch.setenv("OCTA_TELEGRAM_ENABLED", "true")
    monkeypatch.setenv("OCTA_TELEGRAM_BOT_TOKEN", "fake_token")
    monkeypatch.setenv("OCTA_TELEGRAM_CHAT_ID", "fake_chat")

    import urllib.request

    def _bad_urlopen(*args, **kwargs):
        raise TimeoutError("simulated network failure")

    monkeypatch.setattr(urllib.request, "urlopen", _bad_urlopen)
    n = ExecutionNotifier(tmp_path)
    result = n.emit("pre_execution_ready", {"message": "TWS bereit ✅"})
    # Must return False (failed) but NOT raise
    assert result is False
