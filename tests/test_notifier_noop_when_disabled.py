from __future__ import annotations

import json
from pathlib import Path

from octa.execution.notifier import ExecutionNotifier


def test_notifier_noop_when_disabled(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("OCTA_TELEGRAM_ENABLED", "false")
    n = ExecutionNotifier(tmp_path)
    ok = n.emit("execution_start", {"run_id": "r1"})
    assert ok is False
    rows = [json.loads(x) for x in (tmp_path / "notifications.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
    assert len(rows) == 1
    assert rows[0]["error"] == "telegram_disabled"
