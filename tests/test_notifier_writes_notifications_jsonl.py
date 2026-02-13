from __future__ import annotations

import json
from pathlib import Path

from octa.execution.notifier import ExecutionNotifier


def test_notifier_writes_notifications_jsonl(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("OCTA_TELEGRAM_ENABLED", "false")
    n = ExecutionNotifier(tmp_path)
    n.emit("cycle_summary", {"cycle": 1, "orders": 2})
    n.emit("execution_shutdown", {"run_id": "x"})
    lines = (tmp_path / "notifications.jsonl").read_text(encoding="utf-8").splitlines()
    rows = [json.loads(x) for x in lines if x.strip()]
    assert len(rows) == 2
    assert rows[0]["type"] == "cycle_summary"
    assert rows[1]["type"] == "execution_shutdown"
