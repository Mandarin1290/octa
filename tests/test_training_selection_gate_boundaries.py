from __future__ import annotations

from pathlib import Path


def test_execution_paths_do_not_import_training_selection_gate() -> None:
    banned = "training_selection_gate"
    roots = [
        Path("octa/execution"),
        Path("octa/core/gates/execution_engine"),
    ]
    offenders: list[str] = []
    for root in roots:
        for path in sorted(root.rglob("*.py")):
            text = path.read_text(encoding="utf-8")
            if banned in text:
                offenders.append(str(path))
    assert offenders == []

