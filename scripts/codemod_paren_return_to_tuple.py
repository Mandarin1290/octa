#!/usr/bin/env python3
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EXCLUDE = {".venv", ".git", "scripts"}


def ensure_tuple_import(text: str) -> str:
    # if 'Tuple' already imported from typing, do nothing
    if re.search(r"from\s+typing\s+import\s+.*\bTuple\b", text):
        return text
    # attempt to add to an existing typing import line
    m = re.search(r"from\s+typing\s+import\s+([\w_,\s]+)", text)
    if m:
        start, end = m.span(1)
        names = m.group(1).strip()
        new_names = names + ", Tuple"
        return text[:start] + new_names + text[end:]
    # else, insert a new import after any __future__ imports or module docstring
    insert_pos = 0
    # skip module docstring
    doc_m = re.match(r"(\s*\"\"\".*?\"\"\"\s*)", text, re.S)
    if doc_m:
        insert_pos = doc_m.end()
    # skip __future__ imports
    fut_m = re.match(r"(.*?from\s+__future__.*?\n)+", text, re.S)
    if fut_m:
        insert_pos = fut_m.end()
    return text[:insert_pos] + "from typing import Tuple\n" + text[insert_pos:]


def run(apply_changes: bool = False):
    pat = re.compile(r"def\s+[\w_]+\s*\([^)]*\)\s*->\s*\(([^)]+)\)")
    changed = []
    for p in ROOT.rglob("*.py"):
        if any(part in EXCLUDE for part in p.parts):
            continue
        text = p.read_text(encoding="utf-8")
        new_text = pat.sub(
            lambda m: m.group(0).replace(
                "-> (" + m.group(1) + ")", "-> Tuple[" + m.group(1) + "]"
            ),
            text,
        )
        if new_text != text:
            new_text = ensure_tuple_import(new_text)
            if apply_changes:
                p.write_text(new_text, encoding="utf-8")
            changed.append(str(p.relative_to(ROOT)))
    print(f"Converted {len(changed)} files:\n" + "\n".join(changed))


if __name__ == "__main__":
    apply_changes = "--apply" in sys.argv
    run(apply_changes=apply_changes)
