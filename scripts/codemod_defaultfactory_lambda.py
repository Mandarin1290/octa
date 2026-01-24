#!/usr/bin/env python3
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EXCLUDE = {".venv", ".git", "scripts"}

# match Field(default_factory=SomeClass) and replace with lambda factory
# match Field(default_factory=Name) or Field(default_factory=Name()) or dotted names
pat = re.compile(
    r"Field\(\s*default_factory\s*=\s*([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)(\(\))?\s*\)"
)


def run(apply_changes: bool = False):
    changed = []
    for p in ROOT.rglob("*.py"):
        if any(part in EXCLUDE for part in p.parts):
            continue
        text = p.read_text(encoding="utf-8")

        def repl(m):
            name = m.group(1)
            return f"Field(default_factory=lambda: {name}())"

        new_text = pat.sub(repl, text)
        if new_text != text:
            if apply_changes:
                p.write_text(new_text, encoding="utf-8")
            changed.append(str(p.relative_to(ROOT)))
    print(f"Updated {len(changed)} files:\n" + "\n".join(changed))


if __name__ == "__main__":
    apply_changes = "--apply" in sys.argv
    run(apply_changes=apply_changes)
