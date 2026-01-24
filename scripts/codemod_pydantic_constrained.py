#!/usr/bin/env python3
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EXCLUDE = {".venv", ".git", "scripts"}

# Replace confloat(...) / conint(...) / condecimal(...) in annotations with parametric form
# match common forms: confloat(...), pydantic.types.confloat(...), pydantic.confloat(...)
patterns = [
    (
        re.compile(r"(?<![\w.])(?:pydantic\.types\.|pydantic\.)?confloat\([^\)]*\)"),
        "confloat[float]",
    ),
    (
        re.compile(r"(?<![\w.])(?:pydantic\.types\.|pydantic\.)?conint\([^\)]*\)"),
        "conint[int]",
    ),
    (
        re.compile(r"(?<![\w.])(?:pydantic\.types\.|pydantic\.)?condecimal\([^\)]*\)"),
        "condecimal[Decimal]",
    ),
]


def run(apply_changes: bool = False):
    changed = []
    for p in ROOT.rglob("*.py"):
        if any(part in EXCLUDE for part in p.parts):
            continue
        text = p.read_text(encoding="utf-8")
        new_text = text
        for pat, rep in patterns:
            new_text = pat.sub(rep, new_text)
        if new_text != text:
            if apply_changes:
                p.write_text(new_text, encoding="utf-8")
            changed.append(str(p.relative_to(ROOT)))
    print(f"Updated {len(changed)} files:\n" + "\n".join(changed))


if __name__ == "__main__":
    apply_changes = "--apply" in sys.argv
    run(apply_changes=apply_changes)
