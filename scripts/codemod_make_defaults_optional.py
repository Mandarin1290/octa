#!/usr/bin/env python3
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EXCLUDE_DIRS = {".venv", ".git"}

# Match parameter annotations like: name: dict[str, Any] | None = None
param_pattern = re.compile(r"(\b[\w_]+\b\s*:\s*)([^=\n]+?)(\s*=\s*None)\b")

# Skip if annotation already contains | or Optional
contains_optional = re.compile(r"\bOptional\b|\|\s*None")


def process_text(text: str) -> tuple[str, int]:
    changed = 0

    def repl(m):
        ann = m.group(2)
        if contains_optional.search(ann):
            return m.group(0)
        # simple replacement: append ' | None' to annotation
        new_ann = ann.strip() + " | None"
        nonlocal changed
        changed += 1
        return m.group(1) + new_ann + m.group(3)

    new_text = param_pattern.sub(repl, text)
    return new_text, changed


def should_skip(path: Path) -> bool:
    for part in path.parts:
        if part in EXCLUDE_DIRS:
            return True
    return False


def run(paths, apply_changes=False):
    total_changed_files = []
    for p in paths:
        p = p if p.is_absolute() else (ROOT / p)
        if not p.exists():
            print("Missing", p)
            continue
        text = p.read_text(encoding="utf-8")
        new_text, changed = process_text(text)
        if changed:
            total_changed_files.append((p, changed))
            if apply_changes:
                p.write_text(new_text, encoding="utf-8")
    for p, c in total_changed_files:
        try:
            rel = p.relative_to(ROOT)
        except Exception:
            rel = p
        print(f"{rel}: {c} parameters updated")
    print(f"Total files modified: {len(total_changed_files)}")


if __name__ == "__main__":
    # If arguments provided, treat them as files to process; else process all .py files under ROOT excluding EXCLUDE_DIRS
    apply_changes = "--apply" in sys.argv
    files = [Path(a) for a in sys.argv[1:] if not a.startswith("--")]
    if not files:
        files = [p for p in ROOT.rglob("*.py") if not should_skip(p)]
    run(files, apply_changes=apply_changes)
