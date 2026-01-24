#!/usr/bin/env python3
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EXCLUDE = {".venv", ".git", "scripts"}

pat = re.compile(r"(\S+)\s*\|\s*None\s*=\s*None")


def run(apply_changes: bool = False):
    changed = []
    for p in ROOT.rglob("*.py"):
        if any(part in EXCLUDE for part in p.parts):
            continue
        text = p.read_text(encoding="utf-8")
        new_lines = []
        modified = False
        for line in text.splitlines(keepends=True):
            # Capture the current loop value so inner function doesn't close over
            # the loop variable (avoids B023: function binds loop variable).
            current_line = line

            def repl(m, current_line=current_line):
                # Decide whether this is an annotation's '| None = None' (skip)
                # or an accidental 'name | None = None' inside a call/assignment (fix).
                prefix = current_line[: m.start()]
                last_colon = prefix.rfind(":")
                last_open = prefix.rfind("(")
                # If there's a colon and it's after the last '(', it's likely an annotation -> skip
                if last_colon != -1 and last_colon > last_open:
                    return m.group(0)
                return f"{m.group(1)} = None"

            new_line = pat.sub(repl, line)
            if new_line != line:
                modified = True
            new_lines.append(new_line)
        if modified:
            if apply_changes:
                p.write_text("".join(new_lines), encoding="utf-8")
            changed.append(str(p.relative_to(ROOT)))
    print(f"Fixed {len(changed)} files:\n" + "\n".join(changed))


if __name__ == "__main__":
    apply_changes = "--apply" in sys.argv
    run(apply_changes=apply_changes)
#!/usr/bin/env python3
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
pattern = re.compile(r"^(\s*)([A-Za-z_][A-Za-z0-9_]*)\s*\|\s*None\s*=\s*None\b")
changed = []
for p in ROOT.rglob("*.py"):
    if ".venv" in p.parts:
        continue
    text = p.read_text(encoding="utf-8")
    new_lines = []
    modified = False
    for line in text.splitlines():
        m = pattern.match(line)
        if m:
            new_line = f"{m.group(1)}{m.group(2)} = None"
            new_lines.append(new_line)
            modified = True
        else:
            new_lines.append(line)
    if modified:
        p.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
        changed.append(str(p.relative_to(ROOT)))

print(f"Fixed {len(changed)} files:\n" + "\n".join(changed))
