#!/usr/bin/env python3
"""Simple codemod to fix common test anti-patterns across the repo.
Replacements:
 - raise AssertionError("msg")  -> raise AssertionError("msg")
 - audit = lambda e, p: events.append((e, p)) -> def audit(e, p):\n    events.append((e, p))
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PY_GLOB = list(ROOT.rglob("*.py"))

assert_pattern = re.compile(r"assert\s+False\s*,\s*([\"'].*?[\"'])")
audit_lambda_pattern = re.compile(
    r"^(\s*)(\w+)\s*=\s*lambda\s+e\s*,\s*p\s*:\s*events\.append\(\(e\s*,\s*p\)\)\s*$"
)

modified = 0
for p in PY_GLOB:
    try:
        txt = p.read_text()
    except Exception:
        continue
    new = txt
    # replace raise AssertionError('msg') patterns
    new = assert_pattern.sub(r"raise AssertionError(\1)", new)

    # replace simple audit lambda patterns line-by-line
    lines = new.splitlines()
    out_lines = []
    i = 0
    changed = False
    while i < len(lines):
        m = audit_lambda_pattern.match(lines[i])
        if m:
            indent, name = m.groups()
            # replace with def
            out_lines.append(f"{indent}def {name}(e, p):")
            out_lines.append(f"{indent}    events.append((e, p))")
            changed = True
            i += 1
            continue
        out_lines.append(lines[i])
        i += 1
    if changed:
        new = "\n".join(out_lines) + ("\n" if txt.endswith("\n") else "")

    if new != txt:
        p.write_text(new)
        modified += 1

print(f"Codemod applied to {modified} files")
