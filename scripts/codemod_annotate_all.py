#!/usr/bin/env python3
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

pattern = re.compile(r"__all__\s*=\s*([\[\(])")

changed = []
for p in ROOT.rglob("__init__.py"):
    text = p.read_text(encoding="utf-8")
    if "__all__" not in text:
        continue
    # Skip if already annotated
    if re.search(r"__all__\s*:\s*List\[", text):
        continue
    m = pattern.search(text)
    if not m:
        continue
    # Ensure from typing import Callable, List, List exists
    if "from typing import List" not in text:
        # Try to insert after shebang or module docstring or first imports
        lines = text.splitlines()
        insert_at = 0
        # after shebang
        if lines and lines[0].startswith("#!"):
            insert_at = 1
        # after module docstring
        if len(lines) > insert_at and (lines[insert_at].startswith(('"""', "'''"))):
            # find end of docstring
            quote = lines[insert_at][:3]
            for i in range(insert_at + 1, len(lines)):
                if lines[i].endswith(quote):
                    insert_at = i + 1
                    break
        lines.insert(insert_at, "from typing import List")
        text = "\n".join(lines)
    # Replace __all__ assignment with annotated form
    new_text = re.sub(
        r"__all__\s*=\s*([\[\(])", r"__all__: List[str] = \1", text, count=1
    )
    if new_text != text:
        p.write_text(new_text, encoding="utf-8")
        changed.append(str(p.relative_to(ROOT)))

print(f"Annotated __all__ in {len(changed)} files:\n" + "\n".join(changed))
