
#!/usr/bin/env python3
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
venv = ROOT / ".venv"
if not venv.exists():
    print(".venv not found, nothing to revert")
    raise SystemExit(0)

pattern = re.compile(r"__all__\s*:\s*List\[str\]\s*=\s*")
repl = "__all__ = "
changed = []
for p in venv.rglob("__init__.py"):
    text = p.read_text(encoding="utf-8")
    if "__all__: List[str]" in text:
        new_text = re.sub(r"__all__\s*:\s*List\[str\]\s*=\s*", "__all__ = ", text)
        if new_text != text:
            p.write_text(new_text, encoding="utf-8")
            changed.append(str(p.relative_to(ROOT)))

print(f"Reverted annotations in {len(changed)} .venv files")
print("\n".join(changed))
