"""Ensure bootstrap scripts never call bare 'python' — they must use ${PY} or ${OCTA_PY}."""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
SCRIPTS = [
    REPO / "scripts" / "octa_ibkr_bootstrap.sh",
    REPO / "scripts" / "octa_autologin_bootstrap.sh",
    REPO / "scripts" / "octa_x11_bootstrap.sh",
]

# Matches bare 'python -c' or 'python -m' or 'python3 -c' etc.
# Does NOT match ${PY}, ${OCTA_PY}, or /path/to/python.
# Ignores comment lines.
BARE_PYTHON_RE = re.compile(r'(?<![\w\"/}])python[3]?\s+-[cm]\b')


@pytest.mark.parametrize("script", SCRIPTS, ids=lambda p: p.name)
def test_no_bare_python_in_bootstrap(script: Path) -> None:
    if not script.exists():
        pytest.skip(f"{script.name} not found")
    for lineno, line in enumerate(script.read_text().splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        assert not BARE_PYTHON_RE.search(line), (
            f"{script.name}:{lineno} uses bare 'python' instead of "
            f"'$PY' or '${{OCTA_PY}}': {line.strip()}"
        )


@pytest.mark.parametrize("script", SCRIPTS, ids=lambda p: p.name)
def test_no_usr_bin_env_python_in_module_calls(script: Path) -> None:
    """Ensure no /usr/bin/env python usage for module invocations."""
    if not script.exists():
        pytest.skip(f"{script.name} not found")
    env_python_re = re.compile(r'/usr/bin/env\s+python')
    for lineno, line in enumerate(script.read_text().splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        # The shebang #!/usr/bin/env bash is OK — only flag python
        if lineno == 1 and "bash" in line:
            continue
        assert not env_python_re.search(line), (
            f"{script.name}:{lineno} uses '/usr/bin/env python' — "
            f"must use absolute venv path: {line.strip()}"
        )
