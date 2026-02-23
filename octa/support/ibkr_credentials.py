"""octa/support/ibkr_credentials.py
Robust credential loader for IBKR/TWS autologin.

Priority
--------
1. Environment variables (checked in order):
     OCTA_IBKR_USERNAME / OCTA_IBKR_PASSWORD  — primary (matches chain YAML default)
     IBKR_USERNAME      / IBKR_PASSWORD
     TWS_USERNAME       / TWS_PASSWORD
     IBKR_USER          / IBKR_PASS   / IBKR_PW

2. Credentials env file:
     ~/.config/octa/ibkr.env                  — default path
     or the path in  OCTA_IBKR_ENV_FILE        — optional override env var

File format
-----------
- KEY=VALUE lines (optional ``export `` prefix)
- ``#`` comments and blank lines are ignored
- Values may be quoted with ``'`` or ``"``
- CRLF line endings are tolerated
- Leading / trailing whitespace in keys and values is stripped

Returns
-------
``(username, password, source)`` where source is ``"env"``, ``"file"``,
or ``"missing"``.  When both fields are absent from all sources,
returns ``(None, None, "missing")``.

Never logs or prints credential values.  Only lengths or ``***`` appear
in any diagnostic output.
"""
from __future__ import annotations

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Priority lists
# ---------------------------------------------------------------------------

_USERNAME_ENVS: list[str] = [
    "OCTA_IBKR_USERNAME",  # primary — matches the chain YAML default
    "IBKR_USERNAME",
    "TWS_USERNAME",
    "IBKR_USER",
]

_PASSWORD_ENVS: list[str] = [
    "OCTA_IBKR_PASSWORD",  # primary
    "IBKR_PASSWORD",
    "TWS_PASSWORD",
    "IBKR_PASS",
    "IBKR_PW",
]

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _find_in_env(env: dict[str, str], keys: list[str]) -> str:
    """Return the first non-empty stripped value found for any key in ``keys``."""
    for k in keys:
        v = str(env.get(k) or "").strip()
        if v:
            return v
    return ""


def _parse_env_file(text: str) -> dict[str, str]:
    """Parse an env-file string into a dict.

    Handles:
    - CRLF line endings
    - Leading/trailing whitespace
    - ``# comment`` lines
    - Blank lines
    - Optional ``export `` prefix
    - Single-quoted and double-quoted values (outer quotes stripped once)
    """
    result: dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.replace("\r", "").strip()
        if not line or line.startswith("#"):
            continue
        # Strip optional 'export ' prefix (case-sensitive, as per POSIX)
        if line.startswith("export "):
            line = line[len("export "):].strip()
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip()
        # Strip matching outer quotes (single or double), once only
        if len(val) >= 2 and val[0] == val[-1] and val[0] in ('"', "'"):
            val = val[1:-1]
        if key:
            result[key] = val
    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def load_credentials(
    env: dict[str, str] | None = None,
    *,
    env_file: Path | None = None,
) -> tuple[str | None, str | None, str]:
    """Load IBKR credentials from environment or env file.

    Parameters
    ----------
    env:
        Mapping to search for env-var credentials.  Defaults to
        ``os.environ`` at call time if ``None``.
    env_file:
        Explicit path to an env file.  If ``None``, the path is resolved
        from the ``OCTA_IBKR_ENV_FILE`` env var, then falls back to
        ``~/.config/octa/ibkr.env``.

    Returns
    -------
    (username, password, source)
        ``source`` is ``"env"`` | ``"file"`` | ``"missing"``.
        Returns ``(None, None, "missing")`` when no credentials are found.
    """
    if env is None:
        env = dict(os.environ)

    # ---- 1. Environment variables ----
    user = _find_in_env(env, _USERNAME_ENVS)
    pw = _find_in_env(env, _PASSWORD_ENVS)
    if user and pw:
        return user, pw, "env"

    # ---- 2. Env file ----
    if env_file is None:
        override = str(env.get("OCTA_IBKR_ENV_FILE") or "").strip()
        env_file = Path(override) if override else Path.home() / ".config" / "octa" / "ibkr.env"

    if env_file.is_file():
        try:
            file_text = env_file.read_text(encoding="utf-8", errors="replace")
        except OSError:
            file_text = ""
        file_env = _parse_env_file(file_text)
        file_user = _find_in_env(file_env, _USERNAME_ENVS)
        file_pw = _find_in_env(file_env, _PASSWORD_ENVS)
        # Fill in whichever part was missing from the env
        found_user = user or file_user
        found_pw = pw or file_pw
        if found_user and found_pw:
            return found_user, found_pw, "file"

    return None, None, "missing"
