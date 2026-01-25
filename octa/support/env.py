from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable


def load_local_env(path: str = ".env.local") -> bool:
    env_path = Path(path)
    if not env_path.exists():
        print("env: no .env.local")
        return False

    loaded = False
    try:
        try:
            from dotenv import dotenv_values  # type: ignore

            values = dotenv_values(str(env_path))
            _apply_env(values.items())
            loaded = True
        except Exception:
            for key, value in _parse_env_lines(env_path.read_text(encoding="utf-8").splitlines()):
                if key:
                    os.environ.setdefault(key, value)
                    loaded = True
    finally:
        if loaded:
            print("env: loaded .env.local")
        else:
            print("env: no .env.local")
    return loaded


def _apply_env(items: Iterable[tuple[str, str | None]]) -> None:
    for key, value in items:
        if key is None or value is None:
            continue
        os.environ.setdefault(str(key), str(value))


def _parse_env_lines(lines: Iterable[str]) -> Iterable[tuple[str, str]]:
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("\"").strip("'")
        if key:
            yield key, value
