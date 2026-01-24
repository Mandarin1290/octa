"""Dagster integration package (local).

This module provides minimal shims so local orchestration code can import
`dagster.job` and `dagster.op` even when the external Dagster package is absent.
If the real Dagster package is available, we re-export its APIs.
"""

from __future__ import annotations

from typing import Any, Callable, Optional

__all__ = ["job", "op", "repository"]


def _load_real_dagster() -> Optional[Any]:
    try:
        import importlib.util
        import sys

        for path in sys.path[1:]:
            spec = importlib.util.find_spec("dagster", [path])
            if spec and spec.loader and spec.origin and "site-packages" in spec.origin:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                return module
    except Exception:
        return None
    return None


_real = _load_real_dagster()


def _noop_decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
    return fn


def op(*_args: Any, **_kwargs: Any):  # type: ignore[override]
    if _real is not None:
        return _real.op(*_args, **_kwargs)
    if _args and callable(_args[0]) and not _kwargs:
        return _noop_decorator(_args[0])

    def _decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        return fn

    return _decorator


def job(*_args: Any, **_kwargs: Any):  # type: ignore[override]
    if _real is not None:
        return _real.job(*_args, **_kwargs)
    if _args and callable(_args[0]) and not _kwargs:
        return _noop_decorator(_args[0])

    def _decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        return fn

    return _decorator


def repository(*_args: Any, **_kwargs: Any):  # type: ignore[override]
    if _real is not None:
        return _real.repository(*_args, **_kwargs)
    if _args and callable(_args[0]) and not _kwargs:
        return _noop_decorator(_args[0])

    def _decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        return fn

    return _decorator
