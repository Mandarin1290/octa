from __future__ import annotations

import inspect
import importlib
from typing import Any, Dict, Optional, Tuple


def _short_err(exc: Exception, limit: int = 200) -> str:
    msg = str(exc)
    return msg[:limit] if len(msg) > limit else msg


def _default_rate_args(mod: Any) -> tuple[Any, ...]:
    try:
        rate_cls = getattr(mod, "Rate", None) or getattr(mod, "RequestRate", None)
        duration = getattr(mod, "Duration", None)
        if rate_cls is None:
            return ()
        interval = duration.SECOND if duration is not None else 1
        return (rate_cls(1, interval),)
    except Exception:
        return ()


def _filter_kwargs(kwargs: Dict[str, Any], sig: inspect.Signature) -> Dict[str, Any]:
    supported = set(sig.parameters.keys())
    filtered: Dict[str, Any] = {}
    for key, val in kwargs.items():
        if key in supported:
            filtered[key] = val
    if "raise_when_fail" in kwargs and "raise_when_fail" not in supported and "raise_on_fail" in supported:
        filtered["raise_on_fail"] = kwargs.get("raise_when_fail")
    if "raise_on_fail" in kwargs and "raise_on_fail" not in supported and "raise_when_fail" in supported:
        filtered["raise_when_fail"] = kwargs.get("raise_on_fail")
    return filtered


def _make_limiter(*args: Any, **kwargs: Any) -> Tuple[Optional[Any], Dict[str, Any]]:
    meta: Dict[str, Any] = {"limiter_impl": None, "limiter_kwargs_used": {}}
    try:
        mod = importlib.import_module("pyrate_limiter")
        Limiter = getattr(mod, "Limiter")
        meta["limiter_impl"] = f"{Limiter.__module__}.{Limiter.__qualname__}"
        try:
            sig = inspect.signature(Limiter.__init__)
            call_args = args if args else _default_rate_args(mod)
            filtered = _filter_kwargs(dict(kwargs), sig)
            meta["limiter_kwargs_used"] = dict(filtered)
            limiter = Limiter(*call_args, **filtered)
            return limiter, meta
        except Exception:
            call_args = args if args else _default_rate_args(mod)
            limiter = Limiter(*call_args)
            meta["limiter_kwargs_used"] = {}
            return limiter, meta
    except Exception as exc:
        meta["error"] = _short_err(exc)
        return None, meta


def _patch_limiter_for_kwargs(**kwargs: Any) -> Dict[str, Any]:
    info: Dict[str, Any] = {"patched": False}
    try:
        mod = importlib.import_module("pyrate_limiter")
        Limiter = getattr(mod, "Limiter")
        info["limiter_impl"] = f"{Limiter.__module__}.{Limiter.__qualname__}"
        sig = inspect.signature(Limiter.__init__)
        if all(k in sig.parameters for k in kwargs.keys()):
            return info

        class CompatLimiter(Limiter):  # type: ignore[misc]
            def __init__(self, *c_args: Any, **c_kwargs: Any) -> None:
                filtered = _filter_kwargs(c_kwargs, sig)
                super().__init__(*c_args, **filtered)

        setattr(mod, "Limiter", CompatLimiter)
        info["patched"] = True
        return info
    except Exception as exc:
        info["error"] = _short_err(exc)
        return info
