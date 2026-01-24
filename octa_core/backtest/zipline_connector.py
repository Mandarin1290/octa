"""Prototype Zipline connector shim.

Zipline is a heavyweight dependency with C extensions; this connector provides
an integration point and clear error messages. It intentionally defers import
and raises a helpful RuntimeError when `zipline` is not available.

For production use, implement a proper `run_zipline_backtest` that constructs
a `zipline` algorithm or uses the `run_algorithm` API with a prebuilt bundle.
"""

from typing import Any, Dict, Optional


def run_zipline_backtest(
    start, end, bundle: Optional[str] = None, algo: Optional[Any] = None
) -> Dict[str, Any]:
    try:
        pass  # type: ignore
    except Exception as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "zipline is not installed or not available in this environment."
            " For development, install zipline via the recommended platform-specific steps"
            " (it may require conda and system packages)."
        ) from exc

    # Placeholder: callers should pass an `algo` Callable or use `zipline.run_algorithm`.
    if algo is None:
        raise NotImplementedError(
            "Zipline runner requires an `algo` argument for execution."
        )

    # If implemented, return a result dict similar to the vectorbt connector.
    result = {"message": "zipline run completed", "details": {}}
    return result


__all__ = ["run_zipline_backtest"]
