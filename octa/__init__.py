"""OCTA canonical package root."""

__all__ = ["__version__"]

try:
    from .version import __version__
except Exception:
    __version__ = "0.0.0"
