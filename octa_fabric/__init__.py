from typing import List

"""Configuration and dependency wiring."""

from .config import Config, ConfigurationError

__all__: List[str] = ["Config", "ConfigurationError"]
