"""octa_alpha — Research strategy layer.

RESEARCH_LAYER_ONLY: This package is not wired into the production execution path.
It provides alpha signal abstractions, portfolio optimization, hypothesis registry,
and paper-deploy lifecycle for research use only.

Production integration is planned for v0.0.1. See ARCHITECTURE_FORENSICS.md § D6.
"""
from typing import List

RESEARCH_LAYER_ONLY: bool = True  # not wired to production execution path

__all__: List[str] = ["RESEARCH_LAYER_ONLY"]
