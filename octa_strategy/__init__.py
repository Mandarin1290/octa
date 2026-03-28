"""octa_strategy — Research strategy layer.

RESEARCH_LAYER_ONLY: This package is not wired into the production execution path.
It provides strategy lifecycle states, aging analytics, attribution, shadow/paper gates,
capacity, risk budget, and kill rules for research use only.

IMPORTANT — lifecycle order: IDEA → SHADOW → PAPER → LIVE
Shadow validation must come BEFORE paper trading (governance requirement).
This matches octa/core/governance/lifecycle_controller.py.
The previous incorrect order (IDEA→PAPER→SHADOW) was fixed 2026-03-21.

Production integration is planned for v0.0.1. See ARCHITECTURE_FORENSICS.md § D2, D6.
"""
from typing import List

RESEARCH_LAYER_ONLY: bool = True  # not wired to production execution path

__all__: List[str] = ["RESEARCH_LAYER_ONLY", "lifecycle", "state_machine"]
