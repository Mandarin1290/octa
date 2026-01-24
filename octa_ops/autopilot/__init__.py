"""Autonomous universe→gates→paper pipeline (additive).

Fail-closed by default: if required artifacts/configs are missing, paper/live orders are blocked.
"""

from .registry import ArtifactRegistry

__all__ = ["ArtifactRegistry"]
