from typing import List

"""Data ingestion contracts and validation (Parquet-only)."""

from .contracts import ValidationResult, Validator

__all__: List[str] = ["Validator", "ValidationResult"]
