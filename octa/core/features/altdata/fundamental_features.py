from __future__ import annotations

from typing import Any, Mapping


def build(payloads: Mapping[str, Any]) -> dict[str, float]:
    edgar = payloads.get("edgar", {})
    filings = edgar.get("filings", []) if isinstance(edgar, dict) else []
    count = len(filings)
    quality = 1.0 / (1.0 + count)
    return {
        "filing_count": float(count),
        "quality_score": float(quality),
    }
