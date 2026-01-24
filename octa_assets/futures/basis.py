from typing import Dict, List


def compute_basis(
    future_price: float, spot_price: float, multiplier: float = 1.0
) -> float:
    """Compute basis as (future - spot) * multiplier."""
    return (future_price - spot_price) * multiplier


def basis_history_metrics(basis_series: List[float]) -> Dict[str, float]:
    if not basis_series:
        return {"mean": 0.0, "std": 0.0}
    mean = sum(basis_series) / len(basis_series)
    var = sum((x - mean) ** 2 for x in basis_series) / len(basis_series)
    std = var**0.5
    return {"mean": mean, "std": std}
