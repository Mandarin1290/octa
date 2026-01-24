from typing import Dict


def detect_term_structure(front_price: float, next_price: float) -> str:
    """Detect contango/backwardation between front and next futures.

    Returns 'contango' if next_price > front_price, 'backwardation' if next_price < front_price, 'flat' otherwise.
    """
    if next_price > front_price:
        return "contango"
    if next_price < front_price:
        return "backwardation"
    return "flat"


def term_structure_from_curve(prices: Dict[str, float]) -> Dict[str, str]:
    """Given ordered mapping of tenor->price (e.g., '1m','2m','3m'), return pairwise structure.
    Example return: {"1m-2m": "contango", ...}
    """
    keys = list(prices.keys())
    out = {}
    for i in range(len(keys) - 1):
        k1 = keys[i]
        k2 = keys[i + 1]
        out[f"{k1}-{k2}"] = detect_term_structure(prices[k1], prices[k2])
    return out
