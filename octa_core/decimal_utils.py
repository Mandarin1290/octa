from decimal import ROUND_DOWN, Decimal, InvalidOperation, localcontext
from typing import Union


def quantize_8(dec: Union[Decimal, str, float, int]) -> Decimal:
    """Return a Decimal quantized to 8 decimal places with safe fallbacks.

    Uses a string-format approach first for deterministic results, then falls
    back to a `localcontext` quantize with `ROUND_DOWN` and traps disabled to
    avoid `InvalidOperation` exceptions caused by global contexts.
    """
    d = Decimal(dec)
    try:
        return Decimal(f"{d:.8f}")
    except Exception:
        with localcontext() as ctx:
            ctx.traps[InvalidOperation] = False
            ctx.rounding = ROUND_DOWN
            return d.quantize(Decimal("0.00000001"))


__all__ = ["quantize_8"]
