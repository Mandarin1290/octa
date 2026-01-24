from decimal import Decimal

import pytest

from octa_alpha.signals import SignalBuilder


def test_signal_bounds_enforced():
    vals = [0.0, 2.0, 4.0]
    sb = SignalBuilder(vals)
    sb.normalize_minmax()
    sb.encode_direction([1, -1, 1])
    sb.apply_confidence([0.8, 0.5, 0.5])
    # should not raise
    sb.enforce_bounds()
    sig = sb.get().signals
    assert all(abs(s) <= Decimal("1") for s in sig)


def test_invalid_signal_rejected():
    vals = [10.0, 20.0, 30.0]
    sb = SignalBuilder(vals)
    sb.normalize_minmax()
    # force directions so sum(abs)>1 after no confidence
    sb.encode_direction([1, 1, 1])
    # apply confidence 1.0 keeps magnitudes; sum of abs will be >1 -> should raise
    with pytest.raises(ValueError):
        sb.enforce_bounds(allow_implicit_leverage=False)
