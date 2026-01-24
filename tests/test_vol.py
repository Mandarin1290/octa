from octa_assets.vol.convexity import ConvexityTracker, VolPosition
from octa_assets.vol.term_structure import (
    detect_term_structure,
    term_structure_from_curve,
)


def test_term_structure_detected():
    assert detect_term_structure(20.0, 22.0) == "contango"
    assert detect_term_structure(25.0, 20.0) == "backwardation"
    curve = {"1m": 20.0, "2m": 21.0, "3m": 19.0}
    ts = term_structure_from_curve(curve)
    assert ts["1m-2m"] == "contango"
    assert ts["2m-3m"] == "backwardation"


def test_short_vol_cap_enforced():
    sentinel = type(
        "S",
        (),
        {"last": None, "set_gate": lambda self, l, r: setattr(self, "last", (l, r))},
    )()
    ct = ConvexityTracker(audit_fn=lambda e, p: None, sentinel_api=sentinel)
    # record two short vol positions
    ct.record(
        VolPosition(
            instrument="VIXF1",
            type="future",
            qty=-10,
            notional=100000,
            convexity_proxy=0.001,
        )
    )
    ct.record(
        VolPosition(
            instrument="VIXF2",
            type="future",
            qty=-5,
            notional=50000,
            convexity_proxy=0.001,
        )
    )

    ok, breaches = ct.enforce_short_vol_cap(cap_notional=1000000)
    assert ok is True

    # now low cap
    ok2, breaches2 = ct.enforce_short_vol_cap(cap_notional=10000)
    assert ok2 is False
    assert "short_notional" in breaches2
