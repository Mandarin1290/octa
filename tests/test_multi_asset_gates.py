from octa_sentinel.multi_asset_gates import MultiAssetGates


def test_failing_asset_class_blocked_others_unaffected():
    class SentinelMock:
        def __init__(self):
            self.calls = []

        def set_gate(self, level, reason):
            self.calls.append((level, reason))

    sentinel = SentinelMock()
    audit = []
    engine = MultiAssetGates(
        sentinel_api=sentinel, audit_fn=lambda e, p: audit.append((e, p))
    )

    status = {
        "futures": {"roll_tested": False},
        "fx": {"funding_ratio": 0.95},
        "rates": {"stress_passed": True},
        "vol": {"exposure": 0.5, "exposure_cap": 1.0},
        "commodities": {"delivery_guard": True},
    }

    report = engine.evaluate_all(status)

    # futures should be blocked
    assert report["futures"]["ok"] is False
    # others should be allowed
    assert report["fx"]["ok"] is True
    assert report["rates"]["ok"] is True
    assert report["vol"]["ok"] is True
    assert report["commodities"]["ok"] is True

    # sentinel should have been called once for futures
    assert len(sentinel.calls) == 1
    assert sentinel.calls[0][0] == 3


def test_vol_exceeding_blocks_only_vol():
    class SentinelMock:
        def __init__(self):
            self.calls = []

        def set_gate(self, level, reason):
            self.calls.append((level, reason))

    sentinel = SentinelMock()
    engine = MultiAssetGates(sentinel_api=sentinel)

    status = {
        "futures": {"roll_tested": True},
        "fx": {"funding_ratio": 0.95},
        "rates": {"stress_passed": True},
        "vol": {"exposure": 2.5, "exposure_cap": 1.0},
        "commodities": {"delivery_guard": True},
    }

    report = engine.evaluate_all(status)
    assert report["vol"]["ok"] is False
    # ensure only vol triggered sentinel
    assert len(sentinel.calls) == 1
    assert "vol" in sentinel.calls[0][1]
