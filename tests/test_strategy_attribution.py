from octa_strategy.attribution import AttributionEngine


def test_deviation_detected_and_review_triggered():
    calls = []

    class SentinelMock:
        def __init__(self):
            self.calls = []

        def set_gate(self, level, reason):
            self.calls.append((level, reason))

    sentinel = SentinelMock()
    ae = AttributionEngine(
        audit_fn=lambda e, p: calls.append((e, p)),
        sentinel_api=sentinel,
        deviation_threshold=0.03,
    )
    ae.record_expectation("Sx", expected_return=0.05, expected_vol=0.1)
    ae.record_realized("Sx", realized_return=0.01, realized_vol=0.12)

    assert ae.requires_review("Sx") is True
    assert any(c[0] == 2 for c in sentinel.calls)


def test_attribution_reconciles():
    ae = AttributionEngine()
    ae.record_expectation("Sy", expected_return=0.06, expected_vol=0.1)
    ae.record_realized("Sy", realized_return=0.03, realized_vol=0.09)
    ae.record_realized("Sy", realized_return=0.03, realized_vol=0.08)
    # total realized should equal 0.06
    metrics = ae.deviation_metrics("Sy")
    assert abs(metrics["realized_total"] - 0.06) < 1e-9
    assert metrics["reconciles"] is True
