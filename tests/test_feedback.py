from decimal import Decimal

from octa_alpha.feedback import FeedbackEngine


def test_noise_ignored():
    fe = FeedbackEngine(
        window_size=10,
        lag_periods=2,
        min_periods=5,
        learning_rate=Decimal("1.0"),
        significance_threshold=Decimal("0.01"),
        max_adjust_pct=Decimal("0.2"),
    )
    # add a single spike (recent) for alpha X
    for p in range(1, 6):
        # small neutral returns earlier
        fe.add_return("X", p, 0.0)
    # a spike at period 6 (will be within lag if current_period=7)
    fe.add_return("X", 6, 1.0)
    base = {"X": Decimal("1.0")}
    adjusted, mults = fe.adjust_scores(base, current_period=7)
    # spike should be ignored because it's within lag or insufficient sustained signal
    assert mults["X"] == Decimal("1")
    assert adjusted["X"] == Decimal("1.00000000")


def test_persistent_signal_adjusted():
    fe = FeedbackEngine(
        window_size=10,
        lag_periods=1,
        min_periods=3,
        learning_rate=Decimal("1.0"),
        significance_threshold=Decimal("0.01"),
        max_adjust_pct=Decimal("0.5"),
    )
    # feed three positive returns outside lag window
    fe.add_return("Y", 1, 0.02)
    fe.add_return("Y", 2, 0.03)
    fe.add_return("Y", 3, 0.025)
    base = {"Y": Decimal("1.0")}
    adjusted, mults = fe.adjust_scores(base, current_period=5)
    # mean ~0.025 -> multiplier = 1 + 0.025 -> >1
    assert mults["Y"] > Decimal("1")
    assert adjusted["Y"] > base["Y"]
