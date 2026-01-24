from octa.core.cascade.policies import DEFAULT_TIMEFRAMES


def test_cascade_order() -> None:
    assert DEFAULT_TIMEFRAMES == ("1D", "1H", "30M", "5M", "1M")

