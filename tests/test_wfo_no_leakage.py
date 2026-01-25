from octa.core.research.validation.leakage import check_leakage


def test_wfo_no_leakage() -> None:
    report = check_leakage({"leak_score": 0.0})
    assert report.ok is True
