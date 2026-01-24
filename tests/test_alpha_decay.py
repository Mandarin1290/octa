import random
from datetime import datetime, timedelta, timezone

from octa_strategy.alpha_decay import AlphaDecayDetector


def make_series_with_decay(days=200, initial_coeff=0.02, decay_start=120, noise=0.01):
    now = datetime.now(timezone.utc)
    dates = []
    signals = []
    returns = []
    coeff = initial_coeff
    for i in range(days):
        dates.append((now + timedelta(days=i)).isoformat())
        # signal is random +/-1
        s = random.normalvariate(0, 1)
        signals.append(s)
        if i < decay_start:
            r = coeff * s + random.normalvariate(0, noise)
        else:
            # decay to zero alpha
            r = 0.0 * s + random.normalvariate(0, noise)
        returns.append(r)
    return dates, signals, returns


def test_detect_decay_signal():
    random.seed(0)
    dates, sigs, rets = make_series_with_decay()
    d = AlphaDecayDetector(
        long_window=120,
        short_window=20,
        drop_threshold=0.6,
        min_samples=80,
        escalate_count=1,
    )
    for date, s, r in zip(dates, sigs, rets, strict=False):
        d.add_observation(date, "sigA", s, r, regime="normal")

    # after full series, detector should find decay for 'sigA'
    alert = d.detect_decay("sigA", regime="normal")
    assert alert is not None
    assert alert.signal == "sigA"
    assert alert.severity in ("warning", "critical")


def test_minimize_false_positives():
    random.seed(1)
    # generate stationary relation: signal always correlated
    days = 200
    now = datetime.now(timezone.utc)
    dates = [(now + timedelta(days=i)).isoformat() for i in range(days)]
    sigs = [random.normalvariate(0, 1) for _ in range(days)]
    rets = [0.02 * s + random.normalvariate(0, 0.01) for s in sigs]

    d = AlphaDecayDetector(
        long_window=120,
        short_window=20,
        drop_threshold=0.6,
        min_samples=80,
        escalate_count=2,
    )
    for date, s, r in zip(dates, sigs, rets, strict=False):
        d.add_observation(date, "sigB", s, r, regime="normal")

    # detector should not flag decay for consistent signal
    alert = d.detect_decay("sigB", regime="normal")
    assert alert is None


def generate_series(n: int, mu: float = 0.0, sigma: float = 1e-3):
    import random

    rnd = random.Random(12345)
    return [rnd.gauss(mu, sigma) for _ in range(n)]


def test_detects_synthetic_decay():
    # build long stable history then a sustained negative shift
    baseline = generate_series(200, mu=0.001, sigma=0.0005)
    shifted = generate_series(60, mu=-0.002, sigma=0.0005)
    data = baseline + shifted

    det = AlphaDecayDetector(baseline_window=150, recent_window=40)
    rpt = det.detect_decay(data)

    assert rpt.decay_score > 0.5, f"expected strong decay score, got {rpt.decay_score}"
    assert rpt.confidence > 0.9, f"expected high confidence, got {rpt.confidence}"
    assert rpt.changepoint_index is not None


def test_stable_series_not_flagged():
    data = generate_series(300, mu=0.001, sigma=0.0005)
    det = AlphaDecayDetector(baseline_window=150, recent_window=40)
    rpt = det.detect_decay(data)

    assert rpt.decay_score < 0.2, f"expected low decay score, got {rpt.decay_score}"
    assert rpt.confidence < 0.5, f"expected low confidence, got {rpt.confidence}"
