from datetime import datetime, timedelta, timezone

from octa_risk.regime_warning import RegimeWarningSystem


def iso_now(dt):
    return dt.isoformat()


def test_warnings_early_on_regime_shift():
    sys = RegimeWarningSystem(
        long_window=80,
        short_window=20,
        min_samples=50,
        vol_increase_pct=0.2,
        corr_drop_pct=0.15,
        macro_shock_threshold=0.3,
        min_warning_score=0.15,
        escalate_count=1,
    )
    strategy = "alpha-strat"

    now = datetime.now(timezone.utc)
    # baseline: low vol, healthy corr, low macro
    for i in range(60):
        t = now - timedelta(minutes=60 - i)
        sys.record_metrics(
            iso_now(t), strategy, volatility=0.05, avg_corr=0.6, macro_shock=0.05
        )

    # recent: volatility jumps, correlation drops, macro shock rises
    for i in range(30):
        t = now + timedelta(minutes=i)
        sys.record_metrics(
            iso_now(t),
            strategy,
            volatility=0.08 + i * 0.001,
            avg_corr=0.45 - i * 0.001,
            macro_shock=0.35,
        )

    alert = sys.evaluate(strategy)
    assert alert is not None
    assert alert.warning_score >= 0.15
    assert alert.evidence_hash


def test_no_overreaction_on_small_jitter():
    sys = RegimeWarningSystem(
        long_window=80,
        short_window=20,
        min_samples=50,
        vol_increase_pct=0.2,
        corr_drop_pct=0.15,
        macro_shock_threshold=0.3,
        min_warning_score=0.15,
    )
    strategy = "beta-strat"
    now = datetime.now(timezone.utc)
    # stable baseline
    for i in range(120):
        t = now + timedelta(minutes=i)
        sys.record_metrics(
            iso_now(t),
            strategy,
            volatility=0.04 + (i % 5) * 0.0005,
            avg_corr=0.55 + (i % 4) * 0.0004,
            macro_shock=0.02,
        )

    alert = sys.evaluate(strategy)
    assert alert is None
