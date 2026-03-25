"""
Tests for news event classification and importance scoring.

Governance-critical: verifies that Tier 1 (official) sources always outweigh
Tier 3 (media) sources for the same event_type, and that scoring is transparent.
"""
from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest

from octa.core.data.sources.altdata.news.feed_classifier import (
    classify_event,
    score_components,
    _recency_weight,
    _TIER_WEIGHT,
    _EVENT_TYPE_WEIGHT,
    _SEVERITY_WEIGHT,
)
from octa.core.data.sources.altdata.news.feed_normalizer import NewsEvent


def _make_event(
    title: str = "",
    summary: str = "",
    source_tier: int = 1,
    category: str = "central_bank",
    published_at: str = "2024-03-20T18:00:00+00:00",
    source_id: str = "fed_press",
) -> NewsEvent:
    guid = f"https://example.com/{hash(title)}"
    raw_hash = hashlib.sha256(f"{title}|{published_at}|{guid}".encode()).hexdigest()[:16]
    canonical_event_hash = hashlib.sha256(
        f"{source_id}|{guid}|{title[:100]}|{published_at[:10]}".encode()
    ).hexdigest()[:16]
    return NewsEvent(
        source_id=source_id,
        source_name="Test Source",
        source_tier=source_tier,
        fetched_at="2024-03-20T20:00:00+00:00",
        published_at=published_at,
        title=title,
        summary=summary,
        link=guid,
        guid=guid,
        category=category,
        event_type="unclassified",
        severity="low",
        tags=(),
        language="en",
        jurisdiction="US",
        asset_classes=("all",),
        importance_score=0.0,
        raw_hash=raw_hash,
        canonical_event_hash=canonical_event_hash,
    )


# ── Event type classification ─────────────────────────────────────────────────

def test_fomc_statement_classified_as_rates_critical():
    e = _make_event(title="Federal Reserve issues FOMC statement on monetary policy")
    result = classify_event(e, asof_ts="2024-03-20T20:00:00+00:00")
    assert result.event_type == "rates"
    assert result.severity == "critical"


def test_fed_speech_classified_as_central_bank_high():
    e = _make_event(title="Federal Reserve Chair Powell speaks at conference")
    result = classify_event(e, asof_ts="2024-03-20T20:00:00+00:00")
    assert result.event_type in {"central_bank", "rates"}
    assert result.severity in {"medium", "high"}


def test_ecb_rate_decision_classified_correctly():
    e = _make_event(
        title="ECB lowers deposit facility rate by 25 basis points",
        category="central_bank",
        source_id="ecb_press",
    )
    result = classify_event(e, asof_ts="2024-06-06T20:00:00+00:00")
    assert result.event_type in {"rates", "central_bank"}
    assert result.severity in {"high", "critical"}


def test_cpi_report_classified_as_inflation():
    e = _make_event(
        title="Consumer Price Index report shows inflation slowing",
        category="business_news",
        source_tier=3,
        source_id="cnbc_top",
    )
    result = classify_event(e, asof_ts="2024-03-20T20:00:00+00:00")
    assert result.event_type == "inflation"
    assert result.severity in {"medium", "high"}


def test_eia_oil_inventory_classified_as_energy():
    e = _make_event(
        title="EIA petroleum report shows crude oil inventory build",
        category="energy",
        source_id="eia_releases",
    )
    result = classify_event(e, asof_ts="2024-03-20T20:00:00+00:00")
    assert result.event_type == "energy"


def test_nonfarm_payroll_classified_as_labor():
    e = _make_event(
        title="Nonfarm payrolls add 200,000 jobs in March",
        category="business_news",
        source_tier=3,
    )
    result = classify_event(e, asof_ts="2024-03-20T20:00:00+00:00")
    assert result.event_type == "labor"
    assert result.severity in {"medium", "high"}


def test_analyst_upgrade_classified_as_analyst_sentiment():
    e = _make_event(
        title="Goldman Sachs upgrades Apple to buy rating with higher price target",
        category="business_news",
        source_tier=3,
    )
    result = classify_event(e, asof_ts="2024-03-20T20:00:00+00:00")
    assert result.event_type == "analyst_sentiment"
    assert result.severity == "low"


def test_bank_failure_classified_as_liquidity_critical():
    e = _make_event(
        title="FDIC seizes regional bank amid liquidity crisis",
        category="regulation",
    )
    result = classify_event(e, asof_ts="2024-03-20T20:00:00+00:00")
    assert result.event_type == "liquidity"
    assert result.severity == "critical"


def test_geopolitics_war_critical():
    e = _make_event(
        title="Military invasion escalates with missile strikes on capital",
        category="business_news",
        source_tier=3,
    )
    result = classify_event(e, asof_ts="2024-03-20T20:00:00+00:00")
    assert result.event_type == "geopolitics"
    assert result.severity == "high"


def test_unclassified_business_news_gets_business_news_type():
    e = _make_event(
        title="Company announces new CEO appointment",
        category="business_news",
        source_tier=3,
    )
    result = classify_event(e, asof_ts="2024-03-20T20:00:00+00:00")
    assert result.event_type == "business_news"


def test_central_bank_category_floor_medium_severity():
    """Central bank source with unclassified title gets at least medium severity."""
    e = _make_event(
        title="Press release from European Central Bank",
        category="central_bank",
        source_id="ecb_press",
    )
    result = classify_event(e, asof_ts="2024-06-06T20:00:00+00:00")
    # severity must be at least medium
    severity_order = {"low": 0, "medium": 1, "high": 2, "critical": 3}
    assert severity_order[result.severity] >= severity_order["medium"]


def test_emergency_intermeeting_central_bank_action_is_critical():
    e = _make_event(
        title="Federal Reserve announces emergency intermeeting rate cut",
        category="central_bank",
        source_tier=1,
        source_id="fed_press",
    )
    result = classify_event(e, asof_ts="2024-03-20T20:00:00+00:00")
    assert result.event_type == "rates"
    assert result.severity == "critical"


def test_tier3_media_headline_alone_is_not_critical():
    e = _make_event(
        title="CNBC headline says investors brace for shock ahead of Fed decision",
        category="business_news",
        source_tier=3,
        source_id="cnbc_top",
    )
    result = classify_event(e, asof_ts="2024-03-20T20:00:00+00:00")
    assert result.severity != "critical"


def test_tier3_media_rates_event_is_capped_below_critical():
    e = _make_event(
        title="FOMC rate decision announced",
        category="business_news",
        source_tier=3,
        source_id="cnbc_top",
    )
    result = classify_event(e, asof_ts="2024-03-20T20:00:00+00:00")
    assert result.event_type == "rates"
    assert result.severity != "critical"
    assert result.severity in {"low", "medium", "high"}


def test_official_energy_supply_disruption_is_at_least_high():
    e = _make_event(
        title="EIA reports pipeline outage causing energy supply disruption",
        category="energy",
        source_tier=1,
        source_id="eia_releases",
    )
    result = classify_event(e, asof_ts="2024-03-20T20:00:00+00:00")
    assert result.event_type == "energy"
    assert result.severity in {"high", "critical"}


# ── Importance score formula ──────────────────────────────────────────────────

def test_importance_score_range():
    e = _make_event(title="Federal Reserve raises interest rates by 50 bps")
    result = classify_event(e, asof_ts="2024-03-20T20:00:00+00:00")
    assert 0.0 <= result.importance_score <= 1.0


def test_importance_score_positive_for_classified_event():
    e = _make_event(title="FOMC rate decision announced")
    result = classify_event(e, asof_ts="2024-03-20T20:00:00+00:00")
    assert result.importance_score > 0.0


def test_tier1_outscores_tier3_same_content():
    """
    Governance requirement: Tier 1 (official) always outscores Tier 3 (media)
    for the same event_type, severity, and recency — because tier_w is a multiplier.
    """
    asof_ts = "2024-03-20T20:00:00+00:00"
    pub = "2024-03-20T18:00:00+00:00"  # 2h old → recency_w = 1.0

    e_tier1 = _make_event(
        title="FOMC rate decision",
        source_tier=1,
        source_id="fed_press",
        published_at=pub,
    )
    e_tier3 = _make_event(
        title="FOMC rate decision",
        source_tier=3,
        source_id="cnbc_top",
        published_at=pub,
    )
    r1 = classify_event(e_tier1, asof_ts=asof_ts)
    r3 = classify_event(e_tier3, asof_ts=asof_ts)

    # Both classified as rates/critical (same text) — tier1 must score higher
    assert r1.event_type == r3.event_type
    assert r1.importance_score > r3.importance_score


def test_score_components_are_auditable():
    """score_components() must return all formula components with numeric values."""
    e = _make_event(title="Federal Reserve raises rates")
    classified = classify_event(e, asof_ts="2024-03-20T20:00:00+00:00")
    components = score_components(classified, asof_ts="2024-03-20T20:00:00+00:00")

    assert "tier_w" in components
    assert "type_w" in components
    assert "sev_w" in components
    assert "recency_w" in components
    assert "importance_score" in components
    # Formula check: score ≈ tier_w × type_w × sev_w × recency_w
    expected = components["tier_w"] * components["type_w"] * components["sev_w"] * components["recency_w"]
    assert abs(components["importance_score"] - expected) < 1e-9


def test_stale_event_score_near_zero():
    """Events older than 7 days should have near-zero importance."""
    e = _make_event(
        title="Federal Reserve raises interest rates",
        published_at="2024-03-01T12:00:00+00:00",  # 19 days old
    )
    result = classify_event(e, asof_ts="2024-03-20T12:00:00+00:00")
    assert result.importance_score == 0.0


def test_fresh_event_higher_score_than_day_old():
    """Fresh event (1h) should score higher than 24h-old event, all else equal."""
    asof = "2024-03-20T20:00:00+00:00"
    e_fresh = _make_event(title="FOMC rate decision", published_at="2024-03-20T19:00:00+00:00")
    e_old = _make_event(title="FOMC rate decision", published_at="2024-03-19T20:00:00+00:00")
    r_fresh = classify_event(e_fresh, asof_ts=asof)
    r_old = classify_event(e_old, asof_ts=asof)
    assert r_fresh.importance_score > r_old.importance_score


# ── Recency weight unit tests ─────────────────────────────────────────────────

def test_recency_weight_fresh():
    w = _recency_weight("2024-03-20T18:00:00+00:00", asof_ts="2024-03-20T18:30:00+00:00")
    assert w == 1.0  # 30 minutes old → fully fresh


def test_recency_weight_2h_exactly():
    w = _recency_weight("2024-03-20T16:00:00+00:00", asof_ts="2024-03-20T18:00:00+00:00")
    assert w == 1.0  # exactly 2h → still full weight


def test_recency_weight_24h():
    w = _recency_weight(
        "2024-03-19T18:00:00+00:00",
        asof_ts="2024-03-20T18:00:00+00:00",
        source_tier=1,
        event_type="rates",
    )
    assert 0.6 < w < 1.0


def test_recency_weight_stale_168h():
    w = _recency_weight(
        "2024-03-13T18:00:00+00:00",
        asof_ts="2024-03-20T18:00:00+00:00",
        source_tier=1,
        event_type="rates",
    )
    assert 0.0 < w <= 0.35


def test_recency_weight_after_168h_is_zero():
    w = _recency_weight(
        "2024-03-13T17:00:00+00:00",
        asof_ts="2024-03-20T18:00:00+00:00",
        source_tier=1,
        event_type="rates",
    )
    assert w == 0.0


def test_recency_weight_invalid_ts_returns_safe_default():
    w = _recency_weight("not-a-timestamp")
    assert w == 0.5


def test_recency_weight_empty_ts_returns_safe_default():
    w = _recency_weight("")
    assert w == 0.5


def test_recency_weight_future_dated_returns_1():
    w = _recency_weight("2025-01-01T00:00:00+00:00", asof_ts="2024-03-20T00:00:00+00:00")
    assert w == 1.0


def test_tier1_recency_decays_slower_than_tier3():
    asof_ts = "2024-03-20T20:00:00+00:00"
    published_at = "2024-03-19T20:00:00+00:00"
    w_tier1 = _recency_weight(published_at, asof_ts=asof_ts, source_tier=1, event_type="central_bank")
    w_tier3 = _recency_weight(published_at, asof_ts=asof_ts, source_tier=3, event_type="business_news")
    assert w_tier1 > w_tier3


def test_central_bank_recency_decays_slower_than_analyst_sentiment():
    asof_ts = "2024-03-21T12:00:00+00:00"
    published_at = "2024-03-20T00:00:00+00:00"
    w_cb = _recency_weight(published_at, asof_ts=asof_ts, source_tier=1, event_type="central_bank")
    w_analyst = _recency_weight(published_at, asof_ts=asof_ts, source_tier=3, event_type="analyst_sentiment")
    assert w_cb > w_analyst


def test_recency_weight_monotonic_and_non_negative():
    t0 = _recency_weight("2024-03-20T18:00:00+00:00", asof_ts="2024-03-20T19:00:00+00:00", source_tier=1, event_type="rates")
    t1 = _recency_weight("2024-03-20T18:00:00+00:00", asof_ts="2024-03-21T00:00:00+00:00", source_tier=1, event_type="rates")
    t2 = _recency_weight("2024-03-20T18:00:00+00:00", asof_ts="2024-03-23T00:00:00+00:00", source_tier=1, event_type="rates")
    t3 = _recency_weight("2024-03-20T18:00:00+00:00", asof_ts="2024-03-29T00:00:00+00:00", source_tier=1, event_type="rates")
    assert 1.0 >= t0 >= t1 >= t2 >= t3 >= 0.0
