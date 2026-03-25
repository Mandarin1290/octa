"""
Tests for NewsEventsSource integration, fail-closed behaviour, and event_features consumption.
No network calls; all feeds are mocked.
"""
from __future__ import annotations

import hashlib
import pytest
from unittest.mock import patch

from octa.core.data.sources.altdata.news.news_events_source import NewsEventsSource
from octa.core.features.altdata.event_features import build as build_event_features


# ── Fixtures ──────────────────────────────────────────────────────────────────

_MINI_RSS = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Federal Reserve Press Releases</title>
    <item>
      <title>Federal Reserve issues FOMC statement</title>
      <link>https://federalreserve.gov/fomc1</link>
      <guid>https://federalreserve.gov/fomc1</guid>
      <pubDate>Wed, 20 Mar 2024 18:00:00 +0000</pubDate>
      <description>FOMC decided to maintain target range for federal funds rate.</description>
    </item>
  </channel>
</rss>"""

_EIA_RSS = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>EIA Press Releases</title>
    <item>
      <title>EIA petroleum report shows crude oil inventory draw</title>
      <link>https://eia.gov/pr/1</link>
      <guid>https://eia.gov/pr/1</guid>
      <pubDate>Wed, 20 Mar 2024 14:00:00 +0000</pubDate>
      <description>Weekly petroleum status report.</description>
    </item>
  </channel>
</rss>"""


def _feed_bytes_by_url(url, *, headers, timeout_s):
    if "federalreserve" in url:
        return _MINI_RSS, 200
    if "eia" in url:
        return _EIA_RSS, 200
    if "ecb" in url:
        return b"""<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom">
        <entry>
          <id>https://ecb.europa.eu/1</id>
          <title>ECB raises deposit facility rate</title>
          <link href="https://ecb.europa.eu/1"/>
          <updated>2024-03-20T14:15:00Z</updated>
        </entry></feed>""", 200
    if "cnbc" in url or "marketwatch" in url:
        return b"""<?xml version="1.0"?><rss version="2.0"><channel>
          <item>
            <title>Markets mixed as investors await Fed decision</title>
            <link>https://cnbc.com/1</link>
            <guid>https://cnbc.com/1</guid>
            <pubDate>Wed, 20 Mar 2024 16:00:00 +0000</pubDate>
          </item>
        </channel></rss>""", 200
    # Default: return empty feed
    return b"""<?xml version="1.0"?><rss version="2.0"><channel></channel></rss>""", 200


# ── Offline mode (training-safe) ──────────────────────────────────────────────

def test_fetch_raw_offline_returns_none():
    """allow_net=False → returns None immediately (no network, training-safe)."""
    from datetime import date
    src = NewsEventsSource({"enabled": True})
    result = src.fetch_raw(asof=date(2024, 3, 20), allow_net=False)
    assert result is None


def test_fetch_raw_disabled_source_still_offline():
    """Disabled source with allow_net=True still returns None after allow_net=False."""
    from datetime import date
    src = NewsEventsSource({"enabled": False})
    result = src.fetch_raw(asof=date(2024, 3, 20), allow_net=False)
    assert result is None


# ── Online fetch (mocked) ─────────────────────────────────────────────────────

def test_fetch_raw_returns_rows_list():
    from datetime import date

    src = NewsEventsSource({"enabled": True, "timeout_s": 15})
    with patch(
        "octa.core.data.sources.altdata.news.feed_fetcher._fetch_bytes",
        side_effect=_feed_bytes_by_url,
    ):
        result = src.fetch_raw(asof=date(2024, 3, 20), allow_net=True)

    assert isinstance(result, dict)
    assert "rows" in result
    assert "status" in result
    assert result["status"] in {"ok", "empty"}
    assert isinstance(result["rows"], list)


def test_fetch_raw_rows_have_required_fields():
    from datetime import date

    src = NewsEventsSource({"enabled": True, "timeout_s": 15})
    with patch(
        "octa.core.data.sources.altdata.news.feed_fetcher._fetch_bytes",
        side_effect=_feed_bytes_by_url,
    ):
        result = src.fetch_raw(asof=date(2024, 3, 20), allow_net=True)

    for row in result["rows"]:
        assert "source_id" in row
        assert "title" in row
        assert "importance_score" in row
        assert "event_type" in row
        assert "severity" in row
        assert "source_tier" in row
        assert "canonical_event_hash" in row


def test_fetch_raw_meta_contains_dedup_counts():
    from datetime import date

    src = NewsEventsSource({"enabled": True})
    with patch(
        "octa.core.data.sources.altdata.news.feed_fetcher._fetch_bytes",
        side_effect=_feed_bytes_by_url,
    ):
        result = src.fetch_raw(asof=date(2024, 3, 20), allow_net=True)

    meta = result.get("meta", {})
    assert "total_fetched" in meta
    assert "after_dedup" in meta
    assert meta["after_dedup"] <= meta["total_fetched"]


def test_fetch_raw_contains_tier1_events():
    from datetime import date

    src = NewsEventsSource({"enabled": True})
    with patch(
        "octa.core.data.sources.altdata.news.feed_fetcher._fetch_bytes",
        side_effect=_feed_bytes_by_url,
    ):
        result = src.fetch_raw(asof=date(2024, 3, 20), allow_net=True)

    tier1_rows = [r for r in result["rows"] if r.get("source_tier") == 1]
    assert len(tier1_rows) >= 1, "Expected at least one Tier 1 event in results"


# ── Fail-closed behaviour ─────────────────────────────────────────────────────

def test_all_feeds_fail_returns_empty_not_exception():
    """If every feed returns a network error, fetch_raw must NOT raise — returns empty."""
    from datetime import date

    def _fail_all(url, *, headers, timeout_s):
        return None, 503

    src = NewsEventsSource({"enabled": True})
    with patch(
        "octa.core.data.sources.altdata.news.feed_fetcher._fetch_bytes",
        side_effect=_fail_all,
    ):
        result = src.fetch_raw(asof=date(2024, 3, 20), allow_net=True)

    assert result is not None
    assert result["rows"] == []
    assert result["status"] == "empty"
    assert result["meta"]["error_count"] > 0


def test_one_feed_fails_others_continue():
    """Partial failure: some feeds fail, others succeed → partial result, no crash."""
    from datetime import date

    def _partial_fail(url, *, headers, timeout_s):
        if "federalreserve" in url:
            return None, 503  # Fed fails
        return _feed_bytes_by_url(url, headers=headers, timeout_s=timeout_s)

    src = NewsEventsSource({"enabled": True})
    with patch(
        "octa.core.data.sources.altdata.news.feed_fetcher._fetch_bytes",
        side_effect=_partial_fail,
    ):
        result = src.fetch_raw(asof=date(2024, 3, 20), allow_net=True)

    assert result is not None
    # Should have some rows from the non-failing feeds
    assert "rows" in result
    meta = result["meta"]
    # Fed failed → error_count >= 1
    assert meta["error_count"] >= 1
    # But other feeds worked → some fetch_results have status="ok"
    ok_results = [f for f in meta["fetch_results"] if f["status"] == "ok"]
    assert len(ok_results) >= 1


# ── Idempotency ───────────────────────────────────────────────────────────────

def test_normalize_is_passthrough():
    src = NewsEventsSource({"enabled": True})
    payload = {"rows": [{"title": "test"}], "status": "ok"}
    assert src.normalize(payload) is payload


def test_cache_key_format():
    from datetime import date
    src = NewsEventsSource({"enabled": True})
    key = src.cache_key(asof=date(2024, 3, 20))
    assert key == "news_events_2024-03-20"


# ── event_features integration ────────────────────────────────────────────────

def test_event_features_build_with_news_events():
    """build() must include news_* features when news_events payload is present."""
    # Build a minimal payload with one Tier 1 event
    payloads = {
        "news_events": {
            "rows": [
                {
                    "source_id": "fed_press",
                    "source_name": "Federal Reserve",
                    "source_tier": 1,
                    "fetched_at": "2024-03-20T20:00:00+00:00",
                    "published_at": "2024-03-20T18:00:00+00:00",
                    "title": "FOMC rate decision",
                    "summary": "FOMC decided to maintain target range",
                    "link": "https://fed.gov/fomc1",
                    "guid": "https://fed.gov/fomc1",
                    "category": "central_bank",
                    "event_type": "rates",
                    "severity": "critical",
                    "tags": [],
                    "language": "en",
                    "jurisdiction": "US",
                    "asset_classes": ["all"],
                    "importance_score": 0.8,
                    "raw_hash": "abc",
                    "canonical_event_hash": "def",
                }
            ],
            "status": "ok",
        }
    }
    features = build_event_features(payloads, asof_ts="2024-03-20T20:00:00+00:00")

    assert "news_event_count" in features
    assert "news_tier1_count" in features
    assert "news_risk_score" in features


def test_event_features_build_with_scheduled_events_window():
    payloads = {
        "scheduled_events": {
            "rows": [
                {
                    "event_id": "fed_fomc",
                    "scheduled_at": "2024-03-20T18:00:00+00:00",
                    "known_at": "2024-01-01T00:00:00+00:00",
                    "source_id": "fed_schedule",
                    "source_name": "Federal Reserve Schedule",
                    "source_tier": 1,
                    "event_type": "rates",
                    "severity_floor": "high",
                    "pre_window_hours": 24,
                    "post_window_hours": 2,
                }
            ],
            "status": "ok",
        }
    }
    features = build_event_features(payloads, asof_ts="2024-03-20T12:00:00+00:00")

    assert features["scheduled_event_count"] == 1.0
    assert features["scheduled_tier1_count"] == 1.0
    assert features["scheduled_central_bank_flag"] == 1.0
    assert features["scheduled_macro_window_flag"] == 1.0
    assert 0.0 < features["scheduled_event_bonus"] <= 0.15


def test_event_features_scheduled_window_expires_after_post_window():
    payloads = {
        "scheduled_events": {
            "rows": [
                {
                    "event_id": "fed_fomc",
                    "scheduled_at": "2024-03-20T18:00:00+00:00",
                    "known_at": "2024-01-01T00:00:00+00:00",
                    "source_id": "fed_schedule",
                    "source_tier": 1,
                    "event_type": "rates",
                    "severity_floor": "high",
                    "pre_window_hours": 24,
                    "post_window_hours": 2,
                }
            ],
            "status": "ok",
        }
    }
    features = build_event_features(payloads, asof_ts="2024-03-20T21:30:01+00:00")
    assert features["scheduled_event_count"] == 0.0
    assert features["scheduled_event_bonus"] == 0.0


def test_event_features_scheduled_known_at_prevents_future_leakage():
    payloads = {
        "scheduled_events": {
            "rows": [
                {
                    "event_id": "fed_future_known_late",
                    "scheduled_at": "2024-03-20T18:00:00+00:00",
                    "known_at": "2024-03-20T13:00:00+00:00",
                    "source_id": "fed_schedule",
                    "source_tier": 1,
                    "event_type": "rates",
                    "severity_floor": "high",
                    "pre_window_hours": 24,
                    "post_window_hours": 2,
                }
            ],
            "status": "ok",
        }
    }
    features = build_event_features(payloads, asof_ts="2024-03-20T12:00:00+00:00")
    assert features["scheduled_event_count"] == 0.0
    assert features["scheduled_event_bonus"] == 0.0


def test_event_features_empty_scheduled_payload_fail_closed():
    features = build_event_features({"scheduled_events": {"rows": [], "status": "empty"}}, asof_ts="2024-03-20T12:00:00+00:00")
    assert features["scheduled_event_count"] == 0.0
    assert features["scheduled_event_bonus"] == 0.0
    assert features["scheduled_status"] == 1.0


def test_event_features_empty_news_events_returns_zeros():
    """No news_events in payloads → all news_* features return 0 or safe defaults."""
    payloads: dict = {}
    features = build_event_features(payloads, asof_ts="2024-03-20T20:00:00+00:00")

    assert features["news_event_count"] == 0.0
    assert features["news_risk_score"] == 0.0
    assert features["news_critical_flag"] == 0.0
    assert features["news_status"] == 1.0  # missing_cache


def test_event_features_news_leakage_guard():
    """Future-dated events must be filtered out (leakage guard)."""
    payloads = {
        "news_events": {
            "rows": [
                {
                    "source_id": "fed_press",
                    "source_name": "Federal Reserve",
                    "source_tier": 1,
                    "fetched_at": "2024-03-20T20:00:00+00:00",
                    "published_at": "2024-03-21T18:00:00+00:00",  # FUTURE
                    "title": "Future Fed announcement",
                    "summary": "",
                    "link": "https://fed.gov/future",
                    "guid": "https://fed.gov/future",
                    "category": "central_bank",
                    "event_type": "rates",
                    "severity": "critical",
                    "tags": [],
                    "language": "en",
                    "jurisdiction": "US",
                    "asset_classes": ["all"],
                    "importance_score": 0.9,
                    "raw_hash": "abc",
                    "canonical_event_hash": "def",
                }
            ],
            "status": "ok",
        }
    }
    features = build_event_features(payloads, asof_ts="2024-03-20T20:00:00+00:00")

    # Future event must be filtered
    assert features["news_event_count"] == 0.0
    assert features["news_critical_flag"] == 0.0


def test_event_features_tier_weighting():
    """Tier 1 contributes 2× weight to news_risk_score vs Tier 3."""
    payloads = {
        "news_events": {
            "rows": [
                {
                    "source_id": "fed_press",
                    "source_name": "Federal Reserve",
                    "source_tier": 1,
                    "fetched_at": "2024-03-20T20:00:00+00:00",
                    "published_at": "2024-03-20T18:00:00+00:00",
                    "title": "FOMC decision",
                    "summary": "",
                    "link": "https://fed.gov/1",
                    "guid": "https://fed.gov/1",
                    "category": "central_bank",
                    "event_type": "rates",
                    "severity": "critical",
                    "tags": [],
                    "language": "en",
                    "jurisdiction": "US",
                    "asset_classes": ["all"],
                    "importance_score": 0.8,
                    "raw_hash": "h1",
                    "canonical_event_hash": "c1",
                },
                {
                    "source_id": "cnbc_top",
                    "source_name": "CNBC",
                    "source_tier": 3,
                    "fetched_at": "2024-03-20T20:00:00+00:00",
                    "published_at": "2024-03-20T18:00:00+00:00",
                    "title": "CNBC market update",
                    "summary": "",
                    "link": "https://cnbc.com/1",
                    "guid": "https://cnbc.com/1",
                    "category": "business_news",
                    "event_type": "business_news",
                    "severity": "low",
                    "tags": [],
                    "language": "en",
                    "jurisdiction": "US",
                    "asset_classes": ["equity"],
                    "importance_score": 0.2,
                    "raw_hash": "h2",
                    "canonical_event_hash": "c2",
                },
            ],
            "status": "ok",
        }
    }
    features = build_event_features(payloads, asof_ts="2024-03-20T20:00:00+00:00")
    # tier1_count=1 (weight=2), tier3_count=1 (weight=1)
    # weighted_sum = 2*0.8 + 1*0.2 = 1.8; weighted_count = 3; score = 0.6
    assert features["news_tier1_count"] == 1.0
    assert features["news_tier3_count"] == 1.0
    # score should be dominated by the tier1 high-importance event
    assert features["news_risk_score"] > 0.5
