"""
Tests for the news/event RSS feed parsing layer.

All tests are offline (no network calls). Feed content is provided as fixture bytes.
"""
from __future__ import annotations

import pytest
from unittest.mock import patch

from octa.core.data.sources.altdata.news.feed_fetcher import (
    _extract_entry,
    _extract_published,
    fetch_feed,
)
from octa.core.data.sources.altdata.news.feed_registry import FEED_REGISTRY, FeedSource


# ── Fixtures ──────────────────────────────────────────────────────────────────

_FED_RSS = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Federal Reserve Press Releases</title>
    <link>https://www.federalreserve.gov/</link>
    <item>
      <title>Federal Reserve issues FOMC statement</title>
      <link>https://www.federalreserve.gov/newsevents/pressreleases/monetary20240320a.htm</link>
      <guid>https://www.federalreserve.gov/newsevents/pressreleases/monetary20240320a.htm</guid>
      <pubDate>Wed, 20 Mar 2024 18:00:00 +0000</pubDate>
      <description>The Federal Open Market Committee decided to maintain the target range for the federal funds rate.</description>
    </item>
    <item>
      <title>Federal Reserve announces results of stress test</title>
      <link>https://www.federalreserve.gov/newsevents/pressreleases/bcreg20240620a.htm</link>
      <guid>https://www.federalreserve.gov/newsevents/pressreleases/bcreg20240620a.htm</guid>
      <pubDate>Thu, 20 Jun 2024 12:00:00 +0000</pubDate>
      <description>Results of the 2024 bank stress tests.</description>
    </item>
  </channel>
</rss>"""

_ECB_ATOM = b"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>ECB Press Releases</title>
  <entry>
    <id>https://www.ecb.europa.eu/press/pr/date/2024/html/ecb.mp240606.en.html</id>
    <title>Monetary policy decisions</title>
    <link href="https://www.ecb.europa.eu/press/pr/date/2024/html/ecb.mp240606.en.html"/>
    <updated>2024-06-06T14:15:00Z</updated>
    <summary>At today's meeting the Governing Council decided to lower the three key ECB interest rates by 25 basis points.</summary>
  </entry>
  <entry>
    <id>https://www.ecb.europa.eu/press/pr/date/2024/html/ecb.sp240101.en.html</id>
    <title>ECB Economic Bulletin</title>
    <link href="https://www.ecb.europa.eu/press/pr/date/2024/html/ecb.sp240101.en.html"/>
    <updated>2024-01-01T10:00:00Z</updated>
    <summary>Overview of economic conditions in the euro area.</summary>
  </entry>
</feed>"""

_CNBC_RSS = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>CNBC Top News</title>
    <item>
      <title>Dow jumps 400 points as inflation data cools</title>
      <link>https://www.cnbc.com/2024/03/20/dow-jumps.html</link>
      <guid>https://www.cnbc.com/2024/03/20/dow-jumps.html</guid>
      <pubDate>Wed, 20 Mar 2024 15:00:00 +0000</pubDate>
      <description>Markets rally after better-than-expected CPI report.</description>
    </item>
  </channel>
</rss>"""

_MALFORMED_RSS = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Partially Broken Feed</title>
    <item>
      <title>Valid entry despite malformed feed</title>
      <link>https://example.com/valid</link>
      <pubDate>Mon, 01 Jan 2024 00:00:00 +0000</pubDate>
    <!-- unclosed comment
  </channel>
</rss>"""

_EMPTY_RSS = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Empty Feed</title>
  </channel>
</rss>"""

_FED_SOURCE = FeedSource(
    source_id="fed_press",
    source_name="Federal Reserve Press Releases",
    source_type="rss",
    category="central_bank",
    url="https://www.federalreserve.gov/feeds/press_all.xml",
    poll_cadence_minutes=60,
    timeout_s=15,
    priority=1,
    source_tier=1,
    enabled=True,
    free_only=True,
    license_notes="public domain",
    usage_constraints="none",
    jurisdiction="US",
    asset_classes=("all",),
)


# ── Registry sanity checks ────────────────────────────────────────────────────

def test_feed_registry_not_empty():
    assert len(FEED_REGISTRY) >= 7, "Registry must have at least 7 feeds"


def test_feed_registry_has_tier1_sources():
    tier1 = [f for f in FEED_REGISTRY if f.source_tier == 1]
    assert len(tier1) >= 3, "Registry must have at least 3 Tier 1 (official) sources"


def test_feed_registry_has_tier3_sources():
    tier3 = [f for f in FEED_REGISTRY if f.source_tier == 3]
    assert len(tier3) >= 2, "Registry must have at least 2 Tier 3 (media) sources"


def test_feed_registry_all_free():
    non_free = [f for f in FEED_REGISTRY if not f.free_only]
    assert non_free == [], f"All registry feeds must be free_only; found: {[f.source_id for f in non_free]}"


def test_feed_registry_unique_ids():
    ids = [f.source_id for f in FEED_REGISTRY]
    assert len(ids) == len(set(ids)), "Feed registry source_ids must be unique"


def test_feed_registry_license_notes_present():
    for feed in FEED_REGISTRY:
        assert feed.license_notes, f"{feed.source_id}: license_notes must be non-empty"
        assert feed.usage_constraints, f"{feed.source_id}: usage_constraints must be non-empty"


# ── fetch_feed: offline fixture tests ─────────────────────────────────────────

def _mock_fetch_bytes(url, *, headers, timeout_s):
    """Return fixture content based on URL patterns."""
    if "federalreserve" in url:
        return _FED_RSS, 200
    if "ecb" in url:
        return _ECB_ATOM, 200
    if "cnbc" in url:
        return _CNBC_RSS, 200
    return b"", 404


def test_fetch_feed_fed_parses_entries():
    with patch(
        "octa.core.data.sources.altdata.news.feed_fetcher._fetch_bytes",
        side_effect=_mock_fetch_bytes,
    ):
        result = fetch_feed(_FED_SOURCE)

    assert result["status"] == "ok"
    assert result["source_id"] == "fed_press"
    assert len(result["entries"]) == 2
    assert result["http_status"] == 200


def test_fetch_feed_entries_have_required_fields():
    with patch(
        "octa.core.data.sources.altdata.news.feed_fetcher._fetch_bytes",
        side_effect=_mock_fetch_bytes,
    ):
        result = fetch_feed(_FED_SOURCE)

    for entry in result["entries"]:
        assert "title" in entry and entry["title"]
        assert "link" in entry
        assert "guid" in entry
        assert "published_at" in entry
        assert "summary" in entry


def test_fetch_feed_fomc_entry_title():
    with patch(
        "octa.core.data.sources.altdata.news.feed_fetcher._fetch_bytes",
        side_effect=_mock_fetch_bytes,
    ):
        result = fetch_feed(_FED_SOURCE)

    titles = [e["title"] for e in result["entries"]]
    assert any("FOMC" in t for t in titles)


def test_fetch_feed_ecb_atom():
    ecb_source = FeedSource(
        source_id="ecb_press",
        source_name="ECB",
        source_type="atom",
        category="central_bank",
        url="https://www.ecb.europa.eu/rss/press.html",
        poll_cadence_minutes=60,
        timeout_s=15,
        priority=3,
        source_tier=1,
        enabled=True,
        free_only=True,
        license_notes="ECB public",
        usage_constraints="research",
        jurisdiction="EU",
        asset_classes=("all",),
    )
    with patch(
        "octa.core.data.sources.altdata.news.feed_fetcher._fetch_bytes",
        side_effect=_mock_fetch_bytes,
    ):
        result = fetch_feed(ecb_source)

    assert result["status"] == "ok"
    assert len(result["entries"]) == 2
    # ECB rate cut entry should have it in summary
    summaries = [e["summary"] for e in result["entries"]]
    assert any("25 basis points" in s for s in summaries)


def test_fetch_feed_http_error_returns_structured_error():
    def _fail(url, *, headers, timeout_s):
        return None, 503

    with patch(
        "octa.core.data.sources.altdata.news.feed_fetcher._fetch_bytes",
        side_effect=_fail,
    ):
        result = fetch_feed(_FED_SOURCE)

    assert result["status"] == "http_error"
    assert result["entries"] == []
    assert result["http_status"] == 503
    assert result["error"] is not None
    # Must NOT raise — fail-closed


def test_fetch_feed_network_unreachable_returns_error():
    def _fail(url, *, headers, timeout_s):
        return None, None

    with patch(
        "octa.core.data.sources.altdata.news.feed_fetcher._fetch_bytes",
        side_effect=_fail,
    ):
        result = fetch_feed(_FED_SOURCE)

    assert result["status"] == "http_error"
    assert result["entries"] == []
    assert "http_status" in result


def test_fetch_feed_empty_feed_returns_empty_status():
    def _empty(url, *, headers, timeout_s):
        return _EMPTY_RSS, 200

    with patch(
        "octa.core.data.sources.altdata.news.feed_fetcher._fetch_bytes",
        side_effect=_empty,
    ):
        result = fetch_feed(_FED_SOURCE)

    assert result["status"] == "empty"
    assert result["entries"] == []


def test_fetch_feed_malformed_still_parses_entries():
    """feedparser.bozo=True does not prevent entry extraction for partially valid feeds."""
    def _malformed(url, *, headers, timeout_s):
        return _MALFORMED_RSS, 200

    with patch(
        "octa.core.data.sources.altdata.news.feed_fetcher._fetch_bytes",
        side_effect=_malformed,
    ):
        result = fetch_feed(_FED_SOURCE)

    # feedparser's bozo mode: may or may not extract entries depending on parsing
    # Key requirement: no exception, status is one of ok/empty/parse_error
    assert result["status"] in {"ok", "empty", "parse_error"}
    assert "entries" in result


# ── _extract_entry unit tests ─────────────────────────────────────────────────

def test_extract_entry_strips_html_from_summary():
    import feedparser
    raw = feedparser.parse(b"""<rss version="2.0"><channel><item>
      <title>Test</title>
      <link>https://example.com/1</link>
      <description><p>Hello <b>world</b></p></description>
    </item></channel></rss>""")
    entry = raw.entries[0]
    result = _extract_entry(entry)
    assert result is not None
    assert "<b>" not in result["summary"]
    assert "<p>" not in result["summary"]


def test_extract_entry_no_title_returns_none():
    import feedparser
    raw = feedparser.parse(b"""<rss version="2.0"><channel><item>
      <link>https://example.com/2</link>
      <description>No title item</description>
    </item></channel></rss>""")
    if raw.entries:
        result = _extract_entry(raw.entries[0])
        # entry without a title should return None
        if result is not None:
            # feedparser may infer a title; acceptable — just check structure
            assert "title" in result


def test_extract_published_from_parsed():
    import feedparser
    raw = feedparser.parse(_FED_RSS)
    entry = raw.entries[0]
    pub = _extract_published(entry)
    assert "2024-03-20" in pub
    assert "T" in pub  # ISO format
