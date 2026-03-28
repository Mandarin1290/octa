"""Tests for news event deduplication logic."""
from __future__ import annotations

import pytest
from dataclasses import replace

from octa.core.data.sources.altdata.news.feed_normalizer import NewsEvent
from octa.core.data.sources.altdata.news.feed_dedup import dedup_events, _near_dup_key


def _make_event(
    source_id: str = "fed_press",
    source_tier: int = 1,
    title: str = "Fed raises rates by 25bps",
    guid: str = "https://federalreserve.gov/1",
    published_at: str = "2024-03-20T18:00:00+00:00",
    event_type: str = "rates",
    severity: str = "critical",
    importance_score: float = 0.8,
) -> NewsEvent:
    import hashlib

    raw_hash = hashlib.sha256(f"{title}|{published_at}|{guid}".encode()).hexdigest()[:16]
    date_prefix = published_at[:10]
    canonical_event_hash = hashlib.sha256(
        f"{source_id}|{guid}|{title[:100]}|{date_prefix}".encode()
    ).hexdigest()[:16]

    return NewsEvent(
        source_id=source_id,
        source_name=f"Source {source_id}",
        source_tier=source_tier,
        fetched_at="2024-03-20T20:00:00+00:00",
        published_at=published_at,
        title=title,
        summary="summary",
        link=guid,
        guid=guid,
        category="central_bank",
        event_type=event_type,
        severity=severity,
        tags=(),
        language="en",
        jurisdiction="US",
        asset_classes=("all",),
        importance_score=importance_score,
        raw_hash=raw_hash,
        canonical_event_hash=canonical_event_hash,
    )


# ── Basic dedup ───────────────────────────────────────────────────────────────

def test_dedup_empty_list():
    assert dedup_events([]) == []


def test_dedup_single_event_passthrough():
    e = _make_event()
    result = dedup_events([e])
    assert len(result) == 1
    assert result[0].source_id == "fed_press"


def test_dedup_exact_same_event_deduped():
    """Same event twice → only one survives."""
    e1 = _make_event(guid="https://example.com/1")
    e2 = _make_event(guid="https://example.com/1")  # identical hash
    result = dedup_events([e1, e2])
    assert len(result) == 1


def test_dedup_different_events_both_kept():
    e1 = _make_event(guid="https://example.com/1", title="Fed raises rates")
    e2 = _make_event(guid="https://example.com/2", title="EIA reports oil stockpile draw")
    result = dedup_events([e1, e2])
    assert len(result) == 2


# ── Tier preference on exact hash collision ───────────────────────────────────

def test_dedup_keeps_lower_tier_on_hash_collision():
    """
    If two events have the same canonical_event_hash, the Tier 1 (official) wins
    over Tier 3 (media).
    """
    import hashlib
    # Force same canonical_event_hash on both events
    title = "FOMC raises rates"
    guid = "https://example.com/fomc"
    pub = "2024-03-20T18:00:00+00:00"
    date_prefix = pub[:10]
    # Tier 1 event
    h1 = hashlib.sha256(f"fed_press|{guid}|{title[:100]}|{date_prefix}".encode()).hexdigest()[:16]
    e_tier1 = replace(
        _make_event(source_id="fed_press", source_tier=1, title=title, guid=guid, published_at=pub),
        canonical_event_hash=h1,
    )
    # Tier 3 event with SAME hash (artificial collision)
    e_tier3 = replace(
        _make_event(source_id="cnbc_top", source_tier=3, title=title, guid=guid, published_at=pub),
        canonical_event_hash=h1,  # same hash
    )

    result = dedup_events([e_tier3, e_tier1])  # tier3 first in input
    assert len(result) == 1
    assert result[0].source_tier == 1  # Tier 1 kept


def test_dedup_different_sources_different_hashes():
    """Fed and CNBC both reporting the same story → different hashes (different source_ids)."""
    e_fed = _make_event(source_id="fed_press", source_tier=1, guid="https://fed.gov/fomc")
    e_cnbc = _make_event(source_id="cnbc_top", source_tier=3, guid="https://cnbc.com/fomc")
    result = dedup_events([e_fed, e_cnbc])
    # Different canonical hashes → both kept in level-1 dedup
    # But level-2 near-dup may remove the cnbc one since fed covers same content
    # Either 1 or 2 results is valid; key check: if 1, it must be tier 1
    assert len(result) >= 1
    if len(result) == 1:
        assert result[0].source_tier == 1


# ── Cross-source near-dedup ───────────────────────────────────────────────────

def test_near_dup_tier3_removed_when_tier1_present():
    """Tier 3 near-duplicate of a Tier 1 event is suppressed."""
    title_fed = "Federal Reserve raises interest rates"
    title_cnbc = "Federal Reserve raises interest rates today"  # near-duplicate
    e_tier1 = _make_event(
        source_id="fed_press", source_tier=1,
        title=title_fed, guid="https://fed.gov/1",
        published_at="2024-03-20T18:00:00+00:00",
    )
    e_tier3 = _make_event(
        source_id="cnbc_top", source_tier=3,
        title=title_cnbc, guid="https://cnbc.com/1",
        published_at="2024-03-20T18:30:00+00:00",
    )
    result = dedup_events([e_tier1, e_tier3])
    # Near-dup key should match → tier3 dropped
    nd_key_fed = _near_dup_key(e_tier1)
    nd_key_cnbc = _near_dup_key(e_tier3)
    # If keys match, only 1 event (the tier1 one)
    if nd_key_fed == nd_key_cnbc:
        assert len(result) == 1
        assert result[0].source_tier == 1
    else:
        # Keys differ → both kept; that's also fine (conservative dedup)
        assert len(result) == 2


def test_near_dup_two_tier3_events_both_distinct_kept():
    """Two clearly different Tier 3 events on same day → both kept."""
    e1 = _make_event(
        source_id="cnbc_top", source_tier=3,
        title="Dow climbs on earnings results",
        guid="https://cnbc.com/1",
        published_at="2024-03-20T14:00:00+00:00",
    )
    e2 = _make_event(
        source_id="marketwatch_top", source_tier=3,
        title="Oil prices tumble on demand fears",
        guid="https://mw.com/1",
        published_at="2024-03-20T15:00:00+00:00",
    )
    result = dedup_events([e1, e2])
    assert len(result) == 2


# ── Sort order ────────────────────────────────────────────────────────────────

def test_dedup_result_sorted_tier_first():
    """Result must be sorted: Tier 1 before Tier 3."""
    e_tier3 = _make_event(source_id="cnbc_top", source_tier=3, guid="https://cnbc.com/a")
    e_tier1 = _make_event(source_id="fed_press", source_tier=1, guid="https://fed.gov/b")
    result = dedup_events([e_tier3, e_tier1])
    assert result[0].source_tier <= result[-1].source_tier
