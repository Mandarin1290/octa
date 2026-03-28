from __future__ import annotations

import hashlib
import re

from .feed_normalizer import NewsEvent

# Minimum number of significant words to compute a content-based near-dup key
_NEAR_DUP_WORDS = 6

# Stop words to strip before near-dup hashing (English; keep it minimal)
_STOP_WORDS = frozenset(
    "a an the in on at to of for and or is are was were be been being "
    "has have had will would could should may might shall do does did "
    "with by from as its it this that these those".split()
)


def dedup_events(events: list[NewsEvent]) -> list[NewsEvent]:
    """
    Deduplicate events with two levels:

    Level 1 — exact dedup:
        Key = canonical_event_hash (source_id + guid + title[:100] + date_prefix).
        When two events share the same hash, keep the one from the lower source_tier
        (Tier 1 official > Tier 3 media).

    Level 2 — cross-source near-dup (conservative, Tier 3 only):
        Key = hash of first N significant words of title + date_prefix.
        If a Tier ≤ 2 event already covers the same content, the Tier 3 duplicate
        is dropped. Two Tier 3 events that are near-duplicates keep the one from
        the lower source_id priority (earlier in FEED_REGISTRY order).

    Returns deduplicated list sorted by source_tier ASC then published_at DESC
    (most-official and most-recent first).
    """
    if not events:
        return []

    # ── Level 1: exact dedup by canonical_event_hash ──────────────────────────
    exact: dict[str, NewsEvent] = {}
    for event in events:
        key = event.canonical_event_hash
        if key not in exact:
            exact[key] = event
        else:
            # Keep the more authoritative source (lower tier number wins)
            if event.source_tier < exact[key].source_tier:
                exact[key] = event

    deduplicated = list(exact.values())

    # ── Level 2: cross-source near-dup (Tier 3 against Tier ≤ 2 only) ─────────
    # Build a set of near-dup keys from all Tier ≤ 2 events
    authoritative_keys: set[str] = set()
    for event in deduplicated:
        if event.source_tier <= 2:
            authoritative_keys.add(_near_dup_key(event))

    result: list[NewsEvent] = []
    near_dup_seen: set[str] = set(authoritative_keys)
    for event in deduplicated:
        if event.source_tier <= 2:
            result.append(event)
        else:
            # Tier 3+: drop if a Tier ≤ 2 event already covers this content
            nd_key = _near_dup_key(event)
            if nd_key in near_dup_seen:
                continue  # near-duplicate of an authoritative event → skip
            near_dup_seen.add(nd_key)
            result.append(event)

    # Sort: tier ASC (official first), then published_at DESC (newest first)
    result.sort(key=lambda e: (e.source_tier, _invert_iso(e.published_at)))
    return result


def _near_dup_key(event: NewsEvent) -> str:
    """Content-based near-dup key: hash of N significant title words + date prefix."""
    words = re.findall(r"[a-z0-9]+", event.title.lower())
    significant = [w for w in words if w not in _STOP_WORDS][:_NEAR_DUP_WORDS]
    date_prefix = event.published_at[:10] if len(event.published_at) >= 10 else "unknown"
    content = " ".join(significant) + "|" + date_prefix
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:12]


def _invert_iso(ts: str) -> str:
    """
    Lexicographic inversion of ISO timestamp for descending sort.
    Maps each char c → chr(0x7E - ord(c) + 0x20) within printable ASCII range.
    Falls back to empty string for non-ISO values (sorts last).
    """
    if not ts:
        return "~" * 20  # sorts last
    try:
        return "".join(chr(max(32, min(126, 0x9E - ord(c)))) for c in ts[:19])
    except Exception:
        return "~" * 20
