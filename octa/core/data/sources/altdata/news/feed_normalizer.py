from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Tuple


@dataclass(frozen=True)
class NewsEvent:
    """
    Normalised, immutable representation of a single news/event item.

    Fields are filled in two stages:
      1. normalize_entry()  — fills all fields except event_type, severity, importance_score
      2. feed_classifier.classify_event()  — fills event_type, severity, importance_score

    Governance notes:
      - source_tier drives the importance ceiling: Tier 1 always outscores Tier 3 for
        identical event content by design (see feed_classifier weighting formula).
      - importance_score is a transparent, auditable formula (see feed_classifier.py).
      - This layer is risk/context ONLY; it never produces trade signals directly.
    """

    # ── Source provenance ─────────────────────────────────────────────────────
    source_id: str
    source_name: str
    source_tier: int       # 1=official_institution, 2=stats/filings, 3=quality_free_media

    # ── Fetch metadata ────────────────────────────────────────────────────────
    fetched_at: str        # ISO 8601 UTC

    # ── Event content ─────────────────────────────────────────────────────────
    published_at: str      # ISO 8601 UTC (best effort from feed)
    title: str
    summary: str
    link: str
    guid: str              # original guid/id from the feed

    # ── Classification (filled by feed_classifier) ────────────────────────────
    category: str          # from FeedSource.category: "central_bank", "energy", "business_news"
    event_type: str        # rule-classified: "rates", "inflation", "energy", "earnings", …
    severity: str          # "low" | "medium" | "high" | "critical"

    # ── Metadata ──────────────────────────────────────────────────────────────
    tags: Tuple[str, ...]
    language: str
    jurisdiction: str      # "US" | "EU" | "global"
    asset_classes: Tuple[str, ...]  # ("equity", "etf", "futures", "fx", "all")

    # ── Importance (filled by feed_classifier) ────────────────────────────────
    importance_score: float   # 0.0–1.0; formula: tier_w × type_w × sev_w × recency_w

    # ── Integrity ─────────────────────────────────────────────────────────────
    raw_hash: str            # SHA-256[:16] of title+published_at+link
    canonical_event_hash: str  # SHA-256[:16] of source_id+guid+title[:100]+date_prefix


def normalize_entry(
    *,
    raw_entry: dict[str, Any],
    source_id: str,
    source_name: str,
    source_tier: int,
    source_category: str,
    source_jurisdiction: str,
    source_asset_classes: tuple[str, ...],
    fetched_at: str,
) -> NewsEvent:
    """Build a NewsEvent from a raw feed entry dict (output of feed_fetcher._extract_entry)."""
    title = str(raw_entry.get("title", "")).strip()
    summary = str(raw_entry.get("summary", "")).strip()
    link = str(raw_entry.get("link", "")).strip()
    guid = str(raw_entry.get("guid", link)).strip()
    published_at = str(raw_entry.get("published_at", "")).strip()
    tags = tuple(str(t) for t in raw_entry.get("tags", []) if t)
    language = str(raw_entry.get("language", "en")).strip() or "en"

    raw_hash = _sha256_prefix(f"{title}|{published_at}|{link}")
    date_prefix = published_at[:10] if len(published_at) >= 10 else "unknown"
    canonical_event_hash = _sha256_prefix(
        f"{source_id}|{guid}|{title[:100]}|{date_prefix}"
    )

    return NewsEvent(
        source_id=source_id,
        source_name=source_name,
        source_tier=source_tier,
        fetched_at=fetched_at,
        published_at=published_at,
        title=title,
        summary=summary,
        link=link,
        guid=guid,
        category=source_category,
        event_type="unclassified",   # overwritten by classify_event()
        severity="low",              # overwritten by classify_event()
        tags=tags,
        language=language,
        jurisdiction=source_jurisdiction,
        asset_classes=source_asset_classes,
        importance_score=0.0,        # overwritten by classify_event()
        raw_hash=raw_hash,
        canonical_event_hash=canonical_event_hash,
    )


def event_to_dict(event: NewsEvent) -> dict[str, Any]:
    """Serialise a NewsEvent to a JSON-compatible dict for cache storage."""
    return {
        "source_id": event.source_id,
        "source_name": event.source_name,
        "source_tier": event.source_tier,
        "fetched_at": event.fetched_at,
        "published_at": event.published_at,
        "title": event.title,
        "summary": event.summary,
        "link": event.link,
        "guid": event.guid,
        "category": event.category,
        "event_type": event.event_type,
        "severity": event.severity,
        "tags": list(event.tags),
        "language": event.language,
        "jurisdiction": event.jurisdiction,
        "asset_classes": list(event.asset_classes),
        "importance_score": event.importance_score,
        "raw_hash": event.raw_hash,
        "canonical_event_hash": event.canonical_event_hash,
    }


def _sha256_prefix(text: str, *, length: int = 16) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:length]
