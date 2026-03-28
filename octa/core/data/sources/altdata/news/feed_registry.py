from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple


@dataclass(frozen=True)
class FeedSource:
    """Immutable definition of a single RSS/Atom feed source."""

    source_id: str
    source_name: str
    source_type: str           # "rss" | "atom"
    category: str              # "central_bank" | "energy" | "business_news" | ...
    url: str
    poll_cadence_minutes: int
    timeout_s: int
    priority: int              # lower = higher priority (1 = highest)
    source_tier: int           # 1=official_institution, 2=stats/filings, 3=quality_free_media
    enabled: bool
    free_only: bool
    license_notes: str
    usage_constraints: str
    jurisdiction: str          # "US" | "EU" | "global"
    asset_classes: Tuple[str, ...]  # ("equity", "etf", "futures", "fx", "all", ...)


# ── Sources explicitly NOT in this registry ────────────────────────────────────
EXCLUDED_SOURCES_RATIONALE: dict[str, str] = {
    "reuters": (
        "Requires paid Thomson Reuters/LSEG license for automated/systematic consumption; "
        "no free official RSS for institutional use"
    ),
    "bloomberg": "Requires paid Bloomberg Terminal or B-PIPE license; no free public feed",
    "refinitiv": "Requires paid Refinitiv/LSEG license; same legal entity as reuters post-2021",
    "twitter_x": (
        "API v2 severely rate-limited; ToS explicitly prohibits building trading systems "
        "from content; noise >> signal for systematic risk use"
    ),
    "reddit_news_scrape": (
        "Reddit RSS available but rate-limited; r/wallstreetbets noise dominates signal; "
        "existing RedditSource stub is sufficient placeholder"
    ),
    "ap_news": (
        "AP content is wire-distributed under licensing agreements; "
        "no official free RSS endpoint for systematic consumption"
    ),
    "ft_financial_times": (
        "FT requires subscription; RSS requires FT account; not free"
    ),
    "wsj_wall_street_journal": (
        "WSJ requires Dow Jones subscription for systematic access; "
        "MarketWatch (same parent, Dow Jones) provides free RSS endpoint"
    ),
}

# ── Registry ───────────────────────────────────────────────────────────────────
# Ordered by priority (lowest number = highest priority)
FEED_REGISTRY: list[FeedSource] = [
    # ── TIER 1: Official central bank / U.S. Government feeds ────────────────

    FeedSource(
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
        license_notes=(
            "Official U.S. Federal Government website. Content is public domain "
            "(17 U.S.C. § 105). No copyright restrictions."
        ),
        usage_constraints=(
            "No restrictions on research/automated use. "
            "Attribute as 'Federal Reserve' per standard academic/research practice."
        ),
        jurisdiction="US",
        asset_classes=("all",),
    ),

    FeedSource(
        source_id="fed_speeches",
        source_name="Federal Reserve Speeches",
        source_type="rss",
        category="central_bank",
        url="https://www.federalreserve.gov/feeds/speeches.xml",
        poll_cadence_minutes=120,
        timeout_s=15,
        priority=2,
        source_tier=1,
        enabled=True,
        free_only=True,
        license_notes=(
            "Official U.S. Federal Government website. Content is public domain "
            "(17 U.S.C. § 105). No copyright restrictions."
        ),
        usage_constraints=(
            "No restrictions on research/automated use. "
            "Attribute as 'Federal Reserve' per standard academic/research practice."
        ),
        jurisdiction="US",
        asset_classes=("all",),
    ),

    FeedSource(
        source_id="ecb_press",
        source_name="European Central Bank Press Releases",
        source_type="rss",
        category="central_bank",
        url="https://www.ecb.europa.eu/rss/press.html",
        poll_cadence_minutes=60,
        timeout_s=15,
        priority=3,
        source_tier=1,
        enabled=True,
        free_only=True,
        license_notes=(
            "European Central Bank public information. ECB copyright policy allows "
            "reproduction for non-commercial, educational, and research purposes "
            "provided the source is cited."
        ),
        usage_constraints=(
            "Non-commercial research use permitted. "
            "Attribute as 'European Central Bank (ECB)'. "
            "See: https://www.ecb.europa.eu/home/html/disclaimer.en.html"
        ),
        jurisdiction="EU",
        asset_classes=("all",),
    ),

    FeedSource(
        source_id="eia_releases",
        source_name="U.S. Energy Information Administration Press Releases",
        source_type="rss",
        category="energy",
        url="https://www.eia.gov/rss/press_releases.xml",
        poll_cadence_minutes=120,
        timeout_s=15,
        priority=4,
        source_tier=1,
        enabled=True,
        free_only=True,
        license_notes=(
            "Official U.S. Federal Government website. EIA data and press releases "
            "are public domain (17 U.S.C. § 105). No copyright restrictions."
        ),
        usage_constraints=(
            "No restrictions. EIA data is explicitly free to use without license. "
            "See: https://www.eia.gov/about/copyrights_reuse.php"
        ),
        jurisdiction="US",
        asset_classes=("futures", "etf", "equity"),
    ),

    # ── TIER 3: High-quality free business media (secondary context layer) ────
    # These feeds supplement official sources; their importance scores are
    # weighted significantly lower than Tier 1 by the classifier.

    FeedSource(
        source_id="cnbc_top",
        source_name="CNBC Top News",
        source_type="rss",
        category="business_news",
        url="https://search.cnbc.com/rs/search/combinedcms/view.aspx?partnerId=wrss01&id=100003114",
        poll_cadence_minutes=30,
        timeout_s=20,
        priority=10,
        source_tier=3,
        enabled=True,
        free_only=True,
        license_notes=(
            "Officially published RSS feed by CNBC/NBCUniversal. "
            "Feed endpoint is publicly documented and available without authentication."
        ),
        usage_constraints=(
            "Free to consume via official RSS endpoint for non-commercial research monitoring. "
            "Do not republish, redistribute, or display content commercially. "
            "Attribution required. "
            "Monitor ToS: https://www.cnbc.com/nbcuniversal-terms-of-service/ "
            "This layer is secondary context only; never used as primary trading signal."
        ),
        jurisdiction="US",
        asset_classes=("equity", "etf", "futures"),
    ),

    FeedSource(
        source_id="cnbc_markets",
        source_name="CNBC Markets News",
        source_type="rss",
        category="business_news",
        url="https://search.cnbc.com/rs/search/combinedcms/view.aspx?partnerId=wrss01&id=10000664",
        poll_cadence_minutes=30,
        timeout_s=20,
        priority=11,
        source_tier=3,
        enabled=True,
        free_only=True,
        license_notes=(
            "Officially published RSS feed by CNBC/NBCUniversal. "
            "Feed endpoint is publicly documented and available without authentication."
        ),
        usage_constraints=(
            "Free to consume via official RSS endpoint for non-commercial research monitoring. "
            "Do not republish, redistribute, or display content commercially. "
            "Attribution required. "
            "Monitor ToS: https://www.cnbc.com/nbcuniversal-terms-of-service/ "
            "This layer is secondary context only; never used as primary trading signal."
        ),
        jurisdiction="US",
        asset_classes=("equity", "etf", "futures"),
    ),

    FeedSource(
        source_id="marketwatch_top",
        source_name="MarketWatch Top Stories",
        source_type="rss",
        category="business_news",
        url="https://feeds.marketwatch.com/marketwatch/topstories/",
        poll_cadence_minutes=30,
        timeout_s=20,
        priority=12,
        source_tier=3,
        enabled=True,
        free_only=True,
        license_notes=(
            "Officially published RSS feed by MarketWatch / Dow Jones & Company. "
            "Feed endpoint is publicly documented."
        ),
        usage_constraints=(
            "Free to consume via official RSS endpoint for non-commercial research monitoring. "
            "Do not republish or display content commercially. "
            "Attribution required. "
            "Monitor ToS: https://www.marketwatch.com/support/disclaimer.asp "
            "This layer is secondary context only; never used as primary trading signal."
        ),
        jurisdiction="US",
        asset_classes=("equity", "etf"),
    ),
]

# Index by source_id for O(1) lookup
FEED_BY_ID: dict[str, FeedSource] = {f.source_id: f for f in FEED_REGISTRY}
