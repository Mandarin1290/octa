from __future__ import annotations

import re
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any

from .feed_normalizer import NewsEvent

# ── Source tier → base weight ──────────────────────────────────────────────────
# Governance guarantee: a Tier 1 event can NEVER be outweighed by a Tier 3 event
# on the same event_type purely through media volume, because source_tier_weight
# is a *multiplier*, not additive.  The formula is multiplicative:
#   score = tier_w × type_w × sev_w × recency_w
# A Tier 1 event (tier_w=1.0) with medium severity always scores higher than
# a Tier 3 event (tier_w=0.5) with the same classification.
_TIER_WEIGHT: dict[int, float] = {
    1: 1.00,  # official central bank / regulator / government
    2: 0.80,  # official statistics / energy / filings / issuer IR
    3: 0.50,  # CNBC / MarketWatch / quality free business media
    4: 0.30,  # other (not currently in registry)
}

# ── Event type → base weight ───────────────────────────────────────────────────
_EVENT_TYPE_WEIGHT: dict[str, float] = {
    "rates":            1.00,
    "central_bank":     0.95,
    "liquidity":        0.90,
    "market_structure": 0.90,
    "geopolitics":      0.85,
    "inflation":        0.85,
    "labor":            0.80,
    "growth":           0.75,
    "energy":           0.70,
    "regulation":       0.65,
    "earnings":         0.60,
    "commodities":      0.55,
    "analyst_sentiment":0.35,
    "business_news":    0.30,  # generic/uncategorized media headline
    "unclassified":     0.20,
}

# ── Severity → weight multiplier ──────────────────────────────────────────────
_SEVERITY_WEIGHT: dict[str, float] = {
    "critical": 1.00,
    "high":     0.80,
    "medium":   0.55,
    "low":      0.25,
}

_SEVERITY_ORDER: dict[str, int] = {
    "low": 0, "medium": 1, "high": 2, "critical": 3
}

_RECENCY_PROFILES: dict[str, tuple[tuple[float, float], ...]] = {
    "very_slow": (
        (6.0, 1.00),
        (24.0, 0.85),
        (72.0, 0.65),
        (168.0, 0.35),
    ),
    "slow": (
        (4.0, 1.00),
        (24.0, 0.80),
        (72.0, 0.55),
        (168.0, 0.25),
    ),
    "medium": (
        (2.0, 1.00),
        (24.0, 0.75),
        (72.0, 0.40),
        (168.0, 0.15),
    ),
    "fast": (
        (2.0, 1.00),
        (12.0, 0.60),
        (48.0, 0.25),
    ),
}

_EVENT_TYPE_RECENCY_PROFILE: dict[str, str] = {
    "rates": "very_slow",
    "central_bank": "very_slow",
    "liquidity": "very_slow",
    "regulation": "slow",
    "geopolitics": "slow",
    "energy": "slow",
    "inflation": "medium",
    "labor": "medium",
    "growth": "medium",
    "market_structure": "medium",
    "commodities": "medium",
    "earnings": "fast",
    "analyst_sentiment": "fast",
    "business_news": "fast",
    "unclassified": "fast",
}

_TIER_RECENCY_PROFILE: dict[int, str] = {
    1: "slow",
    2: "medium",
    3: "fast",
    4: "fast",
}

# ── Classification rules ───────────────────────────────────────────────────────
# Each rule: (compiled_regex, event_type, severity)
# Rules are applied to title + summary (case-insensitive).
# First matching rule sets event_type; subsequent matches can only escalate severity.
_RULES: list[tuple[re.Pattern[str], str, str]] = [
    # Monetary policy / rates — highest risk relevance
    (
        re.compile(
            r"\b(fomc|federal open market|monetary policy statement|rate decision|"
            r"interest rate decision|policy rate|deposit facility rate|repo rate|"
            r"emergency rate|rate cut|rate hike|rate increase|rate decrease)\b",
            re.I,
        ),
        "rates", "critical",
    ),
    (
        re.compile(
            r"\b(federal reserve|fed chair|fed governor|powell|yellen|fomc minutes|"
            r"beige book|fed statement|fed announcement|fed press release)\b",
            re.I,
        ),
        "central_bank", "high",
    ),
    (
        re.compile(
            r"\b(ecb|european central bank|lagarde|governing council|"
            r"euro area monetary policy|ecb decision|ecb rate)\b",
            re.I,
        ),
        "central_bank", "high",
    ),
    (
        re.compile(
            r"\b(bank of england|boe rate|mpc decision|monetary policy committee|"
            r"boe announcement)\b",
            re.I,
        ),
        "central_bank", "high",
    ),
    (
        re.compile(
            r"\b(bank of japan|boj|yield curve control|negative interest rate policy|"
            r"boj decision)\b",
            re.I,
        ),
        "central_bank", "high",
    ),

    # Inflation / prices
    (
        re.compile(
            r"\b(consumer price index|cpi report|core cpi|pce inflation|"
            r"core pce|inflation data|inflation rate|hyperinflation|deflation|"
            r"inflation expectation)\b",
            re.I,
        ),
        "inflation", "high",
    ),
    (
        re.compile(
            r"\b(producer price|ppi|import price index|export price index)\b",
            re.I,
        ),
        "inflation", "medium",
    ),

    # Labor market
    (
        re.compile(
            r"\b(nonfarm payroll|nonfarm payrolls|jobs report|unemployment rate|"
            r"jobless claims|initial claims|continuing claims|labor market|"
            r"job openings|jolts|hiring freezes?|mass layoffs?|job cuts?)\b",
            re.I,
        ),
        "labor", "high",
    ),

    # Growth / GDP
    (
        re.compile(
            r"\b(gross domestic product|gdp report|gdp growth|gdp contraction|"
            r"economic recession|economic growth data|economic contraction)\b",
            re.I,
        ),
        "growth", "high",
    ),
    (
        re.compile(
            r"\b(manufacturing pmi|services pmi|composite pmi|ism manufacturing|"
            r"ism services|industrial production|capacity utilization|"
            r"factory orders|durable goods)\b",
            re.I,
        ),
        "growth", "medium",
    ),

    # Liquidity / financial stability
    (
        re.compile(
            r"\b(bank failure|bank run|systemic risk|contagion|credit crunch|"
            r"liquidity crisis|fdic|resolution authority|bailout|emergency liquidity)\b",
            re.I,
        ),
        "liquidity", "critical",
    ),
    (
        re.compile(
            r"\b(quantitative easing|qe |qt |quantitative tightening|"
            r"balance sheet reduction|repo market|overnight rate|fed funds rate)\b",
            re.I,
        ),
        "liquidity", "high",
    ),

    # Market structure
    (
        re.compile(
            r"\b(circuit breaker|trading halt|market-wide halt|flash crash|"
            r"margin call|forced selling|market sell.?off|vix spike|volatility spike)\b",
            re.I,
        ),
        "market_structure", "critical",
    ),

    # Regulation
    (
        re.compile(
            r"\b(sec enforcement|cftc action|finra fine|regulatory investigation|"
            r"regulatory probe|sanction|enforcement action|compliance ruling|"
            r"dodd.frank|mifid|emir|capital requirement|stress test|dfast|sr letter)\b",
            re.I,
        ),
        "regulation", "medium",
    ),

    # Energy
    (
        re.compile(
            r"\b(crude oil inventory|eia petroleum|eia natural gas|opec decision|"
            r"opec\+|oil production cut|oil production increase|"
            r"wti crude|brent crude|petroleum report)\b",
            re.I,
        ),
        "energy", "high",
    ),
    (
        re.compile(
            r"\b(natural gas storage|lng export|energy prices|gasoline prices|"
            r"refinery utilization|pipeline outage|energy supply)\b",
            re.I,
        ),
        "energy", "medium",
    ),

    # Geopolitics
    (
        re.compile(
            r"\b(war declaration|military invasion|missile strike|drone attack|"
            r"terrorist attack|terrorism|military escalation|nuclear threat|"
            r"embargo|economic sanction|trade blockade)\b",
            re.I,
        ),
        "geopolitics", "critical",
    ),
    (
        re.compile(
            r"\b(trade war|tariff|trade dispute|wto ruling|trade deal|"
            r"geopolitical risk|geopolitical tension)\b",
            re.I,
        ),
        "geopolitics", "high",
    ),

    # Commodities
    (
        re.compile(
            r"\b(commodity prices|gold prices|silver prices|copper prices|"
            r"agricultural prices|grain prices|wheat|corn prices|"
            r"metals prices|commodity report)\b",
            re.I,
        ),
        "commodities", "medium",
    ),

    # Earnings / issuer-specific
    (
        re.compile(
            r"\b(earnings report|quarterly earnings|eps beat|eps miss|"
            r"revenue beat|revenue miss|full.year guidance|annual outlook|"
            r"profit warning|profit upgrade)\b",
            re.I,
        ),
        "earnings", "medium",
    ),

    # Analyst sentiment (lowest weight event type)
    (
        re.compile(
            r"\b(stock upgrade|stock downgrade|analyst upgrade|analyst downgrade|"
            r"price target raised|price target cut|outperform|underperform|"
            r"overweight|underweight|buy rating|sell rating|hold rating)\b",
            re.I,
        ),
        "analyst_sentiment", "low",
    ),
]

_OFFICIAL_SOURCE_MAX_SEVERITY: dict[int, str] = {
    1: "critical",
    2: "high",
    3: "high",
    4: "medium",
}

_EXTREME_URGENCY_PATTERN = re.compile(
    r"\b("
    r"emergency|intermeeting|unscheduled|surprise|shock|crisis|panic|collapse|"
    r"war escalation|military escalation|invasion|sanction|embargo|"
    r"supply disruption|pipeline outage|bank failure|bank run|"
    r"trading halt|market-wide halt|flash crash"
    r")\b",
    re.I,
)
_ACTION_PATTERN = re.compile(
    r"\b("
    r"raises?|cuts?|hikes?|lowers?|announces?|decision|statement|press release|"
    r"imposes?|launches?|approves?|halts?|suspends?|seizes?|intervenes?"
    r")\b",
    re.I,
)
_SPEECH_PATTERN = re.compile(r"\b(speaks?|speech|remarks?|conference|testimony|interview)\b", re.I)
_OFFICIAL_CATEGORY_PATTERN = re.compile(r"\b(central_bank|energy|regulation)\b", re.I)


def classify_event(event: NewsEvent, *, asof_ts: str | None = None) -> NewsEvent:
    """
    Apply rule-based classification and compute importance_score.

    Returns a new NewsEvent (frozen dataclass) with updated:
      event_type, severity, importance_score

    Score formula (transparent, auditable):
      importance_score = tier_w × event_type_w × severity_w × recency_w

    Where:
      tier_w      = _TIER_WEIGHT[source_tier]         (0.3 – 1.0)
      event_type_w = _EVENT_TYPE_WEIGHT[event_type]   (0.2 – 1.0)
      severity_w  = _SEVERITY_WEIGHT[severity]        (0.25 – 1.0)
      recency_w   = tier-/event-type-sensitive deterministic decay to 0.0 (>7 days old)

    Governance rule: Tier 1 official sources always outscore Tier 3 media
    for the same event_type by design (tier_w is a multiplicative factor).
    """
    text = f"{event.title} {event.summary}"

    event_type, severity = _classify_text(
        text,
        base_category=event.category,
        source_tier=event.source_tier,
    )

    tier_w = _TIER_WEIGHT.get(event.source_tier, 0.3)
    type_w = _EVENT_TYPE_WEIGHT.get(event_type, 0.20)
    sev_w = _SEVERITY_WEIGHT.get(severity, 0.25)
    rec_w = _recency_weight(
        event.published_at,
        asof_ts=asof_ts,
        source_tier=event.source_tier,
        event_type=event_type,
    )

    score = max(0.0, min(1.0, tier_w * type_w * sev_w * rec_w))

    return replace(event, event_type=event_type, severity=severity, importance_score=score)


def score_components(event: NewsEvent, *, asof_ts: str | None = None) -> dict[str, Any]:
    """Return the individual score components for auditability."""
    return {
        "source_id": event.source_id,
        "source_tier": event.source_tier,
        "tier_w": _TIER_WEIGHT.get(event.source_tier, 0.3),
        "event_type": event.event_type,
        "type_w": _EVENT_TYPE_WEIGHT.get(event.event_type, 0.20),
        "severity": event.severity,
        "sev_w": _SEVERITY_WEIGHT.get(event.severity, 0.25),
        "recency_w": _recency_weight(
            event.published_at,
            asof_ts=asof_ts,
            source_tier=event.source_tier,
            event_type=event.event_type,
        ),
        "importance_score": event.importance_score,
    }


# ── Internal helpers ──────────────────────────────────────────────────────────

def _classify_text(text: str, *, base_category: str, source_tier: int) -> tuple[str, str]:
    """Return (event_type, severity) from text + source category."""
    event_type = "unclassified"
    severity = "low"

    for pattern, e_type, e_sev in _RULES:
        if pattern.search(text):
            if event_type == "unclassified":
                event_type = e_type
                severity = e_sev
            else:
                # Already classified; only escalate severity, never downgrade
                severity = _max_severity(severity, e_sev)

    # Category-based floor: central_bank sources always get at least medium
    if base_category == "central_bank" and event_type == "unclassified":
        event_type = "central_bank"
    if base_category == "central_bank" and _SEVERITY_ORDER.get(severity, 0) < _SEVERITY_ORDER["medium"]:
        severity = "medium"

    # Energy sources: always at least energy type if nothing else matched
    if base_category == "energy" and event_type == "unclassified":
        event_type = "energy"

    # Business news without classification → business_news type
    if event_type == "unclassified" and base_category == "business_news":
        event_type = "business_news"

    severity = _apply_severity_policy(
        text=text,
        event_type=event_type,
        severity=severity,
        base_category=base_category,
        source_tier=source_tier,
    )

    return event_type, severity


def _max_severity(a: str, b: str) -> str:
    return a if _SEVERITY_ORDER.get(a, 0) >= _SEVERITY_ORDER.get(b, 0) else b


def _recency_weight(
    published_at: str,
    *,
    asof_ts: str | None = None,
    source_tier: int = 3,
    event_type: str = "unclassified",
) -> float:
    """
    Recency decay is deterministic and policy-based:
      - Tier 1 official events decay slower than Tier 3 media
      - rates / central_bank / liquidity / regulation decay slower
      - analyst_sentiment / business_news decay faster
      - > 168 hours always decays to 0.0
      empty/invalid  → 0.5  (neutral safe default)
    """
    if not published_at:
        return 0.5

    try:
        pub = datetime.fromisoformat(published_at)
        if pub.tzinfo is None:
            pub = pub.replace(tzinfo=timezone.utc)
    except Exception:
        return 0.5

    try:
        ref = datetime.fromisoformat(asof_ts) if asof_ts else datetime.now(timezone.utc)
        if ref.tzinfo is None:
            ref = ref.replace(tzinfo=timezone.utc)
    except Exception:
        ref = datetime.now(timezone.utc)

    hours_old = (ref - pub).total_seconds() / 3600.0

    if hours_old < 0:
        return 1.0    # future-dated entry → treat as fully fresh
    tier_profile = _TIER_RECENCY_PROFILE.get(int(source_tier), "fast")
    event_profile = _EVENT_TYPE_RECENCY_PROFILE.get(str(event_type).strip().lower(), tier_profile)
    profile_name = _merge_recency_profiles(tier_profile, event_profile)
    return _piecewise_decay(hours_old, _RECENCY_PROFILES[profile_name])


def _merge_recency_profiles(tier_profile: str, event_profile: str) -> str:
    order = {"fast": 0, "medium": 1, "slow": 2, "very_slow": 3}
    best = tier_profile if order.get(tier_profile, 0) >= order.get(event_profile, 0) else event_profile
    return best if best in _RECENCY_PROFILES else "fast"


def _piecewise_decay(hours_old: float, profile: tuple[tuple[float, float], ...]) -> float:
    if hours_old <= 0:
        return 1.0
    prev_hours = 0.0
    prev_weight = 1.0
    for cutoff_hours, cutoff_weight in profile:
        if hours_old <= cutoff_hours:
            span = max(cutoff_hours - prev_hours, 1e-9)
            ratio = (hours_old - prev_hours) / span
            weight = prev_weight + (cutoff_weight - prev_weight) * ratio
            return max(0.0, min(1.0, weight))
        prev_hours = cutoff_hours
        prev_weight = cutoff_weight
    if prev_hours >= 168.0 or hours_old > 168.0:
        return 0.0
    tail_ratio = min((hours_old - prev_hours) / max(168.0 - prev_hours, 1e-9), 1.0)
    weight = prev_weight * (1.0 - tail_ratio)
    return max(0.0, min(1.0, weight))


def _apply_severity_policy(
    *,
    text: str,
    event_type: str,
    severity: str,
    base_category: str,
    source_tier: int,
) -> str:
    sev = severity
    is_official = int(source_tier) <= 2 or bool(_OFFICIAL_CATEGORY_PATTERN.search(base_category))
    has_extreme_urgency = bool(_EXTREME_URGENCY_PATTERN.search(text))
    has_hard_action = bool(_ACTION_PATTERN.search(text))
    is_speech_like = bool(_SPEECH_PATTERN.search(text))

    if event_type == "rates":
        if is_official and has_extreme_urgency:
            return "critical"
        sev = _max_severity(sev, "high")
        return _min_severity(sev, "high") if int(source_tier) >= 3 else sev

    if event_type in {"central_bank", "regulation"}:
        if is_official and has_extreme_urgency and has_hard_action:
            return "critical"
        if is_speech_like and not has_hard_action:
            return _max_severity("medium", sev)
        return _max_severity(sev, "high" if is_official else "medium")

    if event_type in {"liquidity", "geopolitics"}:
        if is_official and has_extreme_urgency:
            return "critical"
        sev = _max_severity(sev, "high")
        return _min_severity(sev, "high") if not is_official else sev

    if event_type == "energy":
        if is_official and has_extreme_urgency:
            return "critical"
        sev = _max_severity(sev, "high" if is_official else "medium")
        return _min_severity(sev, "high") if not is_official else sev

    if event_type in {"inflation", "labor", "growth", "market_structure"}:
        return _max_severity(sev, "high" if is_official and has_hard_action else "medium")

    if event_type in {"earnings", "commodities"}:
        return _max_severity(sev, "medium")

    if event_type in {"analyst_sentiment", "business_news", "unclassified"}:
        max_allowed = _OFFICIAL_SOURCE_MAX_SEVERITY.get(int(source_tier), "medium")
        return _min_severity(_max_severity(sev, "low"), max_allowed)

    max_allowed = _OFFICIAL_SOURCE_MAX_SEVERITY.get(int(source_tier), "medium")
    return _min_severity(sev, max_allowed)


def _min_severity(a: str, b: str) -> str:
    return a if _SEVERITY_ORDER.get(a, 0) <= _SEVERITY_ORDER.get(b, 0) else b


def recency_model_spec() -> dict[str, Any]:
    return {
        "tier_profiles": _TIER_RECENCY_PROFILE,
        "event_type_profiles": _EVENT_TYPE_RECENCY_PROFILE,
        "profiles": {k: [{"hours": h, "weight": w} for h, w in v] for k, v in _RECENCY_PROFILES.items()},
    }


def severity_rules_spec() -> dict[str, Any]:
    return {
        "tier_caps": _OFFICIAL_SOURCE_MAX_SEVERITY,
        "extreme_urgency_pattern": _EXTREME_URGENCY_PATTERN.pattern,
        "action_pattern": _ACTION_PATTERN.pattern,
        "speech_pattern": _SPEECH_PATTERN.pattern,
        "rule_count": len(_RULES),
    }
