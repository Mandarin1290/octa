from __future__ import annotations

from typing import Any, Mapping


def build(payloads: Mapping[str, Any]) -> dict[str, float]:
    trends = payloads.get("google_trends", {})
    wiki = payloads.get("wikipedia", {})

    trend_mom = trends.get("momentum", 0.0) if isinstance(trends, dict) else 0.0
    wiki_mom = wiki.get("momentum", 0.0) if isinstance(wiki, dict) else 0.0

    try:
        trend_val = float(trend_mom)
    except Exception:
        trend_val = 0.0
    try:
        wiki_val = float(wiki_mom)
    except Exception:
        wiki_val = 0.0

    return {
        "attention_trends_mom": trend_val,
        "attention_wiki_mom": wiki_val,
        "attention_hype": max(trend_val, wiki_val),
    }
