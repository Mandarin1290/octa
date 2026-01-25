from __future__ import annotations

from typing import Any, Mapping


def build(payloads: Mapping[str, Any]) -> dict[str, float]:
    reddit = payloads.get("reddit", {})
    sentiment = reddit.get("sentiment", 0.0) if isinstance(reddit, dict) else 0.0
    volume = reddit.get("volume", 0.0) if isinstance(reddit, dict) else 0.0
    try:
        sentiment_val = float(sentiment)
    except Exception:
        sentiment_val = 0.0
    try:
        volume_val = float(volume)
    except Exception:
        volume_val = 0.0
    return {
        "reddit_sentiment": sentiment_val,
        "reddit_volume": volume_val,
    }
