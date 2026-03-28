from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Mapping

from .feed_classifier import classify_event
from .feed_dedup import dedup_events
from .feed_fetcher import fetch_feed
from .feed_normalizer import NewsEvent, event_to_dict, normalize_entry
from .feed_registry import FEED_REGISTRY


@dataclass
class NewsEventsSource:
    """
    AltDataSource-compatible implementation for the News/Event layer.

    Fetches official institutional RSS feeds (Tier 1: Fed, ECB, EIA) and high-quality
    free business news RSS feeds (Tier 3: CNBC, MarketWatch).

    Design principles:
    - Fail-closed per feed: any individual feed failure is recorded in errors/meta;
      remaining feeds continue. fetch_raw() never raises.
    - Fail-closed overall: if all feeds fail, returns {"rows": [], "status": "empty"}.
    - Offline mode: allow_net=False → returns None immediately (training-safe).
    - Tier 1 official sources always outweigh Tier 3 media in importance_score by design.
    - No trade signals are generated; this is a risk/context/blocker layer.
    """

    cfg: Mapping[str, Any]
    name: str = "news_events"

    def __post_init__(self) -> None:
        self.enabled = bool(self.cfg.get("enabled", False))
        self._timeout_s = int(self.cfg.get("timeout_s", 20))

    def cache_key(self, *, asof: date) -> str:
        return f"{self.name}_{asof.isoformat()}"

    def fetch_raw(self, *, asof: date, allow_net: bool) -> Mapping[str, Any] | None:
        """
        Fetch all enabled feeds, normalise, deduplicate, classify, and return payload.

        Returns None when offline (allow_net=False) — training-safe.
        Returns {"rows": [...], "status": "ok"|"empty", "meta": {...}} otherwise.
        """
        if not allow_net:
            return None

        asof_ts = datetime.combine(asof, datetime.min.time(), tzinfo=timezone.utc).isoformat()
        feeds_cfg: Mapping[str, Any] = self.cfg.get("feeds", {}) if isinstance(self.cfg, dict) else {}

        all_events: list[NewsEvent] = []
        fetch_results: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []

        for feed_src in FEED_REGISTRY:
            # Per-feed enable override from config
            feed_override = feeds_cfg.get(feed_src.source_id, {})
            if isinstance(feed_override, dict) and feed_override.get("enabled") is False:
                continue
            if not feed_src.enabled:
                continue

            result = fetch_feed(feed_src, timeout_s=self._timeout_s)
            fetch_results.append(
                {
                    "source_id": feed_src.source_id,
                    "source_tier": feed_src.source_tier,
                    "status": result["status"],
                    "entry_count": len(result.get("entries", [])),
                    "elapsed_ms": result.get("elapsed_ms", 0),
                    "error": result.get("error"),
                }
            )

            if result["status"] not in {"ok", "empty"}:
                errors.append(
                    {
                        "source_id": feed_src.source_id,
                        "status": result["status"],
                        "error": result.get("error", "unknown"),
                        "http_status": result.get("http_status"),
                    }
                )
                continue

            fetched_at = str(result["fetched_at"])
            for raw_entry in result.get("entries", []):
                try:
                    event = normalize_entry(
                        raw_entry=raw_entry,
                        source_id=feed_src.source_id,
                        source_name=feed_src.source_name,
                        source_tier=feed_src.source_tier,
                        source_category=feed_src.category,
                        source_jurisdiction=feed_src.jurisdiction,
                        source_asset_classes=feed_src.asset_classes,
                        fetched_at=fetched_at,
                    )
                    event = classify_event(event, asof_ts=asof_ts)
                    all_events.append(event)
                except Exception as exc:
                    errors.append(
                        {
                            "source_id": feed_src.source_id,
                            "status": "normalise_error",
                            "error": f"{type(exc).__name__}: {str(exc)[:200]}",
                            "http_status": None,
                        }
                    )

        deduped = dedup_events(all_events)
        rows = [event_to_dict(e) for e in deduped]

        return {
            "rows": rows,
            "status": "ok" if rows else "empty",
            "meta": {
                "asof": asof.isoformat(),
                "total_fetched": len(all_events),
                "after_dedup": len(deduped),
                "fetch_results": fetch_results,
                "error_count": len(errors),
                "errors": errors,
            },
        }

    def normalize(self, raw: Mapping[str, Any]) -> Mapping[str, Any]:
        return raw
