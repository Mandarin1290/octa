from __future__ import annotations

import time
from calendar import timegm
from datetime import datetime, timezone
from typing import Any

from .feed_registry import FeedSource

_USER_AGENT = "OCTA/altdata"
_ACCEPT = "application/rss+xml, application/atom+xml, application/xml, text/xml, */*"


def fetch_feed(source: FeedSource, *, timeout_s: int | None = None) -> dict[str, Any]:
    """
    Fetch and parse a single RSS/Atom feed.

    Uses the same two-tier HTTP pattern as the rest of the altdata stack:
    1. httpx if available
    2. urllib fallback

    Returns a dict with keys:
      status      : "ok" | "empty" | "timeout" | "http_error" | "parse_error" | "deps_missing"
      source_id   : str
      fetched_at  : str (ISO 8601 UTC)
      entries     : list[dict]
      error       : str | None
      http_status : int | None
      elapsed_ms  : int
      feed_title  : str | None
      bozo        : bool  (feedparser malformed-feed flag; entries may still be present)
    """
    try:
        import feedparser  # type: ignore  # noqa: F401
    except ImportError:
        return _err_result(source.source_id, "deps_missing", "feedparser not installed; run: pip install feedparser")

    t = timeout_s if timeout_s is not None else source.timeout_s
    fetched_at = datetime.now(timezone.utc).isoformat()
    start = time.monotonic()

    headers = {"User-Agent": _USER_AGENT, "Accept": _ACCEPT}
    raw_content, http_status = _fetch_bytes(source.url, headers=headers, timeout_s=t)
    elapsed_ms = int((time.monotonic() - start) * 1000)

    if raw_content is None:
        return {
            "status": "http_error",
            "source_id": source.source_id,
            "fetched_at": fetched_at,
            "entries": [],
            "error": f"HTTP fetch failed (http_status={http_status})",
            "http_status": http_status,
            "elapsed_ms": elapsed_ms,
            "feed_title": None,
            "bozo": False,
        }

    import feedparser  # type: ignore

    try:
        parsed = feedparser.parse(raw_content)
    except Exception as exc:
        return {
            "status": "parse_error",
            "source_id": source.source_id,
            "fetched_at": fetched_at,
            "entries": [],
            "error": f"feedparser: {type(exc).__name__}: {str(exc)[:200]}",
            "http_status": http_status,
            "elapsed_ms": elapsed_ms,
            "feed_title": None,
            "bozo": True,
        }

    entries: list[dict[str, Any]] = []
    for entry in (parsed.entries or []):
        extracted = _extract_entry(entry)
        if extracted:
            entries.append(extracted)

    feed_meta = getattr(parsed, "feed", None)
    feed_title = str(getattr(feed_meta, "title", "") or "").strip() or None

    bozo = bool(getattr(parsed, "bozo", False))
    bozo_exc = getattr(parsed, "bozo_exception", None)

    return {
        "status": "ok" if entries else "empty",
        "source_id": source.source_id,
        "fetched_at": fetched_at,
        "entries": entries,
        "error": str(bozo_exc)[:200] if (bozo and bozo_exc) else None,
        "http_status": http_status,
        "elapsed_ms": elapsed_ms,
        "feed_title": feed_title,
        "bozo": bozo,
    }


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _fetch_bytes(
    url: str, *, headers: dict[str, str], timeout_s: int
) -> tuple[bytes | None, int | None]:
    """Try httpx first, then urllib. Returns (body_bytes, http_status) or (None, code)."""
    # httpx
    try:
        import httpx  # type: ignore

        with httpx.Client(timeout=float(timeout_s)) as client:
            resp = client.get(url, headers=headers)
            if resp.status_code == 200:
                return resp.content, 200
            return None, resp.status_code
    except Exception:
        pass

    # urllib fallback
    try:
        from urllib.request import Request, urlopen

        req = Request(url, headers=headers)
        with urlopen(req, timeout=timeout_s) as resp:
            status = getattr(resp, "status", getattr(resp, "code", 200))
            if status == 200:
                return resp.read(), 200
            return None, int(status)
    except Exception:
        pass

    return None, None


def _err_result(source_id: str, status: str, error: str) -> dict[str, Any]:
    return {
        "status": status,
        "source_id": source_id,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "entries": [],
        "error": error,
        "http_status": None,
        "elapsed_ms": 0,
        "feed_title": None,
        "bozo": False,
    }


# ── Entry extraction ──────────────────────────────────────────────────────────

def _extract_entry(entry: Any) -> dict[str, Any] | None:
    """Extract standardised fields from a feedparser entry object."""
    title = str(getattr(entry, "title", "") or "").strip()
    if not title:
        return None  # skip entries with no title

    guid = str(getattr(entry, "id", "") or getattr(entry, "guid", "") or "").strip()
    link = str(getattr(entry, "link", "") or "").strip()
    guid = guid or link  # fall back to link if no guid

    summary_raw = (
        getattr(entry, "summary", "")
        or getattr(entry, "description", "")
        or ""
    )
    summary = str(summary_raw).strip()
    # Strip HTML tags from summary (simple regex approach, no heavy dep)
    import re
    summary = re.sub(r"<[^>]+>", " ", summary)
    summary = re.sub(r"\s+", " ", summary).strip()[:500]

    published_at = _extract_published(entry)

    # Tags / categories from feedparser
    tags_raw = getattr(entry, "tags", []) or []
    tags: list[str] = []
    for t in tags_raw:
        if isinstance(t, dict):
            term = str(t.get("term", "") or "").strip()
            if term:
                tags.append(term)

    language = str(getattr(entry, "language", "") or "").strip()

    return {
        "guid": guid,
        "link": link,
        "title": title,
        "summary": summary,
        "published_at": published_at,
        "tags": tags,
        "language": language,
    }


def _extract_published(entry: Any) -> str:
    """Return ISO 8601 UTC published timestamp from feedparser entry, or empty string."""
    # feedparser normalises to time.struct_time in *_parsed attributes
    for attr in ("published_parsed", "updated_parsed", "created_parsed"):
        parsed = getattr(entry, attr, None)
        if parsed is not None:
            try:
                ts = timegm(parsed)
                return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
            except Exception:
                pass

    # String fallback
    for attr in ("published", "updated", "created"):
        raw = str(getattr(entry, attr, "") or "").strip()
        if raw:
            return raw

    return ""
