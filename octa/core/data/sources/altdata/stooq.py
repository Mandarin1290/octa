from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Mapping, Tuple


@dataclass
class StooqSource:
    cfg: Mapping[str, Any]
    name: str = "stooq"

    def __post_init__(self) -> None:
        self.enabled = bool(self.cfg.get("enabled", False))

    def cache_key(self, *, asof: date) -> str:
        return f"{self.name}_{asof.isoformat()}"

    def fetch_raw(self, *, asof: date, allow_net: bool) -> Mapping[str, Any] | None:
        if not allow_net:
            return None
        symbols_cfg = self.cfg.get("symbols")
        if not isinstance(symbols_cfg, Mapping):
            return {"rows": [], "resolved": {}, "status": "net_error", "errors": [{"error": "missing_symbols_cfg"}]}
        base_urls = _normalize_base_urls(self.cfg.get("base_urls"))
        window_days = int(self.cfg.get("window_days", 365))
        start = asof - timedelta(days=window_days)
        rows: list[dict[str, Any]] = []
        resolved: dict[str, str] = {}
        errors: list[dict[str, Any]] = []
        ok_fetches = 0
        rate_limit_message: str | None = None
        for proxy, candidates in symbols_cfg.items():
            candidate_list = _normalize_candidates(candidates)
            for candidate in candidate_list:
                data_rows, fetch_meta = _fetch_csv_rows(candidate, base_urls=base_urls)
                if fetch_meta.get("status") != "ok":
                    errors.append(fetch_meta)
                    continue
                ok_fetches += 1
                if rate_limit_message is None:
                    body_head = fetch_meta.get("body_head")
                    if _contains_rate_limit(body_head):
                        rate_limit_message = str(body_head)
                filtered = _filter_rows(data_rows, start=start, end=asof)
                if not filtered:
                    continue
                resolved[proxy] = candidate
                rows.extend(
                    {
                        "proxy": proxy,
                        "symbol": candidate,
                        "ts": row["ts"],
                        "close": row["close"],
                        "volume": row.get("volume"),
                    }
                    for row in filtered
                )
                print(f"stooq: resolved {proxy} -> {candidate}")
                break
        meta = {"errors": errors, "ok_fetches": ok_fetches, "base_urls": base_urls}
        if rate_limit_message:
            meta["rate_limited"] = True
            meta["rate_limit_message"] = rate_limit_message[:200]
        else:
            meta["rate_limited"] = False
        if ok_fetches == 0:
            return {
                "rows": [],
                "resolved": resolved,
                "window_days": window_days,
                "status": "net_error",
                "errors": errors,
                "meta": meta,
            }
        rows.sort(key=lambda r: (r.get("proxy"), r.get("ts")))
        return {
            "rows": rows,
            "resolved": resolved,
            "window_days": window_days,
            "status": "ok",
            "errors": errors,
            "meta": meta,
        }

    def normalize(self, raw: Mapping[str, Any]) -> Mapping[str, Any]:
        return raw


def _normalize_candidates(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value.strip().lower()] if value.strip() else []
    if isinstance(value, (list, tuple)):
        out = []
        for item in value:
            if item is None:
                continue
            s = str(item).strip().lower()
            if s:
                out.append(s)
        return out
    return []


def _fetch_csv_rows(symbol: str, *, base_urls: list[str]) -> Tuple[list[dict[str, Any]], dict[str, Any]]:
    errors: list[dict[str, Any]] = []
    for base_url in base_urls:
        url = f"{base_url.rstrip('/')}/q/d/l/?s={symbol}&i=d"
        resp = _fetch_text(url)
        if resp.get("status") != "ok":
            errors.append(resp)
            continue
        text = resp.get("text") or ""
        if not text:
            return [], {
                "status": "ok",
                "url": url,
                "http_status": 200,
                "base_url": base_url,
                "body_head": resp.get("body_head"),
            }
        return _parse_csv(text), {
            "status": "ok",
            "url": url,
            "http_status": 200,
            "base_url": base_url,
            "body_head": resp.get("body_head"),
        }
    return [], {"status": "net_error", "errors": errors}


def _fetch_text(url: str) -> dict[str, Any]:
    headers = {"User-Agent": "OCTA/altdata", "Accept": "text/csv"}
    try:
        from urllib.request import Request, build_opener, ProxyHandler

        req = Request(url, headers=headers)
        opener = build_opener(ProxyHandler({}))
        with opener.open(req, timeout=10.0) as resp:
            status = getattr(resp, "status", 200)
            body = resp.read().decode("utf-8", errors="ignore")
            if status != 200:
                return {"status": "net_error", "url": url, "http_status": status, "error": "HTTPError"}
            return {
                "status": "ok",
                "text": body,
                "url": url,
                "http_status": status,
                "body_head": body[:200],
            }
    except Exception as exc:
        return {"status": "net_error", "url": url, "error": type(exc).__name__, "message": str(exc)[:200]}


def _parse_csv(text: str) -> list[dict[str, Any]]:
    import csv
    import io

    rows: list[dict[str, Any]] = []
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        ts = row.get("Date") or row.get("date")
        close = row.get("Close") or row.get("close")
        volume = row.get("Volume") or row.get("volume")
        if not ts or close is None:
            continue
        try:
            close_val = float(close)
        except Exception:
            continue
        row_out: dict[str, Any] = {"ts": ts, "close": close_val}
        if volume is not None:
            try:
                row_out["volume"] = float(volume)
            except Exception:
                row_out["volume"] = None
        rows.append(row_out)
    return rows


def _filter_rows(rows: list[dict[str, Any]], *, start: date, end: date) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for row in rows:
        ts = row.get("ts")
        if not ts:
            continue
        dt = _parse_date(ts)
        if dt is None:
            continue
        if dt.date() < start or dt.date() > end:
            continue
        filtered.append(row)
    filtered.sort(key=lambda r: str(r.get("ts") or ""))
    return filtered


def _parse_date(value: str) -> datetime | None:
    try:
        dt = datetime.fromisoformat(str(value))
    except Exception:
        try:
            dt = datetime.strptime(str(value), "%Y-%m-%d")
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _normalize_base_urls(value: Any) -> list[str]:
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, (list, tuple)):
        return ["https://stooq.com", "https://stooq.pl"]
    out = []
    for item in value:
        if not item:
            continue
        s = str(item).strip()
        if s:
            out.append(s)
    return out or ["https://stooq.com", "https://stooq.pl"]


def _contains_rate_limit(value: Any) -> bool:
    needle = "Exceeded the daily hits limit"
    return isinstance(value, str) and needle in value
