from __future__ import annotations

from dataclasses import dataclass
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
import os
from typing import Any, Mapping


@dataclass
class GdeltSource:
    cfg: Mapping[str, Any]
    name: str = "gdelt"

    def __post_init__(self) -> None:
        self.enabled = bool(self.cfg.get("enabled", False))

    def cache_key(self, *, asof: date) -> str:
        return f"{self.name}_{asof.isoformat()}"

    def fetch_raw(self, *, asof: date, allow_net: bool) -> Mapping[str, Any] | None:
        if not allow_net:
            return None
        base_url = str(self.cfg.get("base_url", "https://api.gdeltproject.org/api/v2/doc/doc")).strip()
        packs = self.cfg.get("query_packs")
        if not isinstance(packs, Mapping):
            return None
        lag_hours = int(self.cfg.get("lag_hours", 6))
        timeout_s = float(self.cfg.get("timeout_s", 20))
        max_retries = int(self.cfg.get("max_retries", 2))

        end_dt = datetime.combine(asof, datetime.min.time(), tzinfo=timezone.utc) + timedelta(days=1) - timedelta(seconds=1)
        rows: list[dict[str, Any]] = []
        request_errors: list[dict[str, Any]] = []
        ok_requests = 0
        for pack_name, pack in packs.items():
            if not isinstance(pack, Mapping):
                continue
            windows = pack.get("window_days", [1, 7])
            queries = pack.get("queries", [])
            if not isinstance(queries, list):
                continue
            for query in queries:
                if not isinstance(query, Mapping):
                    continue
                query_id = str(query.get("id", "")).strip()
                query_text = str(query.get("query", "")).strip()
                if not query_id or not query_text:
                    continue
                for window in windows:
                    try:
                        window_days = int(window)
                    except Exception:
                        continue
                    start_dt = datetime.combine(
                        asof - timedelta(days=window_days),
                        datetime.min.time(),
                        tzinfo=timezone.utc,
                    )
                    resp = _request_timeline(
                        base_url=base_url,
                        query=query_text,
                        start_dt=start_dt,
                        end_dt=end_dt,
                        timeout_s=timeout_s,
                        max_retries=max_retries,
                    )
                    if resp is None:
                        continue
                    if resp.get("status") != "ok":
                        request_errors.append({k: v for k, v in resp.items() if k != "payload"})
                        continue
                    ok_requests += 1
                    points = _extract_points(resp.get("payload"))
                    release_ts = datetime.combine(asof, datetime.min.time(), tzinfo=timezone.utc) + timedelta(hours=lag_hours)
                    for point in points:
                        rows.append(
                            {
                                "asof_date": asof.isoformat(),
                                "window": f"{window_days}d",
                                "query_id": query_id,
                                "metric": "volume_intensity",
                                "date": point["date"],
                                "value": float(point["value"]),
                                "release_ts": release_ts.isoformat(),
                                "meta": {"pack": pack_name, "query": query_text},
                            }
                        )
        if ok_requests == 0:
            return {
                "rows": [],
                "status": "net_error",
                "meta": {"errors": request_errors, "requests": ok_requests},
            }
        return {
            "rows": rows,
            "status": "ok",
            "meta": {"errors": request_errors, "requests": ok_requests},
            "lag_hours": lag_hours,
        }

    def normalize(self, raw: Mapping[str, Any]) -> Mapping[str, Any]:
        return raw


def _request_timeline(
    *,
    base_url: str,
    query: str,
    start_dt: datetime,
    end_dt: datetime,
    timeout_s: float,
    max_retries: int,
) -> Mapping[str, Any] | None:
    params = {
        "query": query,
        "mode": "timelinevol",
        "format": "json",
        "timelinesmooth": "0",
        "startdatetime": start_dt.strftime("%Y%m%d%H%M%S"),
        "enddatetime": end_dt.strftime("%Y%m%d%H%M%S"),
    }
    url = f"{base_url}?{_encode_params(params)}"
    for _ in range(max_retries + 1):
        resp = _fetch_response(url, timeout_s=timeout_s)
        if resp is not None:
            return resp
    return None


def _fetch_response(url: str, *, timeout_s: float) -> Mapping[str, Any] | None:
    headers = {"User-Agent": "OCTA/altdata", "Accept": "application/json"}
    disable_proxies = str(os.getenv("OCTA_GDELT_DISABLE_PROXIES", "1")).strip() != "0"
    if not disable_proxies:
        try:
            import httpx  # type: ignore
            import time

            start = time.monotonic()
            with httpx.Client(timeout=timeout_s) as client:
                resp = client.get(url, headers=headers)
                elapsed_ms = int((time.monotonic() - start) * 1000)
                if resp.status_code == 200:
                    return {"status": "ok", "payload": resp.json(), "url": url, "elapsed_ms": elapsed_ms}
                return {
                    "status": "net_error",
                    "url": url,
                    "http_status": resp.status_code,
                    "error": "HTTPError",
                    "body_head": resp.text[:200],
                    "elapsed_ms": elapsed_ms,
                }
        except Exception:
            pass
    try:
        from urllib.request import Request
        import json as _json
        import time

        req = Request(url, headers=headers)
        start = time.monotonic()
        opener = _build_proxy_free_opener()
        with _without_proxy_env(disable_proxies):
            with opener.open(req, timeout=timeout_s) as resp:
                elapsed_ms = int((time.monotonic() - start) * 1000)
                status = getattr(resp, "status", 200)
                body = resp.read().decode("utf-8", errors="ignore")
                if status != 200:
                    return {
                        "status": "net_error",
                        "url": url,
                        "http_status": status,
                        "error": "HTTPError",
                        "body_head": body[:200],
                        "elapsed_ms": elapsed_ms,
                    }
                return {"status": "ok", "payload": _json.loads(body), "url": url, "elapsed_ms": elapsed_ms}
    except Exception as exc:
        return {
            "status": "net_error",
            "url": url,
            "error": type(exc).__name__,
            "message": str(exc)[:200],
        }


def _build_proxy_free_opener():
    from urllib.request import build_opener, ProxyHandler
    import os

    disable = str(os.getenv("OCTA_GDELT_DISABLE_PROXIES", "1")).strip() != "0"
    if disable:
        return build_opener(ProxyHandler({}))
    return build_opener()


@contextmanager
def _without_proxy_env(disable: bool):
    if not disable:
        yield
        return
    keys = (
        "http_proxy",
        "https_proxy",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "all_proxy",
        "ALL_PROXY",
        "no_proxy",
        "NO_PROXY",
    )
    saved = {k: os.environ.get(k) for k in keys if k in os.environ}
    for key in keys:
        os.environ.pop(key, None)
    try:
        yield
    finally:
        for key, value in saved.items():
            if value is not None:
                os.environ[key] = value


def _extract_points(payload: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(payload, Mapping):
        return []
    timeline = payload.get("timeline")
    if not isinstance(timeline, list):
        return []
    points: list[dict[str, Any]] = []
    for entry in timeline:
        if not isinstance(entry, Mapping):
            continue
        data = entry.get("data")
        if isinstance(data, list):
            for point in data:
                if not isinstance(point, Mapping):
                    continue
                if "date" in point and "value" in point:
                    points.append({"date": str(point["date"]), "value": point["value"]})
            continue
        if "date" in entry and "value" in entry:
            points.append({"date": str(entry["date"]), "value": entry["value"]})
    return points


def _encode_params(params: Mapping[str, Any]) -> str:
    from urllib.parse import urlencode

    return urlencode(params)
