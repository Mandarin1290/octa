from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional
from urllib import request

import yaml


@dataclass(frozen=True)
class FinGPTHealth:
    enabled: bool
    ok: bool
    cache_hit: bool
    failures: int
    disabled_until: Optional[str]
    latency_ms: float
    error: Optional[str] = None


@dataclass(frozen=True)
class EventClassification:
    label: str
    risk_score: float
    confidence: float
    health: FinGPTHealth


@dataclass(frozen=True)
class Summary:
    text: str
    health: FinGPTHealth


@dataclass(frozen=True)
class SentimentScore:
    score: float
    health: FinGPTHealth


def load_fingpt_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    path = config_path or os.getenv("OCTA_ALTDATA_LIVE_CONFIG") or str(Path("config") / "altdata_live.yaml")
    try:
        raw = Path(path).read_text()
        cfg = yaml.safe_load(raw) or {}
        if not isinstance(cfg, dict):
            return {}
        fingpt_cfg = cfg.get("fingpt")
        return fingpt_cfg if isinstance(fingpt_cfg, dict) else {}
    except Exception:
        return {}


class NullFinGPT:
    def classify_event(self, text: str, metadata: Mapping[str, Any] | None = None) -> EventClassification:
        return EventClassification(label="neutral", risk_score=0.0, confidence=0.0, health=_neutral_health())

    def summarize_filing(self, text: str) -> Summary:
        return Summary(text="", health=_neutral_health())

    def score_sentiment(self, texts: List[str]) -> SentimentScore:
        return SentimentScore(score=0.0, health=_neutral_health())


class FinGPTClient:
    def __init__(
        self,
        *,
        config: Optional[Mapping[str, Any]] = None,
        requester: Optional[Callable[[str, Dict[str, Any], float], Dict[str, Any]]] = None,
    ) -> None:
        self._cfg = dict(config or load_fingpt_config())
        self._failures = 0
        self._disabled_until: Optional[datetime] = None
        self._requester = requester or _http_request

    def classify_event(self, text: str, metadata: Mapping[str, Any] | None = None) -> EventClassification:
        result = self._call("classify_event", {"text": text, "metadata": dict(metadata or {})})
        return EventClassification(
            label=str(result.get("label", "neutral")),
            risk_score=float(result.get("risk_score", 0.0)),
            confidence=float(result.get("confidence", 0.0)),
            health=result["health"],
        )

    def summarize_filing(self, text: str) -> Summary:
        result = self._call("summarize_filing", {"text": text})
        return Summary(text=str(result.get("summary", "")), health=result["health"])

    def score_sentiment(self, texts: List[str]) -> SentimentScore:
        result = self._call("score_sentiment", {"texts": list(texts)})
        return SentimentScore(score=float(result.get("score", 0.0)), health=result["health"])

    def _call(self, method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        enabled = bool(self._cfg.get("enabled", False))
        if not enabled:
            health = _neutral_health(enabled=False)
            _write_audit(method, payload, health, self._cfg)
            return {"health": health}

        now = datetime.now(timezone.utc)
        if self._disabled_until and now < self._disabled_until:
            health = _neutral_health(
                enabled=True,
                ok=False,
                failures=self._failures,
                disabled_until=self._disabled_until,
                error="circuit_breaker_open",
            )
            _write_audit(method, payload, health, self._cfg)
            return {"health": health}

        cache_root = _cache_root()
        asof = date.today()
        cache_paths = _cache_paths(method, payload, asof, cache_root)
        ttl = int(self._cfg.get("cache_ttl_seconds", 86400))
        cached_payload, cache_meta, cache_hit = _read_cache(cache_paths, ttl)
        if cache_hit and cached_payload is not None:
            health = _neutral_health(enabled=True, ok=True, cache_hit=True, failures=self._failures)
            _write_audit(method, payload, health, self._cfg, cache_meta)
            return {**cached_payload, "health": health}

        endpoint = _endpoint(self._cfg, method)
        if not endpoint:
            health = _neutral_health(enabled=True, ok=False, failures=self._failures, error="missing_endpoint")
            self._register_failure()
            _write_audit(method, payload, health, self._cfg)
            return {"health": health}

        timeout_ms = int(self._cfg.get("timeout_ms", 2000))
        max_latency_ms = int(self._cfg.get("max_latency_ms", 2000))
        started = time.monotonic()
        error = None
        response: Dict[str, Any] = {}
        try:
            response = self._requester(endpoint, payload, timeout_ms / 1000.0)
        except Exception as exc:
            error = str(exc)
        latency_ms = (time.monotonic() - started) * 1000.0

        ok = bool(response) and error is None and latency_ms <= max_latency_ms
        if not ok:
            if error is None and latency_ms > max_latency_ms:
                error = "latency_budget_exceeded"
            self._register_failure()
        else:
            self._failures = 0

        health = _neutral_health(
            enabled=True,
            ok=ok,
            cache_hit=False,
            failures=self._failures,
            latency_ms=latency_ms,
            error=error,
        )
        if ok:
            meta = {
                "method": method,
                "fetched_at": _now_iso(),
                "latency_ms": latency_ms,
            }
            _write_cache(cache_paths, response, meta)
        _write_audit(method, payload, health, self._cfg, {"cache_hit": cache_hit})
        return {**response, "health": health}

    def _register_failure(self) -> None:
        self._failures += 1
        failures_threshold = int(self._cfg.get("circuit_breaker", {}).get("failures", 3))
        cooldown = int(self._cfg.get("circuit_breaker", {}).get("cooldown_seconds", 3600))
        if self._failures >= failures_threshold:
            self._disabled_until = datetime.now(timezone.utc) + _seconds(cooldown)


def _endpoint(cfg: Mapping[str, Any], method: str) -> Optional[str]:
    endpoints = cfg.get("endpoints")
    if isinstance(endpoints, dict) and endpoints.get(method):
        return str(endpoints.get(method))
    return str(cfg.get("endpoint_url")) if cfg.get("endpoint_url") else None


def _http_request(url: str, payload: Dict[str, Any], timeout_s: float) -> Dict[str, Any]:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with request.urlopen(req, timeout=timeout_s) as resp:
        raw = resp.read().decode("utf-8")
    parsed = json.loads(raw) if raw else {}
    return parsed if isinstance(parsed, dict) else {}


def _neutral_health(
    *,
    enabled: bool = True,
    ok: bool = False,
    cache_hit: bool = False,
    failures: int = 0,
    latency_ms: float = 0.0,
    disabled_until: Optional[datetime] = None,
    error: Optional[str] = None,
) -> FinGPTHealth:
    return FinGPTHealth(
        enabled=enabled,
        ok=ok,
        cache_hit=cache_hit,
        failures=failures,
        disabled_until=disabled_until.isoformat() if disabled_until else None,
        latency_ms=latency_ms,
        error=error,
    )


def _cache_root() -> Path:
    env_root = os.getenv("OCTA_FINGPT_CACHE_ROOT")
    return Path(env_root) if env_root else Path("octa") / "var" / "cache" / "fingpt"


def _audit_root() -> Path:
    env_root = os.getenv("OCTA_FINGPT_AUDIT_ROOT")
    return Path(env_root) if env_root else Path("octa") / "var" / "audit" / "fingpt"


def _cache_paths(method: str, payload: Dict[str, Any], asof: date, root: Path) -> dict[str, Path]:
    key = _hash_payload(method, payload)
    out_dir = root / key / asof.isoformat()
    return {
        "dir": out_dir,
        "payload": out_dir / f"{method}.json",
        "meta": out_dir / f"{method}_meta.json",
    }


def _hash_payload(method: str, payload: Dict[str, Any]) -> str:
    raw = json.dumps({"method": method, "payload": payload}, sort_keys=True, default=str).encode("utf-8")
    return sha256(raw).hexdigest()


def _read_cache(paths: dict[str, Path], ttl: int) -> tuple[Optional[Dict[str, Any]], Dict[str, Any], bool]:
    payload_path = paths["payload"]
    meta_path = paths["meta"]
    if not payload_path.exists() or not meta_path.exists():
        return None, {}, False
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        meta = {}
    fetched_at = _parse_ts(meta.get("fetched_at"))
    if fetched_at is None:
        return None, meta, False
    age = (datetime.now(timezone.utc) - fetched_at).total_seconds()
    if ttl > 0 and age > ttl:
        return None, meta, False
    try:
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
    except Exception:
        return None, meta, False
    return payload, meta, True


def _write_cache(paths: dict[str, Path], payload: Dict[str, Any], meta: Dict[str, Any]) -> None:
    out_dir = paths["dir"]
    out_dir.mkdir(parents=True, exist_ok=True)
    tmp_payload = paths["payload"].with_suffix(".json.tmp")
    tmp_meta = paths["meta"].with_suffix(".json.tmp")
    tmp_payload.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    tmp_meta.write_text(json.dumps(meta, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    tmp_payload.replace(paths["payload"])
    tmp_meta.replace(paths["meta"])


def _write_audit(
    method: str,
    payload: Dict[str, Any],
    health: FinGPTHealth,
    cfg: Mapping[str, Any],
    cache_meta: Optional[Mapping[str, Any]] = None,
) -> None:
    root = _audit_root()
    root.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).isoformat()
    safe_ts = ts.replace(":", "").replace("-", "").replace(".", "")
    audit = {
        "timestamp": ts,
        "method": method,
        "enabled": health.enabled,
        "ok": health.ok,
        "cache_hit": health.cache_hit,
        "failures": health.failures,
        "disabled_until": health.disabled_until,
        "latency_ms": health.latency_ms,
        "error": health.error,
        "timeout_ms": cfg.get("timeout_ms"),
        "max_latency_ms": cfg.get("max_latency_ms"),
        "circuit_breaker": cfg.get("circuit_breaker"),
        "cache_meta": cache_meta or {},
        "payload_hash": _hash_payload(method, payload),
    }
    path = root / f"fingpt_{safe_ts}.json"
    path.write_text(json.dumps(audit, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _parse_ts(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value))
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _seconds(value: int) -> Any:
    from datetime import timedelta

    return timedelta(seconds=value)
