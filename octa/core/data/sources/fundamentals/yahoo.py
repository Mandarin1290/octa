from __future__ import annotations

import json
import math
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

import yaml


@dataclass(frozen=True)
class YahooHealth:
    ok: bool
    cache_hit: bool
    errors: list[str]
    latency_ms: float
    endpoint: str
    rate_limited: bool = False


@dataclass(frozen=True)
class FundamentalSnapshot:
    symbol: str
    asof_date: str
    data: Dict[str, Any]
    health: YahooHealth


@dataclass(frozen=True)
class CorporateActions:
    symbol: str
    asof_date: str
    dividends: List[Dict[str, Any]]
    splits: List[Dict[str, Any]]
    health: YahooHealth


@dataclass(frozen=True)
class EarningsEvents:
    symbol: str
    asof_date: str
    events: List[Dict[str, Any]]
    health: YahooHealth


def load_yahoo_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    path = config_path or os.getenv("OCTA_ALTDATA_LIVE_CONFIG") or str(Path("config") / "altdata_live.yaml")
    try:
        raw = Path(path).read_text()
        cfg = yaml.safe_load(raw) or {}
        if not isinstance(cfg, dict):
            return {}
        yahoo_cfg = cfg.get("yahoo")
        return yahoo_cfg if isinstance(yahoo_cfg, dict) else {}
    except Exception:
        return {}


def fetch_yahoo_fundamentals(symbol: str) -> FundamentalSnapshot:
    return _fetch_fundamentals(symbol, load_yahoo_config())


def fetch_yahoo_corporate_actions(symbol: str) -> CorporateActions:
    return _fetch_corporate_actions(symbol, load_yahoo_config())


def fetch_yahoo_earnings_calendar(symbol: str) -> EarningsEvents:
    return _fetch_earnings(symbol, load_yahoo_config())


def build_yahoo_features(
    snapshot: FundamentalSnapshot,
    actions: CorporateActions,
    earnings: EarningsEvents,
) -> dict[str, float | int | str]:
    features: dict[str, float | int | str] = {}
    info = snapshot.data.get("info", {}) if isinstance(snapshot.data, dict) else {}

    def _num(value: Any) -> float:
        try:
            return float(value)
        except Exception:
            return float("nan")

    pe_ttm = _num(info.get("trailingPE"))
    fwd_pe = _num(info.get("forwardPE"))
    market_cap = _num(info.get("marketCap"))
    enterprise_val = _num(info.get("enterpriseValue"))
    trailing_eps = _num(info.get("trailingEps"))
    forward_eps = _num(info.get("forwardEps"))
    dividend_yield = _num(info.get("dividendYield"))
    price_to_book = _num(info.get("priceToBook"))
    beta = _num(info.get("beta"))

    features.update(
        {
            "yahoo__pe_ttm": pe_ttm,
            "yahoo__forward_pe": fwd_pe,
            "yahoo__market_cap": market_cap,
            "yahoo__enterprise_value": enterprise_val,
            "yahoo__trailing_eps": trailing_eps,
            "yahoo__forward_eps": forward_eps,
            "yahoo__dividend_yield": dividend_yield,
            "yahoo__price_to_book": price_to_book,
            "yahoo__beta": beta,
        }
    )

    valuation_stretch = _valuation_stretch(pe_ttm, price_to_book)
    features["yahoo__valuation_stretch"] = valuation_stretch

    earnings_days_to, earnings_within_1d, earnings_within_3d = _earnings_proximity(
        earnings.events
    )
    features["yahoo__earnings_days_to"] = earnings_days_to
    features["yahoo__earnings_within_1d"] = int(earnings_within_1d)
    features["yahoo__earnings_within_3d"] = int(earnings_within_3d)

    corp_guard, days_since_split, days_since_div = _corp_action_guard(actions)
    features["yahoo__corp_action_guard"] = int(corp_guard)
    features["yahoo__corp_action_days_since_split"] = days_since_split
    features["yahoo__corp_action_days_since_dividend"] = days_since_div

    features["yahoo__fundamentals_missing"] = int(not snapshot.health.ok)
    features["yahoo__actions_missing"] = int(not actions.health.ok)
    features["yahoo__earnings_missing"] = int(not earnings.health.ok)

    return features


def _fetch_fundamentals(symbol: str, cfg: Dict[str, Any]) -> FundamentalSnapshot:
    endpoint = "fundamentals"
    started = time.monotonic()
    cache_hit = False
    errors: list[str] = []
    payload: Dict[str, Any] = {}

    cache_root = _cache_root()
    asof = date.today()
    cache_paths = _cache_paths(symbol, asof, endpoint, cache_root)
    ttl = int(cfg.get("cache_ttl_seconds", 86400))
    cached_payload, cache_meta, cache_hit = _read_cache(cache_paths, ttl)
    if cache_hit and cached_payload is not None:
        health = YahooHealth(ok=True, cache_hit=True, errors=[], latency_ms=_elapsed_ms(started), endpoint=endpoint)
        _write_audit(symbol, endpoint, health, cache_meta, errors)
        return FundamentalSnapshot(symbol=symbol, asof_date=asof.isoformat(), data=cached_payload, health=health)

    allow_net = _allow_net(cfg)
    if allow_net:
        try:
            payload = _fetch_yfinance_info(symbol)
        except Exception as exc:
            errors.append(str(exc))

    ok = bool(payload)
    health = YahooHealth(
        ok=ok,
        cache_hit=False,
        errors=errors,
        latency_ms=_elapsed_ms(started),
        endpoint=endpoint,
    )
    if ok:
        meta = {
            "symbol": symbol,
            "endpoint": endpoint,
            "fetched_at": _now_iso(),
        }
        _write_cache(cache_paths, payload, meta)
    _write_audit(symbol, endpoint, health, {"cache_hit": cache_hit}, errors)
    return FundamentalSnapshot(symbol=symbol, asof_date=asof.isoformat(), data=payload, health=health)


def _fetch_corporate_actions(symbol: str, cfg: Dict[str, Any]) -> CorporateActions:
    endpoint = "corporate_actions"
    started = time.monotonic()
    cache_hit = False
    errors: list[str] = []
    dividends: list[dict[str, Any]] = []
    splits: list[dict[str, Any]] = []

    cache_root = _cache_root()
    asof = date.today()
    cache_paths = _cache_paths(symbol, asof, endpoint, cache_root)
    ttl = int(cfg.get("cache_ttl_seconds", 86400))
    cached_payload, cache_meta, cache_hit = _read_cache(cache_paths, ttl)
    if cache_hit and cached_payload is not None:
        dividends = cached_payload.get("dividends", []) if isinstance(cached_payload, dict) else []
        splits = cached_payload.get("splits", []) if isinstance(cached_payload, dict) else []
        health = YahooHealth(ok=True, cache_hit=True, errors=[], latency_ms=_elapsed_ms(started), endpoint=endpoint)
        _write_audit(symbol, endpoint, health, cache_meta, errors)
        return CorporateActions(
            symbol=symbol,
            asof_date=asof.isoformat(),
            dividends=dividends,
            splits=splits,
            health=health,
        )

    allow_net = _allow_net(cfg)
    if allow_net:
        try:
            dividends, splits = _fetch_yfinance_actions(symbol)
        except Exception as exc:
            errors.append(str(exc))

    ok = bool(dividends or splits)
    payload = {"dividends": dividends, "splits": splits}
    health = YahooHealth(
        ok=ok,
        cache_hit=False,
        errors=errors,
        latency_ms=_elapsed_ms(started),
        endpoint=endpoint,
    )
    if ok:
        meta = {
            "symbol": symbol,
            "endpoint": endpoint,
            "fetched_at": _now_iso(),
        }
        _write_cache(cache_paths, payload, meta)
    _write_audit(symbol, endpoint, health, {"cache_hit": cache_hit}, errors)
    return CorporateActions(
        symbol=symbol,
        asof_date=asof.isoformat(),
        dividends=dividends,
        splits=splits,
        health=health,
    )


def _fetch_earnings(symbol: str, cfg: Dict[str, Any]) -> EarningsEvents:
    endpoint = "earnings"
    started = time.monotonic()
    cache_hit = False
    errors: list[str] = []
    events: list[dict[str, Any]] = []

    cache_root = _cache_root()
    asof = date.today()
    cache_paths = _cache_paths(symbol, asof, endpoint, cache_root)
    ttl = int(cfg.get("cache_ttl_seconds", 86400))
    cached_payload, cache_meta, cache_hit = _read_cache(cache_paths, ttl)
    if cache_hit and cached_payload is not None:
        events = cached_payload.get("events", []) if isinstance(cached_payload, dict) else []
        health = YahooHealth(ok=True, cache_hit=True, errors=[], latency_ms=_elapsed_ms(started), endpoint=endpoint)
        _write_audit(symbol, endpoint, health, cache_meta, errors)
        return EarningsEvents(symbol=symbol, asof_date=asof.isoformat(), events=events, health=health)

    allow_net = _allow_net(cfg)
    if allow_net:
        try:
            events = _fetch_yfinance_earnings(symbol)
        except Exception as exc:
            errors.append(str(exc))

    ok = bool(events)
    payload = {"events": events}
    health = YahooHealth(
        ok=ok,
        cache_hit=False,
        errors=errors,
        latency_ms=_elapsed_ms(started),
        endpoint=endpoint,
    )
    if ok:
        meta = {
            "symbol": symbol,
            "endpoint": endpoint,
            "fetched_at": _now_iso(),
        }
        _write_cache(cache_paths, payload, meta)
    _write_audit(symbol, endpoint, health, {"cache_hit": cache_hit}, errors)
    return EarningsEvents(symbol=symbol, asof_date=asof.isoformat(), events=events, health=health)


def _fetch_yfinance_info(symbol: str) -> Dict[str, Any]:
    try:
        import yfinance as yf  # type: ignore
    except Exception as exc:
        raise RuntimeError("yfinance_not_installed") from exc

    ticker = yf.Ticker(symbol)
    info = {}
    if hasattr(ticker, "get_info"):
        info = ticker.get_info()
    if not info and hasattr(ticker, "info"):
        info = ticker.info
    if not isinstance(info, dict):
        info = {}
    return {"info": info}


def _fetch_yfinance_actions(symbol: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    try:
        import yfinance as yf  # type: ignore
    except Exception as exc:
        raise RuntimeError("yfinance_not_installed") from exc

    ticker = yf.Ticker(symbol)
    actions_df = getattr(ticker, "actions", None)
    if actions_df is None:
        return [], []
    actions_df = actions_df.reset_index()
    dividends: list[dict[str, Any]] = []
    splits: list[dict[str, Any]] = []
    for _, row in actions_df.iterrows():
        ts = row.get("Date") if "Date" in row else row.get("index")
        ts_str = _to_iso(ts)
        div = row.get("Dividends")
        split = row.get("Stock Splits")
        if div is not None and not _is_zero(div):
            dividends.append({"ts": ts_str, "value": float(div)})
        if split is not None and not _is_zero(split):
            splits.append({"ts": ts_str, "value": float(split)})
    return dividends, splits


def _fetch_yfinance_earnings(symbol: str) -> list[dict[str, Any]]:
    try:
        import yfinance as yf  # type: ignore
    except Exception as exc:
        raise RuntimeError("yfinance_not_installed") from exc

    ticker = yf.Ticker(symbol)
    events: list[dict[str, Any]] = []
    try:
        cal = ticker.calendar
    except Exception:
        cal = None
    if cal is not None:
        try:
            for idx, row in cal.T.iterrows():
                ts = row.get("Earnings Date")
                if ts is not None:
                    events.append({"ts": _to_iso(ts), "source": "calendar"})
        except Exception:
            pass
    try:
        df = ticker.get_earnings_dates(limit=6)
    except Exception:
        df = None
    if df is not None:
        try:
            df = df.reset_index()
            for _, row in df.iterrows():
                ts = row.get("Earnings Date") or row.get("index")
                evt = {"ts": _to_iso(ts), "source": "earnings_dates"}
                if "EPS Estimate" in row:
                    evt["eps_estimate"] = row.get("EPS Estimate")
                if "Reported EPS" in row:
                    evt["reported_eps"] = row.get("Reported EPS")
                events.append(evt)
        except Exception:
            pass
    return events


def _cache_root() -> Path:
    env_root = os.getenv("OCTA_YAHOO_CACHE_ROOT")
    return Path(env_root) if env_root else Path("octa") / "var" / "cache" / "yahoo"


def _audit_root() -> Path:
    env_root = os.getenv("OCTA_YAHOO_AUDIT_ROOT")
    return Path(env_root) if env_root else Path("octa") / "var" / "audit" / "yahoo_refresh"


def _cache_paths(symbol: str, asof: date, endpoint: str, root: Path) -> dict[str, Path]:
    out_dir = root / symbol.upper() / asof.isoformat()
    key = f"{endpoint}"
    return {
        "dir": out_dir,
        "payload": out_dir / f"{key}.json",
        "meta": out_dir / f"{key}_meta.json",
    }


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
    symbol: str,
    endpoint: str,
    health: YahooHealth,
    cache_meta: Mapping[str, Any],
    errors: list[str],
) -> None:
    root = _audit_root()
    root.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).isoformat()
    safe_ts = ts.replace(":", "").replace("-", "").replace(".", "")
    payload = {
        "timestamp": ts,
        "symbol": symbol,
        "endpoint": endpoint,
        "cache_hit": health.cache_hit,
        "ok": health.ok,
        "errors": errors,
        "latency_ms": health.latency_ms,
        "rate_limited": health.rate_limited,
        "rate_limit_notes": cache_meta.get("rate_limit_notes") if isinstance(cache_meta, dict) else None,
        "cache_meta": cache_meta,
    }
    path = root / f"yahoo_refresh_{safe_ts}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _valuation_stretch(pe_ttm: float, price_to_book: float) -> float:
    stretch = 0.0
    if math.isfinite(pe_ttm) and pe_ttm > 0:
        stretch = max(stretch, min(1.0, pe_ttm / 40.0))
    if math.isfinite(price_to_book) and price_to_book > 0:
        stretch = max(stretch, min(1.0, price_to_book / 8.0))
    return stretch


def _earnings_proximity(events: list[dict[str, Any]]) -> tuple[float, bool, bool]:
    now = datetime.now(timezone.utc)
    future_days: list[float] = []
    for evt in events:
        ts = _parse_ts(evt.get("ts"))
        if ts is None or ts < now:
            continue
        future_days.append((ts - now).total_seconds() / 86400.0)
    if not future_days:
        return float("nan"), False, False
    days = min(future_days)
    return days, days <= 1.0, days <= 3.0


def _corp_action_guard(actions: CorporateActions) -> tuple[bool, float, float]:
    now = datetime.now(timezone.utc)
    split_days = _days_since(actions.splits, now)
    dividend_days = _days_since(actions.dividends, now)
    guard = False
    if math.isfinite(split_days) and split_days <= 7.0:
        guard = True
    if math.isfinite(dividend_days) and dividend_days <= 3.0:
        guard = True
    return guard, split_days, dividend_days


def _days_since(items: list[dict[str, Any]], now: datetime) -> float:
    if not items:
        return float("nan")
    latest = None
    for item in items:
        ts = _parse_ts(item.get("ts"))
        if ts is None:
            continue
        if latest is None or ts > latest:
            latest = ts
    if latest is None:
        return float("nan")
    return max(0.0, (now - latest).total_seconds() / 86400.0)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _elapsed_ms(started: float) -> float:
    return (time.monotonic() - started) * 1000.0


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


def _to_iso(value: Any) -> Optional[str]:
    ts = _parse_ts(value)
    return ts.isoformat() if ts else None


def _allow_net(cfg: Dict[str, Any]) -> bool:
    env = str(os.getenv("OCTA_ALLOW_NET", "")).strip() == "1"
    live_allowed = bool(cfg.get("live_allowed", True))
    enabled = bool(cfg.get("enabled", False))
    return enabled and live_allowed and env


def _is_zero(value: Any) -> bool:
    try:
        return float(value) == 0.0
    except Exception:
        return False
