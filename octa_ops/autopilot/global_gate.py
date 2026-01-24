from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from octa_training.core.io_parquet import load_parquet

from .types import GateDecision


@dataclass
class GlobalGatePolicy:
    min_history_days: int = 252
    max_drawdown_max: float = 0.6
    max_vol_annual: float = 2.5

    # Optional enrichers
    fred_enabled: bool = True
    edgar_enabled: bool = True
    edgar_user_agent: str = "OCTA/1.0 (research; contact=ops@example.com)"

    # Optional: which FRED series to use as global context (best-effort)
    fred_series: tuple[str, ...] = ("FEDFUNDS", "DGS10", "DGS2", "UNRATE")
    # EDGAR rate limit (requests per minute) – conservative
    edgar_rate_limit_per_minute: int = 8


def _max_drawdown(series: pd.Series) -> float:
    x = series.astype(float)
    if x.empty:
        return float("nan")
    cummax = x.cummax()
    dd = (x / cummax) - 1.0
    return float(dd.min())


def evaluate_global_gate(
    *,
    symbol: str,
    parquet_1d_path: str,
    policy: GlobalGatePolicy,
    cache_dir: str,
) -> GateDecision:
    tf = "1D"
    try:
        df = load_parquet(Path(parquet_1d_path))
    except Exception as e:
        return GateDecision(symbol=symbol, timeframe=tf, stage="global", status="FAIL", reason="data_load_failed", details={"error": str(e), "path": parquet_1d_path})

    if "close" not in df.columns:
        return GateDecision(symbol=symbol, timeframe=tf, stage="global", status="FAIL", reason="missing_required_columns", details={"path": parquet_1d_path})
    if not isinstance(df.index, pd.DatetimeIndex):
        return GateDecision(symbol=symbol, timeframe=tf, stage="global", status="FAIL", reason="timestamp_not_datetimeindex", details={"path": parquet_1d_path})

    close = pd.to_numeric(df["close"], errors="coerce")
    ok = ~close.isna()
    close = pd.Series(close[ok].values, index=df.index[ok]).sort_index()
    if len(close) < int(policy.min_history_days):
        return GateDecision(symbol=symbol, timeframe=tf, stage="global", status="FAIL", reason="insufficient_history", details={"n": int(len(close)), "min": int(policy.min_history_days)})
    rets = close.pct_change().dropna()
    vol_ann = float(rets.std(ddof=0) * (252.0 ** 0.5)) if len(rets) else float("nan")
    mdd = float(_max_drawdown(close))

    details: Dict[str, Any] = {
        "path": parquet_1d_path,
        "n": int(len(close)),
        "vol_ann": vol_ann,
        "max_drawdown": mdd,
    }

    if not (mdd >= -float(policy.max_drawdown_max)):
        return GateDecision(symbol=symbol, timeframe=tf, stage="global", status="FAIL", reason="max_drawdown_exceeded", details=details)

    if vol_ann != vol_ann or vol_ann > float(policy.max_vol_annual):
        return GateDecision(symbol=symbol, timeframe=tf, stage="global", status="FAIL", reason="volatility_exceeded", details=details)

    # Optional enrichers: best-effort, never block; cache artifacts under global_features_store
    cache = Path(cache_dir)
    cache.mkdir(parents=True, exist_ok=True)

    # ---- FRED (macro context) ----
    fred_key = os.getenv("FRED_API_KEY")
    if bool(policy.fred_enabled) and bool(fred_key):
        fred_ctx: Dict[str, Any] = {"enabled": True, "series": list(policy.fred_series)}
        try:
            # Try to use our optional AltData connector if available.
            from okta_altdat.connectors.fred_connector import (
                fetch_fred_series,  # type: ignore
            )

            end_dt = close.index.max()
            # Pull a modest window to avoid big downloads; global context only.
            start_ts = pd.Timestamp(end_dt - pd.Timedelta(days=370))
            end_ts = pd.Timestamp(end_dt)
            if start_ts.tz is None:
                start_ts = start_ts.tz_localize("UTC")
            else:
                start_ts = start_ts.tz_convert("UTC")
            if end_ts.tz is None:
                end_ts = end_ts.tz_localize("UTC")
            else:
                end_ts = end_ts.tz_convert("UTC")

            # Cache FRED context under the run's cache_dir, keyed by the end date.
            # This avoids refetching the same macro series for every symbol.
            cache = Path(cache_dir)
            cache.mkdir(parents=True, exist_ok=True)
            fred_cache_dir = cache / "fred"
            fred_cache_dir.mkdir(parents=True, exist_ok=True)
            cache_key = end_ts.strftime("%Y%m%d")
            fred_cache_path = fred_cache_dir / f"fred_context_{cache_key}.json"

            cached = None
            if fred_cache_path.exists():
                try:
                    cached = json.loads(fred_cache_path.read_text(encoding="utf-8"))
                except Exception:
                    cached = None

            if isinstance(cached, dict) and isinstance(cached.get("data"), dict):
                fred_ctx = cached
            else:
                series_data: Dict[str, Any] = {}
                for sid in policy.fred_series:
                    try:
                        r = fetch_fred_series(series_id=str(sid), start_ts=start_ts, end_ts=end_ts, api_key=fred_key)
                        if not getattr(r, "ok", False):
                            series_data[str(sid)] = {"error": getattr(r, "error", None)}
                            continue
                        df_f = getattr(r, "df", None)
                        if df_f is None or getattr(df_f, "empty", True):
                            series_data[str(sid)] = None
                            continue
                        df_f = df_f.copy()
                        df_f["ts"] = pd.to_datetime(df_f["ts"], utc=True, errors="coerce")
                        df_f["value"] = pd.to_numeric(df_f["value"], errors="coerce")
                        df_f = df_f.dropna(subset=["ts", "value"]).sort_values("ts")
                        if df_f.empty:
                            series_data[str(sid)] = None
                            continue
                        s = pd.Series(df_f["value"].values, index=df_f["ts"])
                        last_ts = pd.Timestamp(s.index[-1])
                        if last_ts.tz is None:
                            last_ts = last_ts.tz_localize("UTC")
                        else:
                            last_ts = last_ts.tz_convert("UTC")
                        last_v = float(s.iloc[-1])
                        # 30d delta: nearest observation at/after (last-30d)
                        target = last_ts - pd.Timedelta(days=30)
                        s_after = s[s.index >= target]
                        d30 = float(last_v - float(s_after.iloc[0])) if len(s_after) else None
                        series_data[str(sid)] = {"last": last_v, "last_date": str(last_ts), "d30": d30}
                    except Exception:
                        series_data[str(sid)] = None

                fred_ctx = {
                    "enabled": True,
                    "series": list(policy.fred_series),
                    "window": {"start_ts": str(start_ts), "end_ts": str(end_ts)},
                    "data": series_data,
                }
                try:
                    fred_cache_path.write_text(json.dumps(fred_ctx, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
                except Exception:
                    pass

            details["fred"] = fred_ctx
        except Exception as e:
            fred_ctx["enabled"] = False
            fred_ctx["error"] = str(e)
            details["fred"] = fred_ctx
    else:
        details["fred"] = {"enabled": False}

    # ---- EDGAR (equities/ETFs fundamentals context) ----
    if bool(policy.edgar_enabled) and bool(policy.edgar_user_agent):
        edgar_ctx: Dict[str, Any] = {"enabled": True, "user_agent_configured": True}
        try:
            # lightweight, cached SEC JSON fetch; no dependency on external libs
            import urllib.request

            edgar_cache = cache / "edgar"
            edgar_cache.mkdir(parents=True, exist_ok=True)

            def _fetch_json(url: str, out_path: Path) -> Dict[str, Any]:
                if out_path.exists():
                    try:
                        return json.loads(out_path.read_text(encoding="utf-8"))
                    except Exception:
                        pass
                req = urllib.request.Request(url, headers={"User-Agent": str(policy.edgar_user_agent)})
                with urllib.request.urlopen(req, timeout=20) as resp:
                    raw = resp.read().decode("utf-8")
                out_path.write_text(raw, encoding="utf-8")
                return json.loads(raw)

            # Map ticker->CIK using cached company_tickers.json
            tickers = _fetch_json(
                "https://www.sec.gov/files/company_tickers.json",
                edgar_cache / "company_tickers.json",
            )
            cik = None
            try:
                sym_u = str(symbol).strip().upper()
                for _k, rec in (tickers or {}).items():
                    if str(rec.get("ticker") or "").strip().upper() == sym_u:
                        cik = int(rec.get("cik_str"))
                        break
            except Exception:
                cik = None

            if cik is None:
                edgar_ctx["enabled"] = False
                edgar_ctx["reason"] = "ticker_not_mapped"
            else:
                # Rate limit: simple sleep to keep under policy.edgar_rate_limit_per_minute
                try:
                    rpm = max(1, int(policy.edgar_rate_limit_per_minute))
                    time.sleep(60.0 / float(rpm))
                except Exception:
                    pass
                cik_str = str(cik).zfill(10)
                sub = _fetch_json(
                    f"https://data.sec.gov/submissions/CIK{cik_str}.json",
                    edgar_cache / f"CIK{cik_str}.json",
                )
                # Minimal features: recent filings count + most recent filing date
                recent = (((sub or {}).get("filings") or {}).get("recent") or {})
                forms = recent.get("form") or []
                dates = recent.get("filingDate") or []
                last_date = None
                try:
                    if dates:
                        last_date = str(dates[0])
                except Exception:
                    last_date = None
                cnt_10k = 0
                cnt_10q = 0
                try:
                    for f in forms:
                        ff = str(f).strip().upper()
                        if ff == "10-K":
                            cnt_10k += 1
                        if ff == "10-Q":
                            cnt_10q += 1
                except Exception:
                    pass
                edgar_ctx.update({"cik": cik_str, "recent_10k": cnt_10k, "recent_10q": cnt_10q, "last_filing_date": last_date})
        except Exception as e:
            edgar_ctx["enabled"] = False
            edgar_ctx["error"] = str(e)
        details["edgar"] = edgar_ctx
    else:
        details["edgar"] = {"enabled": False, "user_agent_configured": bool(policy.edgar_user_agent)}

    # Persist per-symbol global feature snapshot for auditability
    try:
        (cache / f"global_features_{symbol}.json").write_text(
            json.dumps(details, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
    except Exception:
        pass

    return GateDecision(symbol=symbol, timeframe=tf, stage="global", status="PASS", reason=None, details=details)


def write_global_outputs(*, run_dir: str, decisions: Dict[str, GateDecision]) -> str:
    out_dir = Path(run_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    p = out_dir / "global_gate_status.json"
    payload = {
        sym: {
            "status": d.status,
            "reason": d.reason,
            "details": d.details,
        }
        for sym, d in decisions.items()
    }
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return str(p)
