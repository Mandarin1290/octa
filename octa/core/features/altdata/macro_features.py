from __future__ import annotations

from datetime import datetime, timezone
from statistics import pstdev
from typing import Any, Mapping

from octa.core.utils.typing_safe import as_float


def build(payloads: Mapping[str, Any], *, asof_ts: str | None = None) -> dict[str, float]:
    fred = payloads.get("fred", {})
    ecb = payloads.get("ecb", {})
    worldbank = payloads.get("worldbank", {})
    oecd = payloads.get("oecd", {})
    eia = payloads.get("eia", {})
    stooq = payloads.get("stooq", {})

    features: dict[str, float] = {}

    def _last(series: list[dict[str, Any]]) -> float | None:
        if not series:
            return None
        val = series[-1].get("value")
        if val is None:
            return None
        parsed = as_float(val, default=float("nan"))
        if parsed != parsed:  # NaN
            return None
        return parsed

    series = fred.get("series", {}) if isinstance(fred, dict) else {}
    fedfunds = _last(series.get("FEDFUNDS", [])) if isinstance(series, dict) else None
    dgs2 = _last(series.get("DGS2", [])) if isinstance(series, dict) else None
    dgs10 = _last(series.get("DGS10", [])) if isinstance(series, dict) else None
    dgs3m = _last(series.get("DGS3MO", [])) if isinstance(series, dict) else None
    cpi = _last(series.get("CPIAUCSL", [])) if isinstance(series, dict) else None

    if fedfunds is not None:
        features["rates_fedfunds"] = fedfunds
    if dgs2 is not None:
        features["rates_2y"] = dgs2
    if dgs10 is not None:
        features["rates_10y"] = dgs10
    if dgs10 is not None and dgs2 is not None:
        features["curve_10y_2y"] = dgs10 - dgs2
    if dgs10 is not None and dgs3m is not None:
        features["curve_10y_3m"] = dgs10 - dgs3m
    if cpi is not None:
        features["inflation_cpi"] = cpi

    energy = eia.get("energy_shock", 0.0) if isinstance(eia, dict) else 0.0
    features["energy_shock"] = as_float(energy, default=0.0)

    ecb_rate = _last(ecb.get("series", {}).get("ECB_RATE", [])) if isinstance(ecb, dict) else None
    if ecb_rate is not None:
        features["ecb_rate"] = ecb_rate

    wb_growth = worldbank.get("gdp_growth") if isinstance(worldbank, dict) else None
    if wb_growth is not None:
        parsed = as_float(wb_growth, default=float("nan"))
        if parsed == parsed:
            features["worldbank_gdp_growth"] = parsed

    oecd_cli = oecd.get("cli") if isinstance(oecd, dict) else None
    if oecd_cli is not None:
        parsed = as_float(oecd_cli, default=float("nan"))
        if parsed == parsed:
            features["oecd_cli"] = parsed

    risk_score = 0.0
    if features.get("curve_10y_2y", 0.0) < 0:
        risk_score += 0.5
    if features.get("energy_shock", 0.0) > 0.5:
        risk_score += 0.5
    features["macro_risk_score"] = risk_score

    stooq_rows = stooq.get("rows", []) if isinstance(stooq, dict) else []
    stooq_cutoff = _parse_ts(asof_ts) if asof_ts else None
    spx_closes = _series_for_proxy(stooq_rows, "spx", stooq_cutoff)
    vix_closes = _series_for_proxy(stooq_rows, "vix", stooq_cutoff)
    dxy_closes = _series_for_proxy(stooq_rows, "dxy", stooq_cutoff)
    gold_closes = _series_for_proxy(stooq_rows, "gold", stooq_cutoff)
    oil_closes = _series_for_proxy(stooq_rows, "oil", stooq_cutoff)

    spx_ret_5d = _ret_n(spx_closes, 5)
    spx_vol_20d = _vol_n(spx_closes, 20)
    vix_level = vix_closes[-1] if vix_closes else None
    dxy_ret_5d = _ret_n(dxy_closes, 5)
    gold_ret_5d = _ret_n(gold_closes, 5)
    oil_ret_5d = _ret_n(oil_closes, 5)

    if spx_ret_5d is not None:
        features["proxy_spx_ret_5d"] = spx_ret_5d
    if spx_vol_20d is not None:
        features["proxy_spx_vol_20d"] = spx_vol_20d
    if vix_level is not None:
        features["proxy_vix_level"] = vix_level
    if dxy_ret_5d is not None:
        features["proxy_dxy_ret_5d"] = dxy_ret_5d
    if gold_ret_5d is not None:
        features["proxy_gold_ret_5d"] = gold_ret_5d
    if oil_ret_5d is not None:
        features["proxy_oil_ret_5d"] = oil_ret_5d

    market_risk = _market_risk_score(
        vix_level=vix_level,
        spx_vol_20d=spx_vol_20d,
        spx_ret_5d=spx_ret_5d,
    )
    if market_risk is not None:
        features["market_risk_score"] = market_risk

    return features


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
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


def _series_for_proxy(rows: list[dict[str, Any]], proxy: str, cutoff: datetime | None) -> list[float]:
    series: list[tuple[datetime, float]] = []
    for row in rows:
        if row.get("proxy") != proxy:
            continue
        ts = _parse_ts(row.get("ts"))
        if ts is None:
            continue
        if cutoff is not None and ts > cutoff:
            continue
        raw_close = row.get("close")
        if raw_close is None:
            continue
        close = as_float(raw_close, default=float("nan"))
        if close != close:  # NaN
            continue
        series.append((ts, close))
    series.sort(key=lambda r: r[0])
    return [v for _, v in series]


def _ret_n(closes: list[float], n: int) -> float | None:
    if len(closes) <= n:
        return None
    base = closes[-(n + 1)]
    if base == 0:
        return None
    return (closes[-1] / base) - 1.0


def _vol_n(closes: list[float], n: int) -> float | None:
    if len(closes) <= n:
        return None
    window = closes[-(n + 1) :]
    returns = []
    for idx in range(1, len(window)):
        prev = window[idx - 1]
        curr = window[idx]
        if prev == 0:
            continue
        returns.append((curr / prev) - 1.0)
    if len(returns) < 2:
        return None
    return pstdev(returns)


def _market_risk_score(
    *,
    vix_level: float | None,
    spx_vol_20d: float | None,
    spx_ret_5d: float | None,
) -> float | None:
    if vix_level is None and spx_vol_20d is None and spx_ret_5d is None:
        return None
    risk = 0.0
    if vix_level is not None:
        risk += 0.5 * min(1.0, vix_level / 40.0)
    if spx_vol_20d is not None:
        risk += 0.3 * min(1.0, spx_vol_20d / 0.04)
    if spx_ret_5d is not None and spx_ret_5d < -0.03:
        risk += 0.2
    return max(0.0, min(1.0, risk))
