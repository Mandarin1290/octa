from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import List, Optional


class AssetClass(str, Enum):
    forex = "forex"
    stock = "stock"
    bond = "bond"
    etf = "etf"
    index = "index"
    future = "future"
    option = "option"
    crypto = "crypto"
    unknown = "unknown"


@dataclass
class EffectiveSettings:
    asset_class: str
    nan_threshold: float
    allow_negative_prices: bool
    resample_enabled: bool
    resample_bar_size: str


def _apply_regex_rules(symbol: str, rules: List[dict]) -> Optional[str]:
    # rules are dicts with pattern, asset_class, priority
    matches = []
    for r in rules:
        try:
            if re.search(r["pattern"], symbol, flags=re.IGNORECASE):
                matches.append((r.get("priority", 0), r["asset_class"]))
        except Exception:
            continue
    if not matches:
        return None
    matches.sort(reverse=True)
    return matches[0][1]


def infer_asset_class(symbol: str, path: str, df_columns: List[str], cfg) -> str:
    # cfg is TrainingConfig
    sym = symbol.upper()
    # 1) direct_map
    # asset_class_overrides can be either a pydantic model or a plain dict
    overrides = getattr(cfg, "asset_class_overrides", None)
    if isinstance(overrides, dict):
        dm = overrides.get("direct_map", {}) or {}
        rules = overrides.get("regex_rules", []) or []
    else:
        dm = overrides.direct_map if overrides is not None and hasattr(overrides, "direct_map") else {}
        rules = overrides.regex_rules if overrides is not None and hasattr(overrides, "regex_rules") else []
    if sym in dm:
        return dm[sym]

    # 2) regex rules
    # convert pydantic objects to dicts if necessary
    rules_list = [r.dict() if hasattr(r, "dict") else r for r in rules]
    rmatch = _apply_regex_rules(sym, rules_list)
    if rmatch:
        return rmatch

    # heuristics
    cols_upper = {str(c).upper() for c in (df_columns or [])}

    # options: detect by greeks/IV columns even if symbol naming is ambiguous
    if any(c in cols_upper for c in {"DELTA", "GAMMA", "VEGA", "THETA", "IV", "IMPLIED_VOL", "IMPLIED_VOLATILITY"}):
        return AssetClass.option.value

    # forex: typical pairs length 6 with common currencies or contains FX
    if len(sym) == 6 and any(sym.startswith(cc) or sym.endswith(cc) for cc in ("EUR","USD","GBP","JPY","AUD","NZD","CAD","CHF")):
        return AssetClass.forex.value
    if "FX" in sym or "FX" in path.upper():
        return AssetClass.forex.value

    # crypto
    if any(x in sym for x in ("BTC","ETH","SOL","XRP","LTC","ADA")) or "CRYPTO" in path.upper():
        return AssetClass.crypto.value

    # future prefixes
    if any(sym.startswith(p) for p in ("ES","NQ","CL","GC","ZN","ZB","YM")) or "FUT" in path.upper():
        return AssetClass.future.value

    # indices: prefer path-based detection to avoid misclassifying 3-5 char index tickers as stocks
    p_up = str(path).upper()
    try:
        p_res = str(Path(path).resolve()).upper()
    except Exception:
        p_res = ""
    if any((k in p_up) or (k in p_res) for k in ("INDICES", "INDICES_PARQUET", "INDEX")):
        return AssetClass.index.value

    # option indicator (name-based)
    if "OPT" in sym or any(re.search(r"\d{3,}-[A-Z]{3}", sym) for _ in [0]):
        return AssetClass.option.value

    # etf: suffix or explicit list in cfg
    if sym.endswith("_ETF"):
        return AssetClass.etf.value

    # bond
    if "BOND" in sym or "GOV" in sym:
        return AssetClass.bond.value

    # stock heuristic: uppercase ticker <=5 chars
    if len(sym) <= 5 and sym.isalpha():
        return AssetClass.stock.value

    return AssetClass.unknown.value


def get_effective_settings(symbol: str, asset_class: str, cfg) -> EffectiveSettings:
    # Merge order: global parquet config < asset_class overrides < symbol overrides
    global_p = cfg.parquet
    nan = global_p.nan_threshold
    allow_neg = global_p.allow_negative_prices
    resample_enabled = global_p.resample_enabled
    resample_bar = global_p.resample_bar_size

    # symbol overrides
    sym_over = getattr(cfg, "symbol_overrides", {}) or {}
    s_over = sym_over.get(symbol.upper(), {})

    # apply symbol overrides
    nan = s_over.get("nan_threshold", nan)
    allow_neg = s_over.get("allow_negative_prices", allow_neg)
    resample_enabled = s_over.get("resample_enabled", resample_enabled)
    resample_bar = s_over.get("resample_bar_size", resample_bar)

    return EffectiveSettings(asset_class=asset_class, nan_threshold=nan, allow_negative_prices=allow_neg, resample_enabled=resample_enabled, resample_bar_size=resample_bar)
