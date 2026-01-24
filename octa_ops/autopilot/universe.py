from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Optional

import yaml

from .types import UniverseSymbol, normalize_timeframe


def _load_asset_map(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    out: Dict[str, str] = {}
    if isinstance(raw, dict):
        for k, v in raw.items():
            if k and v:
                out[str(k).strip().upper()] = str(v).strip().lower()
    return out


def _infer_session(asset_class: str) -> str:
    ac = str(asset_class or "unknown").lower()
    if ac in {"fx"}:
        return "fx_24_5"
    if ac in {"crypto"}:
        return "crypto_24_7"
    if ac in {"equity", "etf", "stock", "index"}:
        return "equities_rth"
    return "unknown"


def _normalize_asset_class(label: Optional[str]) -> str:
    v = str(label or "").strip().lower()
    if v in {"stocks", "stock", "equity"}:
        return "equity"
    if v in {"etf"}:
        return "etf"
    if v in {"forex", "fx"}:
        return "fx"
    if v in {"futures", "future"}:
        return "future"
    if v in {"crypto"}:
        return "crypto"
    if v in {"index"}:
        return "index"
    if v in {"option", "options"}:
        return "option"
    return "unknown"


_TIMEFRAME_PAT = re.compile(r"_(1D|1H|30M|15M|5M|1M)\.parquet$", re.IGNORECASE)


def discover_universe(
    *,
    raw_root: str = "raw",
    stock_dir: str = "raw/Stock_parquet",
    fx_dir: str = "raw/FX_parquet",
    crypto_dir: str = "raw/Crypto_parquet",
    futures_dir: str = "raw/Future_parquet",
    asset_map_path: str = "assets/asset_map.yaml",
    limit: int = 0,
) -> List[UniverseSymbol]:
    """Build a unified symbol list from local datasets + existing asset map.

    Preference order:
    1) parquet directories (symbol/timeframe discovered)
    2) assets/asset_map.yaml for asset_class hint

    Returns a list of UniverseSymbol with parquet_paths filled when available.
    """

    asset_map = _load_asset_map(Path(asset_map_path))

    def scan_dir(p: Path) -> Dict[str, Dict[str, str]]:
        by_sym: Dict[str, Dict[str, str]] = {}
        if not p.exists():
            return by_sym
        for fp in sorted(p.glob("*.parquet")):
            m = _TIMEFRAME_PAT.search(fp.name)
            if not m:
                continue
            tf = normalize_timeframe(m.group(1))
            sym = fp.name[: m.start()].strip().upper()
            if not sym:
                continue
            by_sym.setdefault(sym, {})[tf] = str(fp)
        return by_sym

    stock = scan_dir(Path(stock_dir))
    fx = scan_dir(Path(fx_dir))
    crypto = scan_dir(Path(crypto_dir))
    fut = scan_dir(Path(futures_dir))

    symbols: Dict[str, UniverseSymbol] = {}

    def upsert(sym: str, ac_hint: str, source: str, parquet_paths: Optional[Dict[str, str]] = None) -> None:
        sym_u = str(sym).strip().upper()
        ac = _normalize_asset_class(ac_hint)
        if sym_u in symbols:
            prev = symbols[sym_u]
            pp = dict(prev.parquet_paths or {})
            if parquet_paths:
                pp.update(parquet_paths)
            symbols[sym_u] = UniverseSymbol(
                symbol=sym_u,
                asset_class=prev.asset_class if prev.asset_class != "unknown" else ac,
                currency=prev.currency,
                session=prev.session or _infer_session(prev.asset_class if prev.asset_class != "unknown" else ac),
                source=prev.source or source,
                parquet_paths=pp,
            )
            return
        symbols[sym_u] = UniverseSymbol(
            symbol=sym_u,
            asset_class=ac,
            currency=None,
            session=_infer_session(ac),
            source=source,
            parquet_paths=dict(parquet_paths or {}),
        )

    for sym, pp in stock.items():
        upsert(sym, "equity", "parquet:stock", pp)
    for sym, pp in fx.items():
        upsert(sym, "fx", "parquet:fx", pp)
    for sym, pp in crypto.items():
        upsert(sym, "crypto", "parquet:crypto", pp)
    for sym, pp in fut.items():
        upsert(sym, "future", "parquet:future", pp)

    for sym, ac in sorted(asset_map.items()):
        upsert(sym, ac, "asset_map", None)

    out = list(symbols.values())
    out.sort(key=lambda s: s.symbol)
    if limit and limit > 0:
        out = out[: int(limit)]
    return out
