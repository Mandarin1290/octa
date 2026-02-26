from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .ohlcv import OHLCVBar, OHLCVProvider, Timeframe

_TIMEFRAME_ALIASES: dict[str, Timeframe] = {
    "1m": "1M",
    "1min": "1M",
    "5m": "5M",
    "5min": "5M",
    "30m": "30M",
    "30min": "30M",
    "1h": "1H",
    "1hour": "1H",
    "1d": "1D",
    "1day": "1D",
}

_REQUIRED_TIMEFRAMES: tuple[Timeframe, ...] = ("1D", "30M", "1H", "5M", "1M")
_CATEGORY_HINTS: dict[str, str] = {
    "fx_parquet": "FX",
    "indices_parquet": "INDICES",
    "stock_parquet": "EQUITY",
    "crypto_parquet": "CRYPTO",
    "etf_parquet": "ETF",
}


@dataclass
class ParquetOHLCVProvider(OHLCVProvider):
    root: Path
    layout: str | None = None
    _index: dict[tuple[str, Timeframe], list[Path]] = field(default_factory=dict, init=False)
    _cache: dict[tuple[str, Timeframe], list[OHLCVBar]] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        self.root = Path(self.root)
        if not self.root.exists():
            raise FileNotFoundError(f"Parquet root not found: {self.root}")
        self._index = self._build_index()

    def list_symbols(self) -> list[str]:
        symbols = sorted({symbol for symbol, _ in self._index})
        return symbols

    def has_timeframe(self, symbol: str, timeframe: str) -> bool:
        tf = _normalize_timeframe(timeframe)
        return (symbol, tf) in self._index

    def get_ohlcv(
        self,
        symbol: str,
        timeframe: Timeframe,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
    ) -> Sequence[OHLCVBar]:
        tf = _normalize_timeframe(timeframe)
        cache_key = (symbol, tf)
        if cache_key not in self._cache:
            files = self._index.get(cache_key, [])
            if files:
                bars = _load_parquet_files(files, limit=limit)
            else:
                bars = []
            bars = _filter_bars(bars, start, end, limit)
            self._cache[cache_key] = bars
        else:
            bars = _filter_bars(self._cache[cache_key], start, end, limit)
        return bars

    def _build_index(self) -> dict[tuple[str, Timeframe], list[Path]]:
        index: dict[tuple[str, Timeframe], list[Path]] = {}
        for path in self.root.rglob("*.parquet"):
            symbol, tf = _infer_symbol_timeframe(path, self.root)
            if symbol is None or tf is None:
                continue
            index.setdefault((symbol, tf), []).append(path)
        return index


def find_raw_root() -> Path:
    candidates = [Path("raw"), Path("data/raw"), Path("datasets/raw")]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError("No raw data root found (searched raw/, data/raw/, datasets/raw/)")


def pick_3_complete_symbols(provider: ParquetOHLCVProvider) -> list[str]:
    symbols = provider.list_symbols()
    complete: list[str] = []
    for symbol in symbols:
        if all(provider.has_timeframe(symbol, tf) for tf in _REQUIRED_TIMEFRAMES):
            complete.append(symbol)
        if len(complete) == 3:
            return complete
    counts: dict[str, int] = {tf: 0 for tf in _REQUIRED_TIMEFRAMES}
    for symbol in symbols:
        for tf in _REQUIRED_TIMEFRAMES:
            if provider.has_timeframe(symbol, tf):
                counts[tf] += 1
    raise RuntimeError(f"Insufficient symbols with full coverage. Counts: {counts}")


def pick_5_diverse_complete_symbols(provider: ParquetOHLCVProvider) -> list[str]:
    complete = [symbol for symbol in provider.list_symbols() if _has_full_coverage(provider, symbol)]
    if len(complete) < 5:
        raise RuntimeError(
            f"Insufficient symbols with full coverage. Needed 5, found {len(complete)}."
        )

    symbol_categories = _infer_symbol_categories(provider, complete)
    categories = sorted({category for category in symbol_categories.values() if category != "UNKNOWN"})
    chosen: list[str] = []

    for category in categories:
        for symbol in complete:
            if symbol in chosen:
                continue
            if symbol_categories.get(symbol) == category:
                chosen.append(symbol)
                break

    for symbol in complete:
        if symbol in chosen:
            continue
        chosen.append(symbol)
        if len(chosen) == 5:
            return chosen

    raise RuntimeError("Unable to select 5 symbols with full coverage.")


def infer_symbol_categories(
    provider: ParquetOHLCVProvider, symbols: Sequence[str]
) -> dict[str, str]:
    return _infer_symbol_categories(provider, symbols)


def _normalize_timeframe(timeframe: str) -> Timeframe:
    key = timeframe.lower()
    if key in _TIMEFRAME_ALIASES:
        return _TIMEFRAME_ALIASES[key]
    if timeframe in _REQUIRED_TIMEFRAMES:
        return timeframe  # type: ignore[return-value]
    return timeframe  # type: ignore[return-value]


def _has_full_coverage(provider: ParquetOHLCVProvider, symbol: str) -> bool:
    return all(provider.has_timeframe(symbol, tf) for tf in _REQUIRED_TIMEFRAMES)


def _infer_symbol_timeframe(path: Path, root: Path) -> tuple[str | None, Timeframe | None]:
    parts = path.relative_to(root).parts
    stem = path.stem

    if len(parts) >= 2 and parts[-2] in _REQUIRED_TIMEFRAMES:
        symbol = Path(parts[-1]).stem
        return symbol, _normalize_timeframe(parts[-2])

    if len(parts) >= 3 and parts[-3] in _REQUIRED_TIMEFRAMES:
        symbol = parts[-2]
        return symbol, _normalize_timeframe(parts[-3])

    stem_tf = _normalize_timeframe(stem)
    if len(parts) >= 2 and stem_tf in _REQUIRED_TIMEFRAMES:
        symbol = parts[-2]
        return symbol, stem_tf

    match = re.match(r"(.+)_([A-Za-z0-9]+)$", stem)
    if match:
        symbol = match.group(1)
        tf = match.group(2)
        normalized = _normalize_timeframe(tf)
        if normalized in _REQUIRED_TIMEFRAMES:
            return symbol, normalized

    return None, None


def _infer_symbol_categories(
    provider: ParquetOHLCVProvider, symbols: Sequence[str]
) -> dict[str, str]:
    categories: dict[str, str] = {}
    symbol_set = set(symbols)
    for (symbol, _), paths in provider._index.items():
        if symbol not in symbol_set:
            continue
        for path in paths:
            parts = path.relative_to(provider.root).parts
            if not parts:
                continue
            key = parts[0].lower()
            label = _CATEGORY_HINTS.get(key, "UNKNOWN")
            if label != "UNKNOWN":
                categories[symbol] = label
                break
        if symbol not in categories:
            categories[symbol] = "UNKNOWN"
    return categories


def _load_parquet_files(paths: Sequence[Path], limit: int | None = None) -> list[OHLCVBar]:
    if not paths:
        return []
    frames: list[Mapping[str, Iterable]] = []
    for path in sorted(paths):
        frame = _read_parquet(path, limit=limit)
        if frame:
            frames.append(frame)
    bars: list[OHLCVBar] = []
    for frame in frames:
        bars.extend(_bars_from_frame(frame))
    bars.sort(key=lambda bar: bar.ts)
    return bars


def _read_parquet(path: Path, limit: int | None = None) -> Mapping[str, Iterable] | None:
    if limit is not None:
        try:
            import pyarrow.parquet as pq  # type: ignore

            parquet_file = pq.ParquetFile(path)
            row_groups: list[int] = []
            rows = 0
            for idx in reversed(range(parquet_file.num_row_groups)):
                row_groups.append(idx)
                rows += parquet_file.metadata.row_group(idx).num_rows
                if rows >= limit:
                    break
            table = parquet_file.read_row_groups(sorted(row_groups))
            if table.num_rows > limit:
                table = table.slice(table.num_rows - limit)
            return {"table": table}
        except Exception:
            pass
    try:
        import pandas as pd  # type: ignore

        df = pd.read_parquet(path)
        if limit is not None:
            df = df.tail(limit)
        return {
            "df": df,
        }
    except Exception:
        try:
            import pyarrow.parquet as pq  # type: ignore

            table = pq.read_table(path)
            return {"table": table}
        except Exception:
            return None


def _bars_from_frame(frame: Mapping[str, Any]) -> list[OHLCVBar]:
    if "df" in frame:
        df = frame["df"]
        return _bars_from_dataframe(df)
    if "table" in frame:
        table = frame["table"]
        data = table.to_pydict()
        return _bars_from_columns(data)
    return []


def _bars_from_dataframe(df: Any) -> list[OHLCVBar]:
    df = df.copy()
    ts_col = _find_ts_column(df.columns)
    if ts_col is None and getattr(df, "index", None) is not None:
        df = df.reset_index()
        ts_col = _find_ts_column(df.columns)
    if ts_col is None:
        return []
    df[ts_col] = df[ts_col].apply(_to_datetime)
    df = df.sort_values(ts_col)
    columns = {col.lower(): col for col in df.columns}
    volume_col = columns.get("volume")
    bars: list[OHLCVBar] = []
    for _, row in df.iterrows():
        ts_value = row[ts_col]
        if ts_value is None:
            continue
        open_value = _safe_float(row[columns["open"]])
        high_value = _safe_float(row[columns["high"]])
        low_value = _safe_float(row[columns["low"]])
        close_value = _safe_float(row[columns["close"]])
        if open_value is None or high_value is None or low_value is None or close_value is None:
            continue
        bars.append(
            OHLCVBar(
                ts=ts_value,
                open=open_value,
                high=high_value,
                low=low_value,
                close=close_value,
                volume=_safe_volume(row[volume_col]) if volume_col else 0.0,
            )
        )
    return bars


def _bars_from_columns(columns: Mapping[str, Sequence[Any]]) -> list[OHLCVBar]:
    columns_map = {name.lower(): name for name in columns}
    ts_col = _find_ts_column(columns_map.keys())
    if ts_col is None:
        return []
    ts_name = columns_map[ts_col]
    volume_name = columns_map.get("volume")
    bars: list[OHLCVBar] = []
    for idx, ts in enumerate(columns[ts_name]):
        if ts is None:
            continue
        open_value = _safe_float(columns[columns_map["open"]][idx])
        high_value = _safe_float(columns[columns_map["high"]][idx])
        low_value = _safe_float(columns[columns_map["low"]][idx])
        close_value = _safe_float(columns[columns_map["close"]][idx])
        if open_value is None or high_value is None or low_value is None or close_value is None:
            continue
        bars.append(
            OHLCVBar(
                ts=_to_datetime(ts),
                open=open_value,
                high=high_value,
                low=low_value,
                close=close_value,
                volume=_safe_volume(columns[volume_name][idx]) if volume_name else 0.0,
            )
        )
    return bars


def _find_ts_column(columns: Iterable[str]) -> str | None:
    for name in columns:
        lower = name.lower()
        if lower in {"ts", "timestamp", "datetime", "date", "time"}:
            return name
    return None


def _to_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    try:
        import pandas as pd  # type: ignore

        return pd.to_datetime(value).to_pydatetime()
    except Exception:
        return datetime.fromtimestamp(float(value))


def _filter_bars(
    bars: Sequence[OHLCVBar],
    start: datetime | None,
    end: datetime | None,
    limit: int | None,
) -> list[OHLCVBar]:
    filtered = list(bars)
    if start is not None:
        filtered = [bar for bar in filtered if bar.ts >= start]
    if end is not None:
        filtered = [bar for bar in filtered if bar.ts <= end]
    if limit is not None:
        filtered = filtered[-limit:]
    return filtered


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        numeric = float(value)
        if math.isnan(numeric):
            return None
        return numeric
    except Exception:
        return None


def _safe_volume(value: Any) -> float:
    numeric = _safe_float(value)
    return numeric if numeric is not None else 0.0
