from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import pandas as pd

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from octa_training.core.state import StateRegistry


def _maybe_fix_fx_intraday_timestamp_encoding(path: Path, s: pd.Series) -> pd.Series:
    """Fix a common vendor encoding bug seen in raw/FX_parquet intraday files.

    Some FX intraday parquets have a `timestamp` column that was stored as an integer
    like YYYYMMDD (or YYYYMMDDHH...) but read back as a timezone-aware datetime where
    the integer is interpreted as nanoseconds since epoch (landing in 1970).

    We correct this *in-memory* (no on-disk mutation) so frequency inference and
    annualization do not explode.
    """

    try:
        parts_upper = {p.upper() for p in path.parts}
        is_fx = "FX_PARQUET" in parts_upper
        # Only touch intraday FX files (leave daily `_1D` untouched).
        is_intraday = path.name.upper().endswith("_1H.PARQUET")
        if not (is_fx and is_intraday):
            return s

        # If it already looks like a real timestamp series, do nothing.
        if pd.api.types.is_datetime64_any_dtype(s):
            s_min = s.min()
            s_max = s.max()
            if not (isinstance(s_min, pd.Timestamp) and isinstance(s_max, pd.Timestamp)):
                return s

            # If it isn't clearly the 1970-ns-encoding pathology, do nothing.
            if not (s_min.year == 1970 and s_max.year == 1970):
                return s

            raw = s.astype("int64")
        elif pd.api.types.is_integer_dtype(s) or pd.api.types.is_float_dtype(s):
            raw = pd.to_numeric(s, errors="coerce").dropna().astype("int64")
            if raw.empty:
                return s
        else:
            return s

        # Heuristic: raw should look like YYYYMMDD (or YYYYMMDDHH/MM/SS).
        raw_max = int(raw.max())
        digits = len(str(raw_max))
        fmt_by_digits = {
            8: "%Y%m%d",
            10: "%Y%m%d%H",
            12: "%Y%m%d%H%M",
            14: "%Y%m%d%H%M%S",
        }
        fmt = fmt_by_digits.get(digits)
        if not fmt:
            return s

        # Additional sanity: date-like bounds.
        if not (19000101 <= raw.max() <= 21000101_235959):
            return s

        raw_str = raw.astype(str).str.zfill(digits)
        parsed = pd.to_datetime(raw_str, format=fmt, utc=True, errors="coerce")
        if parsed.isna().any():
            return s

        # Reindex parsed back to original index if we took a dropna path.
        if len(parsed) != len(s):
            out = s.copy()
            out.loc[raw.index] = parsed
            return out
        return parsed
    except Exception:
        return s


@dataclass
class ParquetFileInfo:
    symbol: str
    path: Path
    mtime: float
    size: int
    sha256: Optional[str]


def sanitize_symbol(name: str) -> str:
    # Keep only A-Z0-9_:-, uppercase, replace others with _
    out = []
    for ch in name.upper():
        if ch.isalnum() or ch in "_:-":
            out.append(ch)
        else:
            out.append("_")
    return "".join(out)


def compute_sha256(path: Path, chunk_size: int = 4 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _is_ignored(path: Path, ignore_dirs: List[str]) -> bool:
    parts = [p.upper() for p in path.parts]
    for ig in ignore_dirs:
        if ig.upper() in parts:
            return True
    return False


def discover_parquets(raw_dir: Path, state: Optional[StateRegistry] = None, ignore_dirs: Optional[List[str]] = None) -> List[ParquetFileInfo]:
    if ignore_dirs is None:
        ignore_dirs = ["PKL"]
    raw_dir = Path(raw_dir)
    found: List[ParquetFileInfo] = []
    for p in raw_dir.rglob("*.parquet"):
        if _is_ignored(p, ignore_dirs):
            continue
        try:
            stat = p.stat()
        except FileNotFoundError:
            continue
        mtime = stat.st_mtime
        size = stat.st_size
        symbol = sanitize_symbol(p.stem)

        sha: Optional[str] = None
        # fast-path using state if available and size+mtime match
        if state:
            s = state.get_symbol_state(symbol) or {}
            stored_mtime = s.get("last_parquet_mtime")
            stored_size = s.get("last_parquet_size")
            stored_hash = s.get("last_seen_parquet_hash")
            if stored_hash and stored_mtime == mtime and stored_size == size:
                # stored_hash may be JSON or plain; ensure plain hex
                try:
                    maybe = json.loads(stored_hash)
                    sha = maybe.get("hash")
                except Exception:
                    sha = stored_hash
        if not sha:
            sha = compute_sha256(p)
        found.append(ParquetFileInfo(symbol=symbol, path=p, mtime=mtime, size=size, sha256=sha))
    return found


def _find_time_column(columns: List[str]) -> Optional[str]:
    candidates = ["timestamp", "datetime", "date", "time"]
    cols_lower = [c.lower() for c in columns]
    for cand in candidates:
        if cand in cols_lower:
            return columns[cols_lower.index(cand)]
    return None


def load_parquet(path: Path, *, nan_threshold: float = 0.2, allow_negative_prices: bool = False, resample_enabled: bool = False, resample_bar_size: str = "1D") -> pd.DataFrame:
    # Load via pyarrow if available (faster + lower peak memory with projection), fallback to pandas.
    # Keep return type as pandas.DataFrame for compatibility with the rest of the pipeline.
    def _projected_columns_from_parquet_schema(pq_mod) -> tuple[list[str] | None, str | None]:
        try:
            pf = pq_mod.ParquetFile(str(path))
            schema_names = [str(n) for n in pf.schema.names]
        except Exception:
            return None, None

        cols_lower = [c.lower() for c in schema_names]

        # Always require close.
        if "close" not in cols_lower:
            return None, None

        # Determine time column from schema without loading the entire table.
        time_col = _find_time_column(cols_lower)
        if time_col is None:
            return None, None

        # Map lower-case time column back to original case-sensitive schema name.
        time_col_orig = schema_names[cols_lower.index(time_col)]

        # Columns that might be needed for sanity checks / resampling / inspection.
        optional = [
            "open",
            "high",
            "low",
            "volume",
            "delisted",
            "is_delisted",
            "delisting_date",
            "end_date",
        ]
        needed_lower = [time_col, "close"] + [c for c in optional if c in cols_lower]
        needed_orig = [schema_names[cols_lower.index(c)] for c in needed_lower]
        return needed_orig, time_col_orig

    try:
        import pyarrow.parquet as pq

        projected_cols, _ = _projected_columns_from_parquet_schema(pq)
        # If we cannot safely determine projection, fall back to full read.
        table = pq.read_table(
            str(path),
            columns=projected_cols,
            use_threads=True,
            memory_map=True,
        )
        try:
            df = table.to_pandas(self_destruct=True, split_blocks=True)
        except TypeError:
            # Older pyarrow: no self_destruct/split_blocks.
            df = table.to_pandas()
    except Exception:
        df = pd.read_parquet(path)

    # normalize columns
    df.columns = [str(c).lower() for c in df.columns]

    # require close
    if "close" not in df.columns:
        raise ValueError(f"Parquet file {path} missing required 'close' column")

    # detect time column
    time_col = _find_time_column(list(df.columns))
    if time_col:
        df[time_col] = _maybe_fix_fx_intraday_timestamp_encoding(path, df[time_col])
        df[time_col] = pd.to_datetime(df[time_col], utc=True, errors="coerce")
        df = df.set_index(time_col)
        df = df.sort_index()
        df = df[~df.index.duplicated(keep="first")]
    else:
        raise ValueError(f"Parquet file {path} missing time column (timestamp/datetime/date/time)")

    # minimal NaN check on close
    nan_frac = float(df["close"].isna().mean())
    if nan_frac > nan_threshold:
        raise ValueError(f"Parquet file {path} close column NaN fraction {nan_frac:.3f} exceeds threshold {nan_threshold}")

    # price sanity
    price_cols = [c for c in ("open", "high", "low", "close") if c in df.columns]
    if price_cols:
        if not allow_negative_prices:
            bad_mask = (df[price_cols] <= 0).any(axis=1)
            bad_count = int(bad_mask.sum())
            if bad_count:
                # Some vendors encode missing OHLC as 0.0 on a tiny number of rows.
                # Fail closed for widespread issues; for rare rows we can sanitize.
                # Strategy:
                # - If close is <=0 on the bad rows (often all-zero OHLC), drop those rows (rare-only).
                # - If close is positive but open/high/low are <=0, impute from close and restore invariants.
                frac = bad_count / max(1, len(df))
                if frac <= 0.005:
                    df = df.copy()
                    if "close" in df.columns:
                        drop_mask = bad_mask & (df["close"] <= 0)
                        if int(drop_mask.sum()):
                            df = df.loc[~drop_mask]

                    if len(df) == 0:
                        raise ValueError(f"Parquet file {path} contains non-positive prices in columns {price_cols}")

                    bad_mask = (df[price_cols] <= 0).any(axis=1)
                    if int(bad_mask.sum()):
                        if "close" not in df.columns or not bool((df.loc[bad_mask, "close"] > 0).all()):
                            raise ValueError(f"Parquet file {path} contains non-positive prices in columns {price_cols}")

                        for c in [c for c in ("open", "high", "low") if c in df.columns]:
                            fix = bad_mask & (df[c] <= 0)
                            if int(fix.sum()):
                                df.loc[fix, c] = df.loc[fix, "close"]

                        if "high" in df.columns:
                            cols = [c for c in ("high", "open", "close") if c in df.columns]
                            df.loc[bad_mask, "high"] = df.loc[bad_mask, cols].max(axis=1)
                        if "low" in df.columns:
                            cols = [c for c in ("low", "open", "close") if c in df.columns]
                            df.loc[bad_mask, "low"] = df.loc[bad_mask, cols].min(axis=1)

                    still_bad = (df[price_cols] <= 0).any(axis=None)
                    if still_bad:
                        raise ValueError(f"Parquet file {path} contains non-positive prices in columns {price_cols}")
                else:
                    raise ValueError(f"Parquet file {path} contains non-positive prices in columns {price_cols}")

    # high/low checks
    if "high" in df.columns and "open" in df.columns and "close" in df.columns:
        max_oc = df[["open", "close"]].max(axis=1)
        bad_mask = df["high"] < max_oc
        bad = int(bad_mask.sum())
        if bad:
            frac = bad / max(1, len(df))
            if frac <= 0.005:
                df = df.copy()
                df.loc[bad_mask, "high"] = max_oc.loc[bad_mask]
            else:
                raise ValueError(f"Parquet file {path} has {bad} rows where high < max(open,close)")
    if "low" in df.columns and "open" in df.columns and "close" in df.columns:
        min_oc = df[["open", "close"]].min(axis=1)
        bad_mask = df["low"] > min_oc
        bad = int(bad_mask.sum())
        if bad:
            frac = bad / max(1, len(df))
            if frac <= 0.005:
                df = df.copy()
                df.loc[bad_mask, "low"] = min_oc.loc[bad_mask]
            else:
                raise ValueError(f"Parquet file {path} has {bad} rows where low > min(open,close)")

    # resample if requested and intraday detected
    if resample_enabled and resample_bar_size:
        # detect median spacing
        if len(df.index) >= 2:
            deltas = df.index.to_series().diff().dropna().astype('timedelta64[s]').values
            if len(deltas) and deltas.mean() < 24 * 3600:
                # intraday -> resample
                ohlc = {}
                if "open" in df.columns:
                    ohlc["open"] = "first"
                if "high" in df.columns:
                    ohlc["high"] = "max"
                if "low" in df.columns:
                    ohlc["low"] = "min"
                if "close" in df.columns:
                    ohlc["close"] = "last"
                agg = df.resample(resample_bar_size).agg(ohlc)
                # volumes
                if "volume" in df.columns:
                    agg["volume"] = df["volume"].resample(resample_bar_size).sum()
                df = agg.dropna(how="all")

    return df


def inspect_parquet(path: Path, cfg: Optional[dict] = None) -> dict:
    cfg = cfg or {}
    info = {"path": str(path)}
    try:
        df = load_parquet(path, nan_threshold=cfg.get("nan_threshold", 0.2), allow_negative_prices=cfg.get("allow_negative_prices", False), resample_enabled=cfg.get("resample_enabled", False), resample_bar_size=cfg.get("resample_bar_size", "1D"))
        info["rows"] = len(df)
        info["columns"] = list(df.columns)
        info["start"] = str(df.index.min()) if len(df.index) else None
        info["end"] = str(df.index.max()) if len(df.index) else None
        info["nan_frac_close"] = float(df["close"].isna().mean())
        # detect delisting metadata if present in dataframe columns
        info["delisted"] = False
        info["delisting_date"] = None
        # try common delisting column names
        for col in ("delisted", "is_delisted", "delisting_date", "end_date"):
            if col in df.columns:
                if col in ("delisted", "is_delisted"):
                    try:
                        info["delisted"] = bool(df[col].iloc[-1])
                    except Exception:
                        info["delisted"] = bool(df[col].dropna().iloc[0]) if len(df[col].dropna()) else False
                else:
                    # parse last non-null delisting date
                    try:
                        v = df[col].dropna()
                        if len(v):
                            info["delisting_date"] = str(pd.to_datetime(v.iloc[-1], utc=True, errors="coerce"))
                            info["delisted"] = True
                    except Exception:
                        pass
                break
        # also check Parquet file-level metadata (pyarrow) for delisting keys
        try:
            import pyarrow.parquet as pq

            md = pq.read_metadata(str(path)).metadata
            if md:
                # keys are bytes
                if b'delisted' in md and not info.get('delisted'):
                    try:
                        v = md[b'delisted'].decode()
                        info['delisted'] = bool(int(v)) if v.isdigit() else v.lower() in ('1', 'true', 'yes')
                    except Exception:
                        info['delisted'] = True
                if b'delisting_date' in md and not info.get('delisting_date'):
                    try:
                        info['delisting_date'] = md[b'delisting_date'].decode()
                        info['delisted'] = True
                    except Exception:
                        pass
        except Exception:
            pass
        info["ok"] = True
    except Exception as e:
        info["ok"] = False
        info["error"] = str(e)
    return info
