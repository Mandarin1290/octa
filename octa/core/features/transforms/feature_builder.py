from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

import pandas as pd

from octa.core.data.sources.altdata.edgar_connector import (
    fetch_edgar_filings,
    filings_to_events,
)
from octa.core.data.sources.altdata.fred_connector import fetch_fred_series, fred_to_wide
from octa.core.data.sources.altdata.time_sync import (
    asof_join,
    derive_timewindow_from_bars,
    validate_no_future_leakage,
)
from octa.core.data.sources.altdata.weights import apply_quality_adjustments, normalize_weights
from octa.core.data.storage.altdata.storage import StoragePaths, init_duckdb, make_paths, write_meta_json
from octa.core.features.transforms.filing_features import build_filing_features
from octa.core.features.transforms.macro_features import build_macro_features


@dataclass
class AltDataBuildResult:
    features_df: pd.DataFrame
    meta: Dict[str, Any]


def _hash_obj(o: Any) -> str:
    try:
        raw = json.dumps(o, sort_keys=True, default=str).encode("utf-8")
    except Exception:
        raw = str(o).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def build_altdata_features(
    *,
    bars_df: pd.DataFrame,
    symbol: str,
    altdat_cfg: Dict[str, Any],
    tz: str = "UTC",
) -> AltDataBuildResult:
    """Build AltData features aligned to bars_df.index.

    Fail-closed for AltData: returns empty DF when disabled/unavailable.
    """

    enabled = bool(altdat_cfg.get("enabled", False)) or str(os.getenv("OKTA_ALTDATA_ENABLED", "")).strip() == "1"
    if not enabled:
        return AltDataBuildResult(features_df=pd.DataFrame(index=bars_df.index), meta={"enabled": False, "status": "DISABLED"})

    strict = bool(altdat_cfg.get("strict_mode", False)) or str(os.getenv("OKTA_ALTDATA_STRICT", "")).strip() == "1"

    storage_root = None
    try:
        storage_root = (altdat_cfg.get("storage") or {}).get("root")
    except Exception:
        storage_root = None
    paths: StoragePaths = make_paths(cfg_root=storage_root)
    ok_db, db_err = init_duckdb(paths)

    tw = derive_timewindow_from_bars(bars_df=bars_df, symbol=symbol, tz=tz)
    timeframe = tw.timeframe

    sources = altdat_cfg.get("sources") or {}
    weights_base = (altdat_cfg.get("weights") or {}).get("base") or {}
    weights_quality = (altdat_cfg.get("weights") or {}).get("quality") or {}
    tol_map = (altdat_cfg.get("asof") or {}).get("tolerance_seconds") or {}
    tol_sec = int(tol_map.get(timeframe, 0) or 0)
    tolerance = pd.Timedelta(seconds=tol_sec) if tol_sec > 0 else None

    meta: Dict[str, Any] = {
        "enabled": True,
        "strict": bool(strict),
        "symbol": symbol,
        "timeframe": timeframe,
        "start_ts": str(tw.start_ts),
        "end_ts": str(tw.end_ts),
        "duckdb_ok": bool(ok_db),
        "duckdb_error": db_err,
        "sources": {},
        "coverage": {},
        "weights": {},
        "leakage": {"detected": False, "rows": 0},
    }

    feat_parts = []

    def _duckdb_try_insert(table: str, df: pd.DataFrame) -> None:
        if not ok_db or df is None or df.empty:
            return
        try:
            import duckdb  # type: ignore

            con = duckdb.connect(str(paths.db_path))
            con.register("_tmp", df)
            con.execute(f"INSERT INTO {table} SELECT * FROM _tmp")
            con.unregister("_tmp")
            con.close()
        except Exception:
            # Best-effort only
            return

    def _duckdb_upsert_source(source: str, ok: bool, err: Optional[str]) -> None:
        if not ok_db:
            return
        try:
            import duckdb  # type: ignore

            con = duckdb.connect(str(paths.db_path))
            now = pd.Timestamp(datetime.utcnow(), tz="UTC")
            if ok:
                con.execute(
                    "INSERT INTO meta_sources(source, version, last_ok, last_error, rate_limit_state) VALUES (?, ?, ?, ?, ?) ",
                    [source, "v1", now.to_pydatetime(), None, None],
                )
            else:
                con.execute(
                    "INSERT INTO meta_sources(source, version, last_ok, last_error, rate_limit_state) VALUES (?, ?, ?, ?, ?) ",
                    [source, "v1", None, str(err) if err else "error", None],
                )
            con.close()
        except Exception:
            return

    # FRED / Macro
    fred_cfg = sources.get("fred") or {}
    if bool(fred_cfg.get("enabled", False)):
        api_key_env = str(fred_cfg.get("api_key_env", "FRED_API_KEY") or "FRED_API_KEY")
        api_key = os.getenv(api_key_env)
        series = fred_cfg.get("series") or []
        try:
            series = [str(s).strip() for s in series if str(s).strip()]
        except Exception:
            series = []
        if not api_key:
            meta["sources"]["fred"] = {"ok": False, "error": "missing_api_key", "series": series}
            _duckdb_upsert_source("fred", False, "missing_api_key")
        else:
            results = [fetch_fred_series(series_id=s, start_ts=tw.start_ts, end_ts=tw.end_ts, api_key=api_key) for s in series]
            ok_any = any(r.ok for r in results)
            meta["sources"]["fred"] = {
                "ok": bool(ok_any),
                "series": series,
                "errors": [r.error for r in results if (not r.ok and r.error)],
            }
            _duckdb_upsert_source("fred", bool(ok_any), ";".join([str(r.error) for r in results if r.error]) if not ok_any else None)
            # Persist raw series observations
            for r in results:
                if r.ok and r.df is not None and not r.df.empty:
                    try:
                        ins = r.df.copy()
                        ins.insert(0, "series_id", r.series_id)
                        # Ensure column order matches schema
                        ins = ins[["series_id", "ts", "value", "as_of", "source_time", "ingested_at"]]
                        _duckdb_try_insert("fred_series", ins)
                    except Exception:
                        pass
            wide = fred_to_wide(series=results)
            macro = build_macro_features(wide)
            if not macro.empty:
                macro = macro.reset_index().rename(columns={"index": "ts"})
                j = asof_join(bars_df=bars_df, alt_df=macro, on="ts", tolerance=tolerance)
                leak = validate_no_future_leakage(merged_df=j, bar_index=bars_df.index, alt_ts_col="ts", strict=strict)
                if leak.any():
                    meta["leakage"]["detected"] = True
                    meta["leakage"]["rows"] = int(leak.sum())
                    if strict:
                        j.loc[leak.values, macro.columns] = pd.NA
                # drop ts marker
                j = j.drop(columns=["ts"], errors="ignore")
                j = j.add_prefix("altdat_macro_")
                feat_parts.append(j)
                meta["coverage"]["macro"] = float(j.notna().any(axis=1).mean())

    # EDGAR
    edgar_cfg = sources.get("edgar") or {}
    if bool(edgar_cfg.get("enabled", False)):
        forms = edgar_cfg.get("forms") or ["10-K", "10-Q", "8-K"]
        try:
            forms = [str(x).strip() for x in forms if str(x).strip()]
        except Exception:
            forms = ["10-K", "10-Q", "8-K"]

        r = fetch_edgar_filings(ticker=symbol, forms=forms, start_ts=tw.start_ts, end_ts=tw.end_ts)
        edgar_meta = {"ok": bool(r.ok), "error": (str(r.error)[:200] if r.error else None), "forms": forms}
        if isinstance(r.meta, dict):
            edgar_meta.update(r.meta)
        meta["sources"]["edgar"] = edgar_meta
        _duckdb_upsert_source("edgar", bool(r.ok), r.error)
        # Persist raw filings if any
        try:
            if r.ok and r.df is not None and not r.df.empty:
                _duckdb_try_insert("edgar_filings", r.df)
        except Exception:
            pass
        if r.ok and r.df is not None and not r.df.empty:
            events = filings_to_events(r.df)
            ff = build_filing_features(events, ticker=symbol)
            if not ff.empty:
                ff = ff.reset_index().rename(columns={"index": "ts"})
                j = asof_join(bars_df=bars_df, alt_df=ff, on="ts", tolerance=tolerance)
                leak = validate_no_future_leakage(merged_df=j, bar_index=bars_df.index, alt_ts_col="ts", strict=strict)
                if leak.any():
                    meta["leakage"]["detected"] = True
                    meta["leakage"]["rows"] = int(meta["leakage"]["rows"]) + int(leak.sum())
                    if strict:
                        j.loc[leak.values, ff.columns] = pd.NA
                j = j.drop(columns=["ts"], errors="ignore")
                j = j.add_prefix("altdat_edgar_")
                feat_parts.append(j)
                meta["coverage"]["edgar"] = float(j.notna().any(axis=1).mean())

    # Weights manifest (explainable)
    enabled_map = {
        "edgar": bool((sources.get("edgar") or {}).get("enabled", False)),
        "macro": bool((sources.get("fred") or {}).get("enabled", False)),
        "news": bool((sources.get("gdelt") or {}).get("enabled", False)),
        "satellite": bool((sources.get("satellite") or {}).get("enabled", False)),
        "market": bool((sources.get("market") or {}).get("enabled", False)),
    }
    w0 = normalize_weights(weights_base, enabled=enabled_map)
    w1, reasons = apply_quality_adjustments(
        weights=w0,
        coverage=meta.get("coverage") or {},
        min_coverage=float(weights_quality.get("min_coverage", 0.5) or 0.5),
    )
    meta["weights"] = {"final": w1, "reasons": reasons}

    # Merge features
    if feat_parts:
        feats = pd.concat(feat_parts, axis=1)
        feats = feats.reindex(bars_df.index)
    else:
        feats = pd.DataFrame(index=bars_df.index)

    # Persist metadata (best effort)
    run_id = f"altdat_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}_{symbol}_{timeframe}"
    meta["run_id"] = run_id
    meta["config_hash"] = _hash_obj(altdat_cfg)

    # meta_runs (best effort)
    if ok_db:
        try:
            import duckdb  # type: ignore

            con = duckdb.connect(str(paths.db_path))
            con.execute(
                "INSERT INTO meta_runs(run_id, created_at, git_hash, config_hash, notes) VALUES (?, ?, ?, ?, ?)",
                [run_id, datetime.utcnow(), None, meta.get("config_hash"), None],
            )
            con.close()
        except Exception:
            pass

    try:
        write_meta_json(paths, f"run_{run_id}.json", meta)
        write_meta_json(paths, f"features_{symbol}_{timeframe}_{run_id}.json", {"columns": list(feats.columns), "shape": list(feats.shape)})
    except Exception:
        pass

    # feature_store persist (optional; can be heavy, so best-effort)
    try:
        persist = True
        max_rows = None
        st = altdat_cfg.get("storage") or {}
        if isinstance(st, dict):
            persist = bool(st.get("persist_feature_store", True))
            max_rows = st.get("feature_store_max_rows")
        if persist and ok_db and feats is not None and not feats.empty:
            long = feats.copy()
            long = long.reset_index().rename(columns={"index": "ts"})
            long = long.melt(id_vars=["ts"], var_name="feature_name", value_name="value")
            long = long.dropna(subset=["value", "ts"])
            long.insert(0, "symbol", symbol)
            long.insert(1, "timeframe", timeframe)
            # Conservative as_of equals ts after as-of join.
            long["as_of"] = pd.to_datetime(long["ts"], utc=True, errors="coerce")
            long["provenance_json"] = json.dumps({"run_id": run_id}, ensure_ascii=False)
            long = long[["symbol", "timeframe", "ts", "feature_name", "value", "as_of", "provenance_json"]]
            if isinstance(max_rows, int) and max_rows > 0 and len(long) > max_rows:
                long = long.iloc[:max_rows]
            _duckdb_try_insert("feature_store", long)
    except Exception:
        pass

    return AltDataBuildResult(features_df=feats, meta=meta)
