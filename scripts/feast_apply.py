"""Apply local feature parquet files to a minimal Feast repo programmatically.

This PoC will:
- scan `artifacts/features/*/*/features.parquet`
- for each asset/version: ensure an `event_timestamp` column exists (add if missing)
- copy the parquet to `feast_repo/data/` and create a Feast FileSource
- create an `Entity('asset_id')` if an asset column exists, otherwise use the row index
- create a FeatureView per asset and call `FeatureStore.apply()`

If `feast` is not installed the script prints installation instructions.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

FEAST_REPO = Path("feast_repo")
FEATURES_DIR = Path("artifacts/features")
DATA_DIR = FEAST_REPO / "data"


def find_parquets():
    if not FEATURES_DIR.exists():
        return []
    pats = list(FEATURES_DIR.glob("*/*/features.parquet"))
    return pats


def prepare_parquet(src: Path, dst: Path) -> None:
    df = pd.read_parquet(src)
    if "event_timestamp" not in df.columns:
        df["event_timestamp"] = pd.Timestamp(datetime.utcnow())
    # ensure entity column exists; we'll prefer 'asset' or 'asset_id'
    if "asset" not in df.columns and "asset_id" not in df.columns and "id" not in df.columns:
        # avoid carrying over any existing pandas index columns from source parquet
        df = df.reset_index(drop=True)
        df["row_index"] = list(range(len(df)))
    dst.parent.mkdir(parents=True, exist_ok=True)
    # write without pandas index to avoid extra '__index_level_0__' column
    df.to_parquet(dst, index=False)


def sanitize_and_reorder_parquet(path: Path, ordered_cols: list[str] | None = None) -> None:
    """Ensure Parquet at `path` has no pandas index metadata and columns in the desired order.

    If `ordered_cols` is provided, only those columns (in that order) plus
    `event_timestamp` and `row_index` will be kept/ordered; otherwise we
    write with index=False to remove pandas index metadata.
    """
    import pandas as pd

    df = pd.read_parquet(path)
    if ordered_cols:
        cols = [c for c in ordered_cols if c in df.columns]
        # ensure event_timestamp at end if present
        if 'event_timestamp' in df.columns and 'event_timestamp' not in cols:
            cols.append('event_timestamp')
        if 'row_index' in df.columns and 'row_index' not in cols:
            cols.append('row_index')
        # append any remaining columns to avoid data loss
        for c in df.columns:
            if c not in cols:
                cols.append(c)
        df = df[cols]

    # write clean parquet with no pandas index
    df.to_parquet(path, index=False)


def main():
    try:
        from feast import (
            Entity,
            Feature,
            FeatureStore,
            FeatureView,
            FileSource,
            ValueType,
        )
        from feast.data_format import ParquetFormat
    except Exception:
        print("Feast is not installed. To run the PoC, install dependencies:")
        print("  pip install feast duckdb pandas pyarrow")
        print("Then re-run this script to programmatically apply FeatureViews.")
        return

    parquets = find_parquets()
    if not parquets:
        print("No feature parquet files found under artifacts/features. Run feature materialization first.")
        return

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    FEAST_REPO.mkdir(exist_ok=True)

    fs = FeatureStore(repo_path=str(FEAST_REPO))

    # group parquets by asset to create one FeatureView per asset
    assets = {}
    for p in parquets:
        try:
            asset = p.parts[2]
            version = p.parts[3]
        except Exception:
            asset = p.parent.parent.name
            version = p.parent.name
        assets.setdefault(asset, []).append((p, version))

    to_apply = []
    for asset, items in assets.items():
        # copy all versions for the asset
        for p, version in items:
            out = DATA_DIR / f"{asset}_{version}.parquet"
            prepare_parquet(p, out)

        # pick first file to infer schema
        sample_out = DATA_DIR / f"{asset}_{items[0][1]}.parquet"
        df = pd.read_parquet(sample_out)

        # determine entity column
        if "asset" in df.columns:
            ent_col = "asset"
            value_type = ValueType.STRING
        elif "asset_id" in df.columns:
            ent_col = "asset_id"
            value_type = ValueType.STRING
        elif "id" in df.columns:
            ent_col = "id"
            value_type = ValueType.INT64
        elif "row_index" in df.columns:
            ent_col = "row_index"
            value_type = ValueType.INT64
        else:
            ent_col = df.columns[0]
            value_type = ValueType.STRING

        # entity (use explicit join key)
        entity = Entity(name=f"{asset}_entity", join_keys=[ent_col], value_type=value_type)

        # features: numeric columns except entity and event_timestamp
        feature_cols = [c for c in df.columns if c not in {ent_col, "event_timestamp"}]
        schema_fields = []
        from feast.feature import Feature
        from feast.field import Field

        for c in feature_cols:
            dt = df[c].dtype
            if pd.api.types.is_integer_dtype(dt):
                vt = ValueType.INT64
            else:
                vt = ValueType.DOUBLE
            feat = Feature(name=c, dtype=vt)
            schema_fields.append(Field.from_feature(feat))

        # create a FileSource that points to the latest version file for this asset
        latest_version = items[-1][1]
        latest_file = DATA_DIR / f"{asset}_{latest_version}.parquet"
        # reorder the latest file to match FeatureView schema expectations
        ordered_cols = [f.name for f in schema_fields]
        # ensure entity and timestamp are present/ordered
        if ent_col not in ordered_cols and ent_col in df.columns:
            ordered_cols.insert(0, ent_col)
        if 'event_timestamp' in df.columns and 'event_timestamp' not in ordered_cols:
            ordered_cols.append('event_timestamp')
        sanitize_and_reorder_parquet(latest_file, ordered_cols=ordered_cols)

        # use an absolute path so ibis can read the parquet regardless of CWD
        abs_path = str((FEAST_REPO / 'data' / f"{asset}_{latest_version}.parquet").resolve())
        file_src = FileSource(path=abs_path, file_format=ParquetFormat(), timestamp_field="event_timestamp")
        fv = FeatureView(
            name=f"{asset}_fv",
            source=file_src,
            schema=schema_fields,
            entities=[entity],
            ttl=None,
            online=True,
            offline=True,
        )
        to_apply.append((entity, fv))

    # apply in batch
    ents = [e for e, _ in to_apply]
    fvs = [fv for _, fv in to_apply]
    fs.apply(ents + fvs)
    print(f"Applied {len(fvs)} FeatureViews to Feast repo at {FEAST_REPO}")

    # final sanity pass: ensure all data parquets are sanitized
    for p in DATA_DIR.glob('*.parquet'):
        try:
            sanitize_and_reorder_parquet(p)
        except Exception:
            pass


if __name__ == "__main__":
    main()
