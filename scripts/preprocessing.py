from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd


def build_feature_spec(df: pd.DataFrame, target: str | None = None) -> Dict:
    cols = [c for c in df.columns if c != target]
    features = []
    for c in cols:
        dtype = str(df[c].dtype)
        is_cat = isinstance(df[c].dtype, pd.CategoricalDtype) or df[c].dtype == object
        feat = {"name": c, "dtype": dtype, "is_categorical": bool(is_cat)}
        if is_cat:
            # record categories (as strings)
            cats = pd.Series(df[c].dropna().unique()).astype(str).tolist()
            feat["categories"] = cats
        features.append(feat)
    return {"features": features, "target": target}


def save_spec(spec: Dict, name: str = "default") -> Path:
    p = Path("artifacts/feature_specs")
    p.mkdir(parents=True, exist_ok=True)
    out = p / f"{name}.json"
    out.write_text(json.dumps(spec, indent=2))
    return out


def load_spec(name: str = "default") -> Dict:
    p = Path("artifacts/feature_specs") / f"{name}.json"
    if not p.exists():
        raise FileNotFoundError(p)
    return json.loads(p.read_text())


def preprocess_df(df: pd.DataFrame, target: str | None = None, spec_name: str = "default") -> Tuple[pd.DataFrame, pd.Series, Dict]:
    # build spec from raw df
    spec = build_feature_spec(df, target=target)
    feature_names: List[str] = [f["name"] for f in spec["features"]]

    X = df[feature_names].copy()
    y = df[target].copy() if target and target in df.columns else None

    # convert datetime columns to numeric epoch seconds to keep models numeric-only
    for col in X.columns:
        try:
            if pd.api.types.is_datetime64_any_dtype(X[col]):
                X[col] = X[col].astype('int64') / 1e9
        except Exception:
            # if conversion fails, drop the column from features (avoid crash)
            X = X.drop(columns=[col])
            spec["features"] = [f for f in spec["features"] if f["name"] != col]

    # numeric imputation: median
    for col in X.select_dtypes(include=["number"]).columns:
        med = X[col].median()
        X[col] = X[col].fillna(med)

    # categorical encoding: fill missing -> string 'missing', then convert to codes
    for col in [f["name"] for f in spec["features"] if f.get("is_categorical")]:
        X[col] = X[col].fillna("__MISSING__").astype(str)
        cats = sorted(list(pd.Series(X[col].unique()).astype(str).tolist()))
        mapping = {v: i for i, v in enumerate(cats)}
        X[col] = X[col].map(mapping).astype(int)
        # update spec categories
        for f in spec["features"]:
            if f["name"] == col:
                f["categories"] = cats
                break

    save_spec(spec, name=spec_name)
    return X, y, spec


if __name__ == "__main__":
    # quick CLI for manual testing
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--parquet", required=True)
    p.add_argument("--target", default=None)
    p.add_argument("--spec-name", default="default")
    args = p.parse_args()
    df = pd.read_parquet(args.parquet)
    X, y, spec = preprocess_df(df, target=args.target, spec_name=args.spec_name)
    print("Processed", len(X), "rows, spec saved.")
