from __future__ import annotations

import argparse

import pandas as pd

from octa.core.data.sources.stream.manifest import AssetManifest
from octa.core.data.validation.stream.contracts import ValidationResult
from octa.core.data.validation.stream.lineage import (
    minimal_parquet_metadata,
    parquet_content_hash,
)

REQUIRED_COLUMNS = ["timestamp", "open", "high", "low", "close"]


class ParquetValidator:
    def validate(self, manifest: AssetManifest) -> ValidationResult:
        path = manifest.parquet_path
        # fast metadata
        try:
            meta = minimal_parquet_metadata(path)
        except Exception as e:
            return ValidationResult(False, [f"cannot read parquet metadata: {e}"])

        # read necessary columns for validation to minimize memory use
        try:
            df = pd.read_parquet(path, columns=None)
        except Exception as e:
            return ValidationResult(False, [f"cannot read parquet: {e}"])

        # schema check
        cols = set(df.columns.str.lower())
        missing = [c for c in REQUIRED_COLUMNS if c not in cols]
        if missing:
            return ValidationResult(False, [f"missing required columns: {missing}"])

        # timestamp checks
        if df["timestamp"].isnull().any():
            return ValidationResult(False, ["timestamp contains nulls"])
        # ensure timezone-aware or assume UTC; convert
        try:
            ts = pd.to_datetime(df["timestamp"], utc=True)
        except Exception as e:
            return ValidationResult(False, [f"timestamp parse error: {e}"])
        if not ts.is_monotonic_increasing:
            return ValidationResult(False, ["timestamp not monotonic increasing"])
        if ts.duplicated().any():
            return ValidationResult(False, ["timestamp contains duplicates"])

        # type and sanity checks for prices
        for col in ("open", "high", "low", "close"):
            if not pd.api.types.is_numeric_dtype(df[col]):
                return ValidationResult(False, [f"column {col} is not numeric"])
            if (df[col] < 0).any():
                return ValidationResult(False, [f"column {col} has negative values"])

        # basic OHLC relationships
        if (df["high"] < df[["open", "close", "low"]].max(axis=1)).any():
            return ValidationResult(False, ["high less than other price fields"])
        if (df["low"] > df[["open", "close", "high"]].min(axis=1)).any():
            return ValidationResult(False, ["low greater than other price fields"])

        # volume policy
        # asset_class may be Enum or plain str
        if str(manifest.asset_class) == "FX":
            # volume may be absent or null
            pass
        else:
            if "volume" not in cols:
                return ValidationResult(False, ["volume missing for non-FX asset"])
            if df["volume"].isnull().any():
                return ValidationResult(False, ["volume contains nulls"])
            if not pd.api.types.is_numeric_dtype(df["volume"]):
                return ValidationResult(False, ["volume not numeric"])

        # corporate actions flag
        if getattr(manifest, "ca_provided", False):
            # manifest must indicate true; if true that's allowed
            pass
        else:
            # ensure dataset does not include placeholder columns like 'corporate_action'
            bad_cols = [c for c in df.columns if "corporate" in str(c).lower()]
            if bad_cols:
                return ValidationResult(
                    False,
                    [
                        "corporate action placeholders present; set ca_provided in manifest if CA expected"
                    ],
                )

        # compute lineage hash
        try:
            h = parquet_content_hash(path)
        except Exception:
            h = ""

        metadata = {"num_rows": int(meta.get("num_rows", 0)), "hash": h}
        return ValidationResult(True, [], metadata)


def cli(argv=None) -> int:
    p = argparse.ArgumentParser(prog="octa_stream.validate")
    p.add_argument("--manifest", required=True)
    args = p.parse_args(argv)
    m = AssetManifest.load(args.manifest)
    v = ParquetValidator()
    res = v.validate(m)
    print("eligible:", res.eligible)
    if res.reasons:
        print("reasons:")
        for r in res.reasons:
            print(" -", r)
    if res.metadata:
        print("metadata:", res.metadata)
    return 0 if res.eligible else 2


if __name__ == "__main__":
    raise SystemExit(cli())
