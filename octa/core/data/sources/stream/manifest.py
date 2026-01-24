from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict

import yaml  # type: ignore


class AssetClass(str, Enum):
    EQUITY = "EQUITY"
    ETF = "ETF"
    FUTURE = "FUTURE"
    FX = "FX"
    CRYPTO = "CRYPTO"
    BOND = "BOND"


@dataclass
class AssetManifest:
    asset_id: str
    symbol: str
    asset_class: AssetClass
    venue: str
    currency: str
    parquet_path: str
    ca_provided: bool = False

    @staticmethod
    def load(path: str) -> "AssetManifest":
        p = Path(path)
        raw: Dict[str, Any]
        if p.suffix in (".yaml", ".yml"):
            raw = yaml.safe_load(p.read_text()) or {}
        else:
            raw = json.loads(p.read_text())
        return AssetManifest(
            asset_id=str(raw["asset_id"]),
            symbol=str(raw["symbol"]),
            asset_class=AssetClass(raw["asset_class"]),
            venue=str(raw["venue"]),
            currency=str(raw["currency"]),
            parquet_path=str(raw["parquet_path"]),
            ca_provided=bool(raw.get("ca_provided", False)),
        )
