from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, List, Optional

import joblib


def save_model(
    *,
    model_obj: Any,
    model_name: str,
    out_dir: Path,
    extra_meta: Optional[dict[str, Any]] = None,
) -> List[str]:
    """Persist model artifacts to out_dir.

    - CatBoost: save .cbm
    - Other models: joblib .pkl
    - Always write optional metadata.json if provided
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    paths: List[str] = []

    meta = extra_meta or {}
    if model_name.lower() == "catboost" and hasattr(model_obj, "save_model"):
        cbm_path = out_dir / "model.cbm"
        model_obj.save_model(str(cbm_path))
        paths.append(str(cbm_path))
    else:
        pkl_path = out_dir / "model.pkl"
        joblib.dump(model_obj, str(pkl_path))
        paths.append(str(pkl_path))

    if meta:
        meta_path = out_dir / "model_meta.json"
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        paths.append(str(meta_path))

    return paths


def write_feature_schema(features: Iterable[str], out_path: Path) -> str:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    data = {"features": list(features)}
    out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return str(out_path)
