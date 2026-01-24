from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.train_stocks_pkl_1d_1h import validate_pkl_outputs


def _write_meta(out_root: Path, sym: str, s1d: str, s1h: str, armed: bool) -> None:
    (out_root / "meta").mkdir(parents=True, exist_ok=True)
    payload = {
        "symbol": sym,
        "asset_profile": "stock",
        "run_id": "test",
        "timeframe_status": {"1D": s1d, "1H": s1h},
        "armed": armed,
    }
    (out_root / "meta" / f"{sym}.meta.json").write_text(json.dumps(payload))


def test_armed_marker_rules(tmp_path: Path) -> None:
    out_root = tmp_path / "out"
    (out_root / "1D").mkdir(parents=True)
    (out_root / "1H").mkdir(parents=True)
    (out_root / "ARMED").mkdir(parents=True)

    sym = "BAR"

    # 1D FAIL => ARMED must not exist
    _write_meta(out_root, sym, "FAIL", "SKIP_H1_NOT_ELIGIBLE", armed=False)
    (out_root / "ARMED" / f"{sym}.ok").write_text("{}")
    with pytest.raises(AssertionError):
        validate_pkl_outputs(out_root, [sym])

    # 1D PASS + 1H SKIP => ARMED must exist
    (out_root / "ARMED" / f"{sym}.ok").unlink()
    _write_meta(out_root, sym, "PASS", "SKIP_H1_NOT_ELIGIBLE", armed=True)
    (out_root / "1D" / f"{sym}.pkl").write_bytes(b"pkl")
    (out_root / "ARMED" / f"{sym}.ok").write_text("{}")
    validate_pkl_outputs(out_root, [sym])
