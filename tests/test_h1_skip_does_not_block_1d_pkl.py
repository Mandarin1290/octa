from __future__ import annotations

import json
from pathlib import Path

from scripts.train_stocks_pkl_1d_1h import validate_pkl_outputs


def test_h1_skip_does_not_block_1d(tmp_path: Path) -> None:
    out_root = tmp_path / "out"
    (out_root / "1D").mkdir(parents=True)
    (out_root / "1H").mkdir(parents=True)
    (out_root / "meta").mkdir(parents=True)
    (out_root / "ARMED").mkdir(parents=True)

    sym = "BAZ"

    (out_root / "1D" / f"{sym}.pkl").write_bytes(b"pkl")
    # No 1H pkl should exist

    meta = {
        "symbol": sym,
        "asset_profile": "stock",
        "run_id": "test",
        "timeframe_status": {"1D": "PASS", "1H": "SKIP_H1_NOT_ELIGIBLE"},
        "armed": True,
    }
    (out_root / "meta" / f"{sym}.meta.json").write_text(json.dumps(meta))
    (out_root / "ARMED" / f"{sym}.ok").write_text("{}")

    validate_pkl_outputs(out_root, [sym])
