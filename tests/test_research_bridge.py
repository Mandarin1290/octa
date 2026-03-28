from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from octa.core.data.research_bridge import load_research_export
from octa.core.features.research_features import build_research_features
from octa.core.validation.research_validation import validate_research_payload
from octa.research.export.vectorbt_export import export_strategy_outputs


def _sample_inputs() -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object]]:
    index = pd.date_range("2026-01-01", periods=6, freq="D", tz="UTC")
    df_signals = pd.DataFrame(
        {
            "signal": [1.0, 0.0, -1.0, 1.0, 0.0, -1.0],
            "signal_strength": [0.8, 0.0, 0.9, 0.7, 0.0, 0.85],
        },
        index=index,
    )
    df_returns = pd.DataFrame(
        {
            "strategy_return": [0.01, -0.02, 0.015, 0.0, 0.012, -0.01],
        },
        index=index,
    )
    metadata = {
        "strategy_name": "vectorbt_breakout",
        "timeframe": "1D",
        "params": {"lookback": 20, "threshold": 1.5},
        "source": "synthetic_test",
    }
    return df_signals, df_returns, metadata


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_export_import_roundtrip(tmp_path: Path) -> None:
    df_signals, df_returns, metadata = _sample_inputs()
    export_dir = tmp_path / "export_run"

    manifest = export_strategy_outputs(df_signals, df_returns, metadata, export_dir)
    payload = load_research_export(export_dir)

    pd.testing.assert_frame_equal(payload["signals"], df_signals, check_freq=False)
    pd.testing.assert_frame_equal(payload["returns"], df_returns, check_freq=False)
    assert payload["metadata"] == metadata
    assert manifest["source_env"]["prefix"]

    features = build_research_features(payload["signals"])
    report = validate_research_payload(features, payload["returns"])
    assert report["status"] == "ok"
    assert features.iloc[0]["long_signal"] == 0


def test_hash_validation_for_pristine_export(tmp_path: Path) -> None:
    df_signals, df_returns, metadata = _sample_inputs()
    export_dir = tmp_path / "export_run"

    export_strategy_outputs(df_signals, df_returns, metadata, export_dir)
    manifest = json.loads((export_dir / "export_manifest.json").read_text(encoding="utf-8"))

    for name in ("signals.parquet", "returns.parquet", "metadata.json"):
        assert manifest["files"][name]["sha256"] == _sha256_file(export_dir / name)

    payload = load_research_export(export_dir)
    assert list(payload["signals"].columns) == ["signal", "signal_strength"]


def test_manipulation_fails_hash_validation(tmp_path: Path) -> None:
    df_signals, df_returns, metadata = _sample_inputs()
    export_dir = tmp_path / "export_run"

    export_strategy_outputs(df_signals, df_returns, metadata, export_dir)
    metadata_path = export_dir / "metadata.json"
    metadata_path.write_text('{"tampered": true}\n', encoding="utf-8")

    with pytest.raises(ValueError, match="SHA256_MISMATCH"):
        load_research_export(export_dir)


def test_missing_file_fails_closed(tmp_path: Path) -> None:
    df_signals, df_returns, metadata = _sample_inputs()
    export_dir = tmp_path / "export_run"

    export_strategy_outputs(df_signals, df_returns, metadata, export_dir)
    (export_dir / "returns.parquet").unlink()

    with pytest.raises(FileNotFoundError, match="returns.parquet"):
        load_research_export(export_dir)
