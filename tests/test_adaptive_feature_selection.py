"""Tests for train_evaluate_adaptive — two-pass feature-selection fallback."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, call, patch

import pytest

from octa_training.core.pipeline import (
    PipelineResult,
    _pipeline_annotate_pack,
    train_evaluate_adaptive,
)


# ---------------------------------------------------------------------------
# Minimal stubs
# ---------------------------------------------------------------------------


@dataclass
class _FakeFS:
    enabled: bool = False
    corr_threshold: float = 0.95
    max_features: int = 35


@dataclass
class _FakeCfg:
    feature_selection: _FakeFS = field(default_factory=_FakeFS)


@dataclass
class _FakeMetrics:
    sharpe_oos_over_is: Optional[float] = None


def _result(*, passed: bool, ois: Optional[float] = None, run_id: str = "r") -> PipelineResult:
    m = _FakeMetrics(sharpe_oos_over_is=ois)
    return PipelineResult(
        symbol="SYM",
        run_id=run_id,
        passed=passed,
        metrics=m if ois is not None else None,
    )


# ---------------------------------------------------------------------------
# _pipeline_annotate_pack
# ---------------------------------------------------------------------------


class TestAnnotatePack:
    def test_creates_dict_when_none(self):
        res = _result(passed=True)
        assert res.pack_result is None
        _pipeline_annotate_pack(res, x=1)
        assert res.pack_result == {"x": 1}

    def test_merges_into_existing(self):
        res = _result(passed=True)
        res.pack_result = {"a": 1}
        _pipeline_annotate_pack(res, b=2)
        assert res.pack_result == {"a": 1, "b": 2}


# ---------------------------------------------------------------------------
# train_evaluate_adaptive — single-pass cases
# ---------------------------------------------------------------------------


class TestAdaptiveSinglePass:
    def test_pass1_passes_returns_immediately(self):
        """If Pass 1 passes, no second call is made."""
        cfg = _FakeCfg()
        with patch(
            "octa_training.core.pipeline.train_evaluate_package",
            return_value=_result(passed=True, ois=0.8),
        ) as mock_tep:
            result = train_evaluate_adaptive("SYM", cfg, MagicMock(), "run1")

        assert mock_tep.call_count == 1
        assert result.passed is True
        assert result.pack_result["fs_adaptive_pass"] == 1

    def test_fs_already_enabled_no_retry(self):
        """If feature_selection is already enabled, only one pass runs even on failure."""
        cfg = _FakeCfg(feature_selection=_FakeFS(enabled=True))
        with patch(
            "octa_training.core.pipeline.train_evaluate_package",
            return_value=_result(passed=False, ois=0.0),
        ) as mock_tep:
            result = train_evaluate_adaptive("SYM", cfg, MagicMock(), "run1")

        assert mock_tep.call_count == 1
        assert result.passed is False
        assert result.pack_result["fs_adaptive_pass"] == 1

    def test_no_retry_when_ois_above_threshold(self):
        """OOS/IS above threshold → Pass 1 result returned, no retry."""
        cfg = _FakeCfg()
        with patch(
            "octa_training.core.pipeline.train_evaluate_package",
            return_value=_result(passed=False, ois=0.50),
        ) as mock_tep:
            result = train_evaluate_adaptive(
                "SYM", cfg, MagicMock(), "run1", fs_retry_ois_threshold=0.10
            )

        assert mock_tep.call_count == 1
        assert result.pack_result["fs_adaptive_pass"] == 1

    def test_no_retry_when_ois_is_none(self):
        """Missing OOS/IS metric → no retry (fail-closed)."""
        cfg = _FakeCfg()
        with patch(
            "octa_training.core.pipeline.train_evaluate_package",
            return_value=_result(passed=False, ois=None),
        ) as mock_tep:
            result = train_evaluate_adaptive("SYM", cfg, MagicMock(), "run1")

        assert mock_tep.call_count == 1
        assert result.pack_result["fs_adaptive_pass"] == 1


# ---------------------------------------------------------------------------
# train_evaluate_adaptive — two-pass cases
# ---------------------------------------------------------------------------


class TestAdaptiveTwoPass:
    def test_retry_triggered_when_severe_overfit(self):
        """OOS/IS=0.0 (< 0.10 default threshold) → Pass 2 is executed."""
        cfg = _FakeCfg()
        pass1 = _result(passed=False, ois=0.0, run_id="r1")
        pass2 = _result(passed=True, ois=0.9, run_id="r1_fsretry")

        with patch(
            "octa_training.core.pipeline.train_evaluate_package",
            side_effect=[pass1, pass2],
        ) as mock_tep:
            result = train_evaluate_adaptive("SYM", cfg, MagicMock(), "r1")

        assert mock_tep.call_count == 2
        assert result.passed is True
        assert result.pack_result["fs_adaptive_pass"] == 2
        assert result.pack_result["fs_retry_ois_p1"] == pytest.approx(0.0)

    def test_pass2_run_id_has_fsretry_suffix(self):
        """Pass 2 must use a distinct run_id to avoid state collision."""
        cfg = _FakeCfg()
        captured_run_ids: List[str] = []

        def _fake_tep(symbol, cfg_, state, run_id, **_kw):
            captured_run_ids.append(run_id)
            ois = 0.0 if len(captured_run_ids) == 1 else 0.8
            return _result(passed=len(captured_run_ids) > 1, ois=ois, run_id=run_id)

        with patch("octa_training.core.pipeline.train_evaluate_package", side_effect=_fake_tep):
            train_evaluate_adaptive("SYM", cfg, MagicMock(), "base_run")

        assert captured_run_ids[0] == "base_run"
        assert captured_run_ids[1] == "base_run_fsretry"

    def test_pass2_cfg_has_fs_enabled(self):
        """Pass 2 must be called with feature_selection.enabled=True."""
        cfg = _FakeCfg()
        captured_cfgs: List[Any] = []

        def _fake_tep(symbol, cfg_, state, run_id, **_kw):
            captured_cfgs.append(cfg_)
            ois = 0.0 if len(captured_cfgs) == 1 else 0.8
            return _result(passed=len(captured_cfgs) > 1, ois=ois)

        with patch("octa_training.core.pipeline.train_evaluate_package", side_effect=_fake_tep):
            train_evaluate_adaptive("SYM", cfg, MagicMock(), "run")

        # Pass 1: original cfg (fs off)
        assert captured_cfgs[0].feature_selection.enabled is False
        # Pass 2: deep-copied cfg (fs on)
        assert captured_cfgs[1].feature_selection.enabled is True
        # Original cfg must NOT be mutated
        assert cfg.feature_selection.enabled is False

    def test_pass2_returns_even_if_it_also_fails(self):
        """If neither pass succeeds, return Pass 2 result (tried harder)."""
        cfg = _FakeCfg()
        with patch(
            "octa_training.core.pipeline.train_evaluate_package",
            side_effect=[
                _result(passed=False, ois=0.05),
                _result(passed=False, ois=0.20),
            ],
        ):
            result = train_evaluate_adaptive("SYM", cfg, MagicMock(), "run")

        assert result.passed is False
        assert result.pack_result["fs_adaptive_pass"] == 2

    def test_custom_threshold_respected(self):
        """fs_retry_ois_threshold controls when retry fires."""
        cfg = _FakeCfg()
        # OOS/IS=0.15 should NOT trigger retry with threshold=0.10
        with patch(
            "octa_training.core.pipeline.train_evaluate_package",
            return_value=_result(passed=False, ois=0.15),
        ) as mock_tep:
            train_evaluate_adaptive(
                "SYM", cfg, MagicMock(), "run", fs_retry_ois_threshold=0.10
            )
        assert mock_tep.call_count == 1

        # Same OOS/IS=0.15 SHOULD trigger retry with threshold=0.20
        with patch(
            "octa_training.core.pipeline.train_evaluate_package",
            side_effect=[
                _result(passed=False, ois=0.15),
                _result(passed=True, ois=0.60),
            ],
        ) as mock_tep2:
            train_evaluate_adaptive(
                "SYM", cfg, MagicMock(), "run", fs_retry_ois_threshold=0.20
            )
        assert mock_tep2.call_count == 2

    def test_kwargs_forwarded_to_both_passes(self):
        """Extra kwargs (parquet_path, dataset, …) must reach both passes.
        asset_class= is a legacy alias that is normalised to dataset= before forwarding.
        """
        cfg = _FakeCfg()
        captured: List[dict] = []

        def _fake_tep(symbol, cfg_, state, run_id, **kw):
            captured.append(kw)
            ois = 0.0 if len(captured) == 1 else 0.8
            return _result(passed=len(captured) > 1, ois=ois)

        with patch("octa_training.core.pipeline.train_evaluate_package", side_effect=_fake_tep):
            train_evaluate_adaptive(
                "SYM", cfg, MagicMock(), "run",
                parquet_path="/tmp/foo.parquet", asset_class="stock"
            )

        for call_kwargs in captured:
            assert call_kwargs["parquet_path"] == "/tmp/foo.parquet"
            # asset_class= is normalised → dataset= (new train_evaluate_package API)
            assert call_kwargs["dataset"] == "stock"
