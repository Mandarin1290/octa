from __future__ import annotations

from types import SimpleNamespace

from octa_ops.autopilot import cascade_train as ct


class _Cfg:
    def __init__(self, pkl_dir: str, state_dir: str):
        self.paths = SimpleNamespace(pkl_dir=pkl_dir, state_dir=state_dir)

    def copy(self, deep: bool = False):
        return _Cfg(self.paths.pkl_dir, self.paths.state_dir)


def test_performance_fail_blocks_downstream(monkeypatch, tmp_path):
    """I1: Performance gate fail (GATE_FAIL) must block cascade — 1H must be SKIP."""
    calls: list[str] = []

    monkeypatch.setattr(ct, "load_config", lambda _p: _Cfg(str(tmp_path / "pkl"), str(tmp_path / "state")))
    monkeypatch.setattr(ct, "StateRegistry", lambda _p: object())
    monkeypatch.setattr(ct, "_walkforward_eligibility", lambda **_k: {"eligible": True})

    def _fake_train(**kwargs):
        tf = str(kwargs["cfg"].paths.pkl_dir).split("/")[-1]
        calls.append(tf)
        # 1D performance gate fails (sharpe too low) — not a structural/data failure
        return SimpleNamespace(
            passed=False,
            gate_result=SimpleNamespace(model_dump=lambda: {"reasons": ["sharpe_below_min"]}),
            metrics=SimpleNamespace(model_dump=lambda: {}),
            pack_result={},
            error="",
        )

    monkeypatch.setattr(ct, "train_evaluate_package", _fake_train)

    decisions, _metrics = ct.run_cascade_training(
        run_id="r1",
        config_path="configs/dev.yaml",
        symbol="AAA",
        asset_class="stock",
        parquet_paths={"1D": "a.parquet", "1H": "b.parquet"},
        cascade=ct.CascadePolicy(order=["1D", "1H"]),
        safe_mode=True,
        reports_dir=str(tmp_path),
    )

    # I1: only 1D is trained; 1H is SKIP because 1D's performance_pass=False
    assert calls == ["1D"]
    d1 = next(d for d in decisions if d.timeframe == "1D")
    d2 = next(d for d in decisions if d.timeframe == "1H")
    assert d1.status == "GATE_FAIL"
    assert d1.details["performance_pass"] is False
    assert d2.status == "SKIP"
    assert "cascade_previous_stage_not_passed" in str(d2.reason)


def test_downstream_skips_when_upstream_structural_fails(monkeypatch, tmp_path):
    monkeypatch.setattr(ct, "load_config", lambda _p: _Cfg(str(tmp_path / "pkl"), str(tmp_path / "state")))
    monkeypatch.setattr(ct, "StateRegistry", lambda _p: object())
    monkeypatch.setattr(ct, "_walkforward_eligibility", lambda **_k: {"eligible": True})

    def _fake_train(**kwargs):
        tf = str(kwargs["cfg"].paths.pkl_dir).split("/")[-1]
        if tf == "1D":
            return SimpleNamespace(
                passed=False,
                gate_result=SimpleNamespace(model_dump=lambda: {"reasons": ["DATA_INVALID:bad_schema"]}),
                metrics=SimpleNamespace(model_dump=lambda: {}),
                pack_result={},
                error="data_load_failed",
            )
        raise AssertionError("1H should not run")

    monkeypatch.setattr(ct, "train_evaluate_package", _fake_train)

    decisions, _metrics = ct.run_cascade_training(
        run_id="r2",
        config_path="configs/dev.yaml",
        symbol="BBB",
        asset_class="stock",
        parquet_paths={"1D": "a.parquet", "1H": "b.parquet"},
        cascade=ct.CascadePolicy(order=["1D", "1H"]),
        safe_mode=True,
        reports_dir=str(tmp_path),
    )

    d1 = next(d for d in decisions if d.timeframe == "1D")
    d2 = next(d for d in decisions if d.timeframe == "1H")
    assert d1.details["structural_pass"] is False
    assert d2.status == "SKIP"
    assert d2.reason == "cascade_previous_stage_not_passed"
