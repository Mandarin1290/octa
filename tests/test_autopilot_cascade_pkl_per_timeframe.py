import types
from pathlib import Path


def test_autopilot_cascade_stages_pkl_per_timeframe(tmp_path, monkeypatch):
    from octa_ops.autopilot import cascade_train as ct

    # Stub config object returned by load_config
    class Paths:
        def __init__(self, pkl_dir, state_dir):
            self.pkl_dir = Path(pkl_dir)
            self.state_dir = Path(state_dir)

    class Cfg:
        def __init__(self):
            self.paths = Paths(tmp_path / "pkl", tmp_path / "state")

        def copy(self, deep=False):
            # mimic pydantic v1 copy
            c = Cfg()
            c.paths.pkl_dir = Path(self.paths.pkl_dir)
            c.paths.state_dir = Path(self.paths.state_dir)
            return c

    monkeypatch.setattr(ct, "load_config", lambda _p: Cfg())
    monkeypatch.setattr(ct, "StateRegistry", lambda _p: object())

    seen = []

    def _fake_train_evaluate_package(*, symbol, cfg, **kwargs):
        # capture staged pkl_dir for each call
        seen.append(str(cfg.paths.pkl_dir))
        # emulate a PASS + pack_result writing
        pkl_path = Path(cfg.paths.pkl_dir) / f"{symbol}.pkl"
        pkl_path.parent.mkdir(parents=True, exist_ok=True)
        pkl_path.write_bytes(b"x")
        (Path(str(pkl_path).replace(".pkl", ".sha256"))).write_text("deadbeef")
        res = types.SimpleNamespace(
            passed=True,
            gate_result=types.SimpleNamespace(dict=lambda: {"passed": True}),
            metrics=types.SimpleNamespace(dict=lambda: {"sharpe": 1.0}),
            pack_result={"saved": True, "pkl": str(pkl_path), "pkl_sha": "deadbeef"},
        )
        return res

    monkeypatch.setattr(ct, "train_evaluate_package", _fake_train_evaluate_package)

    decisions, metrics = ct.run_cascade_training(
        run_id="r1",
        config_path="configs/dev.yaml",
        symbol="AAPL",
        asset_class="equity",
        parquet_paths={"1D": "x", "1H": "y"},
        cascade=ct.CascadePolicy(order=["1D", "1H"]),
        safe_mode=True,
        reports_dir=str(tmp_path / "reports"),
    )

    assert [d.status for d in decisions] == ["PASS", "PASS"]
    assert len(seen) == 2
    assert seen[0].endswith("/equity/1D")
    assert seen[1].endswith("/equity/1H")
    assert "1D" in metrics and "1H" in metrics
    assert metrics["1D"]["pack"]["pkl"].endswith("/equity/1D/AAPL.pkl")
    assert metrics["1H"]["pack"]["pkl"].endswith("/equity/1H/AAPL.pkl")


def test_autopilot_cascade_gate_fail_not_train_error(tmp_path, monkeypatch):
    from octa_ops.autopilot import cascade_train as ct

    class Paths:
        def __init__(self, pkl_dir, state_dir):
            self.pkl_dir = Path(pkl_dir)
            self.state_dir = Path(state_dir)

    class Cfg:
        def __init__(self):
            self.paths = Paths(tmp_path / "pkl", tmp_path / "state")

        def copy(self, deep=False):
            c = Cfg()
            c.paths.pkl_dir = Path(self.paths.pkl_dir)
            c.paths.state_dir = Path(self.paths.state_dir)
            return c

    monkeypatch.setattr(ct, "load_config", lambda _p: Cfg())
    monkeypatch.setattr(ct, "StateRegistry", lambda _p: object())

    def _fake_train_evaluate_package(**kwargs):
        return types.SimpleNamespace(
            passed=False,
            gate_result=types.SimpleNamespace(dict=lambda: {"passed": False, "reasons": ["walkforward_failed"]}),
            metrics=types.SimpleNamespace(dict=lambda: {"sharpe": 0.1}),
            pack_result={"saved": False},
            error="walkforward_failed",
        )

    monkeypatch.setattr(ct, "train_evaluate_package", _fake_train_evaluate_package)

    decisions, _ = ct.run_cascade_training(
        run_id="r1",
        config_path="configs/dev.yaml",
        symbol="AAPL",
        asset_class="equity",
        parquet_paths={"1D": "x"},
        cascade=ct.CascadePolicy(order=["1D"]),
        safe_mode=True,
        reports_dir=str(tmp_path / "reports"),
    )

    assert len(decisions) == 1
    assert decisions[0].status == "GATE_FAIL"
    assert decisions[0].reason == "walkforward_failed"
