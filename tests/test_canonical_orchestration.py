from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from octa.core.orchestration import flow as legacy_flow
from octa.core.orchestration import runner as legacy_runner
from octa.core.pipeline import paper_run
from octa.execution.cli import run_execution as run_execution_cli
from octa.foundation import control_plane
from octa.support.ops import run_cascade_with_altdata_smoke, run_training_smoke
from octa.support.ops import run_training, train_universe
from octa.support.ops import v000_full_universe_cascade_train as v000_runner
from octa_ops.autopilot import paper_runner


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_run_training_delegates_to_foundation_control_plane(monkeypatch):
    captured = {}

    def fake_run_foundation_training(**kwargs):
        captured.update(kwargs)
        return {"ok": True}

    monkeypatch.setattr(run_training, "run_foundation_training", fake_run_foundation_training)
    run_training.main(
        config="configs/train.yaml",
        resume=True,
        run_id="train123",
        universe_limit=7,
        symbols="aapl, msft",
        altdata_config=None,
        altdata_run_id=None,
        var_root=None,
    )
    assert captured["config_path"] == "configs/train.yaml"
    assert captured["resume"] is True
    assert captured["run_id"] == "train123"
    assert captured["max_symbols"] == 7
    assert captured["symbols"] == "AAPL,MSFT"


def test_run_training_rejects_non_canonical_overrides():
    with pytest.raises(Exception) as excinfo:
        run_training.main(
            config=None,
            resume=False,
            run_id=None,
            universe_limit=0,
            symbols="",
            altdata_config="cfg.yaml",
            altdata_run_id=None,
            var_root=None,
        )
    assert "non_canonical_training_options" in str(excinfo.value)


def test_foundation_shadow_uses_dry_run(monkeypatch, tmp_path):
    captured = {}

    def fake_run_execution(cfg):
        captured["cfg"] = cfg
        return {"mode": cfg.mode, "ok": True}

    monkeypatch.setattr(control_plane, "run_execution", fake_run_execution)
    summary = control_plane.run_foundation_shadow(
        asset_class="equity",
        max_symbols=1,
        run_id="shadow123",
        evidence_dir=tmp_path / "shadow",
    )
    assert summary["mode"] == "dry-run"
    assert captured["cfg"].mode == "dry-run"
    assert captured["cfg"].evidence_dir == tmp_path / "shadow"


def test_foundation_training_uses_full_cascade(monkeypatch, tmp_path):
    captured = {}

    def fake_run_full_cascade(settings, train_fn):
        captured["settings"] = settings
        captured["train_fn"] = train_fn
        return {"ok": True, "evidence_dir": str(settings.evidence_dir)}

    monkeypatch.setattr(control_plane, "run_full_cascade", fake_run_full_cascade)
    summary = control_plane.run_foundation_training(
        run_id="train456",
        evidence_dir=tmp_path / "evidence",
        root=Path("raw"),
        max_symbols=3,
        symbols="aapl",
        asset_classes=["equity"],
    )
    manifest = (tmp_path / "evidence" / "run_manifest.json").read_text(encoding="utf-8")
    assert summary["ok"] is True
    assert captured["settings"].evidence_dir == tmp_path / "evidence"
    assert captured["settings"].preflight_out == (tmp_path / "evidence" / "preflight")
    assert captured["settings"].symbols_override == ["AAPL"]
    assert captured["settings"].asset_classes == ("equities",)
    assert '"scope": "v0.0.0_foundation"' in manifest


def test_run_execution_cli_rejects_non_dry_run(monkeypatch):
    monkeypatch.setattr(
        "sys.argv",
        ["run_execution.py", "--mode", "paper"],
    )
    with pytest.raises(SystemExit) as excinfo:
        run_execution_cli.main()
    assert excinfo.value.code == 2


def test_train_universe_main_is_blocked():
    with pytest.raises(SystemExit) as excinfo:
        train_universe.main()
    assert "non_canonical_training_entrypoint" in str(excinfo.value)


def test_paper_run_cli_is_blocked():
    with pytest.raises(SystemExit) as excinfo:
        paper_run.main([])
    assert "non_canonical_paper_entrypoint" in str(excinfo.value)


def test_paper_runner_is_unblocked_v0_0_1():
    # v0.0.1: the foundation-scope paper execution block has been removed.
    # Calling run_paper with a nonexistent config should fail at config load,
    # NOT with the v0.0.0 foundation block RuntimeError.
    with pytest.raises(Exception) as excinfo:
        paper_runner.run_paper(run_id="r1", config_path="cfg.yaml")
    assert "paper_execution_blocked_in_v0_0_0_foundation_scope" not in str(excinfo.value)


def test_run_paper_live_script_is_blocked():
    module = _load_module(Path("scripts/run_paper_live.py"), "run_paper_live_test")
    with pytest.raises(SystemExit) as excinfo:
        module.main()
    assert "non_canonical_foundation_entrypoint:scripts/run_paper_live.py" in str(excinfo.value)


def test_legacy_core_runner_is_blocked():
    with pytest.raises(RuntimeError) as excinfo:
        legacy_runner.run_cascade()
    assert "legacy_orchestrator_retired" in str(excinfo.value)


def test_legacy_core_flow_is_blocked():
    class DummyContext:
        op_config = {}

    with pytest.raises(RuntimeError) as excinfo:
        legacy_flow.run_cascade_op(DummyContext())
    assert "legacy_orchestration_flow_retired" in str(excinfo.value)


def test_run_training_smoke_delegates(monkeypatch):
    captured = {}

    def fake_run_foundation_training(**kwargs):
        captured.update(kwargs)
        return {"ok": True}

    monkeypatch.setattr(run_training_smoke, "run_foundation_training", fake_run_foundation_training)
    run_training_smoke.main()
    assert captured["max_symbols"] == 2
    assert captured["dry_run"] is True


def test_altdata_smoke_legacy_runner_is_blocked():
    with pytest.raises(SystemExit) as excinfo:
        run_cascade_with_altdata_smoke.main()
    assert "deprecated_altdata_smoke_orchestrator" in str(excinfo.value)


def test_v000_training_entrypoint_is_blocked():
    with pytest.raises(SystemExit) as excinfo:
        v000_runner.main()
    assert "deprecated_v000_training_entrypoint" in str(excinfo.value)
