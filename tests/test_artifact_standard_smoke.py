from pathlib import Path

from octa.core.orchestration.resources import ensure_run_dirs, write_config_snapshot


def test_artifact_standard_dirs(tmp_path: Path) -> None:
    run_id = "unit_run"
    run_dirs = ensure_run_dirs(run_id, var_root=tmp_path / "var")
    run_root = run_dirs["run_root"]

    assert run_root.exists()
    assert run_dirs["survivors_dir"].exists()
    assert run_dirs["metrics_dir"].exists()
    assert run_dirs["models_dir"].exists()
    assert run_dirs["reports_dir"].exists()

    config_path = write_config_snapshot(run_root, {"hello": "world"})
    assert config_path.exists()

