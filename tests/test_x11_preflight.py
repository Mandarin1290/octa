from __future__ import annotations

from pathlib import Path

from octa.execution import x11_preflight as mod


def test_missing_display_returns_code_10() -> None:
    out = mod.check_display_env(env={})
    assert out["ok"] is False
    assert out["code"] == 10
    assert out["reason"] == "DISPLAY_NOT_SET"


def test_missing_tool_returns_code_13() -> None:
    def _lookup(_name: str):
        return None

    out = mod.check_required_tools(tool_lookup=_lookup)
    assert out["ok"] is False
    assert out["code"] == 13
    assert out["reason"] == "TOOL_MISSING"
    assert sorted(out["details"]["missing_tools"]) == ["wmctrl", "xdotool"]


def test_run_full_preflight_writes_evidence_and_is_deterministic_code(tmp_path: Path) -> None:
    out = mod.run_full_preflight(evidence_root=tmp_path, env={})
    assert out["ok"] is False
    assert out["code"] == 10
    evid = Path(out["evidence_dir"])
    assert evid.exists()
    assert (evid / "result.json").exists()
    assert (evid / "report.md").exists()
    assert (evid / "commands.txt").exists()
    assert (evid / "environment.txt").exists()
    assert (evid / "sha256sum.txt").exists()
