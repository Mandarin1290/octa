from __future__ import annotations

from pathlib import Path

import pytest

from octa.execution.risk_fail_closed_harness import run_case


@pytest.fixture(autouse=True)
def _force_dev_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OCTA_MODE", "dev")


def _assert_case(case: dict) -> None:
    assert case["blocked"] is True
    assert case["broker_router_called"] is False
    assert case["fail_closed_enforced"] is True
    assert case["incident_path"]
    assert Path(case["incident_path"]).exists()
    assert case["sha256_path"]
    assert Path(case["sha256_path"]).exists()
    for k in ("simulated_case", "blocked", "broker_router_called", "fail_closed_enforced", "stack_or_error"):
        assert k in case


def test_risk_fail_closed_exception(tmp_path: Path) -> None:
    case = run_case(simulated_case="exception", root_dir=tmp_path)
    _assert_case(case)
    assert "risk_simulated_exception" in case["stack_or_error"]


def test_risk_fail_closed_invalid_return(tmp_path: Path) -> None:
    case = run_case(simulated_case="invalid_return", root_dir=tmp_path)
    _assert_case(case)
    assert "invalid risk decision object" in case["stack_or_error"]
