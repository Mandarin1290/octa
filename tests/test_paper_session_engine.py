from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pandas.testing as pdt
import pytest

from octa.core.paper.market_data_adapter import InMemoryMarketDataAdapter
from octa.core.paper.paper_session_engine import run_paper_session
from octa.core.paper.paper_session_policy import PaperSessionPolicy
from octa.core.paper.paper_session_validation import validate_paper_session
from octa.core.pipeline.paper_session_runner import run_paper_session_pipeline


def _signals() -> pd.DataFrame:
    index = pd.date_range("2026-02-01", periods=6, freq="D", tz="UTC")
    return pd.DataFrame(
        {
            "long_signal": [0, 1, 1, 0, 0, 0],
            "short_signal": [0, 0, 0, 0, 0, 0],
            "signal_strength": [0.0, 1.0, 1.0, 0.0, 0.0, 0.0],
        },
        index=index,
    )


def _event_frame(close_override: list[float] | None = None) -> pd.DataFrame:
    index = pd.date_range("2026-02-01", periods=6, freq="D", tz="UTC")
    close = close_override or [100.0, 101.0, 103.0, 104.0, 105.0, 106.0]
    return pd.DataFrame(
        {
            "open": [100.0, 100.5, 102.0, 103.0, 104.0, 105.0],
            "high": [101.0, 102.0, 104.0, 105.0, 106.0, 107.0],
            "low": [99.0, 100.0, 101.0, 102.0, 103.0, 104.0],
            "close": close,
            "volume": [10, 10, 10, 10, 10, 10],
        },
        index=index,
    )


def _gate_result(status: str = "PAPER_ELIGIBLE") -> dict:
    return {
        "status": status,
        "checks": [],
        "summary": {
            "promotion_evidence_dir": "/tmp/promotion",
            "shadow_evidence_dir": "/tmp/shadow",
        },
    }


def _policy(**overrides) -> PaperSessionPolicy:
    payload = {
        "require_gate_status": "PAPER_ELIGIBLE",
        "max_session_minutes": 10000,
        "heartbeat_interval_sec": 5,
        "paper_capital": 100000.0,
        "paper_fee": 0.001,
        "paper_slippage": 0.0005,
        "max_open_positions": 1,
        "kill_switch_drawdown": 0.20,
        "allow_short": False,
    }
    payload.update(overrides)
    return PaperSessionPolicy.from_mapping(payload)


def _make_gate_evidence(base: Path, *, gate_status: str, promotion_status: str) -> Path:
    root = base
    gate_dir = root / "paper_gate"
    promotion_dir = root / "promotion"
    shadow_dir = root / "shadow"
    research_dir = root / "research_export"
    for path in (gate_dir, promotion_dir, shadow_dir, research_dir):
        path.mkdir(parents=True, exist_ok=True)

    index = pd.date_range("2026-02-01", periods=6, freq="D", tz="UTC")
    signals = pd.DataFrame(
        {"signal": [1.0, 1.0, 0.0, 0.0, 0.0, 0.0], "signal_strength": [1.0, 1.0, 0.0, 0.0, 0.0, 0.0]},
        index=index,
    )
    returns = pd.DataFrame({"strategy_return": [0.01, 0.01, 0.0, 0.0, 0.0, 0.0]}, index=index)
    signals.to_parquet(research_dir / "signals.parquet")
    returns.to_parquet(research_dir / "returns.parquet")
    (research_dir / "metadata.json").write_text(
        json.dumps({"strategy_name": "s", "timeframe": "1D", "params": {}, "source": "test"}, sort_keys=True, indent=2),
        encoding="utf-8",
    )
    (research_dir / "export_manifest.json").write_text(
        json.dumps(
            {
                "run_id": research_dir.name,
                "source_env": {"prefix": "/tmp/research"},
                "bundle_sha256": "fixture",
                "files": {
                    "signals.parquet": {"path": str((research_dir / "signals.parquet").resolve()), "sha256": ""},
                    "returns.parquet": {"path": str((research_dir / "returns.parquet").resolve()), "sha256": ""},
                    "metadata.json": {"path": str((research_dir / "metadata.json").resolve()), "sha256": ""},
                },
            },
            sort_keys=True,
            indent=2,
        ),
        encoding="utf-8",
    )
    import hashlib
    def sha(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    manifest = json.loads((research_dir / "export_manifest.json").read_text(encoding="utf-8"))
    manifest["files"]["signals.parquet"]["sha256"] = sha(research_dir / "signals.parquet")
    manifest["files"]["returns.parquet"]["sha256"] = sha(research_dir / "returns.parquet")
    manifest["files"]["metadata.json"]["sha256"] = sha(research_dir / "metadata.json")
    canonical = json.dumps(
        {
            "files": {
                "metadata.json": manifest["files"]["metadata.json"]["sha256"],
                "returns.parquet": manifest["files"]["returns.parquet"]["sha256"],
                "signals.parquet": manifest["files"]["signals.parquet"]["sha256"],
            },
            "run_id": research_dir.name,
            "source_env_prefix": "/tmp/research",
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    manifest["bundle_sha256"] = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    (research_dir / "export_manifest.json").write_text(json.dumps(manifest, sort_keys=True, indent=2), encoding="utf-8")

    (shadow_dir / "run_manifest.json").write_text(
        json.dumps({"research_export_path": str(research_dir.resolve())}, sort_keys=True, indent=2),
        encoding="utf-8",
    )
    (promotion_dir / "decision_report.json").write_text(
        json.dumps(
            {
                "decision": {
                    "status": promotion_status,
                    "summary": {"shadow_evidence_dir": str(shadow_dir.resolve())},
                }
            },
            sort_keys=True,
            indent=2,
        ),
        encoding="utf-8",
    )
    (gate_dir / "paper_gate_report.json").write_text(
        json.dumps(
            {
                "promotion_evidence_dir": str(promotion_dir.resolve()),
                "gate_result": {
                    "status": gate_status,
                    "summary": {
                        "promotion_evidence_dir": str(promotion_dir.resolve()),
                        "shadow_evidence_dir": str(shadow_dir.resolve()),
                    },
                },
            },
            sort_keys=True,
            indent=2,
        ),
        encoding="utf-8",
    )
    return gate_dir


def test_paper_blocked_session_does_not_start() -> None:
    adapter = InMemoryMarketDataAdapter.from_dataframe("TEST", _event_frame())
    with pytest.raises(ValueError, match="requires PAPER_ELIGIBLE"):
        run_paper_session(_gate_result("PAPER_BLOCKED"), adapter, _signals(), _policy())


def test_paper_eligible_session_runs() -> None:
    adapter = InMemoryMarketDataAdapter.from_dataframe("TEST", _event_frame())
    result = run_paper_session(_gate_result(), adapter, _signals(), _policy())
    assert result["session_summary"]["status"] == "PAPER_SESSION_COMPLETED"
    assert not result["equity_curve"].empty


def test_deterministic_replay_same_results() -> None:
    adapter_one = InMemoryMarketDataAdapter.from_dataframe("TEST", _event_frame())
    adapter_two = InMemoryMarketDataAdapter.from_dataframe("TEST", _event_frame())
    first = run_paper_session(_gate_result(), adapter_one, _signals(), _policy())
    second = run_paper_session(_gate_result(), adapter_two, _signals(), _policy())
    pdt.assert_frame_equal(first["trades"], second["trades"])
    pdt.assert_frame_equal(first["equity_curve"], second["equity_curve"])
    assert first["metrics"] == second["metrics"]


def test_kill_switch_triggers() -> None:
    adapter = InMemoryMarketDataAdapter.from_dataframe("TEST", _event_frame([100.0, 80.0, 60.0, 50.0, 49.0, 48.0]))
    result = run_paper_session(_gate_result(), adapter, _signals(), _policy(kill_switch_drawdown=0.05))
    assert result["metrics"]["kill_switch_triggered"] is True


def test_max_open_positions_enforced() -> None:
    adapter = InMemoryMarketDataAdapter.from_dataframe("TEST", _event_frame())
    with pytest.raises(ValueError, match="max_open_positions"):
        run_paper_session(_gate_result(), adapter, _signals(), _policy(max_open_positions=0))


def test_inconsistent_inputs_fail_validation() -> None:
    adapter = InMemoryMarketDataAdapter.from_dataframe("TEST", _event_frame())
    result = run_paper_session(_gate_result(), adapter, _signals(), _policy())
    tampered = result["equity_curve"].copy()
    tampered.iloc[1, tampered.columns.get_loc("equity")] = float("nan")
    with pytest.raises(ValueError, match="contains NaNs"):
        validate_paper_session(_gate_result(), {**result, "equity_curve": tampered}, max_open_positions=1, kill_switch_drawdown=0.2)


def test_runner_blocked_and_positive_paths(tmp_path: Path) -> None:
    blocked_gate = _make_gate_evidence(tmp_path / "blocked", gate_status="PAPER_BLOCKED", promotion_status="PROMOTE_BLOCKED")
    adapter = InMemoryMarketDataAdapter.from_dataframe("TEST", _event_frame())
    blocked = run_paper_session_pipeline(
        paper_gate_evidence_dir=blocked_gate,
        market_data_adapter=adapter,
        session_policy=_policy(),
        evidence_root=tmp_path / "evidence",
        run_id="paper_session_blocked",
    )
    assert blocked["status"] == "PAPER_BLOCKED"

    allowed_gate = _make_gate_evidence(tmp_path / "allowed", gate_status="PAPER_ELIGIBLE", promotion_status="PROMOTE_ELIGIBLE")
    allowed = run_paper_session_pipeline(
        paper_gate_evidence_dir=allowed_gate,
        market_data_adapter=InMemoryMarketDataAdapter.from_dataframe("TEST", _event_frame()),
        session_policy=_policy(),
        evidence_root=tmp_path / "evidence",
        run_id="paper_session_allowed",
    )
    assert allowed["status"] == "PAPER_SESSION_COMPLETED"
