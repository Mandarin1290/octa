from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pandas.testing as pdt
import pytest

from octa.core.broker_paper.broker_paper_adapter import InMemoryBrokerPaperAdapter
from octa.core.broker_paper.broker_paper_policy import BrokerPaperPolicy
from octa.core.broker_paper.broker_paper_session import run_broker_paper_session
from octa.core.broker_paper.broker_paper_session_validation import validate_broker_paper_session
from octa.core.paper.market_data_adapter import InMemoryMarketDataAdapter
from octa.core.pipeline.broker_paper_runner import run_broker_paper


def _signals() -> pd.DataFrame:
    index = pd.date_range("2026-02-01", periods=6, freq="D", tz="UTC")
    return pd.DataFrame(
        {"long_signal": [0, 1, 1, 0, 0, 0], "short_signal": [0, 0, 0, 0, 0, 0], "signal_strength": [0, 1, 1, 0, 0, 0]},
        index=index,
    )


def _frame(close_values: list[float] | None = None) -> pd.DataFrame:
    index = pd.date_range("2026-02-01", periods=6, freq="D", tz="UTC")
    close = close_values or [100.0, 101.0, 103.0, 104.0, 105.0, 106.0]
    return pd.DataFrame(
        {
            "open": [100.0, 100.5, 102.0, 103.0, 104.0, 105.0],
            "high": [101.0, 102.0, 104.0, 105.0, 106.0, 107.0],
            "low": [99.0, 100.0, 101.0, 102.0, 103.0, 104.0],
            "close": close,
            "volume": [1000.0] * 6,
        },
        index=index,
    )


def _policy(**overrides) -> BrokerPaperPolicy:
    payload = {
        "require_paper_gate_status": "PAPER_ELIGIBLE",
        "require_min_completed_sessions": 1,
        "require_min_total_trades": 1,
        "require_min_win_rate": 0.0,
        "require_min_profit_factor": 0.0,
        "max_allowed_drawdown": 1.0,
        "require_kill_switch_not_triggered": False,
        "require_hash_integrity": True,
        "require_broker_mode": "PAPER",
        "forbid_live_mode": True,
        "max_session_age_hours": 48.0,
        "paper_capital": 100000.0,
        "paper_fee": 0.0005,
        "paper_slippage": 0.0002,
        "max_open_positions": 1,
        "kill_switch_drawdown": 0.2,
        "allow_short": False,
    }
    payload.update(overrides)
    return BrokerPaperPolicy.from_mapping(payload)


def _gate_result(status: str = "BROKER_PAPER_ELIGIBLE") -> dict:
    return {
        "status": status,
        "checks": [],
        "summary": {
            "paper_session_evidence_dir": "/tmp/paper_session",
            "paper_gate_evidence_dir": "/tmp/paper_gate",
            "promotion_evidence_dir": "/tmp/promotion",
            "shadow_evidence_dir": "/tmp/shadow",
            "research_export_path": "/tmp/research",
        },
    }


def _make_runner_fixture(tmp_path: Path, *, gate_status: str, session_completed: bool) -> Path:
    paper_session_dir = tmp_path / "paper_session"
    paper_gate_dir = tmp_path / "paper_gate"
    promotion_dir = tmp_path / "promotion"
    shadow_dir = tmp_path / "shadow"
    research_dir = tmp_path / "research_export"
    for path in (paper_session_dir, paper_gate_dir, promotion_dir, shadow_dir, research_dir):
        path.mkdir(parents=True, exist_ok=True)

    index = pd.date_range("2026-02-01", periods=6, freq="D", tz="UTC")
    sig = pd.DataFrame({"signal": [1.0, 1.0, 0.0, 0.0, 0.0, 0.0], "signal_strength": [1, 1, 0, 0, 0, 0]}, index=index)
    ret = pd.DataFrame({"strategy_return": [0.01, 0.01, 0, 0, 0, 0]}, index=index)
    sig.to_parquet(research_dir / "signals.parquet")
    ret.to_parquet(research_dir / "returns.parquet")
    (research_dir / "metadata.json").write_text(json.dumps({"strategy_name": "s", "timeframe": "1D", "params": {}, "source": "t"}, sort_keys=True, indent=2), encoding="utf-8")
    import hashlib
    def sha(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    manifest = {
        "run_id": research_dir.name,
        "source_env": {"prefix": "/tmp/research"},
        "files": {
            "signals.parquet": {"path": str((research_dir / "signals.parquet").resolve()), "sha256": sha(research_dir / "signals.parquet")},
            "returns.parquet": {"path": str((research_dir / "returns.parquet").resolve()), "sha256": sha(research_dir / "returns.parquet")},
            "metadata.json": {"path": str((research_dir / "metadata.json").resolve()), "sha256": sha(research_dir / "metadata.json")},
        },
    }
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
    (shadow_dir / "run_manifest.json").write_text(json.dumps({"research_export_path": str(research_dir.resolve())}, sort_keys=True, indent=2), encoding="utf-8")
    (promotion_dir / "decision_report.json").write_text(json.dumps({"decision": {"summary": {"shadow_evidence_dir": str(shadow_dir.resolve())}}}, sort_keys=True, indent=2), encoding="utf-8")
    (paper_gate_dir / "paper_gate_report.json").write_text(
        json.dumps(
            {
                "promotion_evidence_dir": str(promotion_dir.resolve()),
                "gate_result": {
                    "status": "PAPER_ELIGIBLE",
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
    session_summary = {"status": "PAPER_SESSION_COMPLETED"} if session_completed else None
    blocked_reason = None if session_completed else "gate_status=PAPER_BLOCKED"
    (paper_session_dir / "paper_session_report.json").write_text(
        json.dumps(
            {
                "references": {
                    "paper_gate_evidence_dir": str(paper_gate_dir.resolve()),
                    "promotion_evidence_dir": str(promotion_dir.resolve()),
                    "shadow_evidence_dir": str(shadow_dir.resolve()),
                    "research_export_path": str(research_dir.resolve()),
                },
                "blocked_reason": blocked_reason,
                "session_summary": session_summary,
            },
            sort_keys=True,
            indent=2,
        ),
        encoding="utf-8",
    )
    (paper_session_dir / "session_manifest.json").write_text(
        json.dumps(
            {
                "references": {
                    "paper_gate_evidence_dir": str(paper_gate_dir.resolve()),
                    "promotion_evidence_dir": str(promotion_dir.resolve()),
                    "shadow_evidence_dir": str(shadow_dir.resolve()),
                    "research_export_path": str(research_dir.resolve()),
                },
                "metrics": {
                    "n_trades": 2,
                    "win_rate": 1.0,
                    "profit_factor": 2.0,
                    "max_drawdown": 0.01,
                    "kill_switch_triggered": False,
                },
            },
            sort_keys=True,
            indent=2,
        ),
        encoding="utf-8",
    )
    (paper_session_dir / "session_policy.json").write_text(json.dumps({"paper_fee": 0.0005}, sort_keys=True, indent=2), encoding="utf-8")
    from octa.core.data.recycling.common import sha256_file
    (paper_session_dir / "evidence_manifest.json").write_text(
        json.dumps(
            {
                "hashes": {
                    "session_manifest.json": sha256_file(paper_session_dir / "session_manifest.json"),
                    "paper_session_report.json": sha256_file(paper_session_dir / "paper_session_report.json"),
                    "session_policy.json": sha256_file(paper_session_dir / "session_policy.json"),
                }
            },
            sort_keys=True,
            indent=2,
        ),
        encoding="utf-8",
    )
    return paper_session_dir


def test_adapter_mode_not_paper_fails() -> None:
    with pytest.raises(ValueError, match="explicit PAPER mode"):
        InMemoryBrokerPaperAdapter(mode="LIVE")


def test_deterministic_inputs_same_outputs() -> None:
    adapter1 = InMemoryMarketDataAdapter.from_dataframe("TEST", _frame())
    adapter2 = InMemoryMarketDataAdapter.from_dataframe("TEST", _frame())
    broker1 = InMemoryBrokerPaperAdapter(mode="PAPER", fee_rate=0.0005, slippage=0.0002)
    broker2 = InMemoryBrokerPaperAdapter(mode="PAPER", fee_rate=0.0005, slippage=0.0002)
    r1 = run_broker_paper_session(_gate_result(), adapter1, broker1, _signals(), _policy())
    r2 = run_broker_paper_session(_gate_result(), adapter2, broker2, _signals(), _policy())
    pdt.assert_frame_equal(r1["orders"], r2["orders"])
    pdt.assert_frame_equal(r1["fills"], r2["fills"])
    pdt.assert_frame_equal(r1["equity_curve"], r2["equity_curve"])


def test_kill_switch_and_positions_limit() -> None:
    adapter = InMemoryMarketDataAdapter.from_dataframe("TEST", _frame([100.0, 80.0, 60.0, 50.0, 49.0, 48.0]))
    broker = InMemoryBrokerPaperAdapter(mode="PAPER", fee_rate=0.0005, slippage=0.0002)
    result = run_broker_paper_session(_gate_result(), adapter, broker, _signals(), _policy(kill_switch_drawdown=0.05))
    assert result["metrics"]["kill_switch_triggered"] is True
    validate = validate_broker_paper_session(result, require_broker_mode="PAPER", max_open_positions=1, kill_switch_drawdown=0.05)
    assert validate["status"] == "ok"


def test_runner_blocked_and_positive_paths(tmp_path: Path) -> None:
    blocked_dir = _make_runner_fixture(tmp_path / "blocked", gate_status="PAPER_BLOCKED", session_completed=False)
    blocked = run_broker_paper(
        paper_session_evidence_dir=blocked_dir,
        policy=_policy(),
        evidence_root=tmp_path / "evidence",
        run_id="broker_paper_blocked",
    )
    assert blocked["status"] == "BROKER_PAPER_BLOCKED"

    allowed_dir = _make_runner_fixture(tmp_path / "allowed", gate_status="PAPER_ELIGIBLE", session_completed=True)
    allowed = run_broker_paper(
        paper_session_evidence_dir=allowed_dir,
        policy=_policy(),
        market_data_adapter=InMemoryMarketDataAdapter.from_dataframe("TEST", _frame()),
        broker_adapter=InMemoryBrokerPaperAdapter(mode="PAPER", fee_rate=0.0005, slippage=0.0002),
        evidence_root=tmp_path / "evidence",
        run_id="broker_paper_allowed",
    )
    assert allowed["status"] in {"BROKER_PAPER_SESSION_COMPLETED", "BROKER_PAPER_SESSION_ABORTED"}
