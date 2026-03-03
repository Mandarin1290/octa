"""Tests for Phase 1 B2: Risk Aggregator Wiring.

Verifies that aggregate_risk() is called before every execution cycle,
that unknown/missing asset metadata causes fail-closed blocking, and that
exposure snapshots are deterministic.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from octa.execution.runner import (
    ExecutionConfig,
    _build_exposure_dict,
    _ASSET_CLASS_META,
    run_execution,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_stage(tf: str, status: str, metrics: dict | None = None) -> dict:
    return {"timeframe": tf, "status": status, "metrics_summary": metrics or {"n_trades": 1}}


def _make_base(base: Path, symbol: str = "AAA", asset_class: str = "equities") -> None:
    run = base / "run_x"
    (run / "preflight").mkdir(parents=True, exist_ok=True)
    (run / "preflight" / "summary.json").write_text("{}", encoding="utf-8")
    (run / "results").mkdir(parents=True, exist_ok=True)
    (run / "results" / f"{symbol}.json").write_text(
        json.dumps(
            {
                "symbol": symbol,
                "asset_class": asset_class,
                "stages": [
                    _write_stage("1D", "PASS"),
                    _write_stage("1H", "PASS"),
                    _write_stage("30M", "PASS"),
                ],
            }
        ),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Unit tests for _build_exposure_dict
# ---------------------------------------------------------------------------

def test_build_exposure_dict_equity_fields() -> None:
    rows = [{"symbol": "AAPL", "asset_class": "equities"}]
    result = _build_exposure_dict(rows, nav=100_000.0)
    assert result["symbol_count"] == 1
    assert result["by_symbol"]["AAPL"] == pytest.approx(1_000.0)  # 1% NAV
    assert "equities" in result["by_asset_class"]
    assert "SMART" in result["by_exchange"]
    assert "USD" in result["by_currency"]
    assert result["total_notional"] == pytest.approx(1_000.0)


def test_build_exposure_dict_unknown_asset_raises() -> None:
    rows = [{"symbol": "XYZ", "asset_class": "alien_class"}]
    with pytest.raises(RuntimeError, match="UNKNOWN_ASSET_CLASS"):
        _build_exposure_dict(rows, nav=100_000.0)


def test_build_exposure_dict_mixed_currencies() -> None:
    rows = [
        {"symbol": "AAPL", "asset_class": "equities"},
        {"symbol": "EURUSD", "asset_class": "forex"},
    ]
    result = _build_exposure_dict(rows, nav=100_000.0)
    assert "USD" in result["by_currency"]
    assert "FOREIGN" in result["by_currency"]
    assert result["by_currency"]["USD"] == pytest.approx(1_000.0)
    assert result["by_currency"]["FOREIGN"] == pytest.approx(1_000.0)


def test_build_exposure_dict_empty_rows() -> None:
    result = _build_exposure_dict([], nav=100_000.0)
    assert result["symbol_count"] == 0
    assert result["total_notional"] == 0.0
    assert result["by_symbol"] == {}


# ---------------------------------------------------------------------------
# Integration tests: run_execution dry-run
# ---------------------------------------------------------------------------

def test_exposure_snapshot_written_on_dry_run(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("OCTA_TELEGRAM_ENABLED", "false")
    base = tmp_path / "evidence_base"
    _make_base(base, symbol="AAA", asset_class="equities")
    out_dir = tmp_path / "exec"

    run_execution(
        ExecutionConfig(
            mode="dry-run",
            evidence_dir=out_dir,
            base_evidence_dir=base,
            asset_class="equities",
            max_symbols=1,
        )
    )

    snap_file = out_dir / "exposure_snapshot_cycle_001.json"
    assert snap_file.exists(), "exposure_snapshot_cycle_001.json must be written"
    snap = json.loads(snap_file.read_text())
    assert snap["cycle"] == 1
    assert "exposures" in snap
    assert "by_symbol" in snap["exposures"]
    assert "by_asset_class" in snap["exposures"]
    assert "by_exchange" in snap["exposures"]
    assert "by_currency" in snap["exposures"]
    assert "risk_snapshot" in snap
    assert snap["risk_snapshot"]["source"] == "local"


def test_unknown_asset_class_blocks_all_orders(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("OCTA_TELEGRAM_ENABLED", "false")
    base = tmp_path / "evidence_base"
    # Use an asset_class not in _ASSET_CLASS_META
    _make_base(base, symbol="ZZZ", asset_class="alien_asset")
    out_dir = tmp_path / "exec"

    # Must NOT raise — fail-closed means log and continue
    summary = run_execution(
        ExecutionConfig(
            mode="dry-run",
            evidence_dir=out_dir,
            base_evidence_dir=base,
            # No asset_class filter so the alien_asset row passes through
            max_symbols=5,
        )
    )

    ml_orders = json.loads((out_dir / "ml_orders.json").read_text())
    assert ml_orders == [], "Unknown asset class must block all orders"

    fail_file = out_dir / "risk_aggregation_fail_cycle_001.json"
    assert fail_file.exists(), "Fail file must be written on unknown asset class"
    fail_data = json.loads(fail_file.read_text())
    assert fail_data["blocked"] is True
    assert "UNKNOWN_ASSET_CLASS" in fail_data["reason"]


def test_aggregator_exception_blocks_cycle(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("OCTA_TELEGRAM_ENABLED", "false")
    base = tmp_path / "evidence_base"
    _make_base(base, symbol="AAA", asset_class="equities")
    out_dir = tmp_path / "exec"

    # Monkeypatch aggregate_risk to raise
    monkeypatch.setattr(
        "octa.execution.runner.aggregate_risk",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("test_aggregator_error")),
    )

    run_execution(
        ExecutionConfig(
            mode="dry-run",
            evidence_dir=out_dir,
            base_evidence_dir=base,
            asset_class="equities",
            max_symbols=1,
        )
    )

    ml_orders = json.loads((out_dir / "ml_orders.json").read_text())
    assert ml_orders == [], "Aggregator exception must block all orders"

    fail_file = out_dir / "risk_aggregation_fail_cycle_001.json"
    assert fail_file.exists()
    fail_data = json.loads(fail_file.read_text())
    assert fail_data["blocked"] is True
    assert "RISK_AGGREGATION_FAIL" in fail_data["reason"]


def test_exposure_snapshot_deterministic(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("OCTA_TELEGRAM_ENABLED", "false")

    def _make_run(run_id: str) -> dict:
        base = tmp_path / f"base_{run_id}"
        _make_base(base, symbol="AAA", asset_class="equities")
        out_dir = tmp_path / f"exec_{run_id}"
        run_execution(
            ExecutionConfig(
                mode="dry-run",
                evidence_dir=out_dir,
                base_evidence_dir=base,
                asset_class="equities",
                max_symbols=1,
            )
        )
        snap = json.loads((out_dir / "exposure_snapshot_cycle_001.json").read_text())
        return snap["exposures"]

    exp1 = _make_run("run1")
    exp2 = _make_run("run2")

    assert exp1["total_notional"] == exp2["total_notional"]
    assert exp1["by_currency"] == exp2["by_currency"]
    assert exp1["by_asset_class"] == exp2["by_asset_class"]
    assert exp1["symbol_count"] == exp2["symbol_count"]


def test_risk_aggregation_event_in_governance_audit(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("OCTA_TELEGRAM_ENABLED", "false")
    base = tmp_path / "evidence_base"
    _make_base(base, symbol="AAA", asset_class="equities")
    out_dir = tmp_path / "exec"

    run_execution(
        ExecutionConfig(
            mode="dry-run",
            evidence_dir=out_dir,
            base_evidence_dir=base,
            asset_class="equities",
            max_symbols=1,
        )
    )

    from octa.core.governance.governance_audit import GovernanceAudit
    audit = GovernanceAudit(run_id=out_dir.name)
    events = audit.read_events()
    event_types = [
        e.get("payload", {}).get("event_type", e.get("event_type", ""))
        for e in events
    ]
    # The chain stores events with a wrapper; check the data field
    chain_path = audit.chain_path
    chain_lines = chain_path.read_text().splitlines()
    all_event_types = []
    for line in chain_lines:
        try:
            rec = json.loads(line)
            payload = rec.get("payload", {})
            ev = payload.get("event_type", payload.get("data", {}).get("event_type", ""))
            # Also look inside the nested data key
            data = payload.get("data", {})
            if not ev:
                ev = data.get("event_type", "")
            all_event_types.append(ev)
            # Extract from envelope directly
            envelope = payload.get("data", {})
            if "event_type" in envelope:
                all_event_types.append(envelope["event_type"])
        except Exception:
            pass

    # Read raw chain to find RISK_AGGREGATION
    raw = chain_path.read_text()
    assert "RISK_AGGREGATION" in raw, "RISK_AGGREGATION event must appear in governance audit chain"
