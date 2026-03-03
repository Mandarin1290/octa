"""Tests for Phase 2 B1: Multi-Asset Execution Routing.

Verifies:
1-4.  resolve_contract_spec() for known asset classes + raises on unknown
5.    IBKRContractAdapter rejects order with unknown asset_class
6.    IBKRContractAdapter accepts order with known asset_class (equity)
7.    runner.py ML order dict contains asset_class field
8.    runner.py carry order dict contains asset_class field
9.    market_closed pretrade → IBKRContractAdapter REJECTED with MARKET_CLOSED
10.   Monkeypatched check_market_open returns MARKET_CLOSED for closed exchange
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from octa_vertex.broker.asset_class_router import ContractSpec, resolve_contract_spec


# ---------------------------------------------------------------------------
# Helpers (reused from test_risk_aggregation_wiring)
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
# 1-4: resolve_contract_spec unit tests
# ---------------------------------------------------------------------------

def test_resolve_equity() -> None:
    spec = resolve_contract_spec("AAPL", "equities")
    assert isinstance(spec, ContractSpec)
    assert spec.exchange == "SMART"
    assert spec.currency == "USD"
    assert spec.contract_type == "stock"
    assert spec.multiplier is None


def test_resolve_forex() -> None:
    spec = resolve_contract_spec("EURUSD", "forex")
    assert spec.exchange == "IDEALPRO"
    assert spec.currency == "FOREIGN"
    assert spec.contract_type == "forex"
    assert spec.multiplier is None


def test_resolve_futures() -> None:
    spec = resolve_contract_spec("ES", "futures")
    assert spec.exchange == "GLOBEX"
    assert spec.currency == "USD"
    assert spec.contract_type == "future"
    assert spec.multiplier is None


def test_resolve_options_multiplier() -> None:
    spec = resolve_contract_spec("SPY", "options")
    assert spec.exchange == "CBOE"
    assert spec.currency == "USD"
    assert spec.contract_type == "option"
    assert spec.multiplier == 100


def test_resolve_unknown_raises() -> None:
    with pytest.raises(RuntimeError, match="UNKNOWN_ASSET_CLASS"):
        resolve_contract_spec("ZZZ", "alien_asset")


# ---------------------------------------------------------------------------
# 5-6: IBKRContractAdapter validation
# ---------------------------------------------------------------------------

def test_ibkr_contract_rejects_unknown_asset_class() -> None:
    from octa_vertex.broker.ibkr_contract import IBKRConfig, IBKRContractAdapter

    adapter = IBKRContractAdapter(IBKRConfig(supported_instruments=["EURUSD"]))
    order = {
        "order_id": "test-001",
        "instrument": "EURUSD",
        "qty": 10.0,
        "side": "BUY",
        "order_type": "MKT",
        "asset_class": "alien_class",
    }
    result = adapter.submit_order(order)
    assert result["status"] == "REJECTED"
    assert "UNKNOWN_ASSET_CLASS" in result["reason"]


def test_ibkr_contract_accepts_known_asset_class() -> None:
    from octa_vertex.broker.ibkr_contract import IBKRConfig, IBKRContractAdapter

    # Override market-hours check to always return open (avoid calendar dependency)
    import octa_vertex.broker.asset_class_router as router

    original = router.check_market_open
    router.check_market_open = lambda exchange, ts=None: None  # type: ignore[assignment]
    try:
        adapter = IBKRContractAdapter(IBKRConfig(supported_instruments=["AAPL"]))
        order = {
            "order_id": "test-002",
            "instrument": "AAPL",
            "qty": 5.0,
            "side": "BUY",
            "order_type": "MKT",
            "asset_class": "equities",
        }
        result = adapter.submit_order(order)
        assert result["status"] == "PENDING"
    finally:
        router.check_market_open = original  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# 7-8: runner.py order dicts contain asset_class
# ---------------------------------------------------------------------------

def test_runner_ml_order_has_asset_class(tmp_path: Path, monkeypatch) -> None:
    """ML order submitted to broker must contain asset_class field."""
    monkeypatch.setenv("OCTA_TELEGRAM_ENABLED", "false")
    base = tmp_path / "evidence_base"
    _make_base(base, symbol="AAA", asset_class="equities")
    out_dir = tmp_path / "exec"

    captured_orders: list[dict] = []

    from octa.execution import broker_router as br

    orig_place = br.BrokerRouter.place_order

    def _capture_place(self, *, strategy: str, order: dict) -> dict:
        captured_orders.append({"strategy": strategy, "order": dict(order)})
        return {"status": "SIMULATED", "mode": "dry-run", "order_id": order.get("order_id")}

    monkeypatch.setattr(br.BrokerRouter, "place_order", _capture_place)

    from octa.execution.runner import ExecutionConfig, run_execution

    run_execution(
        ExecutionConfig(
            mode="dry-run",
            evidence_dir=out_dir,
            base_evidence_dir=base,
            asset_class="equities",
            max_symbols=1,
        )
    )

    ml_captured = [c for c in captured_orders if c["strategy"] == "ml"]
    assert ml_captured, "At least one ML order must be placed during dry-run"
    order = ml_captured[0]["order"]
    assert "asset_class" in order, "ML order dict must contain asset_class field"
    assert order["asset_class"] == "equities"


def test_runner_carry_order_has_asset_class(tmp_path: Path, monkeypatch) -> None:
    """Carry order submitted to broker must contain asset_class field."""
    monkeypatch.setenv("OCTA_TELEGRAM_ENABLED", "false")
    base = tmp_path / "evidence_base"
    _make_base(base, symbol="AAA", asset_class="equities")
    out_dir = tmp_path / "exec"

    captured_orders: list[dict] = []

    from octa.execution import broker_router as br

    orig_place = br.BrokerRouter.place_order

    def _capture_place(self, *, strategy: str, order: dict) -> dict:
        captured_orders.append({"strategy": strategy, "order": dict(order)})
        return {"status": "SIMULATED", "mode": "dry-run", "order_id": order.get("order_id")}

    monkeypatch.setattr(br.BrokerRouter, "place_order", _capture_place)

    # Force carry to produce an intent
    from octa.execution.carry import CarryIntent

    def _mock_resolve_rates(*args, **kwargs):
        return {
            "enabled": True,
            "source": "test",
            "rates": {"EURUSD": {"bid": 0.03, "ask": 0.03}},
        }

    def _mock_generate_intents(*args, **kwargs):
        intent = CarryIntent(
            instrument="EURUSD",
            direction="LONG_BASE",
            asset_class="fx_carry",
            confidence=0.9,
            rate_diff=0.03,
            expected_net_carry_after_costs=0.02,
            funding_cost=0.005,
            reason="test_intent",
        )
        return [intent], {"instruments_considered": 1}

    monkeypatch.setattr("octa.execution.runner.resolve_carry_rates", _mock_resolve_rates)
    monkeypatch.setattr("octa.execution.runner.generate_carry_intents", _mock_generate_intents)

    # Portfolio preflight blocks EURUSD due to missing returns data — patch to pass
    from octa.core.portfolio.preflight import PreflightResult

    def _mock_preflight(**kwargs):
        return PreflightResult(ok=True, reason="ok", checks={}, blocked_symbols=[])

    monkeypatch.setattr("octa.execution.runner.run_preflight", _mock_preflight)

    from octa.execution.runner import ExecutionConfig, run_execution

    run_execution(
        ExecutionConfig(
            mode="dry-run",
            evidence_dir=out_dir,
            base_evidence_dir=base,
            asset_class="equities",
            max_symbols=1,
            enable_carry=True,
        )
    )

    carry_captured = [c for c in captured_orders if c["strategy"] == "carry"]
    assert carry_captured, "At least one carry order must be placed with mocked intents"
    order = carry_captured[0]["order"]
    assert "asset_class" in order, "Carry order dict must contain asset_class field"
    assert order["asset_class"] == "fx_carry"


# ---------------------------------------------------------------------------
# 9-10: market-closed fail-closed
# ---------------------------------------------------------------------------

def test_market_closed_blocks_ibkr_contract() -> None:
    """When check_market_open returns a MARKET_CLOSED error, IBKRContractAdapter must REJECT."""
    import octa_vertex.broker.asset_class_router as router
    from octa_vertex.broker.ibkr_contract import IBKRConfig, IBKRContractAdapter

    original = router.check_market_open
    router.check_market_open = lambda exchange, ts=None: f"MARKET_CLOSED:NYSE:out_of_session"  # type: ignore[assignment]
    try:
        adapter = IBKRContractAdapter(IBKRConfig(supported_instruments=["AAPL"]))
        order = {
            "order_id": "test-003",
            "instrument": "AAPL",
            "qty": 5.0,
            "side": "BUY",
            "order_type": "MKT",
            "asset_class": "equities",
        }
        result = adapter.submit_order(order)
        assert result["status"] == "REJECTED"
        assert "MARKET_CLOSED" in result["reason"]
    finally:
        router.check_market_open = original  # type: ignore[assignment]


def test_check_market_open_missing_calendar_allows(monkeypatch) -> None:
    """check_market_open must return None (allow) when calendar says missing_calendar."""
    from octa_vertex.broker.asset_class_router import check_market_open

    # Monkeypatch pretrade_check to simulate missing_calendar
    import octa_vertex.broker.asset_class_router as router

    monkeypatch.setattr(
        router,
        "check_market_open",
        lambda exchange, ts=None: None,  # simulate missing calendar → allow
    )
    # Direct test: check logic in the real function via a mock pretrade module
    import octa_vertex.market_hours as mh_module

    original = mh_module.pretrade_check
    mh_module.pretrade_check = lambda instrument, ts=None: {"eligible": False, "reason": "missing_calendar"}  # type: ignore[assignment]
    try:
        result = check_market_open("SMART")
        # missing_calendar → allow (return None)
        assert result is None, "missing_calendar must be treated as open (allow)"
    finally:
        mh_module.pretrade_check = original  # type: ignore[assignment]
