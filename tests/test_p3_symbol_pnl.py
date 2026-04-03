"""P3: Per-symbol P&L Infrastructure tests."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from octa_accounting.symbol_pnl import SymbolPnL, compute_symbol_pnl


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ledger(tmp_path: Path, events: list) -> str:
    """Patch LedgerStore.by_action to return the given events list."""
    return str(tmp_path / "ledger")


def _fill(symbol: str, side: str, qty: float, price: float, ts: str = "2026-01-01T00:00:00") -> dict:
    return {
        "timestamp": ts,
        "payload": {
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "fill_price": price,
        },
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_empty_ledger_returns_empty(tmp_path: Path) -> None:
    with patch("octa_accounting.symbol_pnl._extract_fills", return_value=[]):
        result = compute_symbol_pnl(str(tmp_path / "ledger"))
    assert result == {}


def test_single_buy_no_realized(tmp_path: Path) -> None:
    from octa_accounting.symbol_pnl import TradeRecord

    trades = [TradeRecord("AAPL", "BUY", 10, 150.0, "2026-01-01T00:00:00")]
    with patch("octa_accounting.symbol_pnl._extract_fills", return_value=trades):
        result = compute_symbol_pnl(str(tmp_path / "ledger"))
    assert "AAPL" in result
    pnl = result["AAPL"]
    assert pnl.realized_pnl == 0.0
    assert pnl.open_qty == 10.0
    assert pnl.avg_entry_price == 150.0
    assert pnl.n_trades == 1


def test_round_trip_buy_sell_realized(tmp_path: Path) -> None:
    """BUY 10 @ 100, SELL 10 @ 110 → realized = 100."""
    from octa_accounting.symbol_pnl import TradeRecord

    trades = [
        TradeRecord("ADC", "BUY", 10, 100.0, "2026-01-01T00:00:00"),
        TradeRecord("ADC", "SELL", 10, 110.0, "2026-01-02T00:00:00"),
    ]
    with patch("octa_accounting.symbol_pnl._extract_fills", return_value=trades):
        result = compute_symbol_pnl(str(tmp_path / "ledger"))
    pnl = result["ADC"]
    assert abs(pnl.realized_pnl - 100.0) < 1e-6
    assert pnl.open_qty == 0.0
    assert pnl.n_wins == 1
    assert pnl.n_losses == 0
    assert pnl.win_rate == 1.0


def test_loss_trade(tmp_path: Path) -> None:
    """BUY 5 @ 200, SELL 5 @ 190 → realized = -50."""
    from octa_accounting.symbol_pnl import TradeRecord

    trades = [
        TradeRecord("XYZ", "BUY", 5, 200.0, "2026-01-01T00:00:00"),
        TradeRecord("XYZ", "SELL", 5, 190.0, "2026-01-02T00:00:00"),
    ]
    with patch("octa_accounting.symbol_pnl._extract_fills", return_value=trades):
        result = compute_symbol_pnl(str(tmp_path / "ledger"))
    pnl = result["XYZ"]
    assert abs(pnl.realized_pnl - (-50.0)) < 1e-6
    assert pnl.n_wins == 0
    assert pnl.n_losses == 1
    assert pnl.win_rate == 0.0


def test_unrealized_with_current_price(tmp_path: Path) -> None:
    """BUY 10 @ 100, current price 115 → unrealized = 150."""
    from octa_accounting.symbol_pnl import TradeRecord

    trades = [TradeRecord("MSFT", "BUY", 10, 100.0, "2026-01-01T00:00:00")]
    with patch("octa_accounting.symbol_pnl._extract_fills", return_value=trades):
        result = compute_symbol_pnl(
            str(tmp_path / "ledger"),
            current_prices={"MSFT": 115.0},
        )
    pnl = result["MSFT"]
    assert abs(pnl.unrealized_pnl - 150.0) < 1e-6
    assert abs(pnl.total_pnl - 150.0) < 1e-6


def test_output_json_written(tmp_path: Path) -> None:
    from octa_accounting.symbol_pnl import TradeRecord

    trades = [TradeRecord("ABBV", "BUY", 3, 170.0, "2026-01-01T00:00:00")]
    out_path = str(tmp_path / "ledger_paper" / "symbol_pnl.json")
    with patch("octa_accounting.symbol_pnl._extract_fills", return_value=trades):
        compute_symbol_pnl(str(tmp_path / "ledger"), output_path=out_path)
    p = Path(out_path)
    assert p.exists()
    data = json.loads(p.read_text())
    assert "ABBV" in data
    assert "realized_pnl" in data["ABBV"]


def test_non_fill_events_ignored(tmp_path: Path) -> None:
    """_extract_fills must only return paper.order_filled events."""
    from octa_accounting.symbol_pnl import _extract_fills
    from unittest.mock import MagicMock

    mock_ledger = MagicMock()
    mock_ledger.by_action.return_value = []  # simulate no fills

    with patch("octa_ledger.store.LedgerStore", return_value=mock_ledger):
        trades = _extract_fills(str(tmp_path / "ledger"))

    mock_ledger.by_action.assert_called_once_with("paper.order_filled")
    assert trades == []
