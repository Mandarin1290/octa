"""Tests for FillTracker (Module 4 order audit log)."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from octa.execution.fill_tracker import FillEvent, FillTracker

_TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")
_TODAY_TS = datetime.now(timezone.utc).strftime("%Y-%m-%dT09:00:00Z")
_YESTERDAY_TS = "1970-01-01T09:00:00Z"


def _make_fill(
    order_id: str = "ord-001",
    symbol: str = "AAPL",
    strategy: str = "ml",
    side: str = "BUY",
    qty: float = 10.0,
    status: str = "SIMULATED",
    cycle: int = 1,
    timestamp_utc: str = _TODAY_TS,
    asset_class: str = "equities",
) -> FillEvent:
    return FillEvent(
        order_id=order_id,
        symbol=symbol,
        strategy=strategy,
        side=side,
        qty=qty,
        status=status,
        cycle=cycle,
        timestamp_utc=timestamp_utc,
        asset_class=asset_class,
        raw_result={"status": status},
    )


def test_record_and_reload_fill(tmp_path: Path) -> None:
    ft = FillTracker(tmp_path)
    fill = _make_fill()
    ft.record(fill)
    fills = ft.load_for_date(_TODAY)
    assert len(fills) == 1
    assert fills[0].symbol == "AAPL"
    assert fills[0].status == "SIMULATED"


def test_fills_jsonl_is_append_only(tmp_path: Path) -> None:
    ft = FillTracker(tmp_path)
    ft.record(_make_fill(order_id="a", symbol="AAPL"))
    ft.record(_make_fill(order_id="b", symbol="MSFT"))
    fills = ft.load_for_date(_TODAY)
    assert len(fills) == 2
    symbols = {f.symbol for f in fills}
    assert symbols == {"AAPL", "MSFT"}


def test_load_for_date_filters_by_date(tmp_path: Path) -> None:
    ft = FillTracker(tmp_path)
    ft.record(_make_fill(timestamp_utc=_TODAY_TS))
    ft.record(_make_fill(order_id="b2", timestamp_utc=_YESTERDAY_TS))
    assert len(ft.load_for_date(_TODAY)) == 1
    assert len(ft.load_for_date("1970-01-01")) == 1
    assert len(ft.load_for_date("1969-01-01")) == 0


def test_count_submitted_today(tmp_path: Path) -> None:
    ft = FillTracker(tmp_path)
    ft.record(_make_fill(status="SIMULATED"))
    ft.record(_make_fill(order_id="b", status="REJECTED"))
    assert ft.count_submitted_today(_TODAY) == 2


def test_count_filled_today_excludes_rejected(tmp_path: Path) -> None:
    ft = FillTracker(tmp_path)
    ft.record(_make_fill(status="SIMULATED"))
    ft.record(_make_fill(order_id="r", status="REJECTED"))
    ft.record(_make_fill(order_id="e", status="ERROR"))
    assert ft.count_filled_today(_TODAY) == 1


def test_summary_for_date(tmp_path: Path) -> None:
    ft = FillTracker(tmp_path)
    ft.record(_make_fill(symbol="AAPL", strategy="ml", status="SIMULATED"))
    ft.record(_make_fill(order_id="b", symbol="AAPL", strategy="carry", status="SIMULATED"))
    ft.record(_make_fill(order_id="c", symbol="MSFT", strategy="ml", status="REJECTED"))
    summary = ft.summary_for_date(_TODAY)
    assert summary["total"] == 3
    assert summary["filled"] == 2
    assert summary["by_strategy"]["ml"] == 2
    assert summary["by_strategy"]["carry"] == 1
    assert summary["by_symbol"]["AAPL"] == 2


def test_record_never_raises_on_bad_path(tmp_path: Path) -> None:
    ft = FillTracker(tmp_path)
    # Make fills.jsonl a directory to cause write error
    (tmp_path / "fills.jsonl").mkdir()
    ft.record(_make_fill())  # Must not raise


def test_load_for_date_returns_empty_when_file_missing(tmp_path: Path) -> None:
    ft = FillTracker(tmp_path)
    assert ft.load_for_date(_TODAY) == []


def test_load_for_date_skips_corrupt_lines(tmp_path: Path) -> None:
    ft = FillTracker(tmp_path)
    ft.record(_make_fill())
    # Append a corrupt line
    (tmp_path / "fills.jsonl").open("a").write("not json\n")
    ft.record(_make_fill(order_id="ok2"))
    fills = ft.load_for_date(_TODAY)
    assert len(fills) == 2


def test_creates_state_dir_if_missing(tmp_path: Path) -> None:
    nested = tmp_path / "nested" / "state"
    ft = FillTracker(nested)
    ft.record(_make_fill())
    assert nested.exists()
    assert len(ft.load_for_date(_TODAY)) == 1
