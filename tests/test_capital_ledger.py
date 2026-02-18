"""Tests for deterministic capital ledger."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from octa.accounting.capital_ledger import CapitalLedger, LedgerEntry, VALID_EVENT_TYPES


@pytest.fixture()
def ledger(tmp_path: Path) -> CapitalLedger:
    return CapitalLedger(tmp_path / "test_ledger.jsonl")


def test_append_and_read(ledger: CapitalLedger) -> None:
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    entry = ledger.append("shareholder_equity_in", 50000.0, description="Initial equity", ts=ts)
    assert entry.index == 1
    assert entry.event_type == "shareholder_equity_in"
    assert entry.amount == 50000.0
    assert entry.prev_hash == "GENESIS"
    assert entry.entry_hash

    entries = ledger.read_all()
    assert len(entries) == 1
    assert entries[0]["amount"] == 50000.0


def test_hash_chain_links(ledger: CapitalLedger) -> None:
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    e1 = ledger.append("shareholder_equity_in", 50000.0, ts=ts)
    e2 = ledger.append("broker_funding_in", 50000.0, ts=ts)
    e3 = ledger.append("fees", 100.0, ts=ts)

    assert e2.prev_hash == e1.entry_hash
    assert e3.prev_hash == e2.entry_hash
    assert ledger.verify() is True


def test_verify_empty(ledger: CapitalLedger) -> None:
    assert ledger.verify() is True


def test_verify_tamper_detection(ledger: CapitalLedger) -> None:
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ledger.append("shareholder_equity_in", 50000.0, ts=ts)
    ledger.append("fees", 100.0, ts=ts)
    assert ledger.verify() is True

    # Tamper: change amount in second entry
    lines = ledger.path.read_text(encoding="utf-8").splitlines()
    record = json.loads(lines[1])
    record["amount"] = 999999.0
    lines[1] = json.dumps(record, sort_keys=True)
    ledger.path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    assert ledger.verify() is False


def test_reconcile_basic(ledger: CapitalLedger) -> None:
    ts = datetime(2026, 1, 15, tzinfo=timezone.utc)
    ledger.append("shareholder_equity_in", 100000.0, ts=ts)
    ledger.append("broker_funding_in", 50000.0, ts=ts)
    ledger.append("fees", 500.0, ts=ts)
    ledger.append("pnl_realized", 2000.0, ts=ts)
    ledger.append("pnl_unrealized_snapshot", 1500.0, ts=ts)

    result = ledger.reconcile(as_of="2026-12-31")
    assert result.total_inflows == 150000.0
    assert result.total_outflows == 0.0
    assert result.fees_total == 500.0
    assert result.pnl_realized == 2000.0
    assert result.pnl_unrealized == 1500.0
    assert result.net_capital == 153000.0  # 150000 - 500 + 2000 + 1500
    assert result.integrity_ok is True
    assert result.entry_count == 5


def test_reconcile_with_outflows(ledger: CapitalLedger) -> None:
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ledger.append("shareholder_equity_in", 100000.0, ts=ts)
    ledger.append("broker_funding_out", -20000.0, ts=ts)

    result = ledger.reconcile(as_of="2026-12-31")
    assert result.total_inflows == 100000.0
    assert result.total_outflows == 20000.0
    assert result.net_capital == 80000.0


def test_reconcile_date_filter(ledger: CapitalLedger) -> None:
    ledger.append("shareholder_equity_in", 100000.0, ts=datetime(2026, 1, 1, tzinfo=timezone.utc))
    ledger.append("pnl_realized", 5000.0, ts=datetime(2026, 6, 1, tzinfo=timezone.utc))
    ledger.append("pnl_realized", 3000.0, ts=datetime(2026, 12, 1, tzinfo=timezone.utc))

    # As of mid-year: only first two entries
    result = ledger.reconcile(as_of="2026-06-30")
    assert result.pnl_realized == 5000.0
    assert result.net_capital == 105000.0


def test_invalid_event_type(ledger: CapitalLedger) -> None:
    with pytest.raises(ValueError, match="Unknown event type"):
        ledger.append("BOGUS", 100.0)


def test_all_event_types_accepted(ledger: CapitalLedger) -> None:
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for et in sorted(VALID_EVENT_TYPES):
        ledger.append(et, 100.0, ts=ts)
    assert len(ledger.read_all()) == len(VALID_EVENT_TYPES)
    assert ledger.verify() is True


def test_metadata_preserved(ledger: CapitalLedger) -> None:
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    entry = ledger.append(
        "fees", 50.0, metadata={"broker": "IBKR", "fee_type": "commission"}, ts=ts
    )
    assert entry.metadata["broker"] == "IBKR"
    entries = ledger.read_all()
    assert entries[0]["metadata"]["fee_type"] == "commission"


def test_reconcile_empty(ledger: CapitalLedger) -> None:
    result = ledger.reconcile()
    assert result.entry_count == 0
    assert result.net_capital == 0.0
    assert result.integrity_ok is True
