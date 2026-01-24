from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from octa.core.data.providers.in_memory import InMemoryOHLCVProvider
from octa.core.data.providers.ohlcv import OHLCVBar
from octa.core.pipeline.paper_run import run_paper_cascade


def _bars(start: datetime, count: int, step: timedelta, price: float) -> list[OHLCVBar]:
    bars: list[OHLCVBar] = []
    for idx in range(count):
        ts = start + step * idx
        bars.append(
            OHLCVBar(ts=ts, open=price, high=price * 1.01, low=price * 0.99, close=price, volume=10_000)
        )
        price *= 1.001
    return bars


def test_paper_run_creates_audit(tmp_path: Path) -> None:
    provider = InMemoryOHLCVProvider()
    symbols = ["AAA", "BBB"]
    start = datetime(2024, 1, 1)
    steps = {
        "1D": timedelta(days=1),
        "30M": timedelta(minutes=30),
        "1H": timedelta(hours=1),
        "5M": timedelta(minutes=5),
        "1M": timedelta(minutes=1),
    }

    for symbol in symbols:
        for timeframe, step in steps.items():
            provider.set_bars(symbol, timeframe, _bars(start, 220, step, 100.0))

    audit_path = tmp_path / "audit.jsonl"
    run_paper_cascade(symbols, provider, start, None, audit_path)

    assert audit_path.exists()
    summary_path = audit_path.parent / "performance_summary.json"
    assert summary_path.exists()
    lines = audit_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == len(symbols) * 5 + 1
    assert any('"gate": "portfolio"' in line for line in lines)
    assert all('"execution_mode": "OMS"' in line for line in lines)

    records = [line for line in lines if '\"gate\": \"portfolio\"' not in line]
    for symbol in symbols:
        symbol_records = [
            line for line in records if f'"symbol": "{symbol}"' in line
        ]
        assert len(symbol_records) == 5
        expected_order = ["global_regime", "structure", "signal", "execution", "micro"]
        for idx, gate in enumerate(expected_order):
            assert f'"gate": "{gate}"' in symbol_records[idx]
            assert '"decision"' in symbol_records[idx]
            assert '"artifacts"' in symbol_records[idx]


def test_paper_run_decisions_only(tmp_path: Path) -> None:
    provider = InMemoryOHLCVProvider()
    symbols = ["AAA"]
    start = datetime(2024, 1, 1)
    steps = {
        "1D": timedelta(days=1),
        "30M": timedelta(minutes=30),
        "1H": timedelta(hours=1),
        "5M": timedelta(minutes=5),
        "1M": timedelta(minutes=1),
    }

    for symbol in symbols:
        for timeframe, step in steps.items():
            provider.set_bars(symbol, timeframe, _bars(start, 220, step, 100.0))

    audit_path = tmp_path / "audit.jsonl"
    run_paper_cascade(symbols, provider, start, None, audit_path, execution_mode="DECISIONS_ONLY")

    lines = audit_path.read_text(encoding="utf-8").strip().splitlines()
    assert any('"gate": "oms"' in line for line in lines)
    assert any('"decision": "EXEC_DISABLED"' in line for line in lines)
    assert all('"execution_mode": "DECISIONS_ONLY"' in line for line in lines)
