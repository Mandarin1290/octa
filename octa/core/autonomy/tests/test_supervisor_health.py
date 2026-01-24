from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from octa.core.autonomy.health import HealthLevel, check_audit_writable, check_provider, summarize_health
from octa.core.data.providers.in_memory import InMemoryOHLCVProvider
from octa.core.data.providers.ohlcv import OHLCVBar


def test_health_report_ok(tmp_path: Path) -> None:
    provider = InMemoryOHLCVProvider()
    start = datetime(2024, 1, 1)
    bars = [
        OHLCVBar(
            ts=start + timedelta(days=i),
            open=1,
            high=2,
            low=1,
            close=1.5,
            volume=100,
        )
        for i in range(20)
    ]
    provider.set_bars("AAA", "1D", bars)

    data = check_provider(provider, "AAA", "1D")
    audit = check_audit_writable(tmp_path / "audit.jsonl")
    report = summarize_health([data, audit])

    assert report.overall == HealthLevel.OK
    assert report.recommended_mode == "NORMAL"
