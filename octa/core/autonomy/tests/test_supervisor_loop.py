from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from octa.core.autonomy.supervisor import AutonomySupervisor, SupervisorConfig
from octa.core.data.providers.in_memory import InMemoryOHLCVProvider
from octa.core.data.providers.ohlcv import OHLCVBar


def test_supervisor_loop_records_events(tmp_path: Path) -> None:
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

    calls = {"count": 0}

    def fake_pipeline(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("fail")
        return None

    config = SupervisorConfig(max_cycles=2, sleep_fn=lambda _: None)
    supervisor = AutonomySupervisor(
        audit_dir=tmp_path,
        provider=provider,
        run_pipeline=fake_pipeline,
        config=config,
    )

    supervisor.run(symbols=["AAA"], start=start, end=None)

    events_path = tmp_path / "autonomy_events.jsonl"
    content = events_path.read_text(encoding="utf-8")
    assert "RUN_FAIL" in content
    assert "RUN_SUCCESS" in content
