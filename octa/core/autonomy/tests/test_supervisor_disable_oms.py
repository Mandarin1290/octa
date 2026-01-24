from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from octa.core.autonomy.supervisor import AutonomySupervisor, SupervisorConfig
from octa.core.data.providers.in_memory import InMemoryOHLCVProvider
from octa.core.data.providers.ohlcv import OHLCVBar


def test_supervisor_disable_oms(tmp_path: Path) -> None:
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

    modes: list[str] = []

    def fake_pipeline(*args, **kwargs):
        mode = kwargs.get("execution_mode", "OMS")
        modes.append(mode)
        if len(modes) == 1:
            raise RuntimeError("OMS_FAILURE")
        return None

    config = SupervisorConfig(max_cycles=2, sleep_fn=lambda _: None)
    supervisor = AutonomySupervisor(
        audit_dir=tmp_path,
        provider=provider,
        run_pipeline=fake_pipeline,
        config=config,
    )

    supervisor.run(symbols=["AAA"], start=start, end=None)

    assert modes[0] == "OMS"
    assert "DECISIONS_ONLY" in modes
