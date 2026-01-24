from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from octa.core.autonomy.supervisor import AutonomySupervisor, SupervisorConfig
from octa.core.data.providers.in_memory import InMemoryOHLCVProvider
from octa.core.data.providers.ohlcv import OHLCVBar
from octa.core.governance.audit_chain import AuditChain


def test_supervisor_audit_chain(tmp_path: Path) -> None:
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

    def fake_pipeline(*args, **kwargs):
        return None

    chain = AuditChain(tmp_path / "audit_chain.jsonl")
    supervisor = AutonomySupervisor(
        audit_dir=tmp_path,
        provider=provider,
        run_pipeline=fake_pipeline,
        audit_chain=chain,
        config=SupervisorConfig(max_cycles=1, sleep_fn=lambda _: None),
    )

    supervisor.run(symbols=["AAA"], start=start, end=None)

    assert chain.verify() is True
    assert (tmp_path / "audit_chain.jsonl").exists()
