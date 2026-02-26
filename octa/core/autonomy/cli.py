from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path

from octa.core.autonomy.supervisor import AutonomySupervisor, SupervisorConfig
from octa.core.data.providers.in_memory import InMemoryOHLCVProvider
from octa.core.data.providers.ohlcv import OHLCVBar, Timeframe
from octa.core.governance.audit_chain import AuditChain
from octa.core.pipeline.paper_run import run_paper_cascade


def _populate_provider(provider: InMemoryOHLCVProvider, symbols: list[str], bars: int) -> None:
    start = datetime(2024, 1, 1)
    steps: dict[Timeframe, timedelta] = {
        "1D": timedelta(days=1),
        "30M": timedelta(minutes=30),
        "1H": timedelta(hours=1),
        "5M": timedelta(minutes=5),
        "1M": timedelta(minutes=1),
    }
    for symbol in symbols:
        price = 100.0
        for timeframe, step in steps.items():
            series: list[OHLCVBar] = []
            price = 100.0
            for idx in range(bars):
                ts = start + step * idx
                series.append(
                    OHLCVBar(ts=ts, open=price, high=price * 1.01, low=price * 0.99, close=price, volume=10_000)
                )
                price *= 1.001
            provider.set_bars(symbol, timeframe, series)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run OCTA autonomy supervisor.")
    parser.add_argument("--symbols", required=True, help="Comma-separated symbols")
    parser.add_argument("--cycles", type=int, default=1)
    parser.add_argument("--bars", type=int, default=500)
    parser.add_argument("--audit-dir", type=Path, default=Path("audit"))
    parser.add_argument("--audit-chain", type=Path, default=None)
    args = parser.parse_args()

    symbols = [symbol.strip() for symbol in args.symbols.split(",") if symbol.strip()]
    provider = InMemoryOHLCVProvider()
    _populate_provider(provider, symbols, args.bars)

    audit_chain = AuditChain(args.audit_chain) if args.audit_chain else None
    supervisor = AutonomySupervisor(
        audit_dir=args.audit_dir,
        provider=provider,
        run_pipeline=run_paper_cascade,
        audit_chain=audit_chain,
        config=SupervisorConfig(max_cycles=args.cycles),
    )
    supervisor.run(symbols=symbols, start=datetime(2024, 1, 1), end=None)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
