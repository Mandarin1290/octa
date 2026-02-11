from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from octa.core.autonomy.supervisor import AutonomySupervisor, SupervisorConfig
from octa.core.data.providers.parquet import ParquetOHLCVProvider, find_raw_root, pick_3_complete_symbols
from octa.core.data.providers.ohlcv import OHLCVBar, Timeframe
from octa.core.governance.audit_chain import AuditChain
from octa.core.pipeline.paper_run import run_paper_cascade

pytestmark = pytest.mark.slow


def test_full_autonomy_parquet_e2e(tmp_path: Path) -> None:
    raw_root = find_raw_root()
    base_provider = ParquetOHLCVProvider(raw_root)
    symbols = pick_3_complete_symbols(base_provider)
    provider = _LimitedProvider(base_provider, limit=240)

    audit_dir = tmp_path / "audit"
    audit_chain = AuditChain(audit_dir / "audit_chain.jsonl")
    supervisor = AutonomySupervisor(
        audit_dir=audit_dir,
        provider=provider,
        run_pipeline=run_paper_cascade,
        audit_chain=audit_chain,
        config=SupervisorConfig(max_cycles=1, execution_mode="DECISIONS_ONLY"),
    )

    supervisor.run(symbols=symbols, start=None, end=None)

    paper_run_path = audit_dir / "paper_run.jsonl"
    assert paper_run_path.exists()
    records = _load_jsonl(paper_run_path)

    gate_names = {"global_regime", "structure", "signal", "execution", "micro"}
    gate_records = [record for record in records if record.get("gate") in gate_names]
    for symbol in symbols:
        per_symbol = [record for record in gate_records if record.get("symbol") == symbol]
        assert len(per_symbol) == 5

    assert any(record.get("gate") == "portfolio" for record in records)
    assert any(
        record.get("gate") == "oms" and record.get("decision") == "EXEC_DISABLED"
        for record in records
    )
    assert all(record.get("execution_mode") == "DECISIONS_ONLY" for record in records)

    chain_path = audit_dir / "audit_chain.jsonl"
    assert chain_path.exists()
    assert audit_chain.verify()
    chain_records = _load_jsonl(chain_path)
    assert any(record.get("payload", {}).get("event") == "autonomy" for record in chain_records)


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


class _LimitedProvider:
    def __init__(self, provider: ParquetOHLCVProvider, limit: int) -> None:
        self._provider = provider
        self._limit = limit

    def get_ohlcv(
        self,
        symbol: str,
        timeframe: Timeframe,
        start=None,
        end=None,
        limit: int | None = None,
    ) -> list[OHLCVBar]:
        effective_limit = self._limit if limit is None else min(limit, self._limit)
        return list(self._provider.get_ohlcv(symbol, timeframe, start, end, effective_limit))
