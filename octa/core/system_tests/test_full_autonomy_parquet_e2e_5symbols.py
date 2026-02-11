from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from octa.core.autonomy.supervisor import AutonomySupervisor, SupervisorConfig
from octa.core.data.providers.ohlcv import OHLCVBar, Timeframe
from octa.core.data.providers.parquet import (
    ParquetOHLCVProvider,
    find_raw_root,
    infer_symbol_categories,
    pick_5_diverse_complete_symbols,
)
from octa.core.governance.audit_chain import AuditChain
from octa.core.pipeline.paper_run import run_paper_cascade

pytestmark = pytest.mark.slow


def test_full_autonomy_parquet_e2e_5symbols(tmp_path: Path) -> None:
    raw_root = find_raw_root()
    base_provider = ParquetOHLCVProvider(raw_root)
    symbols = pick_5_diverse_complete_symbols(base_provider)
    categories = infer_symbol_categories(base_provider, symbols)
    provider = _LimitedProvider(base_provider, limit=240)

    audit_dir = tmp_path / "audit_e2e_5sym"
    audit_chain = AuditChain(audit_dir / "audit_chain.jsonl")
    supervisor = AutonomySupervisor(
        audit_dir=audit_dir,
        provider=provider,
        run_pipeline=run_paper_cascade,
        audit_chain=audit_chain,
        config=SupervisorConfig(max_cycles=1, execution_mode="DECISIONS_ONLY"),
    )

    end = _resolve_end(base_provider, symbols[0])
    supervisor.run(symbols=symbols, start=None, end=end)

    paper_run_path = audit_dir / "paper_run.jsonl"
    assert paper_run_path.exists()
    records = _load_jsonl(paper_run_path)

    gate_names = {"global_regime", "structure", "signal", "execution", "micro"}
    gate_records = [record for record in records if record.get("gate") in gate_names]
    for symbol in symbols:
        per_symbol = [record for record in gate_records if record.get("symbol") == symbol]
        assert len(per_symbol) == 5, f"missing gate records for {symbol} categories={categories}"

    assert any(record.get("gate") == "portfolio" for record in records)
    assert any(
        record.get("gate") == "oms" and record.get("decision") == "EXEC_DISABLED"
        for record in records
    )
    assert all(record.get("execution_mode") == "DECISIONS_ONLY" for record in records)

    assert (audit_dir / "audit_chain.jsonl").exists()
    assert audit_chain.verify()
    chain_records = _load_jsonl(audit_dir / "audit_chain.jsonl")
    assert any(record.get("payload", {}).get("event") == "autonomy" for record in chain_records)

    _assert_category_diversity(base_provider, symbols, categories)


def _assert_category_diversity(
    provider: ParquetOHLCVProvider, symbols: list[str], categories: dict[str, str]
) -> None:
    complete_symbols = [
        symbol for symbol in provider.list_symbols() if _has_full_coverage(provider, symbol)
    ]
    available = infer_symbol_categories(provider, complete_symbols)
    available_categories = {category for category in available.values() if category != "UNKNOWN"}
    chosen_categories = {categories.get(symbol, "UNKNOWN") for symbol in symbols if categories.get(symbol)}
    if not available_categories:
        return
    expected = min(len(available_categories), len(symbols))
    assert len({cat for cat in chosen_categories if cat != "UNKNOWN"}) >= expected


def _has_full_coverage(provider: ParquetOHLCVProvider, symbol: str) -> bool:
    return all(provider.has_timeframe(symbol, tf) for tf in ("1D", "30M", "1H", "5M", "1M"))


def _resolve_end(provider: ParquetOHLCVProvider, symbol: str):
    bars = provider.get_ohlcv(symbol, "1H", limit=240)
    if bars:
        return bars[-1].ts
    return None


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
