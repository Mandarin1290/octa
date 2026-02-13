from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from octa.support.ops.train_universe import REQUIRED_TFS, Thresholds, build_worklist
from octa.support.ops.training_state import TrainingStateRecord, TrainingStateStore


def _make_record(
    symbol: str,
    timeframe: str,
    config_hash: str,
    pipeline_version: str,
    status: str,
    artifact_paths: list[str],
    metrics_hash: str = "hash",
    last_train_end_utc: str | None = None,
) -> TrainingStateRecord:
    return TrainingStateRecord(
        symbol=symbol,
        timeframe=timeframe,
        config_hash=config_hash,
        pipeline_version=pipeline_version,
        status=status,
        last_train_end_utc=last_train_end_utc,
        metrics_hash=metrics_hash,
        artifact_paths=artifact_paths,
        report_path=None,
        drift_flag=False,
        last_data_mtime=None,
        reason=None,
    )


def test_resume_skips_passed_stages_after_restart(tmp_path: Path) -> None:
    state_path = tmp_path / "state.jsonl"
    store = TrainingStateStore(state_path)
    config_hash = "abc"
    version = "0.0.0"

    artifacts = []
    for tf in REQUIRED_TFS:
        art = tmp_path / f"artifact_{tf}.txt"
        art.write_text("ok", encoding="utf-8")
        artifacts.append(str(art))

    for tf in REQUIRED_TFS:
        rec = _make_record("AAA", tf, config_hash, version, "PASS_FULL", artifacts)
        store.upsert(rec)
    store.save()

    inventory = {"AAA": {tf: [str(tmp_path / f"AAA_{tf}.parquet")] for tf in REQUIRED_TFS}}
    now = datetime.now(timezone.utc)
    buckets = build_worklist(["AAA"], inventory, store, config_hash, version, Thresholds(3600, 3600), now)

    assert len(buckets["skip"]) == 1
    assert buckets["skip"][0].symbol == "AAA"


def test_partial_cascade_resumes_only_missing_tfs(tmp_path: Path) -> None:
    state_path = tmp_path / "state.jsonl"
    store = TrainingStateStore(state_path)
    config_hash = "abc"
    version = "0.0.0"

    art = tmp_path / "artifact.txt"
    art.write_text("ok", encoding="utf-8")

    for tf in ("1D", "1H"):
        rec = _make_record("BBB", tf, config_hash, version, "PASS_FULL", [str(art)])
        store.upsert(rec)
    store.save()

    inventory = {"BBB": {tf: [str(tmp_path / f"BBB_{tf}.parquet")] for tf in REQUIRED_TFS}}
    now = datetime.now(timezone.utc)
    buckets = build_worklist(["BBB"], inventory, store, config_hash, version, Thresholds(3600, 3600), now)

    assert len(buckets["partial"]) == 1
    decision = buckets["partial"][0]
    assert decision.needed_tfs == ["30M", "5M", "1M"]


def test_config_hash_change_forces_retrain(tmp_path: Path) -> None:
    state_path = tmp_path / "state.jsonl"
    store = TrainingStateStore(state_path)
    old_hash = "old"
    new_hash = "new"
    version = "0.0.0"

    art = tmp_path / "artifact.txt"
    art.write_text("ok", encoding="utf-8")

    for tf in REQUIRED_TFS:
        rec = _make_record("CCC", tf, old_hash, version, "PASS_FULL", [str(art)])
        store.upsert(rec)
    store.save()

    inventory = {"CCC": {tf: [str(tmp_path / f"CCC_{tf}.parquet")] for tf in REQUIRED_TFS}}
    now = datetime.now(timezone.utc)
    buckets = build_worklist(["CCC"], inventory, store, new_hash, version, Thresholds(3600, 3600), now)

    assert len(buckets["stale_config"]) == 1
    assert buckets["stale_config"][0].symbol == "CCC"


def test_new_data_threshold_forces_retrain(tmp_path: Path) -> None:
    state_path = tmp_path / "state.jsonl"
    store = TrainingStateStore(state_path)
    config_hash = "abc"
    version = "0.0.0"

    art = tmp_path / "artifact.txt"
    art.write_text("ok", encoding="utf-8")

    last_end = (datetime.now(timezone.utc) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    for tf in REQUIRED_TFS:
        rec = _make_record("DDD", tf, config_hash, version, "PASS_FULL", [str(art)], last_train_end_utc=last_end)
        store.upsert(rec)
    store.save()

    inventory = {"DDD": {tf: [str(tmp_path / f"DDD_{tf}.parquet")] for tf in REQUIRED_TFS}}
    # Set file mtimes to now to trigger new data threshold
    for tf in REQUIRED_TFS:
        p = Path(inventory["DDD"][tf][0])
        p.write_text("", encoding="utf-8")
        p.touch()

    now = datetime.now(timezone.utc)
    buckets = build_worklist(["DDD"], inventory, store, config_hash, version, Thresholds(3600, 60), now)

    assert len(buckets["stale_data"]) == 1
    assert buckets["stale_data"][0].symbol == "DDD"
