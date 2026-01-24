import json
from pathlib import Path

import pytest

from octa.core.runtime.run_registry import RunRegistry


duckdb = pytest.importorskip("duckdb")


def test_run_registry_smoke(tmp_path: Path) -> None:
    db_path = tmp_path / "metrics.duckdb"
    registry = RunRegistry(db_path)
    registry.record_run_start(run_id="r1", config={"foo": "bar"}, git_sha="abc")
    registry.write_survivors(
        run_id="r1",
        layer="L1_global_1D",
        rows=[
            {
                "symbol": "AAA",
                "timeframe": "1D",
                "decision": "PASS",
                "reason_json": json.dumps({"reason": "ok"}),
            }
        ],
    )
    registry.emit_metric(
        run_id="r1",
        layer="L1_global_1D",
        symbol="AAA",
        timeframe="1D",
        key="vol_ann",
        value=0.2,
    )
    registry.emit_event(
        run_id="r1",
        severity="INFO",
        component="test",
        message="ok",
        payload={"k": 1},
    )
    registry.record_run_end(run_id="r1", status="COMPLETED")

    with duckdb.connect(str(db_path)) as conn:
        runs = conn.execute("SELECT count(*) FROM runs").fetchone()[0]
        survivors = conn.execute("SELECT count(*) FROM survivors").fetchone()[0]
        metrics = conn.execute("SELECT count(*) FROM metrics").fetchone()[0]
        events = conn.execute("SELECT count(*) FROM events").fetchone()[0]

    assert runs == 1
    assert survivors == 1
    assert metrics == 1
    assert events == 1

