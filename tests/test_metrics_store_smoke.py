from pathlib import Path

import pytest

from octa.core.monitoring import store


duckdb = pytest.importorskip("duckdb")


def test_metrics_store_smoke(tmp_path: Path) -> None:
    db_path = tmp_path / "metrics.duckdb"
    store.ensure_db(db_path)
    with store.connect(db_path) as conn:
        tables = conn.execute("SHOW TABLES").fetchall()
    names = {row[0] for row in tables}
    assert {"runs", "survivors", "metrics", "events"}.issubset(names)

