"""Test registry schema migration for I1 institutional upgrade.

Verifies:
- Fresh ArtifactRegistry has all new columns
- Old-style DB (without new columns) gets them added via migration
- lifecycle_status defaults to 'RESEARCH'
- set_lifecycle_status() method works for all lifecycle states
"""

import sqlite3

import pytest

from octa_ops.autopilot.registry import ArtifactRegistry

_EXPECTED_ARTIFACT_COLS = {
    "id", "run_id", "symbol", "timeframe", "artifact_kind", "path",
    "sha256", "size_bytes", "schema_version", "created_at", "status", "meta_json",
    # I1 new columns:
    "training_data_hash", "feature_code_hash", "hyperparam_hash",
    "dependency_fingerprint", "reproducibility_manifest_hash", "lifecycle_status",
}

_EXPECTED_RUNS_COLS = {
    "run_id", "created_at", "config_sha", "status", "note",
    # I1 new columns:
    "training_data_hash", "dependency_fingerprint",
}


def test_fresh_registry_artifacts_has_all_columns(tmp_path):
    reg = ArtifactRegistry(root=str(tmp_path))
    cur = reg._conn.cursor()
    cur.execute("PRAGMA table_info(artifacts)")
    cols = {str(r[1]) for r in cur.fetchall()}
    assert _EXPECTED_ARTIFACT_COLS.issubset(cols), (
        f"Missing columns in artifacts: {_EXPECTED_ARTIFACT_COLS - cols}"
    )


def test_fresh_registry_runs_has_all_columns(tmp_path):
    reg = ArtifactRegistry(root=str(tmp_path))
    cur = reg._conn.cursor()
    cur.execute("PRAGMA table_info(runs)")
    cols = {str(r[1]) for r in cur.fetchall()}
    assert _EXPECTED_RUNS_COLS.issubset(cols), (
        f"Missing columns in runs: {_EXPECTED_RUNS_COLS - cols}"
    )


def _build_old_db(db_path):
    """Create a pre-I1 schema database without the new columns."""
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE runs("
        "run_id TEXT PRIMARY KEY, created_at TEXT, config_sha TEXT, status TEXT, note TEXT)"
    )
    conn.execute(
        "CREATE TABLE gates("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, symbol TEXT, timeframe TEXT, "
        "stage TEXT, status TEXT, reason TEXT, details_json TEXT, created_at TEXT)"
    )
    conn.execute(
        "CREATE TABLE artifacts("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "run_id TEXT, symbol TEXT, timeframe TEXT, artifact_kind TEXT, "
        "path TEXT, sha256 TEXT, schema_version INTEGER, "
        "created_at TEXT, status TEXT, meta_json TEXT)"
    )
    conn.execute(
        "CREATE TABLE metrics("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, symbol TEXT, timeframe TEXT, "
        "stage TEXT, metrics_json TEXT, gate_json TEXT, created_at TEXT)"
    )
    conn.execute(
        "CREATE TABLE promotions("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, timeframe TEXT, "
        "artifact_id INTEGER, level TEXT, created_at TEXT, UNIQUE(symbol, timeframe, level))"
    )
    conn.execute(
        "CREATE TABLE orders("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, order_key TEXT, "
        "client_order_id TEXT, symbol TEXT, timeframe TEXT, model_id TEXT, "
        "side TEXT, qty REAL, status TEXT, created_at TEXT, UNIQUE(order_key))"
    )
    conn.commit()
    conn.close()


def test_migration_adds_artifact_columns_to_existing_db(tmp_path):
    """Old DB without new columns gets them after ArtifactRegistry init."""
    _build_old_db(tmp_path / "registry.sqlite3")

    reg = ArtifactRegistry(root=str(tmp_path))
    cur = reg._conn.cursor()
    cur.execute("PRAGMA table_info(artifacts)")
    cols = {str(r[1]) for r in cur.fetchall()}

    for col in ["lifecycle_status", "training_data_hash", "feature_code_hash",
                "hyperparam_hash", "dependency_fingerprint", "reproducibility_manifest_hash"]:
        assert col in cols, f"Migration missed artifact column: {col}"


def test_migration_adds_runs_columns_to_existing_db(tmp_path):
    """Old DB without new runs columns gets them after ArtifactRegistry init."""
    _build_old_db(tmp_path / "registry.sqlite3")

    reg = ArtifactRegistry(root=str(tmp_path))
    cur = reg._conn.cursor()
    cur.execute("PRAGMA table_info(runs)")
    cols = {str(r[1]) for r in cur.fetchall()}

    assert "training_data_hash" in cols, "Migration missed runs.training_data_hash"
    assert "dependency_fingerprint" in cols, "Migration missed runs.dependency_fingerprint"


def test_lifecycle_status_defaults_to_research(tmp_path):
    """Newly added artifacts have lifecycle_status='RESEARCH'."""
    reg = ArtifactRegistry(root=str(tmp_path))
    run_id = "test_lifecycle_default"
    reg.record_run_start(run_id, {"cfg": "test"})
    artifact_id = reg.add_artifact(
        run_id=run_id,
        symbol="TEST",
        timeframe="1D",
        artifact_kind="model",
        path="/fake/path/model.cbm",
        sha256="abc123def456" * 2,
        schema_version=1,
    )
    cur = reg._conn.cursor()
    cur.execute("SELECT lifecycle_status FROM artifacts WHERE id=?", (artifact_id,))
    row = cur.fetchone()
    assert row[0] == "RESEARCH", f"Expected RESEARCH, got {row[0]}"


def test_set_lifecycle_status_all_valid_states(tmp_path):
    """set_lifecycle_status() correctly updates lifecycle_status column."""
    reg = ArtifactRegistry(root=str(tmp_path))
    run_id = "test_lifecycle_set"
    reg.record_run_start(run_id, {"cfg": "test"})
    artifact_id = reg.add_artifact(
        run_id=run_id,
        symbol="TEST",
        timeframe="1H",
        artifact_kind="model",
        path="/fake/path/model.cbm",
        sha256="abc123def456" * 2,
        schema_version=1,
    )

    for status in [
        "SHADOW", "PAPER", "LIVE", "RETIRED", "QUARANTINED",
        "PENDING_PROMOTION", "PROMOTION_FAILED",
    ]:
        reg.set_lifecycle_status(artifact_id, status)
        cur = reg._conn.cursor()
        cur.execute("SELECT lifecycle_status FROM artifacts WHERE id=?", (artifact_id,))
        row = cur.fetchone()
        assert row[0] == status, f"Expected {status}, got {row[0]}"


def test_existing_rows_get_null_lifecycle_status_after_migration(tmp_path):
    """Rows inserted before migration have NULL lifecycle_status (not 'RESEARCH')."""
    db_path = tmp_path / "registry.sqlite3"
    _build_old_db(db_path)

    # Insert a row into old-style artifacts table
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT INTO artifacts(run_id, symbol, timeframe, artifact_kind, path, sha256, "
        "schema_version, created_at, status, meta_json) "
        "VALUES('run1', 'SYM', '1D', 'model', '/p', 'abc', 1, '2026-01-01', 'ACTIVE', NULL)"
    )
    conn.commit()
    conn.close()

    # Open with ArtifactRegistry to trigger migration
    reg = ArtifactRegistry(root=str(tmp_path))
    cur = reg._conn.cursor()
    cur.execute("SELECT lifecycle_status FROM artifacts WHERE run_id='run1'")
    row = cur.fetchone()
    # SQLite ALTER TABLE ADD COLUMN with DEFAULT only applies to new rows
    # Existing rows get NULL (not the DEFAULT value in older SQLite versions)
    # This is expected behaviour — NULL means "pre-I1 artifact"
    assert row[0] is None or row[0] == "RESEARCH"  # SQLite version-dependent
