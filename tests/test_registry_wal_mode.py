"""Tests for C3: ArtifactRegistry SQLite WAL mode.

Verifies:
1) Fresh registry opens with journal_mode=wal
2) Reopening existing registry preserves WAL mode
3) synchronous=NORMAL (1) set correctly
4) busy_timeout=5000 ms set correctly
5) WAL mode idempotent: calling _apply_wal_pragmas() twice does not raise
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from octa_ops.autopilot.registry import ArtifactRegistry


def _get_pragma(conn: sqlite3.Connection, name: str) -> str:
    return conn.execute(f"PRAGMA {name};").fetchone()[0]


def test_fresh_registry_uses_wal_mode(tmp_path: Path) -> None:
    """A new ArtifactRegistry must open the database in WAL journal mode."""
    reg = ArtifactRegistry(root=str(tmp_path / "reg"))
    mode = _get_pragma(reg._conn, "journal_mode")
    assert mode.lower() == "wal", f"Expected WAL, got {mode!r}"


def test_registry_synchronous_normal(tmp_path: Path) -> None:
    """synchronous must be NORMAL (1) for write performance under WAL."""
    reg = ArtifactRegistry(root=str(tmp_path / "reg"))
    sync = int(_get_pragma(reg._conn, "synchronous"))
    assert sync == 1, f"Expected synchronous=1 (NORMAL), got {sync}"


def test_registry_busy_timeout(tmp_path: Path) -> None:
    """busy_timeout must be 5000 ms to handle concurrent access."""
    reg = ArtifactRegistry(root=str(tmp_path / "reg"))
    bt = int(_get_pragma(reg._conn, "busy_timeout"))
    assert bt == 5000, f"Expected busy_timeout=5000, got {bt}"


def test_reopen_registry_retains_wal_mode(tmp_path: Path) -> None:
    """WAL mode persists in the DB file; reopening must still be WAL."""
    root = tmp_path / "reg"
    reg1 = ArtifactRegistry(root=str(root))
    reg1._conn.close()

    reg2 = ArtifactRegistry(root=str(root))
    mode = _get_pragma(reg2._conn, "journal_mode")
    assert mode.lower() == "wal", f"Reopened registry not WAL: {mode!r}"


def test_apply_wal_pragmas_idempotent(tmp_path: Path) -> None:
    """Calling _apply_wal_pragmas() twice must not raise."""
    reg = ArtifactRegistry(root=str(tmp_path / "reg"))
    reg._apply_wal_pragmas()
    reg._apply_wal_pragmas()
    mode = _get_pragma(reg._conn, "journal_mode")
    assert mode.lower() == "wal"
