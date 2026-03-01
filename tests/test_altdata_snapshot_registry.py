"""Tests for AltDataSnapshotRegistry.

Covers:
- resolve_and_write() writes correct manifest with sha256/row_count
- Missing cache → status="missing" in manifest, no exception
- Idempotency: second call returns existing manifest, no overwrite
- read_manifest() / list_available_snapshots() / get_latest_snapshot_id()
- snapshot_id format: YYYYMMDDThhmmssZ
"""
from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path

import pytest

from octa.core.data.sources.altdata.cache import write_snapshot
from octa.core.data.sources.altdata.snapshot_registry import (
    get_latest_snapshot_id,
    list_available_snapshots,
    read_manifest,
    resolve_and_write,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _seed_fred(tmp_path: Path, asof: date) -> str:
    """Write a minimal FRED snapshot and return expected sha256."""
    payload = {"series": {"FEDFUNDS": [{"ts": "2026-02-10", "value": 5.33},
                                        {"ts": "2026-02-09", "value": 5.33}],
                          "DGS10":    [{"ts": "2026-02-10", "value": 4.21}]}}
    write_snapshot(source="fred", asof=asof, payload=payload,
                   meta={"seed": True}, root=str(tmp_path / "altdata"))
    p = tmp_path / "altdata" / "fred" / asof.isoformat() / f"fred_{asof.isoformat()}.json"
    return hashlib.sha256(p.read_bytes()).hexdigest()


def _seed_gdelt(tmp_path: Path, asof: date) -> str:
    payload = {"event_risk": 0.12, "rows": [{"event": "conflict", "tone": -2.1}]}
    write_snapshot(source="gdelt", asof=asof, payload=payload,
                   meta={"seed": True}, root=str(tmp_path / "altdata"))
    p = tmp_path / "altdata" / "gdelt" / asof.isoformat() / f"gdelt_{asof.isoformat()}.json"
    return hashlib.sha256(p.read_bytes()).hexdigest()


# ---------------------------------------------------------------------------
# Test: resolve_and_write writes correct manifest
# ---------------------------------------------------------------------------

def test_resolve_and_write_creates_manifest(tmp_path):
    asof = date(2026, 2, 10)
    fred_hash = _seed_fred(tmp_path, asof)
    gdelt_hash = _seed_gdelt(tmp_path, asof)

    snap_id, manifest_path = resolve_and_write(
        asof,
        cache_root=str(tmp_path / "altdata"),
        snapshots_root=str(tmp_path / "snapshots"),
        sources=["fred", "gdelt"],
    )

    assert manifest_path.exists()
    m = json.loads(manifest_path.read_text())
    assert m["snapshot_id"] == snap_id
    assert m["asof_date"] == "2026-02-10"
    assert m["sources"]["fred"]["status"] == "ok"
    assert m["sources"]["fred"]["sha256"] == fred_hash
    assert m["sources"]["fred"]["row_count"] == 3  # 2 FEDFUNDS + 1 DGS10
    assert m["sources"]["gdelt"]["status"] == "ok"
    assert m["sources"]["gdelt"]["sha256"] == gdelt_hash
    assert m["total_sources_ok"] == 2
    assert m["total_sources_missing"] == 0


def test_snapshot_id_format(tmp_path):
    """snapshot_id must be YYYYMMDDThhmmssZ."""
    asof = date(2026, 2, 10)
    snap_id, _ = resolve_and_write(
        asof,
        cache_root=str(tmp_path / "altdata"),
        snapshots_root=str(tmp_path / "snapshots"),
        sources=[],
    )
    assert snap_id == "20260210T000000Z"


def test_missing_cache_produces_missing_status_not_exception(tmp_path):
    """If cache file absent → status=missing, no exception, manifest still written."""
    asof = date(2026, 2, 10)

    snap_id, manifest_path = resolve_and_write(
        asof,
        cache_root=str(tmp_path / "altdata"),
        snapshots_root=str(tmp_path / "snapshots"),
        sources=["fred", "gdelt"],
    )

    assert manifest_path.exists()
    m = json.loads(manifest_path.read_text())
    assert m["sources"]["fred"]["status"] == "missing"
    assert m["sources"]["gdelt"]["status"] == "missing"
    assert m["total_sources_ok"] == 0
    assert m["total_sources_missing"] == 2


def test_fallback_to_nearest_prior_date(tmp_path):
    """If exact asof not cached, manifest uses nearest prior date and reports stale_days."""
    prior = date(2026, 2, 7)
    asof = date(2026, 2, 10)
    _seed_fred(tmp_path, prior)  # only 2026-02-07 cached

    snap_id, manifest_path = resolve_and_write(
        asof,
        cache_root=str(tmp_path / "altdata"),
        snapshots_root=str(tmp_path / "snapshots"),
        sources=["fred"],
    )

    m = json.loads(manifest_path.read_text())
    fred = m["sources"]["fred"]
    assert fred["status"] == "ok"
    assert fred["cache_date"] == "2026-02-07"
    assert fred["stale_days"] == 3  # 2026-02-10 - 2026-02-07


# ---------------------------------------------------------------------------
# Test: idempotency
# ---------------------------------------------------------------------------

def test_resolve_and_write_is_idempotent(tmp_path):
    """Second call with same asof returns existing manifest without overwriting."""
    asof = date(2026, 2, 10)
    _seed_fred(tmp_path, asof)

    snap_id1, path1 = resolve_and_write(
        asof, cache_root=str(tmp_path / "altdata"),
        snapshots_root=str(tmp_path / "snapshots"), sources=["fred"],
    )
    mtime1 = path1.stat().st_mtime

    snap_id2, path2 = resolve_and_write(
        asof, cache_root=str(tmp_path / "altdata"),
        snapshots_root=str(tmp_path / "snapshots"), sources=["fred"],
    )

    assert snap_id1 == snap_id2
    assert path1 == path2
    assert path2.stat().st_mtime == mtime1  # file not touched


# ---------------------------------------------------------------------------
# Test: read_manifest / list / get_latest
# ---------------------------------------------------------------------------

def test_read_manifest_returns_none_for_missing(tmp_path):
    result = read_manifest("99991231T000000Z", snapshots_root=str(tmp_path / "snapshots"))
    assert result is None


def test_read_manifest_round_trips(tmp_path):
    asof = date(2026, 2, 10)
    _seed_fred(tmp_path, asof)

    snap_id, _ = resolve_and_write(
        asof, cache_root=str(tmp_path / "altdata"),
        snapshots_root=str(tmp_path / "snapshots"), sources=["fred"],
    )

    m = read_manifest(snap_id, snapshots_root=str(tmp_path / "snapshots"))
    assert m is not None
    assert m["snapshot_id"] == snap_id
    assert m["sources"]["fred"]["status"] == "ok"


def test_list_available_snapshots_empty(tmp_path):
    result = list_available_snapshots(snapshots_root=str(tmp_path / "snapshots"))
    assert result == []


def test_list_available_snapshots_sorted(tmp_path):
    for d in [date(2026, 2, 7), date(2026, 2, 10), date(2026, 2, 9)]:
        _seed_fred(tmp_path, d)
        resolve_and_write(
            d, cache_root=str(tmp_path / "altdata"),
            snapshots_root=str(tmp_path / "snapshots"), sources=["fred"],
        )

    ids = list_available_snapshots(snapshots_root=str(tmp_path / "snapshots"))
    assert ids == ["20260207T000000Z", "20260209T000000Z", "20260210T000000Z"]


def test_get_latest_snapshot_id(tmp_path):
    for d in [date(2026, 2, 7), date(2026, 2, 10)]:
        _seed_fred(tmp_path, d)
        resolve_and_write(
            d, cache_root=str(tmp_path / "altdata"),
            snapshots_root=str(tmp_path / "snapshots"), sources=["fred"],
        )

    latest = get_latest_snapshot_id(snapshots_root=str(tmp_path / "snapshots"))
    assert latest == "20260210T000000Z"


def test_get_latest_snapshot_id_none_if_empty(tmp_path):
    assert get_latest_snapshot_id(snapshots_root=str(tmp_path / "snapshots")) is None


# ---------------------------------------------------------------------------
# Test: manifest sha256 matches actual file hash
# ---------------------------------------------------------------------------

def test_manifest_sha256_matches_file(tmp_path):
    """sha256 in manifest must match actual bytes of the payload file."""
    asof = date(2026, 2, 10)
    expected_hash = _seed_fred(tmp_path, asof)

    _, manifest_path = resolve_and_write(
        asof, cache_root=str(tmp_path / "altdata"),
        snapshots_root=str(tmp_path / "snapshots"), sources=["fred"],
    )

    m = json.loads(manifest_path.read_text())
    assert m["sources"]["fred"]["sha256"] == expected_hash
