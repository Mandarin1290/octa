"""Tests for C1: run_id collision safety (uuid suffix).

Verifies:
1) new_run_id() produces unique IDs across 100 rapid calls
2) run_id format: <prefix>_<timestamp>_<8hex>
3) paper_live run_id includes uuid suffix
"""
from __future__ import annotations

import re
import uuid

from octa.core.orchestration.resources import new_run_id


_RUN_ID_PATTERN = re.compile(r"^[a-z_]+_\d{8}T\d{6}Z_[0-9a-f]{8}$")


def test_new_run_id_unique_across_100_calls() -> None:
    """100 rapid calls must produce 100 distinct IDs."""
    ids = [new_run_id("test") for _ in range(100)]
    assert len(set(ids)) == 100, f"Collision detected: {len(set(ids))} unique / 100"


def test_new_run_id_format() -> None:
    """run_id must match <prefix>_YYYYMMDDTHHMMSSz_<8hex>."""
    rid = new_run_id("cascade")
    assert _RUN_ID_PATTERN.match(rid), f"run_id format mismatch: {rid!r}"


def test_new_run_id_uuid_suffix_is_valid_hex() -> None:
    """UUID suffix must be 8 lowercase hex chars."""
    rid = new_run_id("run")
    suffix = rid.rsplit("_", 1)[-1]
    assert len(suffix) == 8, f"Suffix length {len(suffix)}, expected 8"
    assert all(c in "0123456789abcdef" for c in suffix), f"Non-hex in suffix: {suffix!r}"


def test_new_run_id_prefix_preserved() -> None:
    """Custom prefix must appear as first component."""
    rid = new_run_id("paper_live")
    assert rid.startswith("paper_live_"), f"Prefix missing: {rid!r}"


def test_new_run_id_default_prefix() -> None:
    """Default prefix is 'run'."""
    rid = new_run_id()
    assert rid.startswith("run_"), f"Default prefix missing: {rid!r}"
