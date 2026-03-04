"""Tests for C2: disk budget wired in paper runner.

Verifies:
1) run_paper() accepts max_disk_mb and disk_root parameters (not rejected)
2) ResourceBudgetController enforces disk budget when > 0
3) Default max_disk_mb is non-zero in run_paper() signature
4) BudgetExceeded raised when disk usage exceeds limit
"""
from __future__ import annotations

import inspect
from pathlib import Path

import pytest

from octa_ops.autopilot.budgets import BudgetExceeded, ResourceBudgetController
from octa_ops.autopilot.paper_runner import run_paper


def test_run_paper_has_max_disk_mb_param() -> None:
    """run_paper() must have max_disk_mb parameter with non-zero default."""
    sig = inspect.signature(run_paper)
    assert "max_disk_mb" in sig.parameters, "run_paper missing max_disk_mb param"
    default = sig.parameters["max_disk_mb"].default
    assert isinstance(default, int) and default > 0, f"max_disk_mb default must be >0, got {default!r}"


def test_run_paper_has_disk_root_param() -> None:
    """run_paper() must have disk_root parameter."""
    sig = inspect.signature(run_paper)
    assert "disk_root" in sig.parameters, "run_paper missing disk_root param"


def test_resource_budget_controller_raises_on_disk_exceeded(tmp_path: Path) -> None:
    """BudgetExceeded raised when disk usage exceeds max_disk_mb."""
    # Write ~1MB of data to tmp_path
    (tmp_path / "large.bin").write_bytes(b"x" * (2 * 1024 * 1024))

    budget = ResourceBudgetController(
        max_runtime_s=3600,
        max_ram_mb=100000,
        max_threads=4,
        max_disk_mb=1,  # 1 MB limit
        disk_root=str(tmp_path),
    )
    with pytest.raises(BudgetExceeded) as exc_info:
        budget.checkpoint("test:disk")
    assert "RESOURCE_BUDGET:disk" in exc_info.value.reason


def test_resource_budget_controller_passes_within_disk_limit(tmp_path: Path) -> None:
    """No BudgetExceeded when disk usage is within limit."""
    (tmp_path / "small.bin").write_bytes(b"x" * 100)  # 100 bytes

    budget = ResourceBudgetController(
        max_runtime_s=3600,
        max_ram_mb=100000,
        max_threads=4,
        max_disk_mb=10000,  # 10 GB limit — well above
        disk_root=str(tmp_path),
    )
    budget.checkpoint("test:ok")  # must not raise
