from __future__ import annotations

import json
from datetime import datetime, timezone

import octa.support.scheduler.daily_altdata_job as job
from octa.support.scheduler import scheduler


def test_next_run_utc() -> None:
    now = datetime(2020, 1, 1, 1, 0, tzinfo=timezone.utc)
    run_at = scheduler.next_run_utc(now, "02:00")
    assert run_at == datetime(2020, 1, 1, 2, 0, tzinfo=timezone.utc)

    now = datetime(2020, 1, 1, 2, 0, tzinfo=timezone.utc)
    run_at = scheduler.next_run_utc(now, "02:00")
    assert run_at == datetime(2020, 1, 2, 2, 0, tzinfo=timezone.utc)


def test_daily_refresh_writes_audit(tmp_path, monkeypatch) -> None:
    def _fake_build(*args, **kwargs):
        return {"run_id": "altdata_daily_2020-01-01", "sources": {"fred": {"status": "ok"}}}

    monkeypatch.setattr(job, "build_altdata_stack", _fake_build)
    monkeypatch.setattr(job, "load_altdata_config", lambda path=None: {"cache_dir": str(tmp_path), "sources": {"fred": {"enabled": True}}})
    monkeypatch.setattr(job, "read_snapshot", lambda source, asof, root=None: {"seed": True})

    audit_root = tmp_path / "audit"
    audit = job.run_daily_altdata_refresh(
        now=datetime(2020, 1, 1, 3, 0, tzinfo=timezone.utc),
        symbols=[],
        audit_root=str(audit_root),
    )

    assert audit["success"] is True
    files = list(audit_root.glob("altdata_refresh_*.json"))
    assert files
    data = json.loads(files[0].read_text(encoding="utf-8"))
    assert data["success"] is True
    assert "sources_ok" in data


def test_daily_refresh_fail_closed(tmp_path, monkeypatch) -> None:
    marker = tmp_path / "cache_marker.txt"
    marker.write_text("keep", encoding="utf-8")

    def _boom(*args, **kwargs):
        raise RuntimeError("fetch failed")

    monkeypatch.setattr(job, "build_altdata_stack", _boom)
    monkeypatch.setattr(job, "load_altdata_config", lambda path=None: {"cache_dir": str(tmp_path), "sources": {"fred": {"enabled": True}}})

    audit = job.run_daily_altdata_refresh(
        now=datetime(2020, 1, 1, 3, 0, tzinfo=timezone.utc),
        symbols=[],
        audit_root=str(tmp_path / "audit"),
    )

    assert audit["success"] is False
    assert marker.exists()


def test_scheduler_oneshot(monkeypatch) -> None:
    calls = {"runs": 0, "sleeps": []}

    def _now():
        return datetime(2020, 1, 1, 1, 0, tzinfo=timezone.utc)

    def _sleep(seconds: float) -> None:
        calls["sleeps"].append(seconds)

    def _run(*args, **kwargs):
        calls["runs"] += 1

    monkeypatch.setattr(scheduler, "run_daily_altdata_refresh", _run)
    scheduler.run_loop(schedule="01:01", oneshot=True, now_fn=_now, sleep_fn=_sleep)

    assert calls["runs"] == 1
    assert calls["sleeps"]
