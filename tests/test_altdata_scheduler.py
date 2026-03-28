from __future__ import annotations

import json
from datetime import datetime, timezone

import octa.support.scheduler.daily_altdata_job as job
from octa.support.scheduler import scheduler
from octa.core.data.sources.altdata.orchestrator import build_altdata_stack


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


def test_build_altdata_stack_writes_event_evidence(tmp_path, monkeypatch) -> None:
    cfg_path = tmp_path / "altdata.yaml"
    cfg_path.write_text(
        """
cache_dir: "{root}"
sources:
  scheduled_events:
    enabled: true
    window_days: 2
    events:
      - event_id: fed_fomc
        title: FOMC rate decision
        source_id: fed_schedule
        source_name: Federal Reserve Schedule
        source_tier: 1
        event_type: rates
        severity_floor: high
        category: scheduled_macro
        jurisdiction: US
        asset_classes: ["all"]
        official: true
        scheduled_at: "2024-03-20T18:00:00+00:00"
        known_at: "2024-01-01T00:00:00+00:00"
        pre_window_hours: 24
        post_window_hours: 2
""".format(root=str(tmp_path / "cache")),
        encoding="utf-8",
    )
    monkeypatch.setenv("OCTA_DAILY_REFRESH", "1")
    monkeypatch.delenv("OCTA_CONTEXT", raising=False)

    summary = build_altdata_stack(
        run_id="altdata_daily_2024-03-20",
        symbols=[],
        asof=datetime(2024, 3, 20, tzinfo=timezone.utc).date(),
        allow_net=True,
        config_path=str(cfg_path),
    )

    evidence_dir = tmp_path / "cache" / "evidence" / "2024-03-20"
    assert summary["sources"]["scheduled_events"]["status"] == "ok"
    assert (evidence_dir / "recency_model.json").exists()
    assert (evidence_dir / "severity_rules.json").exists()
    assert (evidence_dir / "scheduled_event_summary.json").exists()
    assert (evidence_dir / "scheduled_event_windows.json").exists()
    assert (evidence_dir / "updated_run_manifest.json").exists()
