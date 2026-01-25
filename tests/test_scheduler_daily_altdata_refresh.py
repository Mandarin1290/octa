from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import octa.support.scheduler.daily_altdata_refresh as daily
from octa.support.scheduler import daemon


def test_daily_refresh_writes_audit(tmp_path, monkeypatch) -> None:
    def _fake_build(*args, **kwargs):
        return {"run_id": "altdata_daily_2020-01-01", "sources": {"fred": {"status": "ok", "rows": 1}}}

    monkeypatch.setattr(daily, "build_altdata_stack", _fake_build)
    monkeypatch.setattr(daily, "load_altdata_config", lambda path=None: {"cache_dir": str(tmp_path), "sources": {"fred": {"enabled": True}}})
    monkeypatch.setattr(daily, "read_snapshot", lambda source, asof, root=None: {"seed": True})
    monkeypatch.setattr(daily, "source_day_dir", lambda source, asof, root=None: Path(tmp_path) / source / asof.isoformat())

    audit_dir = tmp_path / "octa" / "var" / "audit" / "altdata_refresh"
    monkeypatch.chdir(tmp_path)
    audit = daily.run_daily_altdata_refresh(schedule_ts_utc=datetime(2020, 1, 1, 3, 0, tzinfo=timezone.utc))

    assert audit["success"] is True
    assert list(audit_dir.glob("altdata_refresh_*.json"))


def test_daemon_lock_skips(tmp_path, monkeypatch) -> None:
    lock_root = tmp_path / "locks"
    lock_root.mkdir(parents=True, exist_ok=True)
    lock_path = lock_root / "altdata_refresh_2020-01-01.lock"
    lock_path.write_text("locked", encoding="utf-8")

    calls = {"runs": 0}

    def _run(*args, **kwargs):
        calls["runs"] += 1

    monkeypatch.setattr(daemon, "run_daily_altdata_refresh", _run)
    monkeypatch.setattr(daemon, "_sleep_seconds", lambda seconds: None)
    monkeypatch.setattr(daemon, "_next_run", lambda now, hour, minute: datetime(2020, 1, 1, 2, 0, tzinfo=timezone.utc))
    daemon.run_daemon(by_hour=2, by_minute=0, lock_root=str(lock_root), max_loops=1)
    assert calls["runs"] == 0


def test_guard_blocks_live_fetch(monkeypatch, tmp_path) -> None:
    import os
    from octa.core.data.sources.altdata.orchestrator import build_altdata_stack

    os.environ.pop("OCTA_DAILY_REFRESH", None)
    os.environ["OCTA_ALLOW_NET"] = "1"
    os.environ["OCTA_CONTEXT"] = "production"

    def _boom(*args, **kwargs):
        raise RuntimeError("network called")

    monkeypatch.setattr("octa.core.data.sources.altdata.fred.FredSource.fetch_raw", _boom)

    cfg_path = tmp_path / "altdata.yaml"
    cfg_path.write_text(
        """
cache_dir: "{root}"
allow_net_default: false
sources:
  fred:
    enabled: true
    api_key_env: FRED_API_KEY
""".format(root=str(tmp_path)),
        encoding="utf-8",
    )

    summary = build_altdata_stack(run_id="test_run", symbols=["AAA"], asof=datetime(2020, 1, 1).date(), allow_net=True, config_path=str(cfg_path))
    assert summary["sources"]["fred"]["status"] == "missing_cache"


def test_allow_net_effective_daily_refresh(monkeypatch, tmp_path) -> None:
    import os
    from octa.core.data.sources.altdata.orchestrator import build_altdata_stack

    os.environ["OCTA_DAILY_REFRESH"] = "1"
    os.environ["OCTA_ALLOW_NET"] = "1"
    os.environ["OCTA_CONTEXT"] = "production"

    def _ok(*args, **kwargs):
        return {"series": {"FEDFUNDS": [{"ts": "2020-01-01", "value": 1.0}]}}

    monkeypatch.setattr("octa.core.data.sources.altdata.fred.FredSource.fetch_raw", _ok)

    cfg_path = tmp_path / "altdata.yaml"
    cfg_path.write_text(
        """
cache_dir: "{root}"
allow_net_default: false
sources:
  fred:
    enabled: true
    api_key_env: FRED_API_KEY
    series: ["FEDFUNDS"]
""".format(root=str(tmp_path)),
        encoding="utf-8",
    )
    summary = build_altdata_stack(run_id="test_run", symbols=["AAA"], asof=datetime(2020, 1, 1).date(), allow_net=True, config_path=str(cfg_path))
    assert summary["sources"]["fred"]["status"] == "ok"


def test_allow_net_denied_in_research(monkeypatch, tmp_path) -> None:
    import os
    from octa.core.data.sources.altdata.orchestrator import build_altdata_stack

    os.environ["OCTA_DAILY_REFRESH"] = "1"
    os.environ["OCTA_ALLOW_NET"] = "1"
    os.environ["OCTA_CONTEXT"] = "research"

    def _boom(*args, **kwargs):
        raise RuntimeError("should not fetch")

    monkeypatch.setattr("octa.core.data.sources.altdata.fred.FredSource.fetch_raw", _boom)

    cfg_path = tmp_path / "altdata.yaml"
    cfg_path.write_text(
        """
cache_dir: "{root}"
allow_net_default: false
sources:
  fred:
    enabled: true
    api_key_env: FRED_API_KEY
    series: ["FEDFUNDS"]
""".format(root=str(tmp_path)),
        encoding="utf-8",
    )
    summary = build_altdata_stack(run_id="test_run", symbols=["AAA"], asof=datetime(2020, 1, 1).date(), allow_net=True, config_path=str(cfg_path))
    assert summary["sources"]["fred"]["status"] == "missing_cache"


def test_refresh_status_optional_missing(monkeypatch, tmp_path) -> None:
    def _fake_build(*args, **kwargs):
        return {
            "run_id": "altdata_daily_2020-01-01",
            "sources": {"fred": {"status": "ok"}, "edgar": {"status": "ok"}, "cot": {"status": "ok"}, "stooq": {"status": "missing_cache"}},
        }

    cfg_path = tmp_path / "altdata.yaml"
    cfg_path.write_text(
        """
cache_dir: "{root}"
refresh:
  required_sources: ["fred", "edgar", "cot"]
  optional_sources: ["stooq"]
sources:
  fred: {{enabled: true}}
  edgar: {{enabled: true}}
  cot: {{enabled: true}}
  stooq: {{enabled: true}}
""".format(root=str(tmp_path)),
        encoding="utf-8",
    )
    monkeypatch.setattr(daily, "build_altdata_stack", _fake_build)
    monkeypatch.setattr(daily, "load_altdata_config", lambda path=None: {"cache_dir": str(tmp_path), "refresh": {"required_sources": ["fred", "edgar", "cot"], "optional_sources": ["stooq"]}, "sources": {"fred": {"enabled": True}, "edgar": {"enabled": True}, "cot": {"enabled": True}, "stooq": {"enabled": True}}})
    monkeypatch.setattr(daily, "read_snapshot", lambda source, asof, root=None: {"seed": True} if source != "stooq" else None)
    monkeypatch.setattr(daily, "source_day_dir", lambda source, asof, root=None: Path(tmp_path) / source / asof.isoformat())
    monkeypatch.chdir(tmp_path)
    audit = daily.run_daily_altdata_refresh(schedule_ts_utc=datetime(2020, 1, 1, 3, 0, tzinfo=timezone.utc), config_path=str(cfg_path))
    assert audit["status"] == "success_with_warnings"
    assert "stooq" in audit["optional_missing"]


def test_refresh_status_required_missing(monkeypatch, tmp_path) -> None:
    def _fake_build(*args, **kwargs):
        return {
            "run_id": "altdata_daily_2020-01-01",
            "sources": {"fred": {"status": "missing_cache"}, "edgar": {"status": "ok"}, "cot": {"status": "ok"}},
        }

    cfg_path = tmp_path / "altdata.yaml"
    cfg_path.write_text(
        """
cache_dir: "{root}"
refresh:
  required_sources: ["fred", "edgar", "cot"]
sources:
  fred: {{enabled: true}}
  edgar: {{enabled: true}}
  cot: {{enabled: true}}
""".format(root=str(tmp_path)),
        encoding="utf-8",
    )
    monkeypatch.setattr(daily, "build_altdata_stack", _fake_build)
    monkeypatch.setattr(daily, "load_altdata_config", lambda path=None: {"cache_dir": str(tmp_path), "refresh": {"required_sources": ["fred", "edgar", "cot"]}, "sources": {"fred": {"enabled": True}, "edgar": {"enabled": True}, "cot": {"enabled": True}}})
    monkeypatch.setattr(daily, "read_snapshot", lambda source, asof, root=None: None)
    monkeypatch.setattr(daily, "source_day_dir", lambda source, asof, root=None: Path(tmp_path) / source / asof.isoformat())
    monkeypatch.chdir(tmp_path)
    audit = daily.run_daily_altdata_refresh(schedule_ts_utc=datetime(2020, 1, 1, 3, 0, tzinfo=timezone.utc), config_path=str(cfg_path))
    assert audit["status"] == "failed"


def test_refresh_writes_stooq_cache(monkeypatch, tmp_path) -> None:
    from octa.core.data.sources.altdata.stooq import StooqSource

    def _fake_fetch(self, *, asof, allow_net):
        return {"rows": [{"proxy": "spx", "symbol": "spy.us", "ts": asof.isoformat(), "close": 1.0}]}

    monkeypatch.setattr(StooqSource, "fetch_raw", _fake_fetch)
    monkeypatch.setenv("OCTA_CONTEXT", "production")
    cfg_path = tmp_path / "altdata.yaml"
    cfg_path.write_text(
        """
cache_dir: "{root}"
sources:
  stooq:
    enabled: true
    symbols:
      spx: ["spy.us"]
""".format(root=str(tmp_path)),
        encoding="utf-8",
    )
    monkeypatch.setattr(daily, "_discover_symbols", lambda: [])
    audit = daily.run_daily_altdata_refresh(schedule_ts_utc=datetime(2020, 1, 2, 3, 0, tzinfo=timezone.utc), config_path=str(cfg_path))
    stooq_path = tmp_path / "stooq" / "2020-01-02" / "stooq_2020-01-02.json"
    assert stooq_path.exists()


def test_refresh_cache_diagnostics_stooq_rate_limit(monkeypatch, tmp_path) -> None:
    from octa.core.data.sources.altdata.cache import write_snapshot

    monkeypatch.setenv("OCTA_CONTEXT", "production")
    cfg_path = tmp_path / "altdata.yaml"
    cfg_path.write_text(
        """
cache_dir: "{root}"
refresh:
  required_sources: ["fred", "edgar", "cot"]
  optional_sources: ["stooq"]
sources:
  fred: {{enabled: true}}
  edgar: {{enabled: true}}
  cot: {{enabled: true}}
  stooq: {{enabled: true}}
""".format(root=str(tmp_path)),
        encoding="utf-8",
    )
    monkeypatch.setattr(daily, "_discover_symbols", lambda: [])
    monkeypatch.setattr(daily, "build_altdata_stack", lambda **kwargs: {"sources": {"stooq": {"status": "ok"}}})
    asof = datetime(2020, 1, 2, 3, 0, tzinfo=timezone.utc)
    write_snapshot(
        source="stooq",
        asof=asof.date(),
        payload={"rows": []},
        meta={"source_meta": {"rate_limited": True, "rate_limit_message": "Exceeded the daily hits limit."}},
        root=str(tmp_path),
    )
    audit = daily.run_daily_altdata_refresh(schedule_ts_utc=asof, config_path=str(cfg_path))
    diag = audit["cache_diagnostics"]["stooq"]
    assert diag["rate_limited"] is True
    assert "Exceeded the daily hits limit" in diag.get("rate_limit_message", "")
