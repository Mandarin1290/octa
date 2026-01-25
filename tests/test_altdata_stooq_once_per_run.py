from datetime import date

from octa.core.data.sources.altdata.orchestrator import build_altdata_stack
from octa.core.data.sources.altdata.stooq import StooqSource


def test_stooq_fetch_once_per_run(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("OCTA_DAILY_REFRESH", "1")
    monkeypatch.setenv("OCTA_CONTEXT", "production")

    calls = {"count": 0}

    def _fake_fetch(self, *, asof, allow_net):
        calls["count"] += 1
        return {"rows": [{"proxy": "spx", "symbol": "spy.us", "ts": asof.isoformat(), "close": 1.0}]}

    monkeypatch.setattr(StooqSource, "fetch_raw", _fake_fetch)

    cfg_path = tmp_path / "altdata.yaml"
    cfg_path.write_text(
        """
cache_dir: "{root}"
allow_net_default: false
sources:
  stooq:
    enabled: true
    window_days: 10
    symbols:
      spx: ["spy.us"]
""".format(root=str(tmp_path)),
        encoding="utf-8",
    )
    symbols = [f"SYM{i:03d}" for i in range(50)]
    summary = build_altdata_stack(run_id="test_run", symbols=symbols, asof=date(2020, 1, 2), allow_net=True, config_path=str(cfg_path))

    assert summary["sources"]["stooq"]["status"] == "ok"
    assert calls["count"] == 1


def test_stooq_offline_does_not_fetch(monkeypatch, tmp_path) -> None:
    monkeypatch.delenv("OCTA_DAILY_REFRESH", raising=False)
    monkeypatch.setenv("OCTA_CONTEXT", "production")

    def _boom(self, *, asof, allow_net):
        raise RuntimeError("should not fetch")

    monkeypatch.setattr(StooqSource, "fetch_raw", _boom)

    cfg_path = tmp_path / "altdata.yaml"
    cfg_path.write_text(
        """
cache_dir: "{root}"
allow_net_default: false
sources:
  stooq:
    enabled: true
    window_days: 10
    symbols:
      spx: ["spy.us"]
""".format(root=str(tmp_path)),
        encoding="utf-8",
    )
    summary = build_altdata_stack(run_id="test_run", symbols=["AAA"], asof=date(2020, 1, 2), allow_net=True, config_path=str(cfg_path))
    assert summary["sources"]["stooq"]["status"] == "missing_cache"
