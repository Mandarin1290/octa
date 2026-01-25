from datetime import date

from octa.core.data.sources.altdata.orchestrator import build_altdata_stack


def test_stooq_fetch_writes_cache(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("OCTA_DAILY_REFRESH", "1")
    from octa.core.data.sources.altdata import stooq as stooq_mod

    csv_text = (
        "Date,Open,High,Low,Close,Volume\n"
        "2020-01-01,1,2,0.5,1.5,100\n"
        "2020-01-02,1,2,0.5,1.6,110\n"
        "2020-01-03,1,2,0.5,1.7,120\n"
        "2020-01-04,1,2,0.5,1.8,130\n"
        "2020-01-05,1,2,0.5,1.9,140\n"
        "2020-01-06,1,2,0.5,2.0,150\n"
    )

    def _fake_fetch_text(url: str) -> dict:
        return {"status": "ok", "text": csv_text, "url": url, "http_status": 200}

    monkeypatch.setattr(stooq_mod, "_fetch_text", _fake_fetch_text)

    cfg_path = tmp_path / "altdata.yaml"
    cfg_path.write_text(
        """
cache_dir: "{root}"
allow_net_default: false
sources:
  stooq:
    enabled: true
    window_days: 365
    symbols:
      spx: ["spy.us"]
""".format(root=str(tmp_path)),
        encoding="utf-8",
    )

    asof = date(2020, 1, 6)
    summary = build_altdata_stack(run_id="test_run", symbols=["AAA"], asof=asof, allow_net=True, config_path=str(cfg_path))

    assert summary["sources"]["stooq"]["status"] == "ok"
    assert summary["sources"]["stooq"]["rows"] > 0

    stooq_dir = tmp_path / "stooq" / asof.isoformat()
    assert (stooq_dir / f"stooq_{asof.isoformat()}.json").exists()
    assert (stooq_dir / f"stooq_{asof.isoformat()}_meta.json").exists()


def test_stooq_offline_missing_cache(tmp_path) -> None:
    cfg_path = tmp_path / "altdata.yaml"
    cfg_path.write_text(
        """
cache_dir: "{root}"
allow_net_default: false
sources:
  stooq:
    enabled: true
    symbols:
      spx: ["spy.us"]
""".format(root=str(tmp_path)),
        encoding="utf-8",
    )

    asof = date(2020, 1, 6)
    summary = build_altdata_stack(run_id="test_run", symbols=["AAA"], asof=asof, allow_net=False, config_path=str(cfg_path))
    assert summary["sources"]["stooq"]["status"] == "missing_cache"
