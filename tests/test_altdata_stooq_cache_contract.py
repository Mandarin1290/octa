from datetime import date

from octa.core.data.sources.altdata.cache import read_meta
from octa.core.data.sources.altdata.orchestrator import build_altdata_stack


def test_stooq_writes_cache_on_net_error(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("OCTA_DAILY_REFRESH", "1")
    monkeypatch.setenv("OCTA_CONTEXT", "production")

    from octa.core.data.sources.altdata import stooq as stooq_mod

    def _fake_fetch_text(url: str) -> dict:
        return {"status": "net_error", "url": url, "error": "DNS"}

    monkeypatch.setattr(stooq_mod, "_fetch_text", _fake_fetch_text)

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
    asof = date(2020, 1, 2)
    summary = build_altdata_stack(
        run_id="test_run",
        symbols=[],
        asof=asof,
        allow_net=True,
        config_path=str(cfg_path),
    )
    assert summary["sources"]["stooq"]["status"] == "net_error"
    stooq_path = tmp_path / "stooq" / "2020-01-02" / "stooq_2020-01-02.json"
    assert stooq_path.exists()


def test_stooq_writes_cache_on_success(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("OCTA_DAILY_REFRESH", "1")
    monkeypatch.setenv("OCTA_CONTEXT", "production")

    from octa.core.data.sources.altdata import stooq as stooq_mod

    csv_text = "Date,Open,High,Low,Close,Volume\n2020-01-02,1,1,1,1.5,100\n"

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
    symbols:
      spx: ["spy.us"]
""".format(root=str(tmp_path)),
        encoding="utf-8",
    )
    asof = date(2020, 1, 2)
    summary = build_altdata_stack(
        run_id="test_run",
        symbols=[],
        asof=asof,
        allow_net=True,
        config_path=str(cfg_path),
    )
    assert summary["sources"]["stooq"]["status"] == "ok"
    assert summary["sources"]["stooq"]["rows"] == 1
    stooq_path = tmp_path / "stooq" / "2020-01-02" / "stooq_2020-01-02.json"
    assert stooq_path.exists()


def test_stooq_uses_proxy_free_opener(monkeypatch) -> None:
    from octa.core.data.sources.altdata import stooq as stooq_mod

    seen = {"proxy_free": False}

    class _Opener:
        def __init__(self, proxy_free: bool):
            self._proxy_free = proxy_free

        def open(self, req, timeout=10):
            class _Resp:
                status = 200

                def read(self):
                    return b"Date,Close\n2020-01-02,1.0\n"

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

            if self._proxy_free:
                seen["proxy_free"] = True
            return _Resp()

    def _fake_build_opener(*handlers):
        proxy_free = False
        for handler in handlers:
            if handler.__class__.__name__ == "ProxyHandler":
                proxy_free = getattr(handler, "proxies", None) == {}
        return _Opener(proxy_free)

    import urllib.request as _urllib

    monkeypatch.setattr(_urllib, "build_opener", _fake_build_opener)

    resp = stooq_mod._fetch_text("https://stooq.com/q/d/l/?s=spy.us&i=d")
    assert resp["status"] == "ok"
    assert seen["proxy_free"] is True


def test_stooq_rate_limit_flagged(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("OCTA_DAILY_REFRESH", "1")
    monkeypatch.setenv("OCTA_CONTEXT", "production")

    from octa.core.data.sources.altdata import stooq as stooq_mod

    rate_body = "Exceeded the daily hits limit. Try again later."

    def _fake_fetch_text(url: str) -> dict:
        return {"status": "ok", "text": rate_body, "url": url, "http_status": 200, "body_head": rate_body}

    monkeypatch.setattr(stooq_mod, "_fetch_text", _fake_fetch_text)

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
    asof = date(2020, 1, 2)
    summary = build_altdata_stack(
        run_id="test_run",
        symbols=[],
        asof=asof,
        allow_net=True,
        config_path=str(cfg_path),
    )
    assert summary["sources"]["stooq"]["status"] == "ok"
    meta = read_meta(source="stooq", asof=asof, root=str(tmp_path))
    assert meta is not None
    source_meta = meta.get("source_meta", {})
    assert source_meta.get("rate_limited") is True
    assert "Exceeded the daily hits limit" in source_meta.get("rate_limit_message", "")
