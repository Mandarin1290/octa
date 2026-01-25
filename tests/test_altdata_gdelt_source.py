from datetime import date
import json
import types
import sys

from octa.core.data.sources.altdata.cache import read_snapshot, write_snapshot
from octa.core.data.sources.altdata.orchestrator import build_altdata_stack


class _Resp:
    def __init__(self, status_code: int, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = json.dumps(payload)

    def json(self):
        return self._payload


class _Client:
    def __init__(self, status_code: int, payload):
        self._status = status_code
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get(self, url, headers=None):
        return _Resp(self._status, self._payload)


def _install_httpx(monkeypatch, status_code: int, payload):
    fake = types.SimpleNamespace(Client=lambda timeout=20.0: _Client(status_code, payload))
    monkeypatch.setitem(sys.modules, "httpx", fake)


def test_gdelt_offline_missing_cache(tmp_path) -> None:
    cfg_path = tmp_path / "altdata.yaml"
    cfg_path.write_text(
        """
cache_dir: "{root}"
allow_net_default: false
sources:
  gdelt:
    enabled: true
    base_url: "https://api.gdeltproject.org/api/v2/doc/doc"
    query_packs:
      global_risk:
        window_days: [1]
        queries:
          - id: "conflict"
            query: "war"
""".format(root=str(tmp_path)),
        encoding="utf-8",
    )
    asof = date(2020, 1, 2)
    summary = build_altdata_stack(run_id="test_run", symbols=["AAA"], asof=asof, allow_net=False, config_path=str(cfg_path))
    assert summary["sources"]["gdelt"]["status"] == "missing_cache"


def test_gdelt_cache_hit(tmp_path) -> None:
    asof = date(2020, 1, 2)
    write_snapshot(
        source="gdelt",
        asof=asof,
        payload={
            "rows": [
                {
                    "asof_date": "2020-01-02",
                    "window": "1d",
                    "query_id": "conflict",
            "metric": "volume_intensity",
                    "value": 0.2,
                    "release_ts": "2020-01-02T06:00:00+00:00",
                }
            ]
        },
        meta={"seed": True},
        root=str(tmp_path),
    )
    cfg_path = tmp_path / "altdata.yaml"
    cfg_path.write_text(
        """
cache_dir: "{root}"
allow_net_default: false
sources:
  gdelt:
    enabled: true
""".format(root=str(tmp_path)),
        encoding="utf-8",
    )
    summary = build_altdata_stack(run_id="test_run", symbols=["AAA"], asof=asof, allow_net=False, config_path=str(cfg_path))
    assert summary["sources"]["gdelt"]["status"] == "ok"
    payload = read_snapshot(source="gdelt", asof=asof, root=str(tmp_path))
    assert isinstance(payload, dict)
    assert len(payload.get("rows", [])) > 0


def test_gdelt_fetch_writes_cache(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("OCTA_DAILY_REFRESH", "1")
    monkeypatch.setenv("OCTA_CONTEXT", "production")
    monkeypatch.setenv("OCTA_GDELT_DISABLE_PROXIES", "0")
    payload = {
        "timeline": [
            {"data": [{"date": "20200101", "value": 0.3}, {"date": "20200102", "value": 0.4}]}
        ]
    }
    _install_httpx(monkeypatch, 200, payload)
    cfg_path = tmp_path / "altdata.yaml"
    cfg_path.write_text(
        """
cache_dir: "{root}"
allow_net_default: false
sources:
  gdelt:
    enabled: true
    base_url: "https://api.gdeltproject.org/api/v2/doc/doc"
    query_packs:
      global_risk:
        window_days: [1]
        queries:
          - id: "conflict"
            query: "war"
""".format(root=str(tmp_path)),
        encoding="utf-8",
    )
    asof = date(2020, 1, 2)
    summary = build_altdata_stack(run_id="test_run", symbols=["AAA"], asof=asof, allow_net=True, config_path=str(cfg_path))
    assert summary["sources"]["gdelt"]["status"] == "ok"
    payload = read_snapshot(source="gdelt", asof=asof, root=str(tmp_path))
    assert len(payload.get("rows", [])) > 0


def test_gdelt_http_error(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("OCTA_DAILY_REFRESH", "1")
    monkeypatch.setenv("OCTA_CONTEXT", "production")
    monkeypatch.setenv("OCTA_GDELT_DISABLE_PROXIES", "0")
    payload = {"error": "server"}
    _install_httpx(monkeypatch, 500, payload)
    cfg_path = tmp_path / "altdata.yaml"
    cfg_path.write_text(
        """
cache_dir: "{root}"
allow_net_default: false
sources:
  gdelt:
    enabled: true
    base_url: "https://api.gdeltproject.org/api/v2/doc/doc"
    query_packs:
      global_risk:
        window_days: [1]
        queries:
          - id: "conflict"
            query: "war"
""".format(root=str(tmp_path)),
        encoding="utf-8",
    )
    asof = date(2020, 1, 2)
    summary = build_altdata_stack(run_id="test_run", symbols=["AAA"], asof=asof, allow_net=True, config_path=str(cfg_path))
    assert summary["sources"]["gdelt"]["status"] == "net_error"


def test_gdelt_empty_timeline_ok(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("OCTA_DAILY_REFRESH", "1")
    monkeypatch.setenv("OCTA_CONTEXT", "production")
    monkeypatch.setenv("OCTA_GDELT_DISABLE_PROXIES", "0")
    payload = {"timeline": []}
    _install_httpx(monkeypatch, 200, payload)
    cfg_path = tmp_path / "altdata.yaml"
    cfg_path.write_text(
        """
cache_dir: "{root}"
allow_net_default: false
sources:
  gdelt:
    enabled: true
    base_url: "https://api.gdeltproject.org/api/v2/doc/doc"
    query_packs:
      global_risk:
        window_days: [1]
        queries:
          - id: "conflict"
            query: "war"
""".format(root=str(tmp_path)),
        encoding="utf-8",
    )
    asof = date(2020, 1, 2)
    summary = build_altdata_stack(run_id="test_run", symbols=["AAA"], asof=asof, allow_net=True, config_path=str(cfg_path))
    assert summary["sources"]["gdelt"]["status"] == "ok"
    payload = read_snapshot(source="gdelt", asof=asof, root=str(tmp_path))
    assert payload.get("rows") == []


def test_gdelt_urllib_proxy_free(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("OCTA_DAILY_REFRESH", "1")
    monkeypatch.setenv("OCTA_CONTEXT", "production")
    monkeypatch.setenv("OCTA_GDELT_DISABLE_PROXIES", "1")
    monkeypatch.setitem(sys.modules, "httpx", None)

    seen = {"proxy_free": False}

    class _Opener:
        def __init__(self, proxy_free: bool):
            self._proxy_free = proxy_free

        def open(self, req, timeout=10):
            class _Resp:
                status = 200

                def read(self):
                    return b'{"timeline":[{"data":[{"date":"20200101","value":0.1}]}]}'

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
                proxy_free = handler.proxies == {}
        return _Opener(proxy_free)

    import urllib.request as _urllib

    monkeypatch.setattr(_urllib, "build_opener", _fake_build_opener)

    cfg_path = tmp_path / "altdata.yaml"
    cfg_path.write_text(
        """
cache_dir: "{root}"
allow_net_default: false
sources:
  gdelt:
    enabled: true
    query_packs:
      global_risk:
        window_days: [1]
        queries:
          - id: "conflict"
            query: "war"
""".format(root=str(tmp_path)),
        encoding="utf-8",
    )
    summary = build_altdata_stack(run_id="test_run", symbols=["AAA"], asof=date(2020, 1, 2), allow_net=True, config_path=str(cfg_path))
    assert summary["sources"]["gdelt"]["status"] == "ok"
    assert seen["proxy_free"] is True
