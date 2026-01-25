from datetime import date
import types
import sys

from octa.core.data.sources.altdata.orchestrator import build_altdata_stack


class _Resp:
    def __init__(self, status_code: int, payload):
        self.status_code = status_code
        self._payload = payload

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
    fake = types.SimpleNamespace(Client=lambda timeout=10.0: _Client(status_code, payload))
    monkeypatch.setitem(sys.modules, "httpx", fake)


def test_edgar_bootstrap_fetch_writes_cache(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("OCTA_DAILY_REFRESH", "1")
    payload = {
        "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc"},
        "1": {"cik_str": 789019, "ticker": "MSFT", "title": "Microsoft Corp"},
    }
    _install_httpx(monkeypatch, 200, payload)

    cfg_path = tmp_path / "altdata.yaml"
    cfg_path.write_text(
        """
cache_dir: "{root}"
allow_net_default: false
sources:
  edgar:
    enabled: true
    user_agent: "OCTA Research (contact: you@example.com)"
""".format(root=str(tmp_path)),
        encoding="utf-8",
    )

    asof = date(2020, 1, 2)
    summary = build_altdata_stack(run_id="test_run", symbols=["AAA"], asof=asof, allow_net=True, config_path=str(cfg_path))

    assert summary["sources"]["edgar"]["status"] == "ok"
    assert summary["sources"]["edgar"]["rows"] > 0

    edgar_dir = tmp_path / "edgar" / asof.isoformat()
    assert (edgar_dir / f"edgar_{asof.isoformat()}.json").exists()
    assert (edgar_dir / f"edgar_{asof.isoformat()}_meta.json").exists()


def test_edgar_offline_missing_cache(tmp_path) -> None:
    cfg_path = tmp_path / "altdata.yaml"
    cfg_path.write_text(
        """
cache_dir: "{root}"
allow_net_default: false
sources:
  edgar:
    enabled: true
    user_agent: "OCTA Research (contact: you@example.com)"
""".format(root=str(tmp_path)),
        encoding="utf-8",
    )

    asof = date(2020, 1, 2)
    summary = build_altdata_stack(run_id="test_run", symbols=["AAA"], asof=asof, allow_net=False, config_path=str(cfg_path))
    assert summary["sources"]["edgar"]["status"] == "missing_cache"
