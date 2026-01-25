from datetime import date
import io
import types
import sys
import zipfile

from octa.core.data.sources.altdata.orchestrator import build_altdata_stack


def _make_zip(csv_text: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("fut_disagg.txt", csv_text)
    return buf.getvalue()


class _Resp:
    def __init__(self, status_code: int, content: bytes):
        self.status_code = status_code
        self.content = content


class _Client:
    def __init__(self, status_code: int, content: bytes):
        self._status = status_code
        self._content = content

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get(self, url, headers=None):
        return _Resp(self._status, self._content)


def _install_httpx(monkeypatch, status_code: int, content: bytes):
    fake = types.SimpleNamespace(Client=lambda timeout=15.0: _Client(status_code, content))
    monkeypatch.setitem(sys.modules, "httpx", fake)


def test_cot_fetch_writes_cache(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("OCTA_DAILY_REFRESH", "1")
    csv_text = (
        "Market_and_Exchange_Names,Report_Date_as_YYYY-MM-DD,Noncommercial_Long_All,Noncommercial_Short_All,Open_Interest_All\n"
        "E-MINI S&P 500,2020-01-03,20000,15000,100000\n"
    )
    payload = _make_zip(csv_text)
    _install_httpx(monkeypatch, 200, payload)

    cfg_path = tmp_path / "altdata.yaml"
    cfg_path.write_text(
        """
cache_dir: "{root}"
allow_net_default: false
sources:
  cot:
    enabled: true
    targets:
      - id: es
        candidates: ["e-mini s&p 500"]
""".format(root=str(tmp_path)),
        encoding="utf-8",
    )

    asof = date(2020, 1, 6)
    summary = build_altdata_stack(run_id="test_run", symbols=["AAA"], asof=asof, allow_net=True, config_path=str(cfg_path))

    assert summary["sources"]["cot"]["status"] == "ok"
    assert summary["sources"]["cot"]["rows"] > 0

    cot_dir = tmp_path / "cot" / asof.isoformat()
    assert (cot_dir / f"cot_{asof.isoformat()}.json").exists()
    assert (cot_dir / f"cot_{asof.isoformat()}_meta.json").exists()


def test_cot_offline_missing_cache(tmp_path) -> None:
    cfg_path = tmp_path / "altdata.yaml"
    cfg_path.write_text(
        """
cache_dir: "{root}"
allow_net_default: false
sources:
  cot:
    enabled: true
    targets:
      - id: es
        candidates: ["e-mini s&p 500"]
""".format(root=str(tmp_path)),
        encoding="utf-8",
    )

    asof = date(2020, 1, 6)
    summary = build_altdata_stack(run_id="test_run", symbols=["AAA"], asof=asof, allow_net=False, config_path=str(cfg_path))
    assert summary["sources"]["cot"]["status"] == "missing_cache"
