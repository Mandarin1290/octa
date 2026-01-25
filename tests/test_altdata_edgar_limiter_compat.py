import sys
from types import SimpleNamespace

from octa.core.data.sources.altdata import edgar_connector
from octa.core.data.sources.altdata._compat_rate_limiter import _make_limiter


def test_make_limiter_filters_kwargs(monkeypatch):
    class FakeLimiter:
        def __init__(self, arg, clock=None):
            self.arg = arg
            self.clock = clock

    fake_mod = SimpleNamespace(Limiter=FakeLimiter)
    monkeypatch.setitem(sys.modules, "pyrate_limiter", fake_mod)

    limiter, meta = _make_limiter("token", raise_when_fail=False)
    assert limiter is not None
    assert "raise_when_fail" not in (meta.get("limiter_kwargs_used") or {})


def test_edgar_fetch_fail_closed(monkeypatch):
    monkeypatch.setattr(edgar_connector, "_import_downloader", lambda: (None, {"error": "missing"}))
    res = edgar_connector.fetch_edgar_filings(
        ticker="AAPL",
        forms=["10-K"],
        start_ts=edgar_connector.pd.Timestamp("2020-01-01", tz="UTC"),
        end_ts=edgar_connector.pd.Timestamp("2020-12-31", tz="UTC"),
    )
    assert res.ok is False
    assert res.error is not None
