
import pandas as pd

from okta_altdat.sidecar import try_run


class _S:
    symbol = "AAPL"
    timezone = "UTC"


def test_sidecar_disabled_by_default(tmp_path, monkeypatch):
    # ensure no env override
    monkeypatch.delenv("OKTA_ALTDATA_ENABLED", raising=False)
    monkeypatch.delenv("OKTA_ALTDATA_CONFIG", raising=False)
    monkeypatch.delenv("OKTA_ALTDATA_ROOT", raising=False)

    idx = pd.date_range("2020-01-01", periods=5, freq="D", tz="UTC")
    bars = pd.DataFrame({"close": [1, 2, 3, 4, 5]}, index=idx)

    f, meta = try_run(bars_df=bars, settings=_S(), asset_class="stocks")
    assert isinstance(f, pd.DataFrame)
    assert list(f.index) == list(bars.index)
    assert not meta.get("enabled", True)
