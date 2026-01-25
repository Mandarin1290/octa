from datetime import date

from octa.core.data.sources.altdata.cache import read_snapshot, write_snapshot


def test_altdata_cache_roundtrip(tmp_path) -> None:
    payload = {"series": {"X": [{"ts": "2020-01-01", "value": 1.0}]}}
    meta = {"source": "test"}
    asof = date(2020, 1, 2)

    write_snapshot(source="fred", asof=asof, payload=payload, meta=meta, root=str(tmp_path))
    loaded = read_snapshot(source="fred", asof=asof, root=str(tmp_path))

    assert loaded == payload
