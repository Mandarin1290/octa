import pandas as pd

from okta_altdat.time_sync import asof_join


def test_asof_join_backward_never_joins_future():
    idx = pd.date_range("2020-01-01", periods=5, freq="D", tz="UTC")
    bars = pd.DataFrame({"close": [1, 2, 3, 4, 5]}, index=idx)

    alt = pd.DataFrame(
        {
            "ts": [idx[1], idx[3]],
            "x": [10.0, 30.0],
        }
    )
    merged = asof_join(bars_df=bars, alt_df=alt, on="ts")

    # day0 has no prior alt
    assert pd.isna(merged.loc[idx[0], "x"])
    # day1 matches day1
    assert float(merged.loc[idx[1], "x"]) == 10.0
    # day2 should still be 10.0 (backward)
    assert float(merged.loc[idx[2], "x"]) == 10.0
    # day3 matches 30.0
    assert float(merged.loc[idx[3], "x"]) == 30.0


def test_asof_join_with_tolerance_drops_stale():
    idx = pd.date_range("2020-01-01", periods=3, freq="D", tz="UTC")
    bars = pd.DataFrame({"close": [1, 2, 3]}, index=idx)

    alt = pd.DataFrame({"ts": [idx[0]], "x": [1.0]})
    merged = asof_join(bars_df=bars, alt_df=alt, on="ts", tolerance=pd.Timedelta(days=0))

    assert float(merged.loc[idx[0], "x"]) == 1.0
    # tolerance=0 days => day1 and day2 should not carry forward
    assert pd.isna(merged.loc[idx[1], "x"])
    assert pd.isna(merged.loc[idx[2], "x"])
