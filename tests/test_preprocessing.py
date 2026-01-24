from __future__ import annotations

import pandas as pd

from scripts.preprocessing import load_spec, preprocess_df


def test_preprocess_basic(tmp_path):
    df = pd.DataFrame({
        "a": [1, 2, None, 4],
        "b": ["x", None, "y", "x"],
        "target": [0.1, 0.2, 0.3, 0.4],
    })
    X, y, spec = preprocess_df(df, target="target", spec_name="testspec")
    assert "a" in X.columns and "b" in X.columns
    # no nulls in numeric after imputation
    assert X["a"].isnull().sum() == 0
    # spec file should load
    s = load_spec("testspec")
    assert s["target"] == "target"
