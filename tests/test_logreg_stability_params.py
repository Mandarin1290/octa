from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd

from octa_training.core.config import load_config
from octa_training.core.packaging import _train_full_model
from octa_training.core.models import train_models
from octa_training.core.splits import SplitFold


def test_proof_config_enables_causal_quantiles_and_logreg_regularization() -> None:
    cfg = load_config("/home/n-b/Octa/octa_training/config/training_1symbol_proof.yaml")
    assert cfg.signal.causal_quantiles is True
    assert cfg.signal.quantile_window == 252
    assert cfg.logreg_params["C"] == 0.05
    assert cfg.logreg_params["solver"] == "liblinear"


def test_packaging_retrain_uses_logreg_params() -> None:
    X = pd.DataFrame({"a": [0.0, 1.0, 0.0, 1.0], "b": [1.0, 0.0, 1.0, 0.0]})
    y = pd.Series([0, 1, 0, 1])
    settings = SimpleNamespace(logreg_params={"C": 0.07, "solver": "liblinear", "max_iter": 321})
    model, _, _ = _train_full_model(X, y, "logreg", "cls", settings, seed=7)
    assert abs(float(model.C) - 0.07) < 1e-12
    assert int(model.max_iter) == 321
    assert model.solver == "liblinear"


def test_train_models_uses_configured_logreg_params() -> None:
    idx = pd.date_range("2020-01-01", periods=80, freq="D", tz="UTC")
    X = pd.DataFrame(
        {
            "f1": np.sin(np.linspace(0, 8, len(idx))),
            "f2": np.cos(np.linspace(0, 8, len(idx))),
            "f3": np.linspace(-1, 1, len(idx)),
        },
        index=idx,
    )
    y = pd.Series((X["f1"] > 0).astype(int), index=idx)
    settings = SimpleNamespace(
        seed=11,
        scale_linear=True,
        models_order=["logreg"],
        tuning=SimpleNamespace(enabled=False, models_order=["logreg"]),
        logreg_params={"C": 0.03, "solver": "liblinear", "max_iter": 456},
    )
    splits = [SplitFold(train_idx=np.arange(0, 60), val_idx=np.arange(60, 80), fold_meta={})]
    trained = train_models(
        X,
        {"y_cls_1": y},
        splits,
        settings,
        device_profile=None,
        fast=False,
        prices=None,
        eval_settings=None,
    )
    assert trained
    params = trained[0].params
    assert abs(float(params["C"]) - 0.03) < 1e-12
    assert int(params["max_iter"]) == 456
    assert params["solver"] == "liblinear"
