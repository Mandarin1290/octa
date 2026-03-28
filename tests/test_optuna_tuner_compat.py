from __future__ import annotations

from types import SimpleNamespace

import pytest

from octa_training.core.optuna_tuner import (
    _catboost_estimator_compat,
    _lightgbm_callbacks_compat,
    _lightgbm_train_compat,
)


class _FakeLGB:
    __version__ = "4.6.0"

    def __init__(self) -> None:
        self.calls = []

    def early_stopping(self, rounds: int, verbose: bool = False):
        return ("early_stopping", rounds, verbose)

    def train(self, params, train_set, num_boost_round=100, valid_sets=None, callbacks=None):
        self.calls.append(
            {
                "params": dict(params),
                "num_boost_round": num_boost_round,
                "valid_sets": list(valid_sets or []),
                "callbacks": list(callbacks or []),
            }
        )

        class _Booster:
            def predict(self, X):
                return [0.5] * len(X)

        return _Booster()


class _FakeCatBoostClassifier:
    def __init__(self, **kwargs):
        self.kwargs = dict(kwargs)


class _FakeCatBoostRegressor:
    def __init__(self, **kwargs):
        self.kwargs = dict(kwargs)


class _FakeCatModule:
    __version__ = "1.2.8"
    CatBoostClassifier = _FakeCatBoostClassifier
    CatBoostRegressor = _FakeCatBoostRegressor


def test_lightgbm_compat_uses_callback_based_early_stopping() -> None:
    fake = _FakeLGB()
    booster = _lightgbm_train_compat(
        fake,
        params={"objective": "binary", "learning_rate": 0.1},
        dtrain="train",
        dval="val",
        early_stopping_rounds=25,
        call_site="test_lightgbm",
    )
    assert booster is not None
    assert len(fake.calls) == 1
    call = fake.calls[0]
    assert call["num_boost_round"] == 200
    assert call["callbacks"] == [("early_stopping", 25, False)]


def test_lightgbm_callbacks_empty_when_disabled() -> None:
    fake = _FakeLGB()
    assert _lightgbm_callbacks_compat(fake, early_stopping_rounds=0) == []


def test_catboost_compat_uses_task_specific_estimator() -> None:
    clf = _catboost_estimator_compat(
        task="cls",
        params={"learning_rate": 0.05, "depth": 6},
        iterations=200,
        verbose=False,
        call_site="test_catboost",
        catboost_module=_FakeCatModule(),
    )
    assert isinstance(clf, _FakeCatBoostClassifier)
    assert clf.kwargs["learning_rate"] == 0.05
    assert clf.kwargs["iterations"] == 200
    assert clf.kwargs["verbose"] is False


def test_catboost_unsupported_kwargs_fail_closed_with_context() -> None:
    class _BadClassifier:
        def __init__(self, **kwargs):
            raise TypeError("unexpected keyword argument 'bad_param'")

    bad_module = SimpleNamespace(
        __version__="1.2.8",
        CatBoostClassifier=_BadClassifier,
        CatBoostRegressor=_FakeCatBoostRegressor,
    )
    with pytest.raises(RuntimeError, match="unsupported_catboost_params:call_site=test_catboost_fail"):
        _catboost_estimator_compat(
            task="cls",
            params={"bad_param": 1},
            iterations=200,
            verbose=False,
            call_site="test_catboost_fail",
            catboost_module=bad_module,
        )
