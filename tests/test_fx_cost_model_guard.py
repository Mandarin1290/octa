import types

from octa_training.core.pipeline import (
    evaluate_fx_g0_risk_overlay_1d,
    evaluate_fx_g1_alpha_1h,
)


class _Cfg:
    def __init__(self, cost_bps: float, spread_bps: float, require: bool = True):
        self.broker = types.SimpleNamespace(cost_bps=cost_bps, spread_bps=spread_bps, stress_cost_multiplier=3.0, name='ibkr')
        self.signal = types.SimpleNamespace(cost_bps=1.0, spread_bps=0.5, stress_cost_multiplier=3.0)
        self.costs = types.SimpleNamespace(require_nonzero_for_fx=require)


def test_fx_cost_guard_blocks_g0_when_zero_costs(monkeypatch):
    cfg = _Cfg(cost_bps=0.0, spread_bps=0.0, require=True)

    def _should_not_call(**kwargs):
        raise AssertionError('train_evaluate_package should not be called when FX cost guard triggers')

    monkeypatch.setattr('octa_training.core.pipeline.train_evaluate_package', _should_not_call)

    res = evaluate_fx_g0_risk_overlay_1d('EURUSD', cfg, state=None, run_id='r1', parquet_path='/tmp/x.parquet', safe_mode=True)
    assert res.passed is False
    assert res.error == 'fx_cost_model_missing_or_zero'
    assert res.gate_result is not None
    assert res.gate_result.status == 'FAIL_DATA'
    assert any('fx_cost_model_missing_or_zero' in r for r in (res.gate_result.reasons or []))


def test_fx_cost_guard_blocks_g1_when_zero_costs(monkeypatch):
    cfg = _Cfg(cost_bps=0.0, spread_bps=0.0, require=True)

    def _should_not_call(**kwargs):
        raise AssertionError('train_evaluate_package should not be called when FX cost guard triggers')

    monkeypatch.setattr('octa_training.core.pipeline.train_evaluate_package', _should_not_call)

    res = evaluate_fx_g1_alpha_1h('EURUSD', cfg, state=None, run_id='r1', parquet_path='/tmp/x.parquet', safe_mode=True)
    assert res.passed is False
    assert res.error == 'fx_cost_model_missing_or_zero'
    assert res.gate_result is not None
    assert res.gate_result.status == 'FAIL_DATA'
    assert any('fx_cost_model_missing_or_zero' in r for r in (res.gate_result.reasons or []))


def test_fx_cost_guard_can_be_disabled(monkeypatch):
    cfg = _Cfg(cost_bps=0.0, spread_bps=0.0, require=False)

    called = {'n': 0}

    def _fake_train_evaluate_package(**kwargs):
        called['n'] += 1
        # minimal object with required attributes
        return types.SimpleNamespace(gate_result={'passed': False, 'status': 'FAIL_STRUCTURAL', 'reasons': ['x'], 'diagnostics': []}, error=None)

    monkeypatch.setattr('octa_training.core.pipeline.train_evaluate_package', _fake_train_evaluate_package)

    res = evaluate_fx_g0_risk_overlay_1d('EURUSD', cfg, state=None, run_id='r1', parquet_path='/tmp/x.parquet', safe_mode=True)
    assert called['n'] == 1
    assert res is not None
