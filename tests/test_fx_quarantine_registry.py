import io
import json
import types
from pathlib import Path

import pandas as pd

import scripts.global_gate_diagnose as ggd


def test_fx_quarantine_registry_written_and_event_emitted(tmp_path, monkeypatch):
    fx_root = tmp_path / 'fx'
    fx_root.mkdir(parents=True, exist_ok=True)

    fx_parquet_dir = tmp_path / 'FX_parquet'
    fx_parquet_dir.mkdir(parents=True, exist_ok=True)

    # Create minimal parquet placeholders (content never read from disk due to monkeypatched load_parquet).
    (fx_root / 'EURUSD_1D.parquet').write_bytes(b'')
    (fx_parquet_dir / 'EURUSD_1H.parquet').write_bytes(b'')

    # Non-hourly-like 1H data: daily spacing.
    df_1h = pd.DataFrame({'x': range(10)}, index=pd.date_range('2020-01-01', periods=10, freq='D', tz='UTC'))

    monkeypatch.setattr(ggd, 'load_parquet', lambda p: df_1h)

    # Force G0 to PASS so G1 is reached, then the spacing check triggers quarantine.
    def _fake_g0(sym, cfg, state, run_id, parquet_path, safe_mode=True):
        gate = {'passed': True, 'status': 'PASS', 'reasons': [], 'passed_checks': [], 'insufficient_evidence': [], 'robustness': None, 'diagnostics': []}
        return types.SimpleNamespace(gate_result=gate, error=None)

    monkeypatch.setattr(ggd, 'evaluate_fx_g0_risk_overlay_1d', _fake_g0)

    # G1 should NOT be called in this case.
    monkeypatch.setattr(ggd, 'evaluate_fx_g1_alpha_1h', lambda *a, **k: (_ for _ in ()).throw(AssertionError('G1 should not run for non-hourly 1H parquet')))

    cfg = types.SimpleNamespace(paths=types.SimpleNamespace(reports_dir=str(tmp_path / 'reports'), fx_parquet_dir=str(fx_parquet_dir)))

    out = io.StringIO()
    summary = ggd._run_fx_two_stage(
        root=fx_root,
        cfg=cfg,
        state=None,
        run_id='run123',
        out_fh=out,
        limit=0,
        quiet_symbols=True,
        append_quarantine=False,
    )

    assert summary['dataset'] == 'fx'

    reports_dir = Path(cfg.paths.reports_dir)
    txt = reports_dir / 'fx_g1_quarantine_symbols.txt'
    js = reports_dir / 'fx_g1_quarantine_symbols.json'
    assert txt.exists()
    assert js.exists()

    txt_syms = [ln.strip() for ln in txt.read_text(encoding='utf-8').splitlines() if ln.strip()]
    assert txt_syms == ['EURUSD']

    payload = json.loads(js.read_text(encoding='utf-8'))
    assert isinstance(payload, list) and payload
    assert payload[0]['symbol'] == 'EURUSD'
    assert payload[0]['reason'] in {'non_hourly_1h_parquet', 'invalid_1h_data'}

    ndjson = out.getvalue().strip().splitlines()
    quarantine_events = [json.loads(l) for l in ndjson if json.loads(l).get('type') == 'quarantine']
    assert len(quarantine_events) == 1
    assert quarantine_events[0]['symbol'] == 'EURUSD'
