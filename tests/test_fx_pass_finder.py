import json
import types
from pathlib import Path

import scripts.global_gate_diagnose as ggd


def test_fx_pass_finder_writes_candidates_and_summary(tmp_path, monkeypatch):
    reports_dir = tmp_path / 'reports'
    reports_dir.mkdir(parents=True, exist_ok=True)
    # Ensure quarantine file exists (empty)
    (reports_dir / 'fx_g1_quarantine_symbols.txt').write_text('', encoding='utf-8')

    fx_root = tmp_path / 'fx'
    fx_root.mkdir(parents=True, exist_ok=True)

    cfg = types.SimpleNamespace(paths=types.SimpleNamespace(reports_dir=str(reports_dir)), features={})

    def _fake_run_fx_two_stage(
        root,
        cfg,
        state,
        run_id,
        out_fh,
        limit,
        quiet_symbols,
        append_quarantine=False,
        quarantined_symbols=None,
        horizons_tag=None,
        emit_pass_candidates=False,
        pass_candidates_out=None,
        near_miss_out=None,
    ):
        sym = 'EURUSD'
        passed = list(horizons_tag or []) == [3]
        if emit_pass_candidates and passed and pass_candidates_out is not None:
            pass_candidates_out.add(sym)
            out_fh.write(json.dumps({'type': 'pass_candidate', 'dataset': 'fx', 'symbol': sym, 'horizons': horizons_tag, 'run_id': run_id}) + '\n')
        out_fh.write(
            json.dumps(
                {
                    'type': 'symbol',
                    'dataset': 'fx',
                    'symbol': sym,
                    'horizons': horizons_tag,
                    'gate': {'passed': passed, 'status': 'PASS' if passed else 'FAIL_STRUCTURAL', 'reasons': [], 'diagnostics': []},
                }
            )
            + '\n'
        )
        return {'dataset': 'fx', 'status_counts': {'PASS': 1} if passed else {'FAIL_STRUCTURAL': 1}, 'top_reasons': [], 'selected': 1, 'discovered': 1, 'stats': {}}

    monkeypatch.setattr(ggd, '_run_fx_two_stage', _fake_run_fx_two_stage)

    res = ggd._run_fx_pass_finder(cfg=cfg, state=None, run_id='run123', fx_root=fx_root, quiet_symbols=True, sweep_fast=False)
    assert res['dataset'] == 'fx_pass_finder'

    cand = reports_dir / 'fx_pass_candidates.txt'
    assert cand.exists()
    assert [ln.strip() for ln in cand.read_text(encoding='utf-8').splitlines() if ln.strip()] == ['EURUSD']

    summary_path = Path(res['summary_path'])
    assert summary_path.exists()
    summary = json.loads(summary_path.read_text(encoding='utf-8'))
    assert summary['type'] == 'pass_finder_summary'
    assert 'EURUSD' in summary['pass_candidates']

    # Ensure at least one per-horizon output has a pass_candidate event.
    outs = [Path(x['out']) for x in summary['per_horizon_set'] if x.get('out')]
    assert outs
    found = False
    for p in outs:
        txt = p.read_text(encoding='utf-8')
        if 'pass_candidate' in txt:
            found = True
            break
    assert found
