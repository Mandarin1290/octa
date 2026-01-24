import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from core import training_safety_lock as tsl


def write_manifest(tmpdir, run_id='test_run', artifacts_present=True, armed=False):
    gd = Path(tmpdir) / 'gates' / run_id
    ca = Path(tmpdir) / 'cascade' / run_id
    gd.mkdir(parents=True, exist_ok=True)
    ca.mkdir(parents=True, exist_ok=True)
    # create pass/fail/err for 1D
    (ca / 'pass_symbols_1D.txt').write_text('ABC\n')
    (ca / 'fail_symbols_1D.txt').write_text('')
    (ca / 'err_symbols_1D.txt').write_text('')
    manifest = {
        'run_id': run_id,
        'created_utc': datetime.now(timezone.utc).isoformat(),
        'gate_version': 'hf_gate_test',
        'artifacts_dir': str(ca),
        'manifest_dir': str(gd),
        'config_fingerprint': 'dummycfg',
    }
    (gd / 'gate_manifest.json').write_text(json.dumps(manifest))
    if armed:
        (gd / 'ARMED.ok').write_text('armed')
    return manifest, gd, ca


class DummyCfg:
    def model_dump(self):
        return {'dummy': 1}


def test_no_manifest(tmp_path):
    with pytest.raises(tsl.GateManifestMissingError):
        tsl.load_latest_gate_run(tmp_path / 'nonexistent')


def test_missing_armed_blocks(tmp_path):
    manifest, gd, ca = write_manifest(tmp_path, 'r1', armed=False)
    with pytest.raises(tsl.GateArtifactsInvalidError):
        tsl.assert_training_armed(DummyCfg(), 'ABC', '1D', manifest_path_or_dir=gd)


def test_armed_allows(tmp_path, monkeypatch):
    manifest, gd, ca = write_manifest(tmp_path, 'r2', armed=True)
    # make config fingerprint match by monkeypatching verify_config_alignment
    monkeypatch.setattr(tsl, 'verify_config_alignment', lambda m, c: {'ok': True})
    monkeypatch.setattr(tsl, 'verify_strict_cascade_enabled', lambda c: {'ok': True})
    assert tsl.assert_training_armed(DummyCfg(), 'ABC', '1D', manifest_path_or_dir=gd) is None
