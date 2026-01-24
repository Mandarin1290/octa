**Gate → Arm → Train**

Kurzanleitung zur neuen TRAINING SAFETY LOCK (Fail‑Closed)

- Gate Sweep (beispiel):

```bash
./.venv/bin/python scripts/batch_gate_check_indices_1d.py --cascade --cascade-gate-only
```

- Arm nach erfolgreichem Sweep (findet manifest automatisch):

```bash
./.venv/bin/python tools/arm_training_from_gate.py --manifest reports/gates/<run_id>

Secondary per-asset gate
------------------------

After the primary gate run you can optionally run a second, per-asset gate sweep.
This produces per-asset manifests under the original `artifacts_dir` and a consolidated
pass list at `<artifacts_dir>/assets/assets_pass.txt`.

Run:

./.venv/bin/python tools/run_secondary_asset_gates.py --manifest reports/gates/<run_id>

The `tools/run_secondary_asset_gates.py` script runs a safe-mode evaluation for each symbol
in the primary `pass_symbols_1D.txt` and writes `asset_gate_manifest.json` for each asset.
You can then choose to require these secondary gates during arming by using the `--require-secondary`
option in the arm tool (not enabled by default).
```

- Train (zuvor ARMED.ok prüfen wird automatisch von Guard):

```bash
./.venv/bin/python scripts/train_multiframe_symbol.py --symbol DVS --run-id myrun
```

Wenn das Gate nicht ARMed ist, wird `train_multiframe_symbol.py` mit Fehler abbrechen und einen Audit‑Eintrag unter `reports/gates/<run_id>/audit.jsonl` erzeugen.
