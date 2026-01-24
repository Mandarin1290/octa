**PAPER MODE**

Paper mode runs the full OCTA pipeline locally without connecting to a broker. It is identical to live mode except that execution is simulated locally and broker connectivity is not required.

Flow: `octa_stream` → `octa_atlas` → inference stub → portfolio intents → `octa_sentinel` pre-trade → `octa_vertex` paper executor → `octa_ledger` audit.

Usage:

```bash
python -m octa_nexus.paper_boot --manifests ./manifests --atlas ./atlas_repo --ledger ./ledger --vertex ./vertex_store
```

Notes
- Inference is deterministic and implemented as a fixed linear transform; no randomness.
- All steps are audited via `octa_ledger`. Any failure to write audit logs blocks trading.
- Sentinel is evaluated with `broker_connected=True` to avoid broker disconnect blocking in paper tests.
