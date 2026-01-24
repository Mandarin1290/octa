OCTA Fix Pack
===============

This directory contains small focused patches and migration notes produced during
the OCTA audit and wiring-validation work. The intent is to make reviewing and
backporting fixes simple.

Included patches:
- `0001_paper_deploy_fix.patch` — deterministic Decimal quantize fix and notes.
- `0002_broker_adapter.patch` — adds a central `BrokerAdapter` and safe defaults.

How to apply a patch locally:

```bash
git apply OCTA_FIX_PACK/0001_paper_deploy_fix.patch
```

Review the migration notes (if present) before applying.
