# AltData Runbook

## Offline (default)
AltData runs offline by default. If cache is missing, features are empty and gates run unchanged.

```bash
OCTA_ALLOW_NET=0 python -m octa.support.ops.run_altdata_smoke
```

## Online (optional)
Allow network fetches only if you explicitly enable it:

```bash
OCTA_ALLOW_NET=1 python -m octa.support.ops.run_altdata_smoke
```

## Configuration
- `config/altdata.yaml` controls source toggles and feature selection.
- All secrets must be in env vars (see `config/secrets_template.env`).

## Troubleshooting

- GDELT rows=0 usually means no matching coverage for the query pack or an endpoint error. Verify the query pack in `config/altdata.yaml` and check daily refresh audit logs.
- Check cached payloads under `octa/var/altdata/<source>/<YYYY-MM-DD>/`.
- Check feature store in `octa/var/altdata/altdata.duckdb`.
- Use `FeatureRegistry` to inspect stored features.
