Minimal Feast repo used for CI integration test. It defines a FileSource `data/data.parquet` and a `demo_fv` FeatureView.

To run locally:

```bash
pip install feast
feast apply
feast materialize-incremental 2025-01-01T00:00:00 2025-12-31T23:59:59
```
