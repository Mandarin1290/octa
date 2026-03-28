## Phase-2 Data Contract

- `data/raw/`: canonical reserved raw-data subtree. Current loaders still accept repo-root `raw/` and `datasets/raw/`; `data/raw/` exists to satisfy the Phase-2 contract without changing ingestion behavior.
- `data/processed/`: reserved landing area for normalized or transformed datasets.
- `data/features/`: reserved landing area for derived feature datasets.

Notes:

- Existing OCTA training and preflight code still uses repo-root `raw/` as the primary local source when present.
- No execution or training code was redirected in this remediation; only the missing contract directories were materialized.
