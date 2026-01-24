OCTA Artifact Standard

Artifacts (models, features, risk profiles) must be stored with full provenance and integrity.

Structure

/models/<asset_id>/<artifact_type>/<version>/
  - artifact.pkl        # pickled artifact
  - meta.json           # signed metadata header containing provenance
  - integrity.sha256    # SHA256 of artifact.pkl

Metadata required

- `asset_id`, `artifact_type`, `version`, `created_at` (UTC)
- `dataset_hash` (source parquet content hash)
- `training_window` (ISO range)
- `feature_spec_hash` (hash of feature spec)
- `hyperparams` (dict)
- `metrics` (dict)
- `code_fingerprint` (sha256)
- `gate_status` (`PENDING`, `APPROVED`, `REJECTED`)
- `artifact_hash` (sha256 of artifact.pkl)
- `signature` (base64 ed25519 signature over JSON metadata, optional)

Security

- Signing private key must be managed by `octa_fabric` secrets; never commit private keys.
- On load, `code_fingerprint`, `artifact_hash`, and `signature` (if public key provided) are verified; mismatch => fail hard and block trading.
