# API Contracts & Versioning

Purpose
- Make internal APIs explicit, versioned, and auditable to avoid implicit coupling and accidental breaking changes.

Core concepts
- `Contract`: describes `name`, `version`, `input_schema` and `output_schema`.
- `ContractRegistry`: register contract versions and run compatibility checks.

Schema format
- Schemas are simple maps: `field -> { type: <str>, required: <bool> }`.

Versioning rules
- Backward compatible changes (adding optional fields) → `minor` bump.
- Breaking changes (removing required fields or changing types) → `major` bump.
- Non-breaking metadata changes → `patch`.

Compatibility checks
- Use `ContractRegistry.is_compatible(old, new)` to get `(compatible, reason, required_bump)`.
- Integrate this check into CI to prevent accidental breaking merges without version bump.

Contract tests
- Include unit tests that assert compatibility behavior for common changes.
