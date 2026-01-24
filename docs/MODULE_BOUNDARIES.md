# Module Boundaries & IP Ownership

Purpose
- Define strict module boundaries, ownership and enforceable dependency rules to avoid hidden cross-dependencies and protect IP classifications.

Core components
- `ModuleMap`: programmatic inventory of modules, owners and classifications.
- Dependency graph extraction from source via import heuristics.
- `detect_violations`: enforces rules and reports cross-owner/internal dependencies.

Ownership classifications
- `core`: central modules that can be referenced widely.
- `licensable`: modules with IP that may be licensed; cross-owner use must be explicit.
- `internal`: private modules; cross-owner dependencies on `internal` are forbidden.

Enforcement
- Use `ModuleMap.build_from_files(root)` to bootstrap module inventory (unknown owners marked `UNKNOWN`).
- Add owners via `add_module(module, owner, classification)`.
- Call `detect_violations(allowed_cross=set(...))` to find illegal dependencies.

Notes
- This approach uses import heuristics; for strict enforcement integrate with CI by running `detect_violations` and failing builds on any violations.
