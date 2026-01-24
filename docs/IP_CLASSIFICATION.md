# IP Classification & Runtime Enforcement

Purpose
- Explicitly classify modules by IP category and enforce licensing constraints at runtime to prevent accidental inclusion of incompatible code.

Categories
- `CORE_PROPRIETARY` — mission‑critical proprietary code; must not depend on `OPEN_SOURCE_DERIVED`.
- `LICENSABLE` — IP that may be licensed to partners; cross‑owner use requires review.
- `INTERNAL_ONLY` — private modules; cross‑owner imports of `internal` modules are forbidden.
- `OPEN_SOURCE_DERIVED` — code derived from open‑source; may impose license obligations.

Usage
- Create `IPClassifier`, set classifications via `set_classification(module, category)` or `classify_from_module_map(ModuleMap)`.
- Call `validate(module_map)` to get violations or `enforce_runtime(module_map)` to raise on violations (suitable for CI/runtime guardrails).

Notes
- The default constraints block `CORE_PROPRIETARY` from importing `OPEN_SOURCE_DERIVED`.
- For production, maintain an authoritative ownership file (e.g., CODEOWNERS) and run `enforce_runtime` in CI to fail builds on violations.
