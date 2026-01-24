# Regulatory Framework Mapping (Logical)

Purpose: translate high-level regulatory domains into control requirements and map them to OCTA components for engineering and control design. This is a logical, technical mapping — not legal advice.

Target domains:

- `market_abuse_prevention`
- `risk_management`
- `record_keeping`
- `investor_protection`
- `operational_resilience`

Approach:

- Discover top-level `octa_` components in the repo root.
- Apply conservative heuristics to map components to domains (based on component naming and role).
- Provide control requirement templates per domain (e.g., `immutable_logs` for `record_keeping`).
- Validate that every discovered component has at least one domain mapped; unmapped components must be resolved before release.

How to use:

- Programmatically: import `octa_reg.reg_map` and call `generate_mapping()` and `validate_all()`.
- Human review: inspect mappings and adjust per organizational policy.

Limitations:

- This mapping is logical and technical; consult legal/compliance teams for binding regulatory advice.
