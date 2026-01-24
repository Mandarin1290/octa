OCTA Operating Model (research → pm → risk → exec → audit)

Flow:

1. Research produces models and feature definitions; artifacts registered in `octa_atlas`.
2. PM validates business rules and requests risk review.
3. Risk (`octa_sentinel`) runs deterministic checks; gates can block deployment or execution.
4. Execution (`octa_vertex`) receives only approved artifacts and orders; risk gate is consulted before any execution action.
5. Ledger (`octa_ledger`) records immutable audit trail for all decisions and order lifecycle events.

Operational guarantees:

- Safe default: any failure in config, risk, or ledger results in no execution. Fail closed.
- All actions are auditable and recorded in `octa_ledger`.
- Configuration is managed through `octa_fabric` using environment-backed settings.
