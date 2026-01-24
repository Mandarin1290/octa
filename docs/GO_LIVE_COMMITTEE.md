# Go-Live Committee (Investment Committee)

Overview
--------
The Go-Live Committee is a software governance layer that records an immutable committee decision required to enable live trading. Decisions are auditable and enforce a cooling-off period before they become effective.

Decision Model
--------------
- `APPROVED` — committee authorizes live (effective after cooling-off period).
- `DEFERRED` — postpones decision.
- `REJECTED` — committee denies go-live.

Inputs
------
Decisions should reference:
- Checklist result (pre-live checklist)
- Relevant metrics (readiness metrics)
- Incident summary
- Operator attestations (signatures)

Usage
-----
Create a `GoLiveCommittee`, call `propose_decision(...)` with attestations and inputs. Query `decision()` for recorded decision. Use `is_live_authorized()` to check whether approved decision has matured past cooling-off.
