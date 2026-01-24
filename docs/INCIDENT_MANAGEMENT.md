# Incident Management

Overview
--------
This module implements a simple, auditable incident management engine for operational excellence.

Principles
----------
- All incidents are represented as immutable audit events in the ledger. The incident lifecycle (creation, timeline entries, root-cause notes, resolution) is represented by additional audit events that reference the original `incident_id`.
- Any freeze, kill-switch, or L2+ sentinel event creates an incident automatically.
- Incidents are append-only and cannot be modified; resolution is recorded as a separate event.

Event actions
-------------
- `incident.created` — initial incident record (actor: `incidents`).
- `incident.timeline` — timeline notes referencing `incident_id`.
- `incident.root_cause` — human or automated root cause analysis.
- `incident.resolved` — resolution event marking the incident closed.

Usage
-----
Use `octa_ledger.incidents.IncidentStore(ledger_store)` to create and manipulate incidents. All operations write `AuditEvent`s to the ledger, ensuring immutability and auditability.
