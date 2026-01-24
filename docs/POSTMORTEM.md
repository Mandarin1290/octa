# Post‑Incident Review & Root Cause Analysis

Purpose: automate and enforce postmortem reviews for significant incidents (S2+).

Hard rules:

- Every S2+ incident requires a review to be created and owned.
- Root cause categorization must be recorded from an approved set of categories.
- Follow‑up remediation tasks must be tracked to completion.

Features:

- `PostmortemManager.start_review(incident, reviewer)` — create a review with an initial timeline entry.
- Timeline events: `add_timeline_event(incident_id, event, actor, details)` to record investigation steps.
- Cause categorization: `categorize_cause(incident_id, category, rationale, actor)` where `category` is one of `network, data, human, process, system, third_party`.
- Remediation tasks: `add_remediation_task(...)`, `complete_task(...)`, `list_open_tasks(...)`.

Operator guidance:

- Begin the review immediately after recording an S2+ incident.
- Prefer precise rationales when categorizing root causes — these aid future automation and reporting.
- Assign remediation tasks to clear owners and track to completion; close the review only after all critical tasks are done.

Implementation notes:

- See `octa_ops/postmortem.py` for the manager class and data models.
- Persist reviews and audit logs to an append‑only store for compliance and evidence retention.
