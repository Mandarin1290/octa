**Chaos Playbook & Kill‑Switch Drills**

This document describes on‑demand operational drills to validate OCTA's resilience.

Drills
- `audit_degraded`: simulate slow/failing audit writes. Expect fail‑closed and incident.
- `message_bus_backlog`: simulate broker backlog/consumer crash. Expect warning/freeze.
- `execution_ack_timeout`: simulate execution acknowledgement storm. Expect de‑risk/kill.
- `data_integrity_failure`: simulate checksum/data corruption mid‑run. Expect kill & incident.
- `correlation_drawdown`: simulate correlation spike combined with drawdown ladder activation.

Running drills
- Drills are callable functions in `octa_tests.drills.*` and accept injectable stubs for audit, sentinel and incident recording.
- Each drill writes an incident record (append only) and invokes `sentinel.set_gate(level, reason)` when appropriate.

Post‑drill policy
- If a drill returns failure the system must remain in frozen mode until a human reviews and clears incidents.
- Drill results are immutable audit records and should be included in post‑mortem reports.
