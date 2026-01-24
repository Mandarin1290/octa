**TESTING STRATEGY**

- This test harness runs integration scenarios locally using the sqlite-backed `NexusBus`.
- Scenarios are designed to validate safety and auditable traces; they do not assert any trading profitability.
- Each scenario is self-contained and runs quickly using temporary directories.

Scenarios:
- `data_contract_failure` — invalid data causes an `INELIGIBLE` RiskDecision and prevents OrderIntent publication.
- `audit_failure` — simulates a missing/corrupt ledger and verifies `SentinelEngine` returns a blocking decision.
- `missing_heartbeat` — absence of heartbeats triggers the `HealthMonitor` to publish `RiskDecision(FREEZE)`.
- `order_ack_timeout` — simulates stuck consumer, resulting in an `Incident` and a `RiskDecision(FREEZE)`.
- `integrity_failure` — corrupting a saved artifact triggers `FileIntegrityError`; a freeze is published and an audit event appended.

Run locally:

```bash
pytest octa_tests/test_end_to_end.py -q
```
