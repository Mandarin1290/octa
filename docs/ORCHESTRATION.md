**ORCHESTRATION**

- **Goal:** Provide local, reliable process orchestration and an auditable message bus for OCTA components (Training, Inference, Execution, Risk, Audit).
- **Transport:** sqlite-backed durable queue (see `octa_nexus.bus.NexusBus`). Messages are persisted to disk and survive restarts.
- **Message types:** Implemented in `octa_nexus.messages` (SignalEvent, PortfolioIntent, RiskDecision, OrderIntent, OrderStatus, Healthbeat, Incident).
- **Delivery semantics:** At-least-once delivery with claim/ack/nack. Consumers must be idempotent (store processed message ids locally) to achieve exactly-once-ish behavior.
- **Supervision:** `octa_nexus.supervisor.Supervisor` can spawn components as separate processes and restarts crashed ones.
- **Health:** `octa_nexus.health.HealthMonitor` inspects recent `Healthbeat` messages and publishes `RiskDecision` with `FREEZE` when heartbeats are missing.

Demo:

Run the local paper-mode demo:

```bash
python3 scripts/run_nexus_paper.py
```

Testing:

```bash
pytest tests/test_nexus.py -q
```

Notes:

- Consumers should record processed message IDs (local sqlite or durable store) to ensure idempotency.
- The bus uses simple row locking and timestamp-based lock expiry to allow re-claiming messages from crashed consumers.
- This implementation intentionally stays local and process-based to allow future evolution into distributed transports (Redis/Kafka) without changing message contracts.
