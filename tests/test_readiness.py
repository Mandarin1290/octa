import json

from octa_reports.readiness import format_text, load_ledger, summarize, write_csv


def test_readiness_deterministic(tmp_path):
    ledger = tmp_path / "ledger.log"
    events = [
        {
            "ts": "2025-12-28T10:00:00Z",
            "event": "margin.evaluation",
            "margin_utilization": 0.4,
            "headroom": 60000,
        },
        {"ts": "2025-12-28T10:01:00Z", "event": "incident", "detail": "test-1"},
        {
            "ts": "2025-12-28T10:02:00Z",
            "event": "correlation_gate.evaluation",
            "score": 0.2,
        },
    ]
    ledger.write_text("\n".join(json.dumps(e, sort_keys=True) for e in events) + "\n")

    ev = load_ledger(str(ledger))
    s1 = summarize(ev)
    txt1 = format_text(s1)

    # re-load and re-summarize to assert determinism
    ev2 = load_ledger(str(ledger))
    s2 = summarize(ev2)
    txt2 = format_text(s2)

    assert txt1 == txt2

    csvp = tmp_path / "snapshot.csv"
    write_csv(str(csvp), s1)
    assert csvp.exists()
    content = csvp.read_text()
    assert "margin_utilization" in content or "event_count" in content
