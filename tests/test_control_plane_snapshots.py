from __future__ import annotations

import json

import pytest

pytestmark = pytest.mark.slow


def _skip_if_fastapi_missing():
    try:
        import fastapi  # noqa: F401
        from fastapi.testclient import TestClient  # noqa: F401

        return False
    except Exception:
        return True


@pytest.mark.skipif(_skip_if_fastapi_missing(), reason="fastapi not installed")
def test_risk_snapshot_uses_intent_positions(tmp_path):
    from fastapi.testclient import TestClient

    from octa_core.control_plane.api import create_app

    paper_log = tmp_path / "paper_trade_log.ndjson"
    paper_log.write_text(
        "\n".join(
            [
                json.dumps({"decision": "PLACE_PAPER_ORDER", "intent": {"symbol": "AAPL", "side": "BUY", "qty": 0.10}}),
                json.dumps({"decision": "PLACE_PAPER_ORDER", "intent": {"symbol": "AAPL", "side": "SELL", "qty": 0.05}}),
                json.dumps({"decision": "IGNORED", "intent": {"symbol": "MSFT", "side": "BUY", "qty": 1.0}}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    features = tmp_path / "features.yaml"
    features.write_text(
        """
features:
  opengamma:
    enabled: false
    required_for_live: false
  security:
    audit_log:
      enabled: true
      path: artifacts/security/audit.jsonl
modes:
  allow_live: false
thresholds: {}
""".lstrip(),
        encoding="utf-8",
    )

    app = create_app(features_path=str(features))
    c = TestClient(app)
    r = c.get("/risk_snapshot", params={"paper_log_path": str(paper_log), "positions_source": "intent"})
    assert r.status_code == 200
    obj = r.json()
    assert obj["ok"] is True
    assert obj["source"] == "local"
    assert abs(float(obj["exposures"]["AAPL"]) - 0.05) < 1e-9


@pytest.mark.skipif(_skip_if_fastapi_missing(), reason="fastapi not installed")
def test_accounting_snapshot_nav_from_prices_json(tmp_path):
    from fastapi.testclient import TestClient

    from octa_core.control_plane.api import create_app

    paper_log = tmp_path / "paper_trade_log.ndjson"
    paper_log.write_text(
        json.dumps({"decision": "PLACE_PAPER_ORDER", "intent": {"symbol": "AAPL", "side": "BUY", "qty": 2}}) + "\n",
        encoding="utf-8",
    )

    features = tmp_path / "features.yaml"
    features.write_text(
        """
features:
  accounting:
    enabled: true
  security:
    audit_log:
      enabled: true
      path: artifacts/security/audit.jsonl
modes:
  allow_live: false
thresholds: {}
""".lstrip(),
        encoding="utf-8",
    )

    accounting_cfg = tmp_path / "accounting.yaml"
    ledger_db = tmp_path / "ledger.sqlite3"
    accounting_cfg.write_text(
        f"""
accounting:
  enabled: true
  ledger_db_path: "{ledger_db.as_posix()}"
  base_currency: "EUR"
  chart_of_accounts: {{}}
  datev:
    enabled: false
    export_dir: "{(tmp_path / 'datev').as_posix()}"
    delimiter: ";"
""".lstrip(),
        encoding="utf-8",
    )

    app = create_app(features_path=str(features))
    c = TestClient(app)

    r = c.get(
        "/accounting_snapshot",
        params={
            "paper_log_path": str(paper_log),
            "accounting_config_path": str(accounting_cfg),
            "prices_json": json.dumps({"AAPL": 100.0}),
        "positions_source": "intent",
        },
    )
    assert r.status_code == 200
    obj = r.json()
    assert obj["ok"] is True
    assert obj["positions"]["AAPL"] == 2.0
    assert obj["nav"]["base_currency"] == "EUR"
    assert abs(float(obj["nav"]["nav"]) - 200.0) < 1e-9
