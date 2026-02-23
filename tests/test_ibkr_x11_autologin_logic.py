from __future__ import annotations

import json
from pathlib import Path

from octa.execution import ibkr_x11_autologin as mod


def test_watcher_clicks_once_and_emits_events(monkeypatch, tmp_path: Path):
    events = tmp_path / "events.jsonl"

    monkeypatch.setattr(
        mod.store,
        "get_profiles",
        lambda _db: [
            {
                "profile_name": "tws_disclaimer",
                "window_classname": "java",
                "wm_class_class": "Tws",
                "window_title_contains": "Disclaimer",
                "taught_window_width_px": 800,
                "taught_window_height_px": 600,
                "relative_x_percent": 0.5,
                "relative_y_percent": 0.5,
            }
        ],
    )
    monkeypatch.setattr(mod, "_search_candidates", lambda _p: ("classname", ["123"]))
    monkeypatch.setattr(mod, "_get_window_geometry", lambda _w: {"x": 10, "y": 20, "width": 800, "height": 600})
    monkeypatch.setattr(mod, "_run_cmd", lambda _cmd: "")

    res = mod.run_watcher(
        db_path=str(tmp_path / "db.sqlite3"),
        timeout_sec=5,
        keepalive=False,
        dry_run=True,
        events_path=str(events),
    )
    assert res["ok"] is True

    rows = [json.loads(x) for x in events.read_text(encoding="utf-8").splitlines() if x.strip()]
    assert any(r.get("event_type") == "watcher_start" for r in rows)
    assert any(r.get("event_type") == "window_found" for r in rows)
    assert any(r.get("event_type") == "click_performed" for r in rows)
    assert any(r.get("event_type") == "watcher_stop" for r in rows)


def test_watcher_timeout(monkeypatch, tmp_path: Path):
    events = tmp_path / "events.jsonl"
    monkeypatch.setattr(mod.store, "get_profiles", lambda _db: [{"profile_name": "p1", "window_title_contains": "x"}])
    monkeypatch.setattr(mod, "_search_candidates", lambda _p: (None, []))

    res = mod.run_watcher(
        db_path=str(tmp_path / "db.sqlite3"),
        timeout_sec=1,
        keepalive=True,
        dry_run=True,
        events_path=str(events),
    )
    assert res["ok"] is False
    assert res["reason"] == "timeout"
