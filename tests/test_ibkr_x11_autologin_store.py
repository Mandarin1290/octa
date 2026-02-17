from __future__ import annotations

from octa.execution import ibkr_autologin_store as s


def test_store_crud_roundtrip(tmp_path):
    db = str(tmp_path / "ibkr.sqlite3")
    s.ensure_schema(db)
    s.upsert_profile(
        db,
        {
            "profile_name": "tws_disclaimer",
            "window_title": "Disclaimer",
            "window_title_contains": "Disclaimer",
            "window_classname": "java",
            "wm_class_instance": "sun-awt",
            "wm_class_class": "Tws",
            "wm_class_raw": "WM_CLASS(STRING)=\"sun-awt\",\"Tws\"",
            "taught_window_width_px": 800,
            "taught_window_height_px": 600,
            "relative_x_px": 100,
            "relative_y_px": 200,
            "relative_x_percent": 0.125,
            "relative_y_percent": 0.333333,
            "screen_width_px": 1920,
            "screen_height_px": 1080,
        },
    )

    assert s.list_profiles(db) == ["tws_disclaimer"]
    rows = s.get_profiles(db)
    assert len(rows) == 1
    assert rows[0]["profile_name"] == "tws_disclaimer"
    assert rows[0]["relative_x_percent"] == 0.125
    assert rows[0]["relative_y_percent"] == 0.333333

    removed = s.remove_profile(db, "tws_disclaimer")
    assert removed is True
    assert s.list_profiles(db) == []
