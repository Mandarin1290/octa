from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class AutologinProfile:
    profile_name: str
    window_title: str | None
    window_title_contains: str | None
    window_classname: str | None
    wm_class_instance: str | None
    wm_class_class: str | None
    wm_class_raw: str | None
    taught_window_width_px: int
    taught_window_height_px: int
    relative_x_px: int
    relative_y_px: int
    relative_x_percent: float
    relative_y_percent: float
    screen_width_px: int
    screen_height_px: int
    created_utc: str
    updated_utc: str


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _connect(db_path: str) -> sqlite3.Connection:
    p = Path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def ensure_schema(db_path: str) -> None:
    conn = _connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ibkr_autologin_profiles(
                profile_name TEXT PRIMARY KEY,
                window_title TEXT,
                window_title_contains TEXT,
                window_classname TEXT,
                wm_class_instance TEXT,
                wm_class_class TEXT,
                wm_class_raw TEXT,
                taught_window_width_px INTEGER NOT NULL,
                taught_window_height_px INTEGER NOT NULL,
                relative_x_px INTEGER NOT NULL,
                relative_y_px INTEGER NOT NULL,
                relative_x_percent REAL NOT NULL,
                relative_y_percent REAL NOT NULL,
                screen_width_px INTEGER NOT NULL,
                screen_height_px INTEGER NOT NULL,
                created_utc TEXT NOT NULL,
                updated_utc TEXT NOT NULL
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def upsert_profile(db_path: str, profile: dict[str, Any]) -> None:
    ensure_schema(db_path)
    now = _utc_now()
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT created_utc FROM ibkr_autologin_profiles WHERE profile_name=?",
            (str(profile["profile_name"]),),
        )
        row = cur.fetchone()
        created = str(row[0]) if row else now
        cur.execute(
            """
            INSERT OR REPLACE INTO ibkr_autologin_profiles(
                profile_name, window_title, window_title_contains, window_classname,
                wm_class_instance, wm_class_class, wm_class_raw,
                taught_window_width_px, taught_window_height_px,
                relative_x_px, relative_y_px,
                relative_x_percent, relative_y_percent,
                screen_width_px, screen_height_px,
                created_utc, updated_utc
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                str(profile["profile_name"]),
                profile.get("window_title"),
                profile.get("window_title_contains"),
                profile.get("window_classname"),
                profile.get("wm_class_instance"),
                profile.get("wm_class_class"),
                profile.get("wm_class_raw"),
                int(profile["taught_window_width_px"]),
                int(profile["taught_window_height_px"]),
                int(profile["relative_x_px"]),
                int(profile["relative_y_px"]),
                round(float(profile["relative_x_percent"]), 6),
                round(float(profile["relative_y_percent"]), 6),
                int(profile["screen_width_px"]),
                int(profile["screen_height_px"]),
                created,
                now,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def list_profiles(db_path: str) -> list[str]:
    ensure_schema(db_path)
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            "SELECT profile_name FROM ibkr_autologin_profiles ORDER BY profile_name ASC"
        )
        return [str(r[0]) for r in cur.fetchall()]
    finally:
        conn.close()


def get_profiles(db_path: str) -> list[dict[str, Any]]:
    ensure_schema(db_path)
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            """
            SELECT
                profile_name, window_title, window_title_contains, window_classname,
                wm_class_instance, wm_class_class, wm_class_raw,
                taught_window_width_px, taught_window_height_px,
                relative_x_px, relative_y_px,
                relative_x_percent, relative_y_percent,
                screen_width_px, screen_height_px,
                created_utc, updated_utc
            FROM ibkr_autologin_profiles
            ORDER BY profile_name ASC
            """
        )
        out = []
        for r in cur.fetchall():
            out.append(
                {
                    "profile_name": r[0],
                    "window_title": r[1],
                    "window_title_contains": r[2],
                    "window_classname": r[3],
                    "wm_class_instance": r[4],
                    "wm_class_class": r[5],
                    "wm_class_raw": r[6],
                    "taught_window_width_px": int(r[7]),
                    "taught_window_height_px": int(r[8]),
                    "relative_x_px": int(r[9]),
                    "relative_y_px": int(r[10]),
                    "relative_x_percent": round(float(r[11]), 6),
                    "relative_y_percent": round(float(r[12]), 6),
                    "screen_width_px": int(r[13]),
                    "screen_height_px": int(r[14]),
                    "created_utc": str(r[15]),
                    "updated_utc": str(r[16]),
                }
            )
        return out
    finally:
        conn.close()


def remove_profile(db_path: str, profile_name: str) -> bool:
    ensure_schema(db_path)
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            "DELETE FROM ibkr_autologin_profiles WHERE profile_name=?",
            (str(profile_name),),
        )
        conn.commit()
        return int(cur.rowcount) > 0
    finally:
        conn.close()
