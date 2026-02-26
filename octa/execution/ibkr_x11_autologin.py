from __future__ import annotations

import argparse
import json
import re
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import ibkr_autologin_store as store


def _utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _run_cmd(argv: list[str]) -> str:
    cp = subprocess.run(argv, capture_output=True, text=True, check=False)
    if cp.returncode != 0:
        raise RuntimeError(f"command_failed:{argv}:{(cp.stderr or '').strip()}")
    return cp.stdout or ""


def _parse_shell_kv(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for ln in text.splitlines():
        if "=" in ln:
            k, v = ln.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def _wm_class_parts(raw: str) -> tuple[str | None, str | None, str | None]:
    if not raw:
        return None, None, None
    m = re.findall(r'"([^"]+)"', raw)
    if len(m) >= 2:
        return m[0], m[1], raw.strip()
    return None, None, raw.strip()


def _write_event(path: str, payload: dict[str, Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    row = dict(payload)
    row.setdefault("ts", _utc_iso())
    with p.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, sort_keys=True) + "\n")


def _get_window_geometry(winid: str) -> dict[str, int]:
    kv = _parse_shell_kv(_run_cmd(["xdotool", "getwindowgeometry", "--shell", str(winid)]))
    return {
        "x": int(kv.get("X", "0")),
        "y": int(kv.get("Y", "0")),
        "width": int(kv.get("WIDTH", "0")),
        "height": int(kv.get("HEIGHT", "0")),
    }


def _score_window(geom: dict[str, int], profile: dict[str, Any]) -> int:
    dw = abs(int(geom["width"]) - int(profile.get("taught_window_width_px", 0)))
    dh = abs(int(geom["height"]) - int(profile.get("taught_window_height_px", 0)))
    return int(dw + dh)


def _search_candidates(profile: dict[str, Any]) -> tuple[str | None, list[str]]:
    methods: list[tuple[str, list[str]]] = []
    if profile.get("window_classname"):
        methods.append(("classname", ["xdotool", "search", "--onlyvisible", "--classname", str(profile["window_classname"])]))
    if profile.get("wm_class_class"):
        methods.append(("class", ["xdotool", "search", "--onlyvisible", "--class", str(profile["wm_class_class"])]))
    if profile.get("window_title_contains"):
        methods.append(("name", ["xdotool", "search", "--onlyvisible", "--name", str(profile["window_title_contains"])]))

    for method, cmd in methods:
        cp = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if cp.returncode != 0:
            continue
        ids = [ln.strip() for ln in (cp.stdout or "").splitlines() if ln.strip()]
        if ids:
            return method, ids
    return None, []


def _capture_profile(profile_name: str, db_path: str) -> dict[str, Any]:
    mouse = _parse_shell_kv(_run_cmd(["xdotool", "getmouselocation", "--shell"]))
    winid = (_run_cmd(["xdotool", "getactivewindow"]).strip() or "0")
    title = _run_cmd(["xdotool", "getwindowname", winid]).strip()
    try:
        classname = _run_cmd(["xdotool", "getwindowclassname", winid]).strip()
    except Exception:
        classname = ""
    wm_raw = _run_cmd(["xprop", "-id", winid, "WM_CLASS"]).strip()
    wm_inst, wm_cls, wm_raw_out = _wm_class_parts(wm_raw)
    geom = _get_window_geometry(winid)
    disp = _run_cmd(["xdotool", "getdisplaygeometry"]).strip().split()
    screen_w = int(disp[0]) if len(disp) >= 2 else 0
    screen_h = int(disp[1]) if len(disp) >= 2 else 0

    mx = int(mouse.get("X", "0"))
    my = int(mouse.get("Y", "0"))
    rel_x = mx - int(geom["x"])
    rel_y = my - int(geom["y"])
    if geom["width"] <= 0 or geom["height"] <= 0:
        raise RuntimeError("invalid_window_geometry")

    prof = {
        "profile_name": str(profile_name),
        "window_title": title,
        "window_title_contains": title,
        "window_classname": classname or None,
        "wm_class_instance": wm_inst,
        "wm_class_class": wm_cls,
        "wm_class_raw": wm_raw_out,
        "taught_window_width_px": int(geom["width"]),
        "taught_window_height_px": int(geom["height"]),
        "relative_x_px": int(rel_x),
        "relative_y_px": int(rel_y),
        "relative_x_percent": round(float(rel_x) / float(geom["width"]), 6),
        "relative_y_percent": round(float(rel_y) / float(geom["height"]), 6),
        "screen_width_px": int(screen_w),
        "screen_height_px": int(screen_h),
    }
    store.upsert_profile(db_path, prof)
    return prof


def teach_profile(profile_name: str, db_path: str) -> dict[str, Any]:
    # X11 teach flow with an explicit topmost click-to-capture overlay.
    import tkinter as tk

    result: dict[str, Any] = {}

    def _capture() -> None:
        nonlocal result
        result = _capture_profile(profile_name=profile_name, db_path=db_path)
        root.quit()

    root = tk.Tk()
    root.title("OCTA IBKR Teach")
    root.attributes("-topmost", True)
    root.geometry("360x120+40+40")
    lbl = tk.Label(root, text="Focus IBKR popup, hover target button, then click Capture")
    lbl.pack(padx=8, pady=8)
    btn = tk.Button(root, text="Capture", command=_capture)
    btn.pack(padx=8, pady=8)
    root.mainloop()
    root.destroy()
    if not result:
        raise RuntimeError("teach_cancelled")
    return result


def run_watcher(
    *,
    db_path: str,
    timeout_sec: int,
    keepalive: bool,
    dry_run: bool,
    events_path: str,
    cooldown_sec: float = 5.0,
) -> dict[str, Any]:
    profiles = store.get_profiles(db_path)
    if not profiles:
        raise RuntimeError("no_profiles_configured")

    _write_event(events_path, {"event_type": "watcher_start", "profiles": [p["profile_name"] for p in profiles], "dry_run": bool(dry_run)})
    started = time.monotonic()
    last_click_at: dict[tuple[str, str], float] = {}

    while True:
        for profile in profiles:
            pname = str(profile["profile_name"])
            method, candidates = _search_candidates(profile)
            if not candidates:
                continue

            best_id = None
            best_geom = None
            best_score = None
            for cid in candidates:
                try:
                    geom = _get_window_geometry(cid)
                except Exception:
                    continue
                sc = _score_window(geom, profile)
                if best_score is None or sc < best_score:
                    best_score = sc
                    best_id = cid
                    best_geom = geom
            if best_id is None or best_geom is None:
                continue

            click_x = int(best_geom["x"] + round(float(best_geom["width"]) * float(profile["relative_x_percent"])))
            click_y = int(best_geom["y"] + round(float(best_geom["height"]) * float(profile["relative_y_percent"])))
            in_bounds = (
                int(best_geom["width"]) > 50
                and int(best_geom["height"]) > 50
                and int(best_geom["x"]) <= click_x <= int(best_geom["x"]) + int(best_geom["width"])
                and int(best_geom["y"]) <= click_y <= int(best_geom["y"]) + int(best_geom["height"])
            )

            _write_event(
                events_path,
                {
                    "event_type": "window_found",
                    "profile_name": pname,
                    "match_method": method,
                    "winid": str(best_id),
                    "geometry": dict(best_geom),
                    "computed_click_x": int(click_x),
                    "computed_click_y": int(click_y),
                    "dry_run": bool(dry_run),
                },
            )

            if not in_bounds:
                _write_event(
                    events_path,
                    {
                        "event_type": "error",
                        "profile_name": pname,
                        "winid": str(best_id),
                        "error": "computed_click_out_of_bounds_or_small_window",
                    },
                )
                continue

            now = time.monotonic()
            key = (pname, str(best_id))
            last = last_click_at.get(key)
            if last is not None and (now - last) < float(cooldown_sec):
                continue

            if not dry_run:
                _run_cmd(["xdotool", "windowactivate", str(best_id)])
                _run_cmd(["xdotool", "mousemove", "--sync", str(click_x), str(click_y), "click", "1"])

            last_click_at[key] = now
            _write_event(
                events_path,
                {
                    "event_type": "click_performed",
                    "profile_name": pname,
                    "match_method": method,
                    "winid": str(best_id),
                    "geometry": dict(best_geom),
                    "computed_click_x": int(click_x),
                    "computed_click_y": int(click_y),
                    "dry_run": bool(dry_run),
                },
            )

            if not keepalive:
                _write_event(events_path, {"event_type": "watcher_stop", "reason": "first_click_completed"})
                return {"ok": True, "clicked_profile": pname, "clicked_winid": str(best_id)}

        if timeout_sec > 0 and (time.monotonic() - started) >= float(timeout_sec):
            _write_event(events_path, {"event_type": "timeout", "timeout_sec": int(timeout_sec)})
            _write_event(events_path, {"event_type": "watcher_stop", "reason": "timeout"})
            return {"ok": False, "reason": "timeout"}

        time.sleep(0.25)


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="OCTA IBKR X11 autologin watcher/teach tool")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--teach", action="store_true", default=False)
    mode.add_argument("--run", action="store_true", default=False)
    mode.add_argument("--list-profiles", action="store_true", default=False)
    mode.add_argument("--remove-profile", default=None)

    ap.add_argument("--profile-name", default=None)
    ap.add_argument("--db", default="octa/var/runtime/ibkr_autologin.sqlite3")
    ap.add_argument("--timeout-sec", type=int, default=60)
    ap.add_argument("--keepalive", action="store_true", default=False)
    ap.add_argument("--dry-run", action="store_true", default=False)
    ap.add_argument("--events-path", default="octa/var/evidence/ibkr_autologin_events.jsonl")
    return ap.parse_args()


def main() -> int:
    args = _parse_args()
    store.ensure_schema(str(args.db))

    if args.list_profiles:
        payload = {"profiles": store.get_profiles(str(args.db))}
        print(json.dumps(payload, sort_keys=True))
        return 0

    if args.remove_profile is not None:
        removed = store.remove_profile(str(args.db), str(args.remove_profile))
        print(json.dumps({"removed": bool(removed), "profile_name": str(args.remove_profile)}, sort_keys=True))
        return 0 if removed else 2

    if args.teach:
        if not args.profile_name:
            raise SystemExit("--profile-name is required with --teach")
        prof = teach_profile(profile_name=str(args.profile_name), db_path=str(args.db))
        print(json.dumps({"ok": True, "profile": prof}, sort_keys=True))
        return 0

    if args.run:
        try:
            res = run_watcher(
                db_path=str(args.db),
                timeout_sec=int(args.timeout_sec),
                keepalive=bool(args.keepalive),
                dry_run=bool(args.dry_run),
                events_path=str(args.events_path),
            )
            print(json.dumps(res, sort_keys=True))
            return 0 if bool(res.get("ok")) else 2
        except Exception as exc:
            _write_event(str(args.events_path), {"event_type": "error", "error": f"{type(exc).__name__}: {exc}"})
            _write_event(str(args.events_path), {"event_type": "watcher_stop", "reason": "error"})
            print(json.dumps({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, sort_keys=True))
            return 2

    raise SystemExit("invalid_mode")


if __name__ == "__main__":
    raise SystemExit(main())
