"""
octa/support/x11/popup_rules.py
TWS X11 Popup Rule Registry.

Pure-logic module — NO subprocess calls, NO X11 I/O, NO network.
X11 actions are described as data; execution is left entirely to callers:
  - scripts/tws_x11_autologin_chain.py  (Python state machine)
  - octa/support/x11/tws_popup_controller.sh  (shell backup watcher)
  - scripts/tws_popup_smoke_harness.py  (offline trace harness)

Rule priority: LOWER number = processed FIRST.
Each rule fires on ANY matching title token (OR logic).
WM_CLASS filter is optional: empty tuple = accept any class.

Offline-safe: importable with no external dependencies beyond stdlib.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

# ---------------------------------------------------------------------------
# DATA STRUCTURES
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PopupAction:
    """A single action step in a popup dismissal sequence.

    Attributes
    ----------
    kind:
        Action kind — see valid values below.

        "wmctrl_activate"
            Bring window to foreground: ``wmctrl -ia <wid>``
        "wmctrl_close"
            Polite WM close: ``wmctrl -ic <wid>``
        "key"
            Global key/combo: ``xdotool key <value>``
        "key_window"
            Window-scoped key: ``xdotool key --window <wid> <value>``
        "click_pct"
            Click at percentage of window dims: value = "x_pct,y_pct"
            e.g. "0.850,0.880" = 85 % of width, 88 % of height
        "suppress_checkbox"
            Attempt to tick the "do not show again" checkbox.
            Implementation: Tab×N then Space (N determined by caller heuristics).
            Best-effort; caller MUST continue even if this step has no effect.

    value:
        Payload string (key name, "x_pct,y_pct", etc.).  Empty for
        wmctrl_activate, wmctrl_close, and suppress_checkbox.
    verify_absent:
        If True, caller should verify the window is gone after this action.
        On success, the remaining actions for this rule are skipped.
    suppress_checkbox:
        Meta-flag marking this action as the checkbox-suppression attempt.
    """

    kind: str
    value: str = ""
    verify_absent: bool = False
    suppress_checkbox: bool = False


@dataclass(frozen=True)
class PopupRule:
    """A rule for matching and dismissing one category of TWS popup windows.

    Attributes
    ----------
    name:
        Unique, machine-readable rule identifier.
    priority:
        Integer; lower = processed first.  Must be unique across the registry.
    title_tokens:
        Case-insensitive substrings; window matches if ANY token is found in
        the window title (OR logic).
    wm_class_tokens:
        Case-insensitive substrings on WM_CLASS.
        Empty tuple means "accept any WM_CLASS".
    actions:
        Ordered tuple of PopupAction steps.  Callers iterate through them and
        stop early on the first action with verify_absent=True that confirms
        the window is gone.
    suppress_checkbox_label:
        Human-readable label of the "do not show again" checkbox, if present.
        Empty string if no such checkbox is defined for this popup type.
    notes:
        Documentation / operator guidance.  Not used by code.
    """

    name: str
    priority: int
    title_tokens: tuple[str, ...]
    wm_class_tokens: tuple[str, ...]
    actions: tuple[PopupAction, ...]
    suppress_checkbox_label: str = ""
    notes: str = ""


# ---------------------------------------------------------------------------
# ACTION FACTORY HELPERS
# ---------------------------------------------------------------------------


def _activate() -> PopupAction:
    """Bring the window to the foreground via ``wmctrl -ia``."""
    return PopupAction(kind="wmctrl_activate")


def _wmctrl_close(*, verify: bool = False) -> PopupAction:
    """Send WM_DELETE_WINDOW via ``wmctrl -ic``."""
    return PopupAction(kind="wmctrl_close", verify_absent=verify)


def _key(k: str, *, verify: bool = False) -> PopupAction:
    """Send a key/combo globally via ``xdotool key``."""
    return PopupAction(kind="key", value=k, verify_absent=verify)


def _key_w(k: str, *, verify: bool = False) -> PopupAction:
    """Send a key/combo to the specific window via ``xdotool key --window``."""
    return PopupAction(kind="key_window", value=k, verify_absent=verify)


def _click_pct(x_pct: float, y_pct: float, *, verify: bool = False) -> PopupAction:
    """Click at (x_pct, y_pct) relative to window upper-left + dimensions."""
    return PopupAction(kind="click_pct", value=f"{x_pct:.3f},{y_pct:.3f}", verify_absent=verify)


def _suppress() -> PopupAction:
    """Attempt to tick the 'do not show again' checkbox (Tab+Space heuristic)."""
    return PopupAction(kind="suppress_checkbox", suppress_checkbox=True)


# ---------------------------------------------------------------------------
# POPUP RULE REGISTRY
# Rules are SORTED by ascending priority on module load.
# ---------------------------------------------------------------------------

POPUP_REGISTRY: list[PopupRule] = [
    # -----------------------------------------------------------------------
    # Priority 0 — "Programm wird geschlossen" transient shutdown dialog
    # -----------------------------------------------------------------------
    PopupRule(
        name="transient_closing",
        priority=0,
        title_tokens=(
            "Programm wird geschlossen",
            "Programme wird geschlossen",
        ),
        wm_class_tokens=(),
        actions=(
            _activate(),
            _wmctrl_close(verify=True),
            _key_w("alt+F4", verify=True),
            _wmctrl_close(verify=True),
        ),
        notes=(
            "TWS shows this while shutting down or before closing. "
            "Close immediately via wmctrl then Alt+F4 fallback."
        ),
    ),
    # -----------------------------------------------------------------------
    # Priority 1 — win0 placeholder (early Java/TWS startup artefact)
    # -----------------------------------------------------------------------
    PopupRule(
        name="win0_artefact",
        priority=1,
        title_tokens=("win0",),
        wm_class_tokens=(),
        actions=(
            _activate(),
            _wmctrl_close(verify=True),
            _key_w("alt+F4", verify=True),
        ),
        notes=(
            "win0 is an early TWS/Java startup artefact window. "
            "Close it immediately."
        ),
    ),
    # -----------------------------------------------------------------------
    # Priority 2 — Börsenspiegel market-news window
    # -----------------------------------------------------------------------
    PopupRule(
        name="boersenspiegel",
        priority=2,
        title_tokens=(
            "Börsenspiegel",
            "Boersenspiegel",
            "börsenspiegel",
            "boersenspiegel",
        ),
        wm_class_tokens=(),
        actions=(
            _activate(),
            _wmctrl_close(verify=True),
            _key_w("alt+F4", verify=True),
        ),
        notes=(
            "Börsenspiegel market-news popup. "
            "wmctrl polite close, Alt+F4 as fallback."
        ),
    ),
    # -----------------------------------------------------------------------
    # Priority 3 — Dow Jones / Top 10 news windows
    #
    # "Dow Jones Heutige Top 10" is the canonical German-locale title.
    # Alt+F4 is preferred over wmctrl close because TWS news windows
    # sometimes ignore WM_DELETE_WINDOW.
    # -----------------------------------------------------------------------
    PopupRule(
        name="dow_jones_top10",
        priority=3,
        title_tokens=(
            "Dow Jones Heutige Top 10",
            "Dow Jones",
            "Heutige Top 10",
            "Top 10 Today",
        ),
        wm_class_tokens=(),
        actions=(
            _activate(),
            _key_w("alt+F4", verify=True),
            _wmctrl_close(verify=True),
        ),
        notes=(
            "Dow Jones / Top-10 market news popup.  Alt+F4 is more reliable "
            "than wmctrl close for this popup type.  wmctrl close as fallback."
        ),
    ),
    # -----------------------------------------------------------------------
    # Priority 4 — Login Messages / Message Center popup
    #
    # Suppress-checkbox attempt is performed first (best-effort).
    # Then Escape → Return → Alt+F4 → wmctrl close ladder.
    # -----------------------------------------------------------------------
    PopupRule(
        name="login_messages",
        priority=4,
        title_tokens=(
            "Login Messages",
            "Login Message",
            "Login Messenger",
            "IBKR Login Messenger",
            "Message Center",
            "Messages",
            "Messenger",
        ),
        wm_class_tokens=(),
        actions=(
            _activate(),
            _suppress(),                     # try "Do not show again" checkbox first
            _key_w("Escape", verify=False),
            _key_w("Return", verify=False),
            _key_w("alt+F4", verify=True),
            _wmctrl_close(verify=True),
        ),
        suppress_checkbox_label="Do not show again",
        notes=(
            "Login Messages popup.  Suppress checkbox attempted first, "
            "then Escape / Return / Alt+F4 / wmctrl close ladder."
        ),
    ),
    # -----------------------------------------------------------------------
    # Priority 5 — Disclaimer / Warnhinweis / Warning / Agreement
    #
    # German TWS UI shows "Warnhinweis" (risk warning); English shows
    # "Disclaimer" or "Risk Disclosure Agreement".
    #
    # Suppress-checkbox ("Nicht mehr anzeigen") attempted before clicking OK.
    # Click ladder uses percentage-based window coordinates so it is
    # resolution-independent: (85 %, 88 %) targets the OK/Accept button at
    # bottom-right; (50 %, 88 %) is the center-bottom fallback.
    # -----------------------------------------------------------------------
    PopupRule(
        name="disclaimer_warnhinweis",
        priority=5,
        title_tokens=(
            "Warnhinweis",
            "Risikohinweis",
            "Disclaimer",
            "Risk Disclosure",
            "Warning",
            "Haftung",
            "Agreement",
        ),
        wm_class_tokens=(),
        actions=(
            _activate(),
            _suppress(),                          # try "Nicht mehr anzeigen" first
            _click_pct(0.85, 0.88, verify=True),  # OK / Accept (bottom-right)
            _click_pct(0.50, 0.88, verify=True),  # fallback: center-bottom
            _key_w("Return", verify=True),
            _key("space"),
            _key("Return", verify=True),
            _key_w("alt+F4", verify=True),
            _wmctrl_close(verify=True),
        ),
        suppress_checkbox_label="Nicht mehr anzeigen",
        notes=(
            "Disclaimer / Warnhinweis / Risikohinweis.  Click OK at 85 %,88 % "
            "of window dims (bottom-right button area).  Suppress checkbox "
            "('Nicht mehr anzeigen') attempted via Tab+Space before clicking OK."
        ),
    ),
    # -----------------------------------------------------------------------
    # Priority 6 — Generic harmless info / notice dialogs
    #
    # FAIL-CLOSED note: if this rule fires and the window is NOT dismissed
    # after all actions, the caller must log evidence and fail closed —
    # never silently ignore an undismissed modal.
    # -----------------------------------------------------------------------
    PopupRule(
        name="generic_info_dialog",
        priority=6,
        title_tokens=(
            "Important Information",
            "Information",
            "Hinweis",
            "Notice",
            "Alert",
            "Notification",
            "Update Available",
            "New Version",
            "API Connection",
            "Connection",
        ),
        wm_class_tokens=(),
        actions=(
            _activate(),
            _key_w("Return", verify=True),
            _key_w("Escape", verify=True),
            _key_w("alt+F4", verify=True),
            _wmctrl_close(verify=True),
        ),
        notes=(
            "Generic harmless info / notice dialogs.  Return → Escape → Alt+F4 "
            "→ wmctrl close ladder.  If window survives all steps, caller must "
            "fail closed with evidence."
        ),
    ),
]

# Enforce ascending priority order.
POPUP_REGISTRY.sort(key=lambda r: r.priority)


# ---------------------------------------------------------------------------
# MATCHING ENGINE
# ---------------------------------------------------------------------------


def match_rule(title: str, wm_class: str = "") -> PopupRule | None:
    """Return the highest-priority (lowest priority number) matching rule.

    Matching logic
    --------------
    - title_tokens : at least one token must appear as a case-insensitive
      substring of ``title``.
    - wm_class_tokens : if the rule has any class tokens, at least one must
      appear as a case-insensitive substring of ``wm_class``; if the rule
      has no class tokens (empty tuple) any WM_CLASS is accepted.

    Returns ``None`` if no rule matches.
    """
    title_l = str(title or "").lower()
    wm_class_l = str(wm_class or "").lower()

    for rule in POPUP_REGISTRY:
        if not any(
            str(tok).lower() in title_l
            for tok in rule.title_tokens
            if str(tok).strip()
        ):
            continue
        if rule.wm_class_tokens and not any(
            str(tok).lower() in wm_class_l
            for tok in rule.wm_class_tokens
            if str(tok).strip()
        ):
            continue
        return rule

    return None


def match_and_sort_windows(
    windows: list[dict[str, str]],
) -> list[tuple[dict[str, str], PopupRule]]:
    """Match a window list against the registry; return sorted (win, rule) pairs.

    Each window dict must provide at minimum ``"title"``; ``"wm_class"`` is
    optional.  Windows with no matching rule are excluded.

    The result is sorted ascending by ``(rule.priority, title.lower())``
    so the caller's state-machine loop always processes the highest-priority
    popup first, deterministically.
    """
    matched: list[tuple[dict[str, str], PopupRule]] = []
    for win in windows:
        title = str(win.get("title", "") or "")
        wm_class = str(win.get("wm_class", "") or "")
        rule = match_rule(title, wm_class)
        if rule is not None:
            matched.append((win, rule))
    matched.sort(key=lambda pair: (pair[1].priority, pair[0].get("title", "").lower()))
    return matched


# ---------------------------------------------------------------------------
# EVIDENCE HELPERS
# ---------------------------------------------------------------------------


def describe_action_sequence(rule: PopupRule) -> list[dict[str, Any]]:
    """Return a JSON-serializable description of a rule's action sequence."""
    steps: list[dict[str, Any]] = []
    for i, action in enumerate(rule.actions, start=1):
        step: dict[str, Any] = {"step": i, "kind": action.kind}
        if action.value:
            step["value"] = action.value
        if action.verify_absent:
            step["verify_absent"] = True
        if action.suppress_checkbox:
            step["suppress_checkbox"] = True
        steps.append(step)
    return steps


def popup_rules_inventory() -> list[dict[str, Any]]:
    """Return a JSON-serializable inventory of all popup rules.

    Written to ``evidence/popup_rules_inventory.json`` by the smoke harness
    and the autologin evidence pack.
    """
    return [
        {
            "name": rule.name,
            "priority": rule.priority,
            "title_tokens": list(rule.title_tokens),
            "wm_class_tokens": list(rule.wm_class_tokens),
            "suppress_checkbox_label": rule.suppress_checkbox_label,
            "notes": rule.notes,
            "actions": describe_action_sequence(rule),
        }
        for rule in POPUP_REGISTRY
    ]


def rules_as_json_str(*, indent: int = 2) -> str:
    """Return the popup rules inventory as a formatted JSON string."""
    return json.dumps(popup_rules_inventory(), indent=indent, sort_keys=False)
