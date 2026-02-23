"""Offline unit tests for the TWS popup rule registry.

No X11, no subprocess calls, no network. Pure logic only.
All tests are deterministic and suitable for CI without a display.

Coverage:
  - Registry structure (sorted, unique, well-formed)
  - Title matching (German + English variants, case-insensitive)
  - WM_CLASS filtering
  - Priority ordering
  - Action sequence completeness
  - Batch matching + deterministic sort (match_and_sort_windows)
  - Bounded-loop drain simulation
  - Inventory JSON serialisation
  - Parameterised smoke suite: known-blocking vs known-non-blocking titles
"""
from __future__ import annotations

import json

import pytest

from octa.support.x11.popup_rules import (
    POPUP_REGISTRY,
    PopupAction,
    PopupRule,
    describe_action_sequence,
    match_and_sort_windows,
    match_rule,
    popup_rules_inventory,
    rules_as_json_str,
)

# ---------------------------------------------------------------------------
# Registry structure
# ---------------------------------------------------------------------------


class TestRuleRegistryStructure:
    """The registry must be well-formed, sorted, and duplicate-free."""

    def test_registry_is_nonempty(self) -> None:
        assert len(POPUP_REGISTRY) >= 5, "Registry must have at least 5 rules"

    def test_registry_sorted_ascending_by_priority(self) -> None:
        priorities = [r.priority for r in POPUP_REGISTRY]
        assert priorities == sorted(priorities), "Registry not sorted by ascending priority"

    def test_all_rules_have_unique_names(self) -> None:
        names = [r.name for r in POPUP_REGISTRY]
        assert len(names) == len(set(names)), f"Duplicate rule names: {names}"

    def test_all_rules_have_unique_priorities(self) -> None:
        prios = [r.priority for r in POPUP_REGISTRY]
        assert len(prios) == len(set(prios)), f"Duplicate rule priorities: {prios}"

    def test_all_rules_have_nonempty_name(self) -> None:
        for rule in POPUP_REGISTRY:
            assert rule.name.strip(), f"Rule has empty name: {rule!r}"

    def test_all_rules_have_at_least_one_title_token(self) -> None:
        for rule in POPUP_REGISTRY:
            assert len(rule.title_tokens) > 0, f"Rule {rule.name!r} has no title_tokens"

    def test_all_rules_have_at_least_one_action(self) -> None:
        for rule in POPUP_REGISTRY:
            assert len(rule.actions) > 0, f"Rule {rule.name!r} has no actions"

    def test_all_actions_have_valid_kind(self) -> None:
        valid_kinds = {
            "wmctrl_activate",
            "wmctrl_close",
            "key",
            "key_window",
            "click_pct",
            "suppress_checkbox",
        }
        for rule in POPUP_REGISTRY:
            for action in rule.actions:
                assert action.kind in valid_kinds, (
                    f"Invalid action kind {action.kind!r} in rule {rule.name!r}"
                )

    def test_all_rules_first_action_is_activate(self) -> None:
        for rule in POPUP_REGISTRY:
            assert rule.actions[0].kind == "wmctrl_activate", (
                f"Rule {rule.name!r} does not start with wmctrl_activate; "
                f"got {rule.actions[0].kind!r}"
            )

    def test_click_pct_values_are_valid_floats_in_range(self) -> None:
        for rule in POPUP_REGISTRY:
            for action in rule.actions:
                if action.kind == "click_pct":
                    parts = action.value.split(",")
                    assert len(parts) == 2, (
                        f"click_pct value malformed in rule {rule.name!r}: {action.value!r}"
                    )
                    x_pct, y_pct = float(parts[0]), float(parts[1])
                    assert 0.0 < x_pct <= 1.0, (
                        f"click_pct x_pct={x_pct} out of (0,1] in rule {rule.name!r}"
                    )
                    assert 0.0 < y_pct <= 1.0, (
                        f"click_pct y_pct={y_pct} out of (0,1] in rule {rule.name!r}"
                    )

    def test_popup_rule_is_hashable(self) -> None:
        """PopupRule is frozen; must be usable as a dict key or set member."""
        rule_set = set(POPUP_REGISTRY)
        assert len(rule_set) == len(POPUP_REGISTRY)


# ---------------------------------------------------------------------------
# match_rule — title matching
# ---------------------------------------------------------------------------


class TestMatchRuleByTitle:
    """Title-based window matching (case-insensitive substring)."""

    # -- Disclaimer / Warnhinweis / Risikohinweis --

    def test_warnhinweis_exact(self) -> None:
        rule = match_rule("Warnhinweis")
        assert rule is not None
        assert rule.name == "disclaimer_warnhinweis"

    def test_warnhinweis_uppercase(self) -> None:
        rule = match_rule("WARNHINWEIS")
        assert rule is not None
        assert rule.name == "disclaimer_warnhinweis"

    def test_disclaimer_with_broker_suffix(self) -> None:
        rule = match_rule("Disclaimer - Interactive Brokers")
        assert rule is not None
        assert rule.name == "disclaimer_warnhinweis"

    def test_risikohinweis(self) -> None:
        rule = match_rule("Risikohinweis")
        assert rule is not None
        assert rule.name == "disclaimer_warnhinweis"

    def test_risk_disclosure_agreement(self) -> None:
        rule = match_rule("Risk Disclosure Agreement")
        assert rule is not None
        assert rule.name == "disclaimer_warnhinweis"

    def test_warning(self) -> None:
        rule = match_rule("Warning")
        assert rule is not None
        assert rule.name == "disclaimer_warnhinweis"

    def test_haftung(self) -> None:
        rule = match_rule("Haftungsausschluss (Haftung)")
        assert rule is not None
        assert rule.name == "disclaimer_warnhinweis"

    # -- Login Messages --

    def test_login_messages_exact(self) -> None:
        rule = match_rule("Login Messages")
        assert rule is not None
        assert rule.name == "login_messages"

    def test_login_message_singular(self) -> None:
        rule = match_rule("Login Message")
        assert rule is not None
        assert rule.name == "login_messages"

    def test_ibkr_login_messenger(self) -> None:
        rule = match_rule("IBKR Login Messenger")
        assert rule is not None
        assert rule.name == "login_messages"

    def test_message_center(self) -> None:
        rule = match_rule("Message Center")
        assert rule is not None
        assert rule.name == "login_messages"

    def test_messages_exact(self) -> None:
        rule = match_rule("Messages")
        assert rule is not None
        assert rule.name == "login_messages"

    def test_messenger_exact(self) -> None:
        rule = match_rule("Messenger")
        assert rule is not None
        assert rule.name == "login_messages"

    # -- Dow Jones / Top 10 --

    def test_dow_jones_top10_german(self) -> None:
        rule = match_rule("Dow Jones Heutige Top 10")
        assert rule is not None
        assert rule.name == "dow_jones_top10"

    def test_dow_jones_alone(self) -> None:
        rule = match_rule("Dow Jones")
        assert rule is not None
        assert rule.name == "dow_jones_top10"

    def test_heutige_top_10(self) -> None:
        rule = match_rule("Heutige Top 10")
        assert rule is not None
        assert rule.name == "dow_jones_top10"

    def test_top_10_today_english(self) -> None:
        rule = match_rule("Top 10 Today")
        assert rule is not None
        assert rule.name == "dow_jones_top10"

    # -- Börsenspiegel --

    def test_boersenspiegel_umlaut(self) -> None:
        rule = match_rule("Börsenspiegel")
        assert rule is not None
        assert rule.name == "boersenspiegel"

    def test_boersenspiegel_ascii(self) -> None:
        rule = match_rule("Boersenspiegel")
        assert rule is not None
        assert rule.name == "boersenspiegel"

    def test_boersenspiegel_lowercase_umlaut(self) -> None:
        rule = match_rule("börsenspiegel")
        assert rule is not None
        assert rule.name == "boersenspiegel"

    # -- Transient closing --

    def test_programm_wird_geschlossen(self) -> None:
        rule = match_rule("Programm wird geschlossen")
        assert rule is not None
        assert rule.name == "transient_closing"

    def test_programm_wird_geschlossen_ellipsis(self) -> None:
        rule = match_rule("Programm wird geschlossen...")
        assert rule is not None
        assert rule.name == "transient_closing"

    # -- win0 artefact --

    def test_win0_exact(self) -> None:
        rule = match_rule("win0")
        assert rule is not None
        assert rule.name == "win0_artefact"

    # -- No match (main window / unrelated) --

    def test_trader_workstation_does_not_match(self) -> None:
        assert match_rule("Trader Workstation - PAPER U9999999") is None

    def test_trader_workstation_plain_does_not_match(self) -> None:
        assert match_rule("Trader Workstation") is None

    def test_interactive_brokers_does_not_match(self) -> None:
        assert match_rule("Interactive Brokers") is None

    def test_ibkr_paper_does_not_match(self) -> None:
        assert match_rule("IBKR - Paper Trading") is None

    def test_empty_title_does_not_match(self) -> None:
        assert match_rule("") is None

    def test_whitespace_title_does_not_match(self) -> None:
        assert match_rule("   ") is None

    def test_unrelated_title_does_not_match(self) -> None:
        assert match_rule("Something Completely Different") is None


# ---------------------------------------------------------------------------
# match_rule — WM_CLASS filtering
# ---------------------------------------------------------------------------


class TestMatchRuleByWmClass:
    """Rules with empty wm_class_tokens accept any class; non-empty must match."""

    def test_all_current_rules_accept_any_wm_class(self) -> None:
        """Current registry has no WM_CLASS restrictions (empty tuples)."""
        rule = match_rule("Warnhinweis", wm_class="NonExistentApp.NonExistentApp")
        assert rule is not None

    def test_wm_class_ignored_when_rule_has_no_restriction(self) -> None:
        rule_a = match_rule("Login Messages", wm_class="")
        rule_b = match_rule("Login Messages", wm_class="ibcalpha.ibc.IbcTws")
        assert rule_a is not None and rule_b is not None
        assert rule_a.name == rule_b.name

    def test_rules_with_wm_class_tokens_not_present_skip_test(self) -> None:
        rules_with_class = [r for r in POPUP_REGISTRY if r.wm_class_tokens]
        # No such rules in current registry; test skips cleanly
        if not rules_with_class:
            pytest.skip("No WM_CLASS-filtered rules in registry (expected)")
        rule = rules_with_class[0]
        # Non-matching class should cause match failure
        result = match_rule(rule.title_tokens[0], wm_class="NonMatchingApp.X")
        assert result is None or result.name != rule.name


# ---------------------------------------------------------------------------
# Priority ordering
# ---------------------------------------------------------------------------


class TestPriorityOrdering:
    """Higher-priority rules (lower number) must dominate lower-priority ones."""

    def test_transient_closing_has_priority_0(self) -> None:
        rule = match_rule("Programm wird geschlossen")
        assert rule is not None
        assert rule.priority == 0

    def test_boersenspiegel_priority_less_than_disclaimer(self) -> None:
        r_b = match_rule("Börsenspiegel")
        r_d = match_rule("Warnhinweis")
        assert r_b is not None and r_d is not None
        assert r_b.priority < r_d.priority

    def test_dow_jones_priority_less_than_disclaimer(self) -> None:
        r_dj = match_rule("Dow Jones Heutige Top 10")
        r_d = match_rule("Warnhinweis")
        assert r_dj is not None and r_d is not None
        assert r_dj.priority < r_d.priority

    def test_login_messages_priority_less_than_disclaimer(self) -> None:
        r_lm = match_rule("Login Messages")
        r_d = match_rule("Warnhinweis")
        assert r_lm is not None and r_d is not None
        assert r_lm.priority < r_d.priority

    def test_transient_closes_before_everything(self) -> None:
        r_t = match_rule("Programm wird geschlossen")
        for rule in POPUP_REGISTRY:
            if rule.name != "transient_closing":
                assert r_t is not None
                assert r_t.priority <= rule.priority


# ---------------------------------------------------------------------------
# Action sequences
# ---------------------------------------------------------------------------


class TestActionSequences:
    """Action sequences are complete and correct for each rule category."""

    def test_warnhinweis_first_action_is_activate(self) -> None:
        rule = match_rule("Warnhinweis")
        assert rule is not None
        assert rule.actions[0].kind == "wmctrl_activate"

    def test_warnhinweis_has_suppress_checkbox_action(self) -> None:
        rule = match_rule("Warnhinweis")
        assert rule is not None
        assert any(a.kind == "suppress_checkbox" for a in rule.actions)

    def test_warnhinweis_suppress_checkbox_label_set(self) -> None:
        rule = match_rule("Warnhinweis")
        assert rule is not None
        assert rule.suppress_checkbox_label, "suppress_checkbox_label must be non-empty for disclaimer"

    def test_warnhinweis_has_click_pct_action(self) -> None:
        rule = match_rule("Warnhinweis")
        assert rule is not None
        assert any(a.kind == "click_pct" for a in rule.actions)

    def test_warnhinweis_click_pct_at_bottom_right(self) -> None:
        rule = match_rule("Warnhinweis")
        assert rule is not None
        click_pct_actions = [a for a in rule.actions if a.kind == "click_pct"]
        # First click_pct must be in the bottom-right area (x>0.5, y>0.7)
        first = click_pct_actions[0]
        x_pct, y_pct = (float(v) for v in first.value.split(","))
        assert x_pct > 0.5, "First click_pct should be in the right half (OK button)"
        assert y_pct > 0.7, "First click_pct should be in the lower portion (OK button)"

    def test_login_messages_has_alt_f4(self) -> None:
        rule = match_rule("Login Messages")
        assert rule is not None
        alt_f4 = [a for a in rule.actions if a.kind in ("key", "key_window") and a.value == "alt+F4"]
        assert len(alt_f4) >= 1

    def test_login_messages_has_suppress_checkbox(self) -> None:
        rule = match_rule("Login Messages")
        assert rule is not None
        assert any(a.kind == "suppress_checkbox" for a in rule.actions)

    def test_login_messages_suppress_checkbox_label_set(self) -> None:
        rule = match_rule("Login Messages")
        assert rule is not None
        assert rule.suppress_checkbox_label

    def test_dow_jones_has_alt_f4(self) -> None:
        rule = match_rule("Dow Jones Heutige Top 10")
        assert rule is not None
        alt_f4 = [a for a in rule.actions if a.kind in ("key", "key_window") and a.value == "alt+F4"]
        assert len(alt_f4) >= 1

    def test_boersenspiegel_has_wmctrl_close(self) -> None:
        rule = match_rule("Börsenspiegel")
        assert rule is not None
        assert any(a.kind == "wmctrl_close" for a in rule.actions)

    def test_transient_closing_has_wmctrl_close(self) -> None:
        rule = match_rule("Programm wird geschlossen")
        assert rule is not None
        assert any(a.kind == "wmctrl_close" for a in rule.actions)

    def test_suppress_checkbox_has_suppress_flag(self) -> None:
        for rule in POPUP_REGISTRY:
            for action in rule.actions:
                if action.kind == "suppress_checkbox":
                    assert action.suppress_checkbox is True, (
                        f"suppress_checkbox action in rule {rule.name!r} "
                        f"must have suppress_checkbox=True"
                    )


# ---------------------------------------------------------------------------
# match_and_sort_windows — batch matching + deterministic sort
# ---------------------------------------------------------------------------


class TestMatchAndSortWindows:
    """Batch matching returns sorted (window, rule) pairs."""

    def test_empty_list_returns_empty(self) -> None:
        assert match_and_sort_windows([]) == []

    def test_single_matching_window(self) -> None:
        windows = [{"title": "Warnhinweis", "wm_class": ""}]
        result = match_and_sort_windows(windows)
        assert len(result) == 1
        win, rule = result[0]
        assert win["title"] == "Warnhinweis"
        assert rule.name == "disclaimer_warnhinweis"

    def test_non_matching_window_excluded(self) -> None:
        windows = [{"title": "Trader Workstation - PAPER", "wm_class": ""}]
        assert match_and_sort_windows(windows) == []

    def test_mixed_list_excludes_non_matching(self) -> None:
        windows = [
            {"title": "Trader Workstation - DEMO", "wm_class": ""},
            {"title": "Warnhinweis", "wm_class": ""},
            {"title": "Unrelated App", "wm_class": ""},
        ]
        result = match_and_sort_windows(windows)
        assert len(result) == 1
        assert result[0][1].name == "disclaimer_warnhinweis"

    def test_sorted_ascending_by_priority(self) -> None:
        windows = [
            {"title": "Warnhinweis", "wm_class": ""},            # priority 5
            {"title": "Programm wird geschlossen", "wm_class": ""},  # priority 0
            {"title": "Dow Jones Heutige Top 10", "wm_class": ""},   # priority 3
            {"title": "Login Messages", "wm_class": ""},              # priority 4
        ]
        result = match_and_sort_windows(windows)
        assert len(result) == 4
        prios = [rule.priority for _, rule in result]
        assert prios == sorted(prios), "Result must be sorted by ascending priority"

    def test_deterministic_same_input_same_output(self) -> None:
        windows = [
            {"title": "Warnhinweis", "wm_class": ""},
            {"title": "Login Messages", "wm_class": ""},
            {"title": "Dow Jones Heutige Top 10", "wm_class": ""},
        ]
        r1 = [(w["title"], rule.name) for w, rule in match_and_sort_windows(windows)]
        r2 = [(w["title"], rule.name) for w, rule in match_and_sort_windows(windows)]
        assert r1 == r2

    def test_priority_0_comes_before_priority_5(self) -> None:
        windows = [
            {"title": "Warnhinweis", "wm_class": ""},
            {"title": "Programm wird geschlossen", "wm_class": ""},
        ]
        result = match_and_sort_windows(windows)
        assert result[0][1].name == "transient_closing"
        assert result[1][1].name == "disclaimer_warnhinweis"

    def test_window_without_wm_class_key_handled_gracefully(self) -> None:
        windows = [{"title": "Warnhinweis"}]  # no "wm_class" key
        result = match_and_sort_windows(windows)
        assert len(result) == 1

    def test_window_with_none_title_does_not_crash(self) -> None:
        windows = [{"title": None, "wm_class": ""}]  # type: ignore[list-item]
        result = match_and_sort_windows(windows)
        assert result == []

    def test_bounded_drain_loop_simulation(self) -> None:
        """Simulate the popup drain state machine: each iteration processes the
        highest-priority window and 'removes' it.  Must converge within max_iters."""
        max_iters = 60
        initial = [
            {"title": "Warnhinweis", "wm_class": ""},            # prio 5
            {"title": "Login Messages", "wm_class": ""},          # prio 4
            {"title": "Dow Jones Heutige Top 10", "wm_class": ""},  # prio 3
            {"title": "Börsenspiegel", "wm_class": ""},           # prio 2
            {"title": "Programm wird geschlossen", "wm_class": ""},  # prio 0
        ]
        remaining = list(initial)
        closed_order: list[str] = []

        for _ in range(max_iters):
            if not remaining:
                break
            matched = match_and_sort_windows(remaining)
            if not matched:
                break
            target_win, target_rule = matched[0]
            closed_order.append(target_win["title"])
            remaining = [w for w in remaining if w is not target_win]

        assert not remaining, f"Drain did not complete: {remaining}"
        assert len(closed_order) == 5
        # Verify ascending-priority processing order
        closed_prios = [match_rule(t).priority for t in closed_order if match_rule(t)]  # type: ignore[union-attr]
        assert closed_prios == sorted(closed_prios), (
            f"Drain order not priority-ascending: {list(zip(closed_order, closed_prios))}"
        )

    def test_all_popups_closed_within_max_iters(self) -> None:
        """Even a large window list must be fully processed within 60 iterations."""
        max_iters = 60
        all_blocking = [
            {"title": t, "wm_class": ""}
            for t, _ in [
                ("Warnhinweis", "disclaimer_warnhinweis"),
                ("Login Messages", "login_messages"),
                ("Dow Jones Heutige Top 10", "dow_jones_top10"),
                ("Börsenspiegel", "boersenspiegel"),
                ("Programm wird geschlossen", "transient_closing"),
                ("win0", "win0_artefact"),
            ]
        ]
        remaining = list(all_blocking)
        for _ in range(max_iters):
            if not remaining:
                break
            matched = match_and_sort_windows(remaining)
            if not matched:
                break
            target_win, _ = matched[0]
            remaining = [w for w in remaining if w is not target_win]
        assert not remaining, f"Not all popups closed within {max_iters} iters: {remaining}"


# ---------------------------------------------------------------------------
# Inventory + JSON serialisation
# ---------------------------------------------------------------------------


class TestInventoryAndSerialization:
    """popup_rules_inventory() must be JSON-serialisable and complete."""

    def test_inventory_is_list(self) -> None:
        assert isinstance(popup_rules_inventory(), list)

    def test_inventory_length_equals_registry(self) -> None:
        assert len(popup_rules_inventory()) == len(POPUP_REGISTRY)

    def test_inventory_required_keys(self) -> None:
        required = {"name", "priority", "title_tokens", "wm_class_tokens", "actions"}
        for item in popup_rules_inventory():
            missing = required - set(item.keys())
            assert not missing, f"Inventory item missing keys: {missing}"

    def test_inventory_actions_have_step_numbers(self) -> None:
        for item in popup_rules_inventory():
            for action in item["actions"]:
                assert "step" in action
                assert isinstance(action["step"], int)
                assert action["step"] >= 1

    def test_inventory_is_json_serialisable(self) -> None:
        serialised = json.dumps(popup_rules_inventory())
        assert isinstance(serialised, str)
        assert len(serialised) > 0

    def test_rules_as_json_str_is_valid_json(self) -> None:
        raw = rules_as_json_str()
        parsed = json.loads(raw)
        assert isinstance(parsed, list)
        assert len(parsed) == len(POPUP_REGISTRY)

    def test_inventory_title_tokens_are_lists(self) -> None:
        for item in popup_rules_inventory():
            assert isinstance(item["title_tokens"], list)

    def test_inventory_wm_class_tokens_are_lists(self) -> None:
        for item in popup_rules_inventory():
            assert isinstance(item["wm_class_tokens"], list)


# ---------------------------------------------------------------------------
# describe_action_sequence
# ---------------------------------------------------------------------------


class TestDescribeActionSequence:
    """describe_action_sequence returns well-structured step dicts."""

    def test_steps_numbered_sequentially(self) -> None:
        for rule in POPUP_REGISTRY:
            steps = describe_action_sequence(rule)
            for i, step in enumerate(steps, start=1):
                assert step["step"] == i, (
                    f"Rule {rule.name!r}: step {i} has step number {step['step']}"
                )

    def test_all_steps_have_kind(self) -> None:
        for rule in POPUP_REGISTRY:
            for step in describe_action_sequence(rule):
                assert "kind" in step

    def test_verify_absent_key_present_only_when_true(self) -> None:
        for rule in POPUP_REGISTRY:
            for action, step in zip(rule.actions, describe_action_sequence(rule)):
                if action.verify_absent:
                    assert step.get("verify_absent") is True
                else:
                    assert "verify_absent" not in step

    def test_suppress_checkbox_key_present_only_when_true(self) -> None:
        for rule in POPUP_REGISTRY:
            for action, step in zip(rule.actions, describe_action_sequence(rule)):
                if action.suppress_checkbox:
                    assert step.get("suppress_checkbox") is True
                else:
                    assert "suppress_checkbox" not in step


# ---------------------------------------------------------------------------
# Parameterised smoke suite
# ---------------------------------------------------------------------------


KNOWN_BLOCKING: list[tuple[str, str]] = [
    ("Warnhinweis", "disclaimer_warnhinweis"),
    ("WARNHINWEIS", "disclaimer_warnhinweis"),
    ("Warnhinweis - Wichtiger Hinweis", "disclaimer_warnhinweis"),
    ("Risikohinweis", "disclaimer_warnhinweis"),
    ("Disclaimer - Interactive Brokers", "disclaimer_warnhinweis"),
    ("Risk Disclosure Agreement", "disclaimer_warnhinweis"),
    ("Warning", "disclaimer_warnhinweis"),
    ("Haftungsausschluss (Haftung)", "disclaimer_warnhinweis"),
    ("Login Messages", "login_messages"),
    ("Login Message", "login_messages"),
    ("IBKR Login Messenger", "login_messages"),
    ("Message Center", "login_messages"),
    ("Messages", "login_messages"),
    ("Messenger", "login_messages"),
    ("Dow Jones Heutige Top 10", "dow_jones_top10"),
    ("Dow Jones", "dow_jones_top10"),
    ("Heutige Top 10", "dow_jones_top10"),
    ("Top 10 Today", "dow_jones_top10"),
    ("Börsenspiegel", "boersenspiegel"),
    ("Boersenspiegel", "boersenspiegel"),
    ("börsenspiegel", "boersenspiegel"),
    ("boersenspiegel", "boersenspiegel"),
    ("Programm wird geschlossen", "transient_closing"),
    ("Programm wird geschlossen...", "transient_closing"),
    ("win0", "win0_artefact"),
]

KNOWN_NON_BLOCKING: list[str] = [
    "Trader Workstation - PAPER U9999999",
    "Trader Workstation",
    "Trader Workstation - DEMO",
    "Interactive Brokers",
    "Interactive Brokers - DUH",
    "IBKR",
    "IBKR - Paper Trading",
    "Mosaic - Paper Trading",
    "",
    "   ",
    "Chrome",
    "Firefox",
    "Terminal",
    "Unrelated App",
]


class TestSmokeSuite:
    """Parameterised smoke tests: blocking titles must match; main-window / unrelated titles must not."""

    @pytest.mark.parametrize("title,expected_rule_name", KNOWN_BLOCKING)
    def test_blocking_popup_matches_expected_rule(
        self, title: str, expected_rule_name: str
    ) -> None:
        rule = match_rule(title)
        assert rule is not None, f"No rule matched title: {title!r}"
        assert rule.name == expected_rule_name, (
            f"title={title!r}: expected rule {expected_rule_name!r}, got {rule.name!r}"
        )

    @pytest.mark.parametrize("title", KNOWN_NON_BLOCKING)
    def test_non_blocking_title_does_not_match(self, title: str) -> None:
        rule = match_rule(title)
        rule_name = rule.name if rule is not None else None
        assert rule is None, (
            f"Unexpected match for non-blocking title={title!r}: rule={rule_name!r}"
        )
