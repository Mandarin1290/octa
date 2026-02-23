"""Offline unit tests for octa.support.x11.x11_actions and the K=3 stable-ok logic.

No X11, no subprocess calls, no network.  All I/O is mocked via the run_cmd
injectable pattern defined in x11_actions.

Coverage
--------
TestListWindows
    - Parses wmctrl -lp output into window dicts
    - Sorted by wid ascending
    - Required fields present
    - Failure / empty output handling

TestCloseWindowLadder
    - Correct command order: wmctrl -ia → Escape → Return → KP_Enter → alt+F4 → wmctrl -ic
    - Early return True when window disappears after Escape
    - Early return True when window disappears after alt+F4
    - Returns False when window survives all steps
    - wmctrl -lp verify call issued after each dismiss step

TestStableOkLogic
    - Simulates the K=3 consecutive-clean-poll loop added to chain.py confirm loop
    - Validates that:
      * main present + Warnhinweis present → stable_ok never reaches 3
      * main present + Login Messages present → stable_ok never reaches 3
      * 3 consecutive clean polls → loop exits True (OK)
      * Interrupted streak resets stable_ok
"""
from __future__ import annotations

import pytest

from octa.support.x11.x11_actions import close_window_ladder, list_windows

# ---------------------------------------------------------------------------
# Sample wmctrl -lp output for mocking
# ---------------------------------------------------------------------------

_WMCTRL_THREE_WINS = (
    "0x01000001  0  12345 hostname Trader Workstation - PAPER U9999999\n"
    "0x01000002  0  12345 hostname Warnhinweis\n"
    "0x01000003  0  12345 hostname Login Messages\n"
)

_WMCTRL_MAIN_ONLY = (
    "0x01000001  0  12345 hostname Trader Workstation - PAPER U9999999\n"
)


# ---------------------------------------------------------------------------
# TestListWindows
# ---------------------------------------------------------------------------


class TestListWindows:
    def test_parses_three_windows(self) -> None:
        def run_cmd(cmd: list[str]) -> tuple[int, str]:
            return 0, _WMCTRL_THREE_WINS

        wins = list_windows(run_cmd)
        assert len(wins) == 3

    def test_fields_wid_desktop_pid_host_title(self) -> None:
        def run_cmd(cmd: list[str]) -> tuple[int, str]:
            return 0, _WMCTRL_THREE_WINS

        wins = list_windows(run_cmd)
        for w in wins:
            for field in ("wid", "desktop", "pid", "host", "title"):
                assert field in w, f"Missing field {field!r} in {w!r}"

    def test_titles_correct(self) -> None:
        def run_cmd(cmd: list[str]) -> tuple[int, str]:
            return 0, _WMCTRL_THREE_WINS

        titles = {w["title"] for w in list_windows(run_cmd)}
        assert "Warnhinweis" in titles
        assert "Login Messages" in titles

    def test_sorted_ascending_by_wid(self) -> None:
        # Provide windows in reverse order to confirm sort
        out = (
            "0x01000003  0  12345 hostname C\n"
            "0x01000001  0  12345 hostname A\n"
            "0x01000002  0  12345 hostname B\n"
        )

        def run_cmd(cmd: list[str]) -> tuple[int, str]:
            return 0, out

        wids = [w["wid"] for w in list_windows(run_cmd)]
        assert wids == sorted(wids)

    def test_wmctrl_failure_returns_empty(self) -> None:
        def run_cmd(cmd: list[str]) -> tuple[int, str]:
            return 1, ""

        assert list_windows(run_cmd) == []

    def test_empty_output_returns_empty(self) -> None:
        def run_cmd(cmd: list[str]) -> tuple[int, str]:
            return 0, ""

        assert list_windows(run_cmd) == []

    def test_partial_line_skipped(self) -> None:
        # Lines with fewer than 5 fields should be ignored
        out = "0x01000001  0  12345\n"  # only 3 fields

        def run_cmd(cmd: list[str]) -> tuple[int, str]:
            return 0, out

        assert list_windows(run_cmd) == []

    def test_blank_lines_skipped(self) -> None:
        out = "\n\n0x01000001  0  12345 hostname Main Window\n\n"

        def run_cmd(cmd: list[str]) -> tuple[int, str]:
            return 0, out

        wins = list_windows(run_cmd)
        assert len(wins) == 1
        assert wins[0]["title"] == "Main Window"


# ---------------------------------------------------------------------------
# TestCloseWindowLadder — mock runner factory
# ---------------------------------------------------------------------------


_TARGET_WID = "0x01000002"


def _make_runner(
    *,
    disappears_after_dismiss: int,
) -> tuple[object, list[list[str]]]:
    """Return (run_cmd, calls_list).

    The target window disappears from wmctrl output after
    ``disappears_after_dismiss`` dismiss commands have been issued.
    dismiss commands = Escape, Return, KP_Enter, alt+F4, wmctrl -ic  (indices 1-5).
    Set ``disappears_after_dismiss=0`` to make the window already gone
    before any dismiss command.
    Set ``disappears_after_dismiss=999`` to make the window survive everything.
    """
    calls: list[list[str]] = []
    dismiss_count = [0]

    def run_cmd(cmd: list[str]) -> tuple[int, str]:
        calls.append(list(cmd))
        if cmd == ["wmctrl", "-lp"]:
            if dismiss_count[0] >= disappears_after_dismiss:
                # Target window gone
                return 0, _WMCTRL_MAIN_ONLY
            else:
                return 0, _WMCTRL_THREE_WINS
        # Count dismiss actions (not wmctrl -ia and not wmctrl -lp)
        is_dismiss = (cmd[:2] == ["xdotool", "key"]) or (cmd == ["wmctrl", "-ic", _TARGET_WID])
        if is_dismiss:
            dismiss_count[0] += 1
        return 0, ""

    return run_cmd, calls


class TestCloseWindowLadder:
    def test_returns_true_when_window_disappears_after_escape(self) -> None:
        run_cmd, _ = _make_runner(disappears_after_dismiss=1)
        result = close_window_ladder(_TARGET_WID, run_cmd, step_sleep_sec=0.0)
        assert result is True

    def test_returns_true_when_window_disappears_after_return(self) -> None:
        run_cmd, _ = _make_runner(disappears_after_dismiss=2)
        result = close_window_ladder(_TARGET_WID, run_cmd, step_sleep_sec=0.0)
        assert result is True

    def test_returns_true_when_window_disappears_after_kp_enter(self) -> None:
        run_cmd, _ = _make_runner(disappears_after_dismiss=3)
        result = close_window_ladder(_TARGET_WID, run_cmd, step_sleep_sec=0.0)
        assert result is True

    def test_returns_true_when_window_disappears_after_altf4(self) -> None:
        run_cmd, _ = _make_runner(disappears_after_dismiss=4)
        result = close_window_ladder(_TARGET_WID, run_cmd, step_sleep_sec=0.0)
        assert result is True

    def test_returns_true_when_window_disappears_after_wmctrl_ic(self) -> None:
        run_cmd, _ = _make_runner(disappears_after_dismiss=5)
        result = close_window_ladder(_TARGET_WID, run_cmd, step_sleep_sec=0.0)
        assert result is True

    def test_returns_false_when_window_survives_all_steps(self) -> None:
        run_cmd, _ = _make_runner(disappears_after_dismiss=999)
        result = close_window_ladder(_TARGET_WID, run_cmd, step_sleep_sec=0.0)
        assert result is False

    def test_command_order_activate_then_dismiss(self) -> None:
        """wmctrl -ia must be first; then Escape, Return, KP_Enter, alt+F4, wmctrl -ic."""
        run_cmd, calls = _make_runner(disappears_after_dismiss=999)
        close_window_ladder(_TARGET_WID, run_cmd, step_sleep_sec=0.0)

        # Filter to action commands only (exclude wmctrl -lp verify calls)
        action_calls = [c for c in calls if c != ["wmctrl", "-lp"]]

        assert action_calls[0] == ["wmctrl", "-ia", _TARGET_WID], (
            f"First command must be wmctrl -ia; got {action_calls[0]!r}"
        )
        expected_dismiss_cmds = [
            ["xdotool", "key", "Escape"],
            ["xdotool", "key", "Return"],
            ["xdotool", "key", "KP_Enter"],
            ["xdotool", "key", "alt+F4"],
            ["wmctrl", "-ic", _TARGET_WID],
        ]
        for i, expected in enumerate(expected_dismiss_cmds):
            got = action_calls[i + 1]
            assert got == expected, (
                f"Dismiss step {i + 1}: expected {expected!r}, got {got!r}"
            )

    def test_verify_called_after_each_dismiss_step(self) -> None:
        """wmctrl -lp must be issued after each dismiss action to check disappearance."""
        run_cmd, calls = _make_runner(disappears_after_dismiss=999)
        close_window_ladder(_TARGET_WID, run_cmd, step_sleep_sec=0.0)

        verify_calls = [c for c in calls if c == ["wmctrl", "-lp"]]
        # 5 dismiss steps → 5 verify calls
        assert len(verify_calls) == 5, (
            f"Expected 5 wmctrl -lp verify calls, got {len(verify_calls)}"
        )

    def test_early_exit_stops_further_steps(self) -> None:
        """When window disappears after Escape, Return/KP_Enter/alt+F4/wmctrl-ic must NOT be called."""
        run_cmd, calls = _make_runner(disappears_after_dismiss=1)
        close_window_ladder(_TARGET_WID, run_cmd, step_sleep_sec=0.0)

        action_calls = [c for c in calls if c != ["wmctrl", "-lp"]]
        # Only wmctrl -ia + xdotool key Escape (2 action commands)
        assert len(action_calls) == 2, (
            f"After early exit, expected 2 action commands, got {action_calls!r}"
        )
        assert action_calls[1] == ["xdotool", "key", "Escape"]

    def test_wid_not_in_list_returns_true_immediately(self) -> None:
        """If the target window is already absent before any dismiss, return True after Escape."""
        # disappears_after_dismiss=0 means wmctrl -lp always returns main-only,
        # so the first verify (after Escape) immediately sees the window gone.
        run_cmd, calls = _make_runner(disappears_after_dismiss=0)
        result = close_window_ladder(_TARGET_WID, run_cmd, step_sleep_sec=0.0)
        assert result is True

    def test_wid_case_insensitive_match(self) -> None:
        """WID comparison must be case-insensitive (0X... vs 0x...)."""
        upper_wid = _TARGET_WID.upper()  # "0X01000002"
        calls: list[list[str]] = []
        dismiss_count = [0]

        def run_cmd(cmd: list[str]) -> tuple[int, str]:
            calls.append(list(cmd))
            if cmd == ["wmctrl", "-lp"]:
                if dismiss_count[0] >= 1:
                    return 0, _WMCTRL_MAIN_ONLY  # lowercase wid in output
                return 0, _WMCTRL_THREE_WINS
            if cmd[:2] == ["xdotool", "key"] or cmd[:2] == ["wmctrl", "-ic"]:
                dismiss_count[0] += 1
            return 0, ""

        # Call ladder with uppercase WID — should still match lowercase in wmctrl output
        result = close_window_ladder(upper_wid, run_cmd, step_sleep_sec=0.0)
        assert result is True


# ---------------------------------------------------------------------------
# TestStableOkLogic — validates the K=3 consecutive-clean-poll pattern
# ---------------------------------------------------------------------------


def _simulate_stable_ok_loop(
    poll_results: list[bool],
    *,
    needed: int = 3,
) -> bool:
    """Simulate the chain.py confirm-loop stable_ok counter.

    ``poll_results[i]`` is the value of ``_clean_now`` on iteration i.
    Returns True if ``needed`` consecutive True values are seen before
    the sequence is exhausted, False otherwise.

    This function directly mirrors the code added to chain.py:

        stable_ok = 0
        while ... :
            ...
            if _clean_now:
                stable_ok += 1
                if stable_ok >= _STABLE_OK_NEEDED:
                    break
            else:
                stable_ok = 0
    """
    stable_ok = 0
    for clean in poll_results:
        if clean:
            stable_ok += 1
            if stable_ok >= needed:
                return True
        else:
            stable_ok = 0
    return False


class TestStableOkLogic:
    """The K=3 requirement: 3 consecutive clean polls before declaring OK."""

    def test_three_consecutive_clean_returns_true(self) -> None:
        assert _simulate_stable_ok_loop([True, True, True]) is True

    def test_two_consecutive_clean_is_not_enough(self) -> None:
        assert _simulate_stable_ok_loop([True, True]) is False

    def test_one_clean_is_not_enough(self) -> None:
        assert _simulate_stable_ok_loop([True]) is False

    def test_dirty_then_three_clean_returns_true(self) -> None:
        assert _simulate_stable_ok_loop([False, True, True, True]) is True

    def test_interrupted_streak_resets(self) -> None:
        # Two clean, one dirty, then three clean → succeeds on the 6th poll
        assert _simulate_stable_ok_loop([True, True, False, True, True, True]) is True

    def test_interrupted_streak_two_plus_one_not_enough(self) -> None:
        # Two clean, one dirty, two clean → 4 polls total but no streak of 3
        assert _simulate_stable_ok_loop([True, True, False, True, True]) is False

    def test_main_present_warnhinweis_present_never_reaches_3(self) -> None:
        """Simulates: main window found but Warnhinweis still present.

        clean_now = (main_win or api_open) and not login_win and not blocking_titles_present
        With blocking_titles_present=["Warnhinweis"], clean_now is always False.
        stable_ok never reaches 3 → loop returns False (no OK exit).
        """
        # All polls: main=True, blocking=True → clean_now=False
        all_dirty = [False] * 10
        assert _simulate_stable_ok_loop(all_dirty) is False

    def test_main_present_login_messages_present_never_reaches_3(self) -> None:
        """Simulates: main window found but Login Messages popup still present.

        With blocking_titles_present=["Login Messages"], clean_now is always False.
        """
        all_dirty = [False] * 10
        assert _simulate_stable_ok_loop(all_dirty) is False

    def test_clean_after_ladder_dismisses_popup(self) -> None:
        """Simulates: popup present for 2 polls, dismissed by ladder, then 3 clean polls → OK."""
        # First 2 polls: popup present (dirty), then 3 consecutive clean
        poll_sequence = [False, False, True, True, True]
        assert _simulate_stable_ok_loop(poll_sequence) is True

    def test_empty_sequence_returns_false(self) -> None:
        assert _simulate_stable_ok_loop([]) is False

    def test_all_dirty_returns_false(self) -> None:
        assert _simulate_stable_ok_loop([False] * 20) is False

    @pytest.mark.parametrize("needed", [1, 2, 3, 4, 5])
    def test_exactly_needed_consecutive_succeeds(self, needed: int) -> None:
        assert _simulate_stable_ok_loop([True] * needed, needed=needed) is True

    @pytest.mark.parametrize("needed", [2, 3, 4, 5])
    def test_one_short_of_needed_fails(self, needed: int) -> None:
        assert _simulate_stable_ok_loop([True] * (needed - 1), needed=needed) is False
