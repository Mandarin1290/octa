"""Tests: OCTA LLM Lifecycle Management — deterministic, offline-safe.
No real ollama calls; subprocess is mocked.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any
from unittest.mock import patch, MagicMock

import pytest


# ── helpers ───────────────────────────────────────────────────────────────────

def _fake_ollama_list_output() -> str:
    return (
        "NAME                   ID              SIZE      MODIFIED\n"
        "llama2:latest          78e26419b446    3.8 GB    2 months ago\n"
        "mistral:7b-instruct    6577803aa9a0    4.4 GB    3 months ago\n"
        "deepseek-coder:6.7b    ce298d984115    3.8 GB    3 months ago\n"
    )


def _id_hash(model_id: str) -> str:
    return hashlib.sha256(model_id.encode("utf-8")).hexdigest()


def _mock_run(stdout: str):
    """Return a mock subprocess.run result with given stdout."""
    result = MagicMock()
    result.stdout = stdout
    result.stderr = ""
    result.returncode = 0
    return result


# ── tests ─────────────────────────────────────────────────────────────────────

class TestRunOllamaList:
    def test_parses_installed_models(self, monkeypatch):
        from octa.llm import lifecycle as lc

        with patch("subprocess.run", return_value=_mock_run(_fake_ollama_list_output())):
            models = lc._run_ollama_list()

        names = [m["name"] for m in models]
        assert "llama2:latest" in names
        assert "mistral:7b-instruct" in names
        assert "deepseek-coder:6.7b" in names

    def test_sorted_deterministically(self, monkeypatch):
        from octa.llm import lifecycle as lc

        with patch("subprocess.run", return_value=_mock_run(_fake_ollama_list_output())):
            models = lc._run_ollama_list()

        names = [m["name"] for m in models]
        assert names == sorted(names)

    def test_empty_on_ollama_not_found(self):
        from octa.llm import lifecycle as lc
        import subprocess

        with patch("subprocess.run", side_effect=FileNotFoundError("ollama not found")):
            models = lc._run_ollama_list()

        assert models == []

    def test_parses_id_field(self):
        from octa.llm import lifecycle as lc

        with patch("subprocess.run", return_value=_mock_run(_fake_ollama_list_output())):
            models = lc._run_ollama_list()

        llama = next(m for m in models if m["name"] == "llama2:latest")
        assert llama["id"] == "78e26419b446"


class TestComputeIdHash:
    def test_deterministic(self):
        from octa.llm.lifecycle import _compute_id_hash
        h1 = _compute_id_hash("78e26419b446")
        h2 = _compute_id_hash("78e26419b446")
        assert h1 == h2

    def test_different_ids_produce_different_hashes(self):
        from octa.llm.lifecycle import _compute_id_hash
        assert _compute_id_hash("78e26419b446") != _compute_id_hash("6577803aa9a0")

    def test_sha256_length(self):
        from octa.llm.lifecycle import _compute_id_hash
        h = _compute_id_hash("78e26419b446")
        assert len(h) == 64  # SHA-256 hex = 64 chars


class TestVerify:
    def test_first_time_registers_and_marks_verified(self, tmp_path):
        from octa.llm import lifecycle as lc

        with (
            patch.object(lc, "_REGISTRY_PATH", tmp_path / "llm_registry.json"),
            patch.object(lc, "_HASH_REGISTRY_PATH", tmp_path / "llm_hash_registry.json"),
            patch("subprocess.run", return_value=_mock_run(_fake_ollama_list_output())),
        ):
            rc = lc.cmd_verify()

        assert rc == 0
        hash_reg = json.loads((tmp_path / "llm_hash_registry.json").read_text())
        assert "llama2:latest" in hash_reg
        assert hash_reg["llama2:latest"] == _id_hash("78e26419b446")

        reg = json.loads((tmp_path / "llm_registry.json").read_text())
        assert reg["models"]["llama2:latest"]["state"] == "VERIFIED"

    def test_matching_hash_passes(self, tmp_path):
        from octa.llm import lifecycle as lc

        # Pre-populate with correct hash
        hash_reg = {"llama2:latest": _id_hash("78e26419b446")}
        (tmp_path / "llm_hash_registry.json").write_text(json.dumps(hash_reg))

        with (
            patch.object(lc, "_REGISTRY_PATH", tmp_path / "llm_registry.json"),
            patch.object(lc, "_HASH_REGISTRY_PATH", tmp_path / "llm_hash_registry.json"),
            patch("subprocess.run", return_value=_mock_run(
                "NAME                   ID              SIZE      MODIFIED\n"
                "llama2:latest          78e26419b446    3.8 GB    2 months ago\n"
            )),
        ):
            rc = lc.cmd_verify()

        assert rc == 0

    def test_hash_mismatch_fails_closed(self, tmp_path):
        from octa.llm import lifecycle as lc

        # Store WRONG hash
        hash_reg = {"llama2:latest": "deadbeef" * 8}
        (tmp_path / "llm_hash_registry.json").write_text(json.dumps(hash_reg))

        with (
            patch.object(lc, "_REGISTRY_PATH", tmp_path / "llm_registry.json"),
            patch.object(lc, "_HASH_REGISTRY_PATH", tmp_path / "llm_hash_registry.json"),
            patch("subprocess.run", return_value=_mock_run(
                "NAME                   ID              SIZE      MODIFIED\n"
                "llama2:latest          78e26419b446    3.8 GB    2 months ago\n"
            )),
        ):
            rc = lc.cmd_verify()

        assert rc == 1  # fail-closed
        reg = json.loads((tmp_path / "llm_registry.json").read_text())
        # State demoted back to CANDIDATE (not VERIFIED or ACTIVE)
        assert reg["models"]["llama2:latest"]["state"] == "CANDIDATE"

    def test_history_logged_on_change(self, tmp_path):
        from octa.llm import lifecycle as lc

        with (
            patch.object(lc, "_REGISTRY_PATH", tmp_path / "llm_registry.json"),
            patch.object(lc, "_HASH_REGISTRY_PATH", tmp_path / "llm_hash_registry.json"),
            patch("subprocess.run", return_value=_mock_run(
                "NAME                   ID              SIZE      MODIFIED\n"
                "llama2:latest          78e26419b446    3.8 GB    2 months ago\n"
            )),
        ):
            lc.cmd_verify()

        reg = json.loads((tmp_path / "llm_registry.json").read_text())
        assert len(reg["history"]) >= 1
        entry = reg["history"][0]
        assert entry["model"] == "llama2:latest"
        assert entry["reason"] == "initial_verification"
        assert "timestamp_utc" in entry


class TestRotate:
    def test_verified_model_rotates_to_active(self, tmp_path):
        from octa.llm import lifecycle as lc

        # Pre-state: llama2 is VERIFIED with correct hash, policy rotation=auto
        hash_reg = {"llama2:latest": _id_hash("78e26419b446")}
        reg = {
            "models": {"llama2:latest": {"name": "llama2:latest", "id": "78e26419b446", "state": "VERIFIED"}},
            "history": [],
        }
        (tmp_path / "llm_hash_registry.json").write_text(json.dumps(hash_reg))
        (tmp_path / "llm_registry.json").write_text(json.dumps(reg))

        fake_policy = [
            {"name": "llama2", "tag": "latest", "pinned_version": "llama2:latest",
             "rotation": "auto", "require_hash": "true", "allow_pruning": "false"},
        ]
        with (
            patch.object(lc, "_REGISTRY_PATH", tmp_path / "llm_registry.json"),
            patch.object(lc, "_HASH_REGISTRY_PATH", tmp_path / "llm_hash_registry.json"),
            patch.object(lc, "_load_policy", return_value=fake_policy),
            patch("subprocess.run", return_value=_mock_run(
                "NAME                   ID              SIZE      MODIFIED\n"
                "llama2:latest          78e26419b446    3.8 GB    2 months ago\n"
            )),
        ):
            rc = lc.cmd_rotate()

        assert rc == 0
        reg2 = json.loads((tmp_path / "llm_registry.json").read_text())
        assert reg2["models"]["llama2:latest"]["state"] == "ACTIVE"
        assert any(e["reason"] == "auto_rotation" for e in reg2["history"])

    def test_hash_mismatch_blocks_rotation(self, tmp_path):
        from octa.llm import lifecycle as lc

        # Wrong hash stored
        hash_reg = {"llama2:latest": "wronghash" * 5}
        reg = {
            "models": {"llama2:latest": {"name": "llama2:latest", "id": "78e26419b446", "state": "VERIFIED"}},
            "history": [],
        }
        (tmp_path / "llm_hash_registry.json").write_text(json.dumps(hash_reg))
        (tmp_path / "llm_registry.json").write_text(json.dumps(reg))

        fake_policy = [
            {"name": "llama2", "pinned_version": "llama2:latest", "rotation": "auto"},
        ]
        with (
            patch.object(lc, "_REGISTRY_PATH", tmp_path / "llm_registry.json"),
            patch.object(lc, "_HASH_REGISTRY_PATH", tmp_path / "llm_hash_registry.json"),
            patch.object(lc, "_load_policy", return_value=fake_policy),
            patch("subprocess.run", return_value=_mock_run(
                "NAME                   ID              SIZE      MODIFIED\n"
                "llama2:latest          78e26419b446    3.8 GB    2 months ago\n"
            )),
        ):
            lc.cmd_rotate()

        # State must NOT be ACTIVE
        reg2 = json.loads((tmp_path / "llm_registry.json").read_text())
        assert reg2["models"]["llama2:latest"]["state"] != "ACTIVE"

    def test_manual_rotation_policy_skipped(self, tmp_path):
        from octa.llm import lifecycle as lc

        hash_reg = {"llama2:latest": _id_hash("78e26419b446")}
        reg = {
            "models": {"llama2:latest": {"name": "llama2:latest", "id": "78e26419b446", "state": "VERIFIED"}},
            "history": [],
        }
        (tmp_path / "llm_hash_registry.json").write_text(json.dumps(hash_reg))
        (tmp_path / "llm_registry.json").write_text(json.dumps(reg))

        fake_policy = [
            {"name": "llama2", "pinned_version": "llama2:latest", "rotation": "manual"},
        ]
        with (
            patch.object(lc, "_REGISTRY_PATH", tmp_path / "llm_registry.json"),
            patch.object(lc, "_HASH_REGISTRY_PATH", tmp_path / "llm_hash_registry.json"),
            patch.object(lc, "_load_policy", return_value=fake_policy),
            patch("subprocess.run", return_value=_mock_run(
                "NAME                   ID              SIZE      MODIFIED\n"
                "llama2:latest          78e26419b446    3.8 GB    2 months ago\n"
            )),
        ):
            lc.cmd_rotate()

        reg2 = json.loads((tmp_path / "llm_registry.json").read_text())
        # State must remain VERIFIED (not promoted to ACTIVE via rotation=manual)
        assert reg2["models"]["llama2:latest"]["state"] == "VERIFIED"

    def test_candidate_not_auto_rotated_to_active(self, tmp_path):
        from octa.llm import lifecycle as lc

        hash_reg = {"llama2:latest": _id_hash("78e26419b446")}
        reg = {
            "models": {"llama2:latest": {"name": "llama2:latest", "id": "78e26419b446", "state": "CANDIDATE"}},
            "history": [],
        }
        (tmp_path / "llm_hash_registry.json").write_text(json.dumps(hash_reg))
        (tmp_path / "llm_registry.json").write_text(json.dumps(reg))

        fake_policy = [
            {"name": "llama2", "pinned_version": "llama2:latest", "rotation": "auto"},
        ]
        with (
            patch.object(lc, "_REGISTRY_PATH", tmp_path / "llm_registry.json"),
            patch.object(lc, "_HASH_REGISTRY_PATH", tmp_path / "llm_hash_registry.json"),
            patch.object(lc, "_load_policy", return_value=fake_policy),
            patch("subprocess.run", return_value=_mock_run(
                "NAME                   ID              SIZE      MODIFIED\n"
                "llama2:latest          78e26419b446    3.8 GB    2 months ago\n"
            )),
        ):
            lc.cmd_rotate()

        reg2 = json.loads((tmp_path / "llm_registry.json").read_text())
        # CANDIDATE must NOT auto-rotate to ACTIVE; must be VERIFIED first
        assert reg2["models"]["llama2:latest"]["state"] == "CANDIDATE"


class TestCLI:
    def test_status_returns_zero(self):
        from octa.llm.lifecycle import main
        with patch("subprocess.run", return_value=_mock_run(_fake_ollama_list_output())):
            rc = main(["status"])
        assert rc == 0

    def test_unknown_command_returns_nonzero(self):
        from octa.llm.lifecycle import main
        rc = main(["badcmd"])
        assert rc == 2

    def test_no_args_returns_nonzero(self):
        from octa.llm.lifecycle import main
        rc = main([])
        assert rc == 2
