"""Test key rotation schedule and rotate_key() functionality (I2).

Verifies:
- check_rotation_due(): correctly identifies overdue keys
- rotate_key(): generates new keypair, re-signs models, emits events
- load_rotation_policy(): parses YAML config
"""

import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest import mock

import pytest

from octa.core.governance.artifact_signing import generate_keypair, verify_artifact
from octa.core.governance.key_rotation import (
    check_rotation_due,
    load_rotation_policy,
    rotate_key,
)


# ---------------------------------------------------------------------------
# check_rotation_due tests
# ---------------------------------------------------------------------------


def test_new_key_not_due_for_rotation(tmp_path):
    """Freshly created key (age ~0 days) is not due for rotation."""
    priv_key = tmp_path / "signing.key"
    pub_key = tmp_path / "verify.pub"
    generate_keypair(priv_key, pub_key)

    result = check_rotation_due(priv_key, interval_days=90)
    assert result["due"] is False
    assert result["key_age_days"] == 0
    assert result["days_until_due"] == 90
    assert "error" not in result


def test_old_key_due_for_rotation(tmp_path):
    """Key with mtime 100 days ago is due for 90-day rotation interval."""
    priv_key = tmp_path / "signing.key"
    pub_key = tmp_path / "verify.pub"
    generate_keypair(priv_key, pub_key)

    old_time = (datetime.now(timezone.utc) - timedelta(days=100)).timestamp()
    os.utime(str(priv_key), (old_time, old_time))

    result = check_rotation_due(priv_key, interval_days=90)
    assert result["due"] is True
    assert result["key_age_days"] >= 100
    assert result["days_until_due"] <= -10


def test_key_at_exact_boundary_is_due(tmp_path):
    """Key exactly interval_days old is due (age >= interval_days)."""
    priv_key = tmp_path / "signing.key"
    pub_key = tmp_path / "verify.pub"
    generate_keypair(priv_key, pub_key)

    boundary_time = (datetime.now(timezone.utc) - timedelta(days=90)).timestamp()
    os.utime(str(priv_key), (boundary_time, boundary_time))

    result = check_rotation_due(priv_key, interval_days=90)
    assert result["due"] is True


def test_key_one_day_before_boundary_not_due(tmp_path):
    """Key 89 days old is NOT due for a 90-day interval."""
    priv_key = tmp_path / "signing.key"
    pub_key = tmp_path / "verify.pub"
    generate_keypair(priv_key, pub_key)

    past_time = (datetime.now(timezone.utc) - timedelta(days=89)).timestamp()
    os.utime(str(priv_key), (past_time, past_time))

    result = check_rotation_due(priv_key, interval_days=90)
    assert result["due"] is False
    assert result["days_until_due"] >= 1


def test_missing_key_returns_due_false_with_error(tmp_path):
    """Non-existent key returns due=False with error key."""
    result = check_rotation_due(tmp_path / "nonexistent.key", interval_days=90)
    assert result["due"] is False
    assert "error" in result
    assert "key_not_found" in result["error"]


def test_check_rotation_custom_interval(tmp_path):
    """check_rotation_due() respects custom interval_days."""
    priv_key = tmp_path / "signing.key"
    pub_key = tmp_path / "verify.pub"
    generate_keypair(priv_key, pub_key)

    past_time = (datetime.now(timezone.utc) - timedelta(days=30)).timestamp()
    os.utime(str(priv_key), (past_time, past_time))

    # 30-day interval: 30 days old → due
    assert check_rotation_due(priv_key, interval_days=30)["due"] is True
    # 90-day interval: 30 days old → not due
    assert check_rotation_due(priv_key, interval_days=90)["due"] is False


# ---------------------------------------------------------------------------
# rotate_key tests
# ---------------------------------------------------------------------------


def test_rotate_key_generates_new_keypair(tmp_path):
    """rotate_key() creates new private and public key files."""
    old_priv = tmp_path / "old.key"
    old_pub = tmp_path / "old.pub"
    new_priv = tmp_path / "new.key"
    new_pub = tmp_path / "new.pub"
    generate_keypair(old_priv, old_pub)

    with mock.patch("octa.core.governance.governance_audit.GovernanceAudit") as MockGA:
        MockGA.return_value = mock.MagicMock()
        report = rotate_key(
            old_private_path=old_priv,
            old_public_path=old_pub,
            new_private_path=new_priv,
            new_public_path=new_pub,
            run_id="test_rotation_keypair",
        )

    assert new_priv.exists(), "New private key not created"
    assert new_pub.exists(), "New public key not created"
    assert report["status"] == "rotated"
    assert str(new_priv) == report["new_private_path"]
    assert str(new_pub) == report["new_public_path"]
    # Old key NOT deleted by rotate_key (caller's responsibility)
    assert old_priv.exists()


def test_rotate_key_emits_key_rotated_and_key_revoked(tmp_path):
    """rotate_key() emits exactly KEY_ROTATED then KEY_REVOKED events."""
    old_priv = tmp_path / "old.key"
    old_pub = tmp_path / "old.pub"
    new_priv = tmp_path / "new.key"
    new_pub = tmp_path / "new.pub"
    generate_keypair(old_priv, old_pub)

    with mock.patch("octa.core.governance.governance_audit.GovernanceAudit") as MockGA:
        mock_instance = mock.MagicMock()
        MockGA.return_value = mock_instance
        rotate_key(
            old_private_path=old_priv,
            old_public_path=old_pub,
            new_private_path=new_priv,
            new_public_path=new_pub,
            run_id="test_rotation_events",
        )

    MockGA.assert_called_once_with(run_id="test_rotation_events")
    assert mock_instance.emit.call_count == 2
    event_types = [call[0][0] for call in mock_instance.emit.call_args_list]
    assert "KEY_ROTATED" in event_types
    assert "KEY_REVOKED" in event_types


def test_rotate_key_resigns_approved_models(tmp_path):
    """rotate_key() re-signs all .cbm files so new public key verifies them."""
    from octa.core.governance.artifact_signing import sign_artifact

    old_priv = tmp_path / "old.key"
    old_pub = tmp_path / "old.pub"
    new_priv = tmp_path / "new.key"
    new_pub = tmp_path / "new.pub"
    generate_keypair(old_priv, old_pub)

    # Create model signed with old key
    approved_root = tmp_path / "approved"
    model_dir = approved_root / "AAPL" / "1D"
    model_dir.mkdir(parents=True)
    model_file = model_dir / "model.cbm"
    model_file.write_bytes(b"fake model for key rotation test")
    sign_artifact(model_file, old_priv)

    assert verify_artifact(model_file, old_pub), "Old key should verify before rotation"

    with mock.patch("octa.core.governance.governance_audit.GovernanceAudit") as MockGA:
        MockGA.return_value = mock.MagicMock()
        report = rotate_key(
            old_private_path=old_priv,
            old_public_path=old_pub,
            new_private_path=new_priv,
            new_public_path=new_pub,
            run_id="test_rotation_resign",
            approved_root=approved_root,
            auto_resign=True,
        )

    assert verify_artifact(model_file, new_pub), "New key should verify after rotation"
    assert len(report["resigned_models"]) == 1
    assert str(model_file) in report["resigned_models"]
    assert report["resign_errors"] == []


def test_rotate_key_auto_resign_false_skips_resigning(tmp_path):
    """rotate_key() with auto_resign=False does not re-sign models."""
    from octa.core.governance.artifact_signing import sign_artifact

    old_priv = tmp_path / "old.key"
    old_pub = tmp_path / "old.pub"
    new_priv = tmp_path / "new.key"
    new_pub = tmp_path / "new.pub"
    generate_keypair(old_priv, old_pub)

    approved_root = tmp_path / "approved"
    model_dir = approved_root / "AAPL" / "1D"
    model_dir.mkdir(parents=True)
    model_file = model_dir / "model.cbm"
    model_file.write_bytes(b"model content")
    sign_artifact(model_file, old_priv)

    with mock.patch("octa.core.governance.governance_audit.GovernanceAudit") as MockGA:
        MockGA.return_value = mock.MagicMock()
        report = rotate_key(
            old_private_path=old_priv,
            old_public_path=old_pub,
            new_private_path=new_priv,
            new_public_path=new_pub,
            run_id="test_no_resign",
            approved_root=approved_root,
            auto_resign=False,
        )

    assert report["resigned_models"] == []
    # Old key still works (model was not re-signed)
    assert verify_artifact(model_file, old_pub)
    # New key does NOT work (model was not re-signed with new key)
    assert not verify_artifact(model_file, new_pub)


def test_rotate_key_missing_old_private_raises(tmp_path):
    """rotate_key() raises FileNotFoundError if old private key is missing."""
    new_priv = tmp_path / "new.key"
    new_pub = tmp_path / "new.pub"

    with pytest.raises(FileNotFoundError, match="Old private key not found"):
        rotate_key(
            old_private_path=tmp_path / "nonexistent.key",
            old_public_path=tmp_path / "nonexistent.pub",
            new_private_path=new_priv,
            new_public_path=new_pub,
            run_id="test_missing_key",
        )


# ---------------------------------------------------------------------------
# load_rotation_policy tests
# ---------------------------------------------------------------------------


def test_load_rotation_policy_returns_defaults_for_missing_file(tmp_path):
    """load_rotation_policy() returns defaults when file does not exist."""
    result = load_rotation_policy(tmp_path / "nonexistent.yaml")
    assert result["interval_days"] == 90
    assert result["alert_days_before"] == 7
    assert result["auto_resign_on_rotation"] is True


def test_load_rotation_policy_reads_yaml(tmp_path):
    """load_rotation_policy() reads key_rotation section from YAML."""
    policy_file = tmp_path / "policy.yaml"
    policy_file.write_text(
        "key_rotation:\n"
        "  interval_days: 30\n"
        "  alert_days_before: 3\n"
        "  auto_resign_on_rotation: false\n",
        encoding="utf-8",
    )
    result = load_rotation_policy(policy_file)
    assert result["interval_days"] == 30
    assert result["alert_days_before"] == 3
    assert result["auto_resign_on_rotation"] is False


def test_load_rotation_policy_partial_yaml_uses_defaults(tmp_path):
    """Partial key_rotation section fills missing keys with defaults."""
    policy_file = tmp_path / "policy.yaml"
    policy_file.write_text(
        "key_rotation:\n  interval_days: 60\n",
        encoding="utf-8",
    )
    result = load_rotation_policy(policy_file)
    assert result["interval_days"] == 60
    assert result["alert_days_before"] == 7  # default
    assert result["auto_resign_on_rotation"] is True  # default


def test_load_rotation_policy_no_key_rotation_section(tmp_path):
    """YAML without key_rotation section returns all defaults."""
    policy_file = tmp_path / "policy.yaml"
    policy_file.write_text("mode:\n  default: shadow\n", encoding="utf-8")
    result = load_rotation_policy(policy_file)
    assert result["interval_days"] == 90
    assert result["alert_days_before"] == 7
    assert result["auto_resign_on_rotation"] is True


def test_load_rotation_policy_from_real_config():
    """load_rotation_policy() can read the actual configs/policy.yaml."""
    policy_path = Path("configs/policy.yaml")
    if not policy_path.exists():
        pytest.skip("configs/policy.yaml not found")
    result = load_rotation_policy(policy_path)
    assert isinstance(result["interval_days"], int)
    assert result["interval_days"] > 0
