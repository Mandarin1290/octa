import pytest

from octa_training.core.asset_profiles import (
    STOCKS_PROFILE_NAME,
    AssetProfile,
    AssetProfileMismatchError,
    ensure_canonical_profile_for_dataset,
)


def test_stocks_profile_is_not_legacy():
    resolved = AssetProfile(name=STOCKS_PROFILE_NAME, kind="stock", gates={})
    # should not raise
    ensure_canonical_profile_for_dataset(dataset="stocks", resolved=resolved, applied_thresholds={"sharpe_min": 0.5}, gate_version="v1")


def test_stocks_profile_mismatch_fails_closed():
    resolved = AssetProfile(name="legacy", kind="legacy", gates={})
    with pytest.raises(AssetProfileMismatchError) as exc:
        ensure_canonical_profile_for_dataset(dataset="stocks", resolved=resolved, applied_thresholds={}, gate_version="v1")
    assert "STOCKS_PROFILE_MISMATCH" in str(exc.value)


def test_non_stocks_not_enforced():
    resolved = AssetProfile(name="legacy", kind="legacy", gates={})
    # fx should not raise
    ensure_canonical_profile_for_dataset(dataset="fx", resolved=resolved, applied_thresholds={}, gate_version=None)
