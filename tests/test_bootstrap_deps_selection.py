from __future__ import annotations

from octa_core.bootstrap.deps import select_required_packages


def test_deps_selection_quantlib_off_by_default():
    cfg = {
        "features": {
            "quantlib": {"enabled": False},
            "control_plane": {"enabled": False},
            "security": {"encryption_at_rest": {"enabled": False}},
            "execution": {"ibkr_ib_insync": {"enabled": False}},
            "telegram_control": {"enabled": False},
            "portfolio_optim": {"riskfolio": {"enabled": False}, "cvxpy": {"enabled": False}},
        }
    }
    pkgs = select_required_packages(cfg)
    assert all(p.import_name != "QuantLib" for p in pkgs)


def test_deps_selection_quantlib_on_adds_quantlib():
    cfg = {
        "features": {
            "quantlib": {"enabled": True},
            "control_plane": {"enabled": False},
            "security": {"encryption_at_rest": {"enabled": False}},
            "execution": {"ibkr_ib_insync": {"enabled": False}},
            "telegram_control": {"enabled": False},
            "portfolio_optim": {"riskfolio": {"enabled": False}, "cvxpy": {"enabled": False}},
        }
    }
    pkgs = select_required_packages(cfg)
    assert any(p.import_name == "QuantLib" for p in pkgs)
