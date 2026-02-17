from __future__ import annotations

import os
from pathlib import Path


SCRIPTS = [
    "scripts/octa_systemd_entrypoint.sh",
    "scripts/octa_x11_bootstrap.sh",
    "scripts/octa_ibkr_bootstrap.sh",
    "scripts/octa_autologin_bootstrap.sh",
    "scripts/octa_v000_loop.sh",
]

UNITS = [
    "systemd/octa-x11.service",
    "systemd/octa-ibkr.service",
    "systemd/octa-autologin.service",
    "systemd/octa-v000.service",
    "systemd/octa.target",
]


def test_scripts_exist_executable_and_strict_shell():
    for rel in SCRIPTS:
        p = Path(rel)
        assert p.exists(), rel
        text = p.read_text(encoding="utf-8")
        assert "set -euo pipefail" in text
        st = os.stat(p)
        assert bool(st.st_mode & 0o111), rel


def test_units_reference_env_and_wrappers():
    for rel in UNITS:
        p = Path(rel)
        assert p.exists(), rel
        text = p.read_text(encoding="utf-8")
        if rel.endswith(".service"):
            assert "EnvironmentFile=%h/.config/octa/env" in text
            assert "WorkingDirectory=${OCTA_REPO}" in text
            assert "Restart=always" in text
            assert "RestartSec=2" in text
            assert "StartLimitIntervalSec=0" in text
            assert "StandardOutput=journal" in text
            assert "StandardError=journal" in text
