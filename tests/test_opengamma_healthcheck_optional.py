from __future__ import annotations

import os

import pytest

from octa_core.risk_institutional.opengamma_client import (
    OpenGammaAuth,
    OpenGammaClient,
    OpenGammaConfig,
)


@pytest.mark.skipif(not os.getenv("OPENGAMMA_TEST_URL"), reason="OPENGAMMA_TEST_URL not set")
def test_opengamma_healthcheck():
    url = os.environ["OPENGAMMA_TEST_URL"].strip()
    cfg = OpenGammaConfig(base_url=url, auth=OpenGammaAuth(mode="none"))
    client = OpenGammaClient(cfg)
    assert client.health_check() in {True, False}
