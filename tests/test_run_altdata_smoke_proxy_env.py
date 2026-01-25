import os

from octa.support.ops import run_altdata_smoke as smoke


def test_run_altdata_smoke_clears_proxy_env(monkeypatch):
    monkeypatch.setenv("OCTA_ALLOW_NET", "1")
    monkeypatch.setenv("OCTA_SMOKE_SEED", "0")
    monkeypatch.setenv("HTTP_PROXY", "http://badproxy:3128")
    monkeypatch.setenv("HTTPS_PROXY", "http://badproxy:3128")

    seen = {"cleared": False}

    def _fake_build(*args, **kwargs):
        assert "HTTP_PROXY" not in os.environ
        assert "HTTPS_PROXY" not in os.environ
        seen["cleared"] = True
        return {"sources": {}}

    class _Registry:
        def __init__(self, run_id):
            self.run_id = run_id

        def get_market_feature_vector(self, **kwargs):
            return {}

        def get_feature_vector(self, **kwargs):
            return {}

    monkeypatch.setattr(smoke, "build_altdata_stack", _fake_build)
    monkeypatch.setattr(smoke, "FeatureRegistry", _Registry)

    smoke.main()

    assert seen["cleared"] is True
    assert os.environ.get("HTTP_PROXY") == "http://badproxy:3128"
    assert os.environ.get("HTTPS_PROXY") == "http://badproxy:3128"
