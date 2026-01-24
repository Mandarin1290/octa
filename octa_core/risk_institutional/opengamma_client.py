from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests
from tenacity import retry, stop_after_attempt, wait_fixed


@dataclass(frozen=True)
class OpenGammaAuth:
    mode: str = "none"  # none|bearer_env
    bearer_token: Optional[str] = None


@dataclass(frozen=True)
class OpenGammaConfig:
    base_url: str
    connect_timeout_s: float = 3.0
    read_timeout_s: float = 20.0
    retries_attempts: int = 3
    retries_wait_s: float = 1.0
    auth: OpenGammaAuth = OpenGammaAuth()

    def timeouts(self):
        return (float(self.connect_timeout_s), float(self.read_timeout_s))


class OpenGammaClient:
    """REST client for an OpenGamma-compatible risk service.

    This is intentionally thin and does not fabricate results. If the service is absent,
    callers must fail-closed when required.
    """

    def __init__(self, cfg: OpenGammaConfig):
        self.cfg = cfg

    def _headers(self) -> Dict[str, str]:
        h = {"Accept": "application/json"}
        if self.cfg.auth.mode == "bearer_env" and self.cfg.auth.bearer_token:
            h["Authorization"] = f"Bearer {self.cfg.auth.bearer_token}"
        return h

    def _url(self, path: str) -> str:
        return self.cfg.base_url.rstrip("/") + "/" + path.lstrip("/")

    def _retry(self):
        return retry(stop=stop_after_attempt(int(self.cfg.retries_attempts)), wait=wait_fixed(float(self.cfg.retries_wait_s)), reraise=True)

    def health_check(self) -> bool:
        @self._retry()
        def _do() -> bool:
            r = requests.get(self._url("/health"), headers=self._headers(), timeout=self.cfg.timeouts())
            if r.status_code != 200:
                return False
            # If JSON, accept {"status":"ok"} or similar; otherwise accept plain OK.
            try:
                obj = r.json()
                s = str(obj.get("status", "")).lower()
                return s in {"ok", "healthy", "up"} or bool(obj.get("ok", False))
            except Exception:
                return True

        return bool(_do())

    def submit_portfolio(self, exposures: Dict[str, Any]) -> str:
        """Submit exposures snapshot. Returns job_id."""

        @self._retry()
        def _do() -> str:
            r = requests.post(
                self._url("/portfolio"),
                headers={**self._headers(), "Content-Type": "application/json"},
                data=json.dumps(exposures, default=str),
                timeout=self.cfg.timeouts(),
            )
            r.raise_for_status()
            obj = r.json()
            job_id = obj.get("job_id") or obj.get("id")
            if not job_id:
                raise RuntimeError("opengamma_missing_job_id")
            return str(job_id)

        return _do()

    def request_var_es(self, *, confidence: float, horizon_days: int, job_id: str) -> str:
        @self._retry()
        def _do() -> str:
            payload = {"confidence": float(confidence), "horizon_days": int(horizon_days), "job_id": str(job_id)}
            r = requests.post(
                self._url("/risk/var_es"),
                headers={**self._headers(), "Content-Type": "application/json"},
                data=json.dumps(payload),
                timeout=self.cfg.timeouts(),
            )
            r.raise_for_status()
            obj = r.json()
            rid = obj.get("request_id") or obj.get("id")
            if not rid:
                raise RuntimeError("opengamma_missing_request_id")
            return str(rid)

        return _do()

    def request_stress(self, *, scenario_id: str, job_id: str) -> str:
        @self._retry()
        def _do() -> str:
            payload = {"scenario_id": str(scenario_id), "job_id": str(job_id)}
            r = requests.post(
                self._url("/risk/stress"),
                headers={**self._headers(), "Content-Type": "application/json"},
                data=json.dumps(payload),
                timeout=self.cfg.timeouts(),
            )
            r.raise_for_status()
            obj = r.json()
            rid = obj.get("request_id") or obj.get("id")
            if not rid:
                raise RuntimeError("opengamma_missing_request_id")
            return str(rid)

        return _do()

    def fetch_results(self, *, request_id: str) -> Dict[str, Any]:
        @self._retry()
        def _do() -> Dict[str, Any]:
            r = requests.get(self._url(f"/risk/results/{request_id}"), headers=self._headers(), timeout=self.cfg.timeouts())
            r.raise_for_status()
            obj = r.json()
            if not isinstance(obj, dict):
                raise RuntimeError("opengamma_bad_results")
            return obj

        return _do()


__all__ = ["OpenGammaAuth", "OpenGammaConfig", "OpenGammaClient"]
