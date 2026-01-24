from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, obj) -> None:
        body = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802
        if self.path.rstrip("/") == "/health":
            self._send(200, {"status": "ok", "stub": True})
            return
        self._send(404, {"error": "not_found", "path": self.path})

    def do_POST(self):  # noqa: N802
        # Never fabricate risk outputs. This stub is for wiring/health only.
        self._send(501, {"error": "stub_service_no_risk_outputs"})


def main() -> None:
    host = "0.0.0.0"
    port = int(os.getenv("OPENGAMMA_PORT", "8090"))
    httpd = HTTPServer((host, port), Handler)
    httpd.serve_forever()


if __name__ == "__main__":
    main()
