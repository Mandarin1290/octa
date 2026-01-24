from __future__ import annotations

import json
import sys
from typing import Any

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway


def read_canary(path: str) -> dict[str, Any]:
    with open(path, "r") as f:
        return json.load(f)


def push_metrics(canary_report_path: str, job: str, gateway: str, grouping: dict[str, str] | None = None):
    data = read_canary(canary_report_path)
    registry = CollectorRegistry()
    g_drift_share = Gauge("octa_canary_drift_share", "Dataset drift share", registry=registry)
    g_drift_score = Gauge("octa_canary_drift_score", "Dataset drift score", registry=registry)
    g_passed = Gauge("octa_canary_passed", "Canary passed (1=true,0=false)", registry=registry)

    g_drift_share.set(float(data.get("drift_share", 0.0)))
    g_drift_score.set(float(data.get("drift_score", 0.0)))
    g_passed.set(1.0 if data.get("passed") else 0.0)

    push_to_gateway(gateway, job=job, registry=registry, grouping_key=grouping or {})


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--canary", required=True, help="Path to canary_report.json")
    p.add_argument("--gateway", required=True, help="Prometheus Pushgateway URL, e.g. http://pushgateway:9091")
    p.add_argument("--job", default="octa_canary", help="Prometheus job name")
    p.add_argument("--group", action="append", help="Grouping labels key=value", default=[])
    args = p.parse_args()

    grouping = {}
    for kv in args.group:
        if "=" in kv:
            k, v = kv.split("=", 1)
            grouping[k] = v

    try:
        push_metrics(args.canary, args.job, args.gateway, grouping)
        print("PUSHED_METRICS=1")
        sys.exit(0)
    except Exception as e:
        print("PUSH_FAILED:", e)
        sys.exit(2)
