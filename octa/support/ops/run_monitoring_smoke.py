from __future__ import annotations

from octa.core.monitoring.notify.telegram import notify_run_alert
from octa.support.env import load_local_env
from octa.core.monitoring.readers import (
    get_latest_run_id,
    get_layer_debug_table,
    get_run_overview,
    query_metrics,
)


def main() -> None:
    load_local_env()
    run_id = get_latest_run_id()
    if not run_id:
        raise SystemExit("No runs found")

    overview = get_run_overview(run_id)
    print("run_id:", run_id)
    print("survivor_counts:", overview.get("survivor_counts"))
    print("top_rejection_reasons:", overview.get("top_rejection_reasons"))

    for layer in ["L2_signal_1H", "L3_structure_30M", "L4_exec_5M", "L5_micro_1M"]:
        df = get_layer_debug_table(run_id, layer)
        print("debug", layer, "rows", len(df))

    metrics_df = query_metrics(run_id=run_id, limit=10)
    print("metrics_rows:", len(metrics_df))

    notify_run_alert(run_id, title_prefix="OCTA monitoring smoke")


if __name__ == "__main__":
    main()
