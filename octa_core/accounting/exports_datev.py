from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from octa_core.accounting.ledger import DoubleEntryLedger


@dataclass(frozen=True)
class DatevExportConfig:
    export_dir: str
    delimiter: str = ";"


def export_datev_csv(*, ledger: DoubleEntryLedger, cfg: DatevExportConfig, out_name: str = "datev_export.csv") -> str:
    """Generate a DATEV-style CSV (minimal)."""

    out_dir = Path(cfg.export_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / out_name

    # Minimal export: entries table only (lines can be joined later).
    rows = ledger.list_entries(limit=100000)
    with out_path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh, delimiter=cfg.delimiter)
        w.writerow(["Belegnr", "Datum", "Text"])
        for entry_id, ts, ref in reversed(rows):
            w.writerow([entry_id, ts, ref])

    return str(out_path)


__all__ = ["DatevExportConfig", "export_datev_csv"]
