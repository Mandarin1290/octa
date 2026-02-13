from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


@dataclass
class TrainingStateRecord:
    symbol: str
    timeframe: str
    config_hash: str
    pipeline_version: str
    status: str
    last_train_end_utc: Optional[str] = None
    metrics_hash: Optional[str] = None
    artifact_paths: Optional[List[str]] = None
    report_path: Optional[str] = None
    drift_flag: bool = False
    last_data_mtime: Optional[float] = None
    reason: Optional[str] = None
    updated_at_utc: Optional[str] = None

    def key(self) -> Tuple[str, str, str, str]:
        return (self.symbol, self.timeframe, self.config_hash, self.pipeline_version)

    def to_dict(self) -> Dict[str, object]:
        return {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "config_hash": self.config_hash,
            "pipeline_version": self.pipeline_version,
            "status": self.status,
            "last_train_end_utc": self.last_train_end_utc,
            "metrics_hash": self.metrics_hash,
            "artifact_paths": list(self.artifact_paths or []),
            "report_path": self.report_path,
            "drift_flag": bool(self.drift_flag),
            "last_data_mtime": self.last_data_mtime,
            "reason": self.reason,
            "updated_at_utc": self.updated_at_utc,
        }

    @staticmethod
    def from_dict(raw: Dict[str, object]) -> "TrainingStateRecord":
        return TrainingStateRecord(
            symbol=str(raw.get("symbol", "")),
            timeframe=str(raw.get("timeframe", "")),
            config_hash=str(raw.get("config_hash", "")),
            pipeline_version=str(raw.get("pipeline_version", "")),
            status=str(raw.get("status", "")),
            last_train_end_utc=_optional_str(raw.get("last_train_end_utc")),
            metrics_hash=_optional_str(raw.get("metrics_hash")),
            artifact_paths=_optional_list(raw.get("artifact_paths")),
            report_path=_optional_str(raw.get("report_path")),
            drift_flag=bool(raw.get("drift_flag", False)),
            last_data_mtime=_optional_float(raw.get("last_data_mtime")),
            reason=_optional_str(raw.get("reason")),
            updated_at_utc=_optional_str(raw.get("updated_at_utc")),
        )


def _optional_str(val: object) -> Optional[str]:
    return str(val) if val is not None else None


def _optional_list(val: object) -> Optional[List[str]]:
    if val is None:
        return None
    if isinstance(val, list):
        return [str(v) for v in val]
    return [str(val)]


def _optional_float(val: object) -> Optional[float]:
    try:
        return float(val) if val is not None else None
    except Exception:
        return None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class TrainingStateStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._records: Dict[Tuple[str, str, str, str], TrainingStateRecord] = {}
        self.load()

    def load(self) -> None:
        self._records = {}
        if not self.path.exists():
            return
        for line in self.path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            raw = json.loads(line)
            rec = TrainingStateRecord.from_dict(raw)
            if rec.symbol and rec.timeframe and rec.config_hash and rec.pipeline_version:
                self._records[rec.key()] = rec

    def get(self, symbol: str, timeframe: str, config_hash: str, pipeline_version: str) -> Optional[TrainingStateRecord]:
        key = (symbol, timeframe, config_hash, pipeline_version)
        return self._records.get(key)

    def find_by_symbol_tf(self, symbol: str, timeframe: str) -> List[TrainingStateRecord]:
        out: List[TrainingStateRecord] = []
        for (sym, tf, _, _), rec in self._records.items():
            if sym == symbol and tf == timeframe:
                out.append(rec)
        return out

    def upsert(self, rec: TrainingStateRecord) -> None:
        rec.updated_at_utc = _utc_now_iso()
        self._records[rec.key()] = rec

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        items = sorted(self._records.values(), key=lambda r: (r.symbol, r.timeframe, r.config_hash, r.pipeline_version))
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as fh:
            for rec in items:
                fh.write(json.dumps(rec.to_dict(), ensure_ascii=False, sort_keys=True))
                fh.write("\n")
        tmp_path.replace(self.path)

    def records(self) -> Iterable[TrainingStateRecord]:
        return list(self._records.values())
