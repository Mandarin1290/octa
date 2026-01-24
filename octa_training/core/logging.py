from __future__ import annotations

import json
import logging
import logging.handlers
from pathlib import Path
from typing import Any, Dict
from uuid import uuid4


class JsonLineFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "ts": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "msg": record.getMessage(),
            "logger": record.name,
        }
        # include extra fields if present
        if hasattr(record, "extra") and isinstance(record.extra, dict):
            payload.update(record.extra)
        # include exception info
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def setup_logging(log_dir: Path, run_id: str | None = None) -> logging.Logger:
    """Configure structured and human logging.

    Creates `octa_training.jsonl` for structured logs and `octa_training.log` rotating human logs.
    Returns the root logger.
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    run_id = run_id or str(uuid4())
    logger = logging.getLogger("octa_training")
    logger.setLevel(logging.INFO)
    # avoid duplicate handlers
    if logger.handlers:
        return logger

    json_path = log_dir / "octa_training.jsonl"
    human_path = log_dir / "octa_training.log"

    json_handler = logging.FileHandler(json_path, encoding="utf-8")
    json_handler.setLevel(logging.INFO)
    json_handler.setFormatter(JsonLineFormatter())

    human_handler = logging.handlers.RotatingFileHandler(human_path, maxBytes=10_000_000, backupCount=5, encoding="utf-8")
    human_handler.setLevel(logging.INFO)
    human_formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s [%(run_id)s] %(message)s")
    human_handler.setFormatter(human_formatter)

    # add contextual run_id via Filter
    class RunIdFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            record.run_id = run_id
            # attach extra map for JSON formatter
            record.extra = getattr(record, "extra", {})
            record.extra.setdefault("run_id", run_id)
            return True

    run_filter = RunIdFilter()
    json_handler.addFilter(run_filter)
    human_handler.addFilter(run_filter)

    logger.addHandler(json_handler)
    logger.addHandler(human_handler)

    # also configure a null handler for library modules
    logging.getLogger().addHandler(logging.NullHandler())
    return logger
