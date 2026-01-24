from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


@dataclass(frozen=True)
class LedgerLine:
    account: str
    amount: float
    currency: str = "EUR"
    memo: str = ""


@dataclass(frozen=True)
class LedgerEntry:
    ts: str
    ref: str
    lines: List[LedgerLine]
    meta_json: str = "{}"


class DoubleEntryLedger:
    """Deterministic minimal double-entry ledger (SQLite).

    - Accounts are config-driven (see octa_core/config/accounting.yaml).
    - Entries must balance to 0.0 per currency.
    """

    def __init__(self, *, db_path: str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(str(self.db_path))
        c.execute("PRAGMA journal_mode=WAL")
        return c

    def _init_db(self) -> None:
        with self._conn() as c:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS entries(
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  ts TEXT,
                  ref TEXT,
                  meta_json TEXT
                )
                """
            )
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS lines(
                  entry_id INTEGER,
                  account TEXT,
                  amount REAL,
                  currency TEXT,
                  memo TEXT
                )
                """
            )

    def _validate_balanced(self, lines: Iterable[LedgerLine]) -> None:
        by_ccy: Dict[str, float] = {}
        for ln in lines:
            by_ccy[ln.currency] = by_ccy.get(ln.currency, 0.0) + float(ln.amount)
        for ccy, s in by_ccy.items():
            if abs(s) > 1e-6:
                raise ValueError(f"unbalanced_entry currency={ccy} sum={s}")

    def post(self, *, ref: str, lines: List[LedgerLine], meta_json: str = "{}", ts: Optional[str] = None) -> int:
        if not lines:
            raise ValueError("empty_entry")
        self._validate_balanced(lines)
        ts2 = ts or _now_iso()

        with self._conn() as c:
            cur = c.cursor()
            cur.execute("INSERT INTO entries(ts, ref, meta_json) VALUES(?,?,?)", (ts2, str(ref), str(meta_json)))
            entry_id = int(cur.lastrowid)
            cur.executemany(
                "INSERT INTO lines(entry_id, account, amount, currency, memo) VALUES(?,?,?,?,?)",
                [(entry_id, ln.account, float(ln.amount), ln.currency, ln.memo) for ln in lines],
            )
            return entry_id

    def list_entries(self, limit: int = 100) -> List[Tuple[int, str, str]]:
        with self._conn() as c:
            rows = c.execute("SELECT id, ts, ref FROM entries ORDER BY id DESC LIMIT ?", (int(limit),)).fetchall()
        return [(int(r[0]), str(r[1]), str(r[2])) for r in rows]


__all__ = ["LedgerLine", "LedgerEntry", "DoubleEntryLedger"]
