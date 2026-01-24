from __future__ import annotations

import os
import sqlite3
from typing import Optional

from octa_core.types import Identifier

from .models import ExecutionReport, Order


class OrderStore:
    def __init__(self, path: str) -> None:
        os.makedirs(path, exist_ok=True)
        self.db = sqlite3.connect(os.path.join(path, "orders.db"), isolation_level=None)
        self._init()

    def _init(self) -> None:
        cur = self.db.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS orders(
                id TEXT PRIMARY KEY,
                intent_id TEXT,
                symbol TEXT,
                side TEXT,
                qty REAL,
                price REAL,
                status TEXT,
                filled_qty REAL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS reports(
                report_id TEXT PRIMARY KEY,
                order_id TEXT,
                status TEXT,
                filled_qty REAL,
                remaining_qty REAL,
                msg TEXT,
                ts TEXT
            )
            """
        )

    def upsert_order(self, order: Order) -> None:
        cur = self.db.cursor()
        cur.execute(
            "REPLACE INTO orders(id, intent_id, symbol, side, qty, price, status, filled_qty) VALUES(?,?,?,?,?,?,?,?)",
            (
                str(order.id),
                order.intent_id,
                order.symbol,
                order.side.value,
                order.qty,
                order.price,
                order.status.value,
                order.filled_qty,
            ),
        )

    def insert_report(self, report: ExecutionReport) -> None:
        cur = self.db.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO reports(report_id, order_id, status, filled_qty, remaining_qty, msg, ts) VALUES(?,?,?,?,?,?,?)",
            (
                report.report_id,
                str(report.order_id),
                report.status.value,
                report.filled_qty,
                report.remaining_qty,
                report.msg,
                report.ts,
            ),
        )

    def get_order(self, order_id: Identifier) -> Optional[Order]:
        cur = self.db.cursor()
        cur.execute(
            "SELECT id,intent_id,symbol,side,qty,price,status,filled_qty FROM orders WHERE id = ?",
            (str(order_id),),
        )
        r = cur.fetchone()
        if not r:
            return None
        from .models import Order, OrderSide, OrderStatus

        return Order(
            id=r[0],
            intent_id=r[1],
            symbol=r[2],
            side=OrderSide(r[3]),
            qty=r[4],
            price=r[5],
            status=OrderStatus(r[6]),
            filled_qty=r[7],
        )
