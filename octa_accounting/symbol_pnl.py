"""Per-symbol P&L computation from ledger fill events.

Reads ``paper.order_filled`` events from LedgerStore, computes FIFO realized
P&L per symbol, and optionally writes ``symbol_pnl.json``.

Usage::

    from octa_accounting.symbol_pnl import compute_symbol_pnl

    results = compute_symbol_pnl(
        ledger_path="artifacts/ledger_paper",
        positions_path="artifacts/ledger_paper/position_state_paper.json",
        current_prices={"AAPL": 175.0},
        output_path="artifacts/ledger_paper/symbol_pnl.json",
    )
"""
from __future__ import annotations

import json
from collections import deque
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class SymbolPosition:
    symbol: str
    qty: float = 0.0
    avg_price: float = 0.0
    cost_basis: float = 0.0


@dataclass
class TradeRecord:
    symbol: str
    side: str          # "BUY" or "SELL"
    qty: float
    price: float
    ts: str
    run_id: str = ""
    model_id: str = ""
    confidence: float = 0.0


@dataclass
class SymbolPnL:
    symbol: str
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    total_pnl: float = 0.0
    n_trades: int = 0
    n_wins: int = 0
    n_losses: int = 0
    win_rate: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    total_volume: float = 0.0
    open_qty: float = 0.0
    avg_entry_price: float = 0.0
    trade_returns: List[float] = field(default_factory=list)
    last_updated: str = ""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _extract_fills(ledger_path: str) -> List[TradeRecord]:
    """Load all ``paper.order_filled`` events from the ledger."""
    from octa_ledger.store import LedgerStore

    ledger = LedgerStore(ledger_path)
    events = ledger.by_action("paper.order_filled")
    trades: List[TradeRecord] = []
    for ev in events:
        p = ev.get("payload", {}) if isinstance(ev, dict) else {}
        sym = str(p.get("symbol") or "").strip()
        side = str(p.get("side") or p.get("action") or "").strip().upper()
        qty = p.get("qty") or p.get("quantity") or p.get("filled_qty") or 0.0
        price = p.get("fill_price") or p.get("price") or p.get("avg_fill_price") or 0.0
        ts = str(ev.get("timestamp") or p.get("ts") or "")
        if not sym or not side or not qty or not price:
            continue
        trades.append(TradeRecord(
            symbol=sym,
            side=side,
            qty=float(qty),
            price=float(price),
            ts=ts,
            run_id=str(p.get("run_id") or ""),
            model_id=str(p.get("model_id") or ""),
            confidence=float(p.get("confidence") or 0.0),
        ))
    return trades


def _fifo_realized(trades: List[TradeRecord]) -> Tuple[float, List[float], float, float]:
    """Compute FIFO realized P&L for a single symbol's sorted trade list.

    Returns:
        (realized_pnl, trade_returns, open_qty, avg_entry_price)
    """
    # FIFO queue of (qty_remaining, cost_price) lots
    lots: Deque[Tuple[float, float]] = deque()
    realized = 0.0
    trade_returns: List[float] = []

    for tr in trades:
        if tr.side == "BUY":
            lots.append((tr.qty, tr.price))
        elif tr.side == "SELL":
            sell_qty = tr.qty
            sell_price = tr.price
            while sell_qty > 1e-9 and lots:
                lot_qty, lot_price = lots[0]
                matched = min(sell_qty, lot_qty)
                pnl = matched * (sell_price - lot_price)
                realized += pnl
                trade_returns.append(pnl)
                sell_qty -= matched
                remaining = lot_qty - matched
                if remaining > 1e-9:
                    lots[0] = (remaining, lot_price)
                else:
                    lots.popleft()
            # If more sold than bought (short selling / mis-ordered) — ignore residual

    # Remaining open position
    open_qty = sum(l[0] for l in lots)
    avg_entry = 0.0
    if open_qty > 1e-9:
        avg_entry = sum(l[0] * l[1] for l in lots) / open_qty

    return realized, trade_returns, open_qty, avg_entry


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compute_symbol_pnl(
    ledger_path: str,
    positions_path: Optional[str] = None,
    current_prices: Optional[Dict[str, float]] = None,
    output_path: Optional[str] = None,
) -> Dict[str, SymbolPnL]:
    """Compute per-symbol P&L from ledger fill events.

    Args:
        ledger_path: Path to LedgerStore directory.
        positions_path: Optional path to ``position_state_*.json`` for open qty
            cross-check (currently unused beyond reference).
        current_prices: Optional dict of symbol → current price for unrealized P&L.
        output_path: If given, write ``symbol_pnl.json`` here.

    Returns:
        Dict mapping symbol → SymbolPnL.
    """
    trades = _extract_fills(ledger_path)
    current_prices = current_prices or {}

    # Group trades by symbol (preserve arrival order for FIFO)
    by_symbol: Dict[str, List[TradeRecord]] = {}
    last_ts: Dict[str, str] = {}
    for tr in trades:
        by_symbol.setdefault(tr.symbol, []).append(tr)
        last_ts[tr.symbol] = tr.ts

    results: Dict[str, SymbolPnL] = {}
    for sym, sym_trades in sorted(by_symbol.items()):
        realized, trade_rets, open_qty, avg_entry = _fifo_realized(sym_trades)

        # Unrealized P&L from current price
        unrealized = 0.0
        if open_qty > 1e-9 and sym in current_prices:
            unrealized = open_qty * (current_prices[sym] - avg_entry)

        # Win/loss stats (only on closed trades with non-zero P&L)
        wins = [r for r in trade_rets if r > 0]
        losses = [r for r in trade_rets if r < 0]
        n_closed = len(trade_rets)
        win_rate = len(wins) / n_closed if n_closed > 0 else 0.0
        avg_win = sum(wins) / len(wins) if wins else 0.0
        avg_loss = sum(losses) / len(losses) if losses else 0.0

        total_volume = sum(t.qty * t.price for t in sym_trades)

        pnl = SymbolPnL(
            symbol=sym,
            realized_pnl=round(realized, 6),
            unrealized_pnl=round(unrealized, 6),
            total_pnl=round(realized + unrealized, 6),
            n_trades=len(sym_trades),
            n_wins=len(wins),
            n_losses=len(losses),
            win_rate=round(win_rate, 6),
            avg_win=round(avg_win, 6),
            avg_loss=round(avg_loss, 6),
            total_volume=round(total_volume, 6),
            open_qty=round(open_qty, 6),
            avg_entry_price=round(avg_entry, 6),
            trade_returns=trade_rets,
            last_updated=last_ts.get(sym, ""),
        )
        results[sym] = pnl

    if output_path:
        _write_output(results, output_path)

    return results


def _write_output(results: Dict[str, SymbolPnL], output_path: str) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload: Dict[str, Any] = {sym: asdict(pnl) for sym, pnl in results.items()}
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
