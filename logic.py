from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class TradeInputs:
    usdt_spent: Decimal
    buy_price: Decimal
    sell_price: Decimal


def compute_qty(trade: TradeInputs) -> Decimal:
    # We treat the user input "USDT spent" as notional, so coin qty = usdt / buy_price.
    if trade.buy_price == 0:
        raise ZeroDivisionError("buy_price cannot be zero")
    return trade.usdt_spent / trade.buy_price


def compute_trade_profit(trade: TradeInputs) -> Decimal:
    qty = compute_qty(trade)
    return (trade.sell_price - trade.buy_price) * qty


def compute_weighted_averages(trades: list[TradeInputs]) -> dict[str, Decimal]:
    total_qty = sum(compute_qty(t) for t in trades)
    if total_qty == 0:
        return {"avg_buy": Decimal(0), "avg_sell": Decimal(0), "total_qty": Decimal(0)}
    avg_buy = sum((t.buy_price * compute_qty(t)) for t in trades) / total_qty
    avg_sell = sum((t.sell_price * compute_qty(t)) for t in trades) / total_qty
    return {"avg_buy": avg_buy, "avg_sell": avg_sell, "total_qty": total_qty}


def compute_roi(avg_buy: Decimal, avg_sell: Decimal) -> Decimal:
    if avg_buy == 0:
        return Decimal(0)
    return ((avg_sell - avg_buy) / avg_buy) * Decimal(100)

