"""Aggregation helpers for presenting persisted stock lots."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import List, Optional

from ..persistence import SQLiteRepository
from ..persistence.repository import (
    StockLotStatusFilter,
    StoredStockLot,
)
from .json_serializer import serialize_decimal


@dataclass(frozen=True)
class StockLotSummary:
    """Derived presentation model for a stored stock lot."""

    account_name: str
    account_number: Optional[str]
    symbol: str
    status: str
    direction: str
    quantity: int
    opened_at: str
    closed_at: Optional[str]
    basis_total: Decimal
    basis_per_share: Decimal
    realized_pnl_total: Decimal
    realized_pnl_per_share: Decimal
    share_price_total: Decimal
    share_price_per_share: Decimal
    net_credit_total: Decimal
    net_credit_per_share: Decimal
    open_premium_total: Decimal
    open_premium_per_share: Decimal
    open_fee_total: Decimal
    assignment_kind: Optional[str]
    strike_price: Decimal
    option_type: Optional[str]
    expiration: Optional[str]
    source_transaction_id: Optional[int]
    created_at: str
    updated_at: str


def fetch_stock_lot_summaries(
    repository: SQLiteRepository,
    *,
    account_name: Optional[str] = None,
    account_number: Optional[str] = None,
    ticker: Optional[str] = None,
    status: StockLotStatusFilter = "all",
) -> List[StockLotSummary]:
    """Fetch stock lots from persistence and compute derived metrics."""

    stored = repository.fetch_stock_lots(
        account_name=account_name,
        account_number=account_number,
        ticker=ticker,
        status=status,
    )
    return [_summarize_lot(lot) for lot in stored]


def serialize_stock_lot_summary(summary: StockLotSummary) -> dict[str, object]:
    """Serialize a stock lot summary into JSON-friendly primitives."""

    return {
        "account_name": summary.account_name,
        "account_number": summary.account_number,
        "symbol": summary.symbol,
        "status": summary.status,
        "direction": summary.direction,
        "quantity": summary.quantity,
        "opened_at": summary.opened_at,
        "closed_at": summary.closed_at,
        "basis_total": serialize_decimal(summary.basis_total),
        "basis_per_share": serialize_decimal(summary.basis_per_share),
        "realized_pnl_total": serialize_decimal(summary.realized_pnl_total),
        "realized_pnl_per_share": serialize_decimal(summary.realized_pnl_per_share),
        "share_price_total": serialize_decimal(summary.share_price_total),
        "share_price_per_share": serialize_decimal(summary.share_price_per_share),
        "net_credit_total": serialize_decimal(summary.net_credit_total),
        "net_credit_per_share": serialize_decimal(summary.net_credit_per_share),
        "open_premium_total": serialize_decimal(summary.open_premium_total),
        "open_premium_per_share": serialize_decimal(summary.open_premium_per_share),
        "open_fee_total": serialize_decimal(summary.open_fee_total),
        "assignment_kind": summary.assignment_kind,
        "strike_price": serialize_decimal(summary.strike_price),
        "option_type": summary.option_type,
        "expiration": summary.expiration,
        "source_transaction_id": summary.source_transaction_id,
        "created_at": summary.created_at,
        "updated_at": summary.updated_at,
    }


def _summarize_lot(lot: StoredStockLot) -> StockLotSummary:
    quantity = lot.quantity
    share_count = abs(quantity)
    divisor = Decimal(share_count) if share_count else Decimal("1")

    is_closed = lot.status.lower() == "closed"
    direction_sign = Decimal("1") if quantity >= 0 else Decimal("-1")
    basis_total = lot.share_price_total - (direction_sign * lot.net_credit_total)
    basis_per_share = basis_total / divisor

    realized_total = lot.net_credit_total if is_closed else Decimal("0")
    realized_per_share = realized_total / divisor

    return StockLotSummary(
        account_name=lot.account_name,
        account_number=lot.account_number,
        symbol=lot.symbol,
        status=lot.status,
        direction=lot.direction,
        quantity=quantity,
        opened_at=lot.opened_at,
        closed_at=lot.closed_at,
        basis_total=basis_total,
        basis_per_share=basis_per_share,
        realized_pnl_total=realized_total,
        realized_pnl_per_share=realized_per_share,
        share_price_total=lot.share_price_total,
        share_price_per_share=lot.share_price_per_share,
        net_credit_total=lot.net_credit_total,
        net_credit_per_share=lot.net_credit_per_share,
        open_premium_total=lot.open_premium_total,
        open_premium_per_share=lot.open_premium_per_share,
        open_fee_total=lot.open_fee_total,
        assignment_kind=lot.assignment_kind,
        strike_price=lot.strike_price,
        option_type=lot.option_type,
        expiration=lot.expiration,
        source_transaction_id=lot.source_transaction_id,
        created_at=lot.created_at,
        updated_at=lot.updated_at,
    )
