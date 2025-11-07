"""Services for rebuilding stock lots from assignments and stock trades."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from typing import List, Optional

from ..persistence import PersistedStockLot, SQLiteRepository, StoredStockTransaction
from .leg_matching import MatchedLeg, MatchedLegLot
from .transaction_loader import fetch_normalized_transactions, match_legs_from_transactions

SHARES_PER_CONTRACT = Decimal("100")


@dataclass(frozen=True)
class ShareEvent:
    symbol: str
    activity_date: date
    quantity: int  # positive for buy, negative for sell
    price_per_share: Decimal
    total_value: Decimal  # absolute cash paid/received
    fee_total: Decimal
    premium_total: Decimal
    source: str
    source_id: Optional[int]


@dataclass
class LotState:
    symbol: str
    opened_at: date
    quantity_remaining: int
    cost_per_share: Decimal
    cost_basis_remaining: Decimal
    open_fee_total: Decimal
    premium_total: Decimal
    source: str
    source_id: Optional[int]


@dataclass
class ShortLotState:
    symbol: str
    opened_at: date
    quantity_remaining: int
    proceeds_per_share: Decimal
    proceeds_remaining: Decimal
    open_fee_total: Decimal
    source: str
    source_id: Optional[int]


def rebuild_stock_lots(
    repository: SQLiteRepository,
    *,
    account_name: str,
    account_number: Optional[str],
) -> None:
    """Rebuild the persisted stock lots for an account."""

    normalized_option_txns = fetch_normalized_transactions(
        repository,
        account_name=account_name,
        account_number=account_number,
    )
    matched_legs = match_legs_from_transactions(normalized_option_txns)
    assignment_events = _build_assignment_events(matched_legs)

    stored_stock_txns = repository.fetch_stock_transactions(
        account_name=account_name,
        account_number=account_number,
    )
    stock_events = [_build_stock_event(txn) for txn in stored_stock_txns]

    all_events = assignment_events + stock_events
    all_events.sort(
        key=lambda ev: (
            ev.activity_date,
            ev.symbol,
            0 if ev.source.startswith("assignment") else 1,
        )
    )

    lot_rows = _match_stock_lots(all_events)
    repository.replace_stock_lots(
        account_name=account_name,
        account_number=account_number,
        rows=lot_rows,
    )


def _build_stock_event(txn: StoredStockTransaction) -> ShareEvent:
    amount = Decimal(txn.amount)
    quantity = int(txn.quantity)
    sign = 1 if txn.action.upper() == "BUY" else -1
    shares = quantity
    total_value = abs(amount)
    price_per_share = (total_value / Decimal(shares)).quantize(
        Decimal("0.0001"), rounding=ROUND_HALF_UP
    )
    return ShareEvent(
        symbol=txn.instrument,
        activity_date=date.fromisoformat(txn.activity_date),
        quantity=shares * sign,
        price_per_share=price_per_share,
        total_value=total_value,
        fee_total=Decimal("0"),
        premium_total=Decimal("0"),
        source=f"stock_{txn.action.lower()}",
        source_id=txn.id,
    )


def _build_assignment_events(matched_legs: List[MatchedLeg]) -> List[ShareEvent]:
    events: List[ShareEvent] = []
    for leg in matched_legs:
        for lot in leg.lots:
            if not lot.close_portions:
                continue
            for portion in lot.close_portions:
                if not portion.fill.is_assignment:
                    continue
                event = _assignment_portion_to_event(lot, portion)
                if event:
                    events.append(event)
    return events


def _assignment_portion_to_event(
    lot: MatchedLegLot,
    portion,
) -> Optional[ShareEvent]:
    option_type = lot.contract.option_type.upper()
    if option_type not in {"CALL", "PUT"}:
        return None

    portion_contracts = Decimal(portion.quantity)
    share_quantity = int(portion_contracts * SHARES_PER_CONTRACT)
    if share_quantity == 0:
        return None

    raw_txn = portion.fill.transaction.raw or {}
    source_id = raw_txn.get("__transaction_id")

    strike_price = lot.contract.strike
    premium_ratio = portion_contracts / Decimal(lot.quantity)
    premium_total = (lot.open_premium * premium_ratio).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )
    price_per_share = strike_price
    total_value = (price_per_share * Decimal(abs(share_quantity))).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )

    if option_type == "PUT":
        quantity = share_quantity  # positive -> buy shares
        source = "assignment_put"
    else:
        quantity = -share_quantity  # selling shares
        source = "assignment_call"
        premium_total = Decimal("0")

    return ShareEvent(
        symbol=lot.contract.symbol,
        activity_date=portion.activity_date,
        quantity=quantity,
        price_per_share=price_per_share,
        total_value=total_value,
        fee_total=Decimal("0"),
        premium_total=premium_total if option_type == "PUT" else Decimal("0"),
        source=source,
        source_id=int(source_id) if source_id is not None else None,
    )


def _match_stock_lots(events: List[ShareEvent]) -> List[PersistedStockLot]:
    long_lots: deque[LotState] = deque()
    short_lots: deque[ShortLotState] = deque()
    results: List[PersistedStockLot] = []

    for event in events:
        if event.quantity > 0:
            remaining = event.quantity
            remaining_cost = event.total_value - event.premium_total
            remaining_premium = event.premium_total

            # cover existing short positions first
            remaining, remaining_cost, remaining_premium = _close_short_lots(
                results,
                short_lots,
                event,
                remaining,
                remaining_cost,
                remaining_premium,
            )
            if remaining == 0:
                continue

            per_share_cost = (remaining_cost / Decimal(remaining)).quantize(
                Decimal("0.0001"), rounding=ROUND_HALF_UP
            )
            per_share_premium = (
                (remaining_premium / Decimal(remaining)).quantize(
                    Decimal("0.0001"), rounding=ROUND_HALF_UP
                )
                if remaining_premium
                else Decimal("0")
            )
            long_state = LotState(
                symbol=event.symbol,
                opened_at=event.activity_date,
                quantity_remaining=remaining,
                cost_per_share=per_share_cost,
                cost_basis_remaining=Decimal(remaining) * per_share_cost,
                open_fee_total=Decimal("0"),
                premium_total=per_share_premium * Decimal(remaining),
                source=event.source,
                source_id=event.source_id,
            )
            long_lots.append(long_state)
        else:
            sell_qty = abs(event.quantity)
            sell_total = event.total_value
            sell_price_per_share = (sell_total / Decimal(sell_qty)).quantize(
                Decimal("0.0001"), rounding=ROUND_HALF_UP
            )

            sell_qty = _close_long_lots(
                results,
                long_lots,
                event,
                sell_qty,
                sell_price_per_share,
            )
            if sell_qty > 0:
                # open short position for remaining quantity
                per_share_proceeds = sell_price_per_share
                short_state = ShortLotState(
                    symbol=event.symbol,
                    opened_at=event.activity_date,
                    quantity_remaining=sell_qty,
                    proceeds_per_share=per_share_proceeds,
                    proceeds_remaining=per_share_proceeds * Decimal(sell_qty),
                    open_fee_total=Decimal("0"),
                    source=event.source,
                    source_id=event.source_id,
                )
                short_lots.append(short_state)

    while long_lots:
        lot_state = long_lots.popleft()
        results.append(
            PersistedStockLot(
                symbol=lot_state.symbol,
                opened_at=lot_state.opened_at,
                closed_at=None,
                quantity=lot_state.quantity_remaining,
                direction="long",
                cost_basis_total=lot_state.cost_basis_remaining,
                cost_basis_per_share=lot_state.cost_per_share,
                open_fee_total=lot_state.open_fee_total,
                assignment_premium_total=lot_state.premium_total,
                proceeds_total=None,
                proceeds_per_share=None,
                close_fee_total=Decimal("0"),
                realized_pnl_total=None,
                realized_pnl_per_share=None,
                open_source=lot_state.source,
                open_source_id=lot_state.source_id,
                close_source=None,
                close_source_id=None,
                status="open",
            )
        )

    while short_lots:
        short_state = short_lots.popleft()
        results.append(
            PersistedStockLot(
                symbol=short_state.symbol,
                opened_at=short_state.opened_at,
                closed_at=None,
                quantity=-short_state.quantity_remaining,
                direction="short",
                cost_basis_total=Decimal("0"),
                cost_basis_per_share=Decimal("0"),
                open_fee_total=short_state.open_fee_total,
                assignment_premium_total=Decimal("0"),
                proceeds_total=short_state.proceeds_per_share
                * Decimal(short_state.quantity_remaining),
                proceeds_per_share=short_state.proceeds_per_share,
                close_fee_total=Decimal("0"),
                realized_pnl_total=None,
                realized_pnl_per_share=None,
                open_source=short_state.source,
                open_source_id=short_state.source_id,
                close_source=None,
                close_source_id=None,
                status="open",
            )
        )

    return results


def _close_long_lots(
    results: List[PersistedStockLot],
    long_lots: deque[LotState],
    event: ShareEvent,
    sell_qty: int,
    sell_price_per_share: Decimal,
) -> int:
    remaining_sale = sell_qty
    while remaining_sale > 0 and long_lots:
        lot = long_lots[0]
        close_qty = min(remaining_sale, lot.quantity_remaining)
        cost_total = lot.cost_per_share * Decimal(close_qty)
        proceeds_total = sell_price_per_share * Decimal(close_qty)
        realized = proceeds_total - cost_total

        results.append(
            PersistedStockLot(
                symbol=lot.symbol,
                opened_at=lot.opened_at,
                closed_at=event.activity_date,
                quantity=close_qty,
                direction="long",
                cost_basis_total=cost_total,
                cost_basis_per_share=lot.cost_per_share,
                open_fee_total=lot.open_fee_total,
                assignment_premium_total=lot.premium_total,
                proceeds_total=proceeds_total,
                proceeds_per_share=sell_price_per_share,
                close_fee_total=Decimal("0"),
                realized_pnl_total=realized,
                realized_pnl_per_share=(realized / Decimal(close_qty)).quantize(
                    Decimal("0.0001"), rounding=ROUND_HALF_UP
                ),
                open_source=lot.source,
                open_source_id=lot.source_id,
                close_source=event.source,
                close_source_id=event.source_id,
                status="closed",
            )
        )

        lot.quantity_remaining -= close_qty
        lot.cost_basis_remaining -= cost_total
        remaining_sale -= close_qty
        if lot.quantity_remaining == 0:
            long_lots.popleft()
    return remaining_sale


def _close_short_lots(
    results: List[PersistedStockLot],
    short_lots: deque[ShortLotState],
    event: ShareEvent,
    buy_qty: int,
    buy_cost_total: Decimal,
    buy_premium_total: Decimal,
) -> tuple[int, Decimal, Decimal]:
    remaining_buy = buy_qty
    remaining_cost = buy_cost_total
    remaining_premium = buy_premium_total
    price_per_share = (buy_cost_total / Decimal(buy_qty)).quantize(
        Decimal("0.0001"), rounding=ROUND_HALF_UP
    )
    premium_per_share = (
        (buy_premium_total / Decimal(buy_qty)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        if buy_premium_total
        else Decimal("0")
    )
    while remaining_buy > 0 and short_lots:
        lot = short_lots[0]
        cover_qty = min(remaining_buy, lot.quantity_remaining)
        cover_cost = price_per_share * Decimal(cover_qty)
        cover_premium = premium_per_share * Decimal(cover_qty)
        proceeds_total = lot.proceeds_per_share * Decimal(cover_qty)
        realized = proceeds_total - cover_cost

        results.append(
            PersistedStockLot(
                symbol=lot.symbol,
                opened_at=lot.opened_at,
                closed_at=event.activity_date,
                quantity=-cover_qty,
                direction="short",
                cost_basis_total=cover_cost,
                cost_basis_per_share=price_per_share,
                open_fee_total=lot.open_fee_total,
                assignment_premium_total=Decimal("0"),
                proceeds_total=proceeds_total,
                proceeds_per_share=lot.proceeds_per_share,
                close_fee_total=Decimal("0"),
                realized_pnl_total=realized,
                realized_pnl_per_share=(realized / Decimal(cover_qty)).quantize(
                    Decimal("0.0001"), rounding=ROUND_HALF_UP
                ),
                open_source=lot.source,
                open_source_id=lot.source_id,
                close_source=event.source,
                close_source_id=event.source_id,
                status="closed",
            )
        )

        lot.quantity_remaining -= cover_qty
        lot.proceeds_remaining -= proceeds_total
        remaining_buy -= cover_qty
        remaining_cost -= cover_cost
        if buy_premium_total:
            remaining_premium -= cover_premium
        if lot.quantity_remaining == 0:
            short_lots.popleft()
    return remaining_buy, remaining_cost, remaining_premium
