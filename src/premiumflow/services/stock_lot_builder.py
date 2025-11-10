"""Services for deriving and persisting consolidated stock lots."""

from __future__ import annotations

import datetime as dt
from collections import defaultdict, deque
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Deque, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from ..persistence import (
    AssignmentStockLotRecord,
    SQLiteRepository,
    StockLotRecord,
    StoredStockTransaction,
)
from .leg_matching import MatchedLeg, MatchedLegLot
from .transaction_loader import fetch_normalized_transactions, match_legs_from_transactions

SHARES_PER_CONTRACT = Decimal("100")
CURRENCY_QUANTIZER = Decimal("0.01")
PER_SHARE_QUANTIZER = Decimal("0.0001")


@dataclass(frozen=True)
class ShareEvent:
    """Normalized representation of a share-affecting event."""

    symbol: str
    date: dt.date
    quantity: int
    purchase_price_per_share: Decimal
    sale_price_per_share: Decimal
    additional_credit_per_share: Decimal
    premium_per_share: Decimal
    fee_per_share: Decimal
    option_type: str
    strike_price: Decimal
    expiration: dt.date
    assignment_kind: Optional[str]
    source_transaction_id: Optional[int]
    sequence: int


@dataclass
class LotPosition:
    """Tracked open lot state during FIFO reconciliation."""

    symbol: str
    direction: str
    opened_at: dt.date
    remaining_quantity: int
    cost_per_share: Decimal
    credit_per_share: Decimal
    premium_per_share: Decimal
    fee_per_share: Decimal
    option_type: str
    strike_price: Decimal
    expiration: dt.date
    assignment_kind: Optional[str]
    source_transaction_id: Optional[int]


@dataclass
class _AssignmentAccumulator:
    """Mutable accumulator for merging assignment-driven records."""

    symbol: str
    opened_at: dt.date
    direction: str
    option_type: str
    strike_price: Decimal
    expiration: dt.date
    share_quantity: int
    share_price_total: Decimal
    open_premium_total: Decimal
    open_fee_total: Decimal
    net_credit_total: Decimal
    assignment_kind: str
    source_transaction_id: int


def rebuild_stock_lots(
    repository: SQLiteRepository,
    *,
    account_name: str,
    account_number: Optional[str],
) -> None:
    """Rebuild stock lots by combining assignments with stored equity trades."""

    transactions = fetch_normalized_transactions(
        repository,
        account_name=account_name,
        account_number=account_number,
    )
    matched_legs = match_legs_from_transactions(transactions)
    assignment_records: List[AssignmentStockLotRecord] = []
    for leg in matched_legs:
        if leg.lots:
            assignment_records.extend(_build_assignment_records_from_leg(leg))

    stock_transactions = repository.fetch_stock_transactions(
        account_name=account_name,
        account_number=account_number,
    )

    events = _build_share_events(assignment_records, stock_transactions)
    records = _build_stock_lot_records(events)

    repository.replace_stock_lots(
        account_name=account_name,
        account_number=account_number,
        records=records,
    )


def rebuild_assignment_stock_lots(
    repository: SQLiteRepository,
    *,
    account_name: str,
    account_number: Optional[str],
) -> None:
    """Backward compatible wrapper for legacy callers."""

    rebuild_stock_lots(
        repository,
        account_name=account_name,
        account_number=account_number,
    )


def _build_share_events(
    assignments: Sequence[AssignmentStockLotRecord],
    trades: Sequence[StoredStockTransaction],
) -> List[ShareEvent]:
    events: List[ShareEvent] = []
    assignment_events = _assignment_events(assignments)
    events.extend(assignment_events)
    assignment_keys = {(event.symbol, event.date, event.quantity) for event in assignment_events}
    assignment_prices: Dict[Tuple[str, dt.date, int], Decimal] = {
        (event.symbol, event.date, event.quantity): (
            event.purchase_price_per_share if event.quantity > 0 else event.sale_price_per_share
        )
        for event in assignment_events
    }
    events.extend(_stock_trade_events(trades, assignment_keys, assignment_prices))
    events.sort(key=lambda event: (event.date, event.sequence, event.symbol))
    return events


def _build_stock_lot_records(events: Sequence[ShareEvent]) -> List[StockLotRecord]:
    long_lots: dict[str, Deque[LotPosition]] = defaultdict(deque)
    short_lots: dict[str, Deque[LotPosition]] = defaultdict(deque)
    records: List[StockLotRecord] = []

    for event in events:
        symbol = event.symbol
        if event.quantity > 0:
            quantity_remaining = event.quantity
            queue = short_lots.get(symbol)
            while quantity_remaining > 0 and queue:
                lot = queue[0]
                match_qty = min(quantity_remaining, lot.remaining_quantity)
                records.append(_close_short_lot(lot, event, match_qty))
                lot.remaining_quantity -= match_qty
                quantity_remaining -= match_qty
                if lot.remaining_quantity == 0:
                    queue.popleft()
                    if not queue:
                        short_lots.pop(symbol, None)
                        queue = None
            if quantity_remaining > 0:
                new_lot = _create_long_lot(event, quantity_remaining)
                if new_lot is not None:
                    long_lots[symbol].append(new_lot)
        elif event.quantity < 0:
            quantity_remaining = -event.quantity
            queue = long_lots.get(symbol)
            while quantity_remaining > 0 and queue:
                lot = queue[0]
                match_qty = min(quantity_remaining, lot.remaining_quantity)
                records.append(_close_long_lot(lot, event, match_qty))
                lot.remaining_quantity -= match_qty
                quantity_remaining -= match_qty
                if lot.remaining_quantity == 0:
                    queue.popleft()
                    if not queue:
                        long_lots.pop(symbol, None)
                        queue = None
            if quantity_remaining > 0:
                new_short = _create_short_lot(event, quantity_remaining)
                if new_short is not None:
                    short_lots[symbol].append(new_short)

    for queue in long_lots.values():
        for lot in queue:
            if lot.remaining_quantity:
                records.append(_lot_to_open_record(lot))
    for queue in short_lots.values():
        for lot in queue:
            if lot.remaining_quantity:
                records.append(_lot_to_open_record(lot))

    records.sort(
        key=lambda record: (
            record.opened_at,
            record.closed_at or record.opened_at,
            record.symbol,
            record.direction,
            record.status,
        )
    )
    return records


def _assignment_events(
    records: Sequence[AssignmentStockLotRecord],
) -> List[ShareEvent]:
    events: List[ShareEvent] = []
    for record in records:
        quantity = record.share_quantity
        if quantity == 0:
            continue

        share_count = abs(quantity)
        fee_per_share = _per_share(
            (record.open_fee_total / Decimal(share_count)) if share_count else Decimal("0")
        )
        premium_per_share = record.open_premium_per_share.quantize(
            PER_SHARE_QUANTIZER, rounding=ROUND_HALF_UP
        )
        additional_credit_per_share = record.net_credit_per_share.quantize(
            PER_SHARE_QUANTIZER, rounding=ROUND_HALF_UP
        )
        strike_price = record.share_price_per_share.quantize(
            PER_SHARE_QUANTIZER, rounding=ROUND_HALF_UP
        )

        if quantity > 0:
            events.append(
                ShareEvent(
                    symbol=record.symbol,
                    date=record.opened_at,
                    quantity=quantity,
                    purchase_price_per_share=strike_price,
                    sale_price_per_share=Decimal("0"),
                    additional_credit_per_share=additional_credit_per_share,
                    premium_per_share=premium_per_share,
                    fee_per_share=fee_per_share,
                    option_type=record.option_type,
                    strike_price=record.strike_price,
                    expiration=record.expiration,
                    assignment_kind=record.assignment_kind,
                    source_transaction_id=record.source_transaction_id,
                    sequence=record.source_transaction_id or 0,
                )
            )
        else:
            events.append(
                ShareEvent(
                    symbol=record.symbol,
                    date=record.opened_at,
                    quantity=quantity,
                    purchase_price_per_share=Decimal("0"),
                    sale_price_per_share=strike_price,
                    additional_credit_per_share=additional_credit_per_share,
                    premium_per_share=premium_per_share,
                    fee_per_share=fee_per_share,
                    option_type=record.option_type,
                    strike_price=record.strike_price,
                    expiration=record.expiration,
                    assignment_kind=record.assignment_kind,
                    source_transaction_id=record.source_transaction_id,
                    sequence=record.source_transaction_id or 0,
                )
            )
    return events


def _stock_trade_events(
    transactions: Sequence[StoredStockTransaction],
    assignment_keys: Optional[Set[Tuple[str, dt.date, int]]] = None,
    assignment_prices: Optional[Mapping[Tuple[str, dt.date, int], Decimal]] = None,
) -> List[ShareEvent]:
    events: List[ShareEvent] = []
    key_counts: Dict[Tuple[str, dt.date, int], int] = {}
    for txn in transactions:
        quantity = _safe_int_quantity(txn.quantity)
        if quantity is None or quantity == 0:
            continue

        symbol = (txn.instrument or "").strip().upper()
        if not symbol:
            continue

        activity_date = dt.date.fromisoformat(txn.activity_date)
        action = (txn.action or "").strip().upper()
        event_quantity = quantity if action == "BUY" else -quantity
        key = (symbol, activity_date, event_quantity)
        key_counts[key] = key_counts.get(key, 0) + 1

    fractional_long: Dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    fractional_short: Dict[str, Decimal] = defaultdict(lambda: Decimal("0"))

    for txn in transactions:
        symbol = (txn.instrument or "").strip().upper()
        if not symbol:
            continue

        activity_date = dt.date.fromisoformat(txn.activity_date)
        price = Decimal(txn.price).quantize(PER_SHARE_QUANTIZER, rounding=ROUND_HALF_UP)
        sequence = 1_000_000 + txn.row_index
        action = (txn.action or "").strip().upper()
        quantity_decimal = Decimal(txn.quantity)
        if quantity_decimal == 0:
            continue

        integer_quantity = _safe_int_quantity(txn.quantity)
        if integer_quantity is not None and integer_quantity != 0:
            event_quantity_int = integer_quantity if action == "BUY" else -integer_quantity
            key = (symbol, activity_date, event_quantity_int)
            if assignment_keys and key in assignment_keys:
                if _looks_like_assignment_follow_on(txn) or _is_unique_assignment_match(
                    key, txn, key_counts, assignment_prices
                ):
                    continue

        quantity_abs = abs(quantity_decimal)
        whole_shares = int(quantity_abs // Decimal("1"))
        fractional_shares = quantity_abs - Decimal(whole_shares)

        if action == "BUY":
            if fractional_shares:
                short_fraction = fractional_short[symbol]
                if short_fraction:
                    coverage = min(fractional_shares, short_fraction)
                    short_fraction -= coverage
                    fractional_shares -= coverage
                    fractional_short[symbol] = short_fraction
                long_fraction = fractional_long[symbol] + fractional_shares
                extra_from_fraction = 0
                while long_fraction >= Decimal("1"):
                    extra_from_fraction += 1
                    long_fraction -= Decimal("1")
                fractional_long[symbol] = long_fraction
                whole_shares += extra_from_fraction

            if whole_shares <= 0:
                continue

            events.append(
                ShareEvent(
                    symbol=symbol,
                    date=activity_date,
                    quantity=whole_shares,
                    purchase_price_per_share=price,
                    sale_price_per_share=Decimal("0"),
                    additional_credit_per_share=Decimal("0"),
                    premium_per_share=Decimal("0"),
                    fee_per_share=Decimal("0"),
                    option_type="STOCK",
                    strike_price=price,
                    expiration=activity_date,
                    assignment_kind=None,
                    source_transaction_id=None,
                    sequence=sequence,
                )
            )
        elif action == "SELL":
            if fractional_shares:
                long_fraction = fractional_long[symbol]
                if long_fraction >= fractional_shares:
                    long_fraction -= fractional_shares
                else:
                    deficit = fractional_shares - long_fraction
                    long_fraction = Decimal("0")
                    fractional_short[symbol] += deficit
                fractional_long[symbol] = long_fraction

            short_fraction = fractional_short[symbol]
            extra_short_shares = 0
            while short_fraction >= Decimal("1"):
                extra_short_shares += 1
                short_fraction -= Decimal("1")
            fractional_short[symbol] = short_fraction
            whole_shares += extra_short_shares

            if whole_shares <= 0:
                continue

            events.append(
                ShareEvent(
                    symbol=symbol,
                    date=activity_date,
                    quantity=-whole_shares,
                    purchase_price_per_share=Decimal("0"),
                    sale_price_per_share=price,
                    additional_credit_per_share=Decimal("0"),
                    premium_per_share=Decimal("0"),
                    fee_per_share=Decimal("0"),
                    option_type="STOCK",
                    strike_price=price,
                    expiration=activity_date,
                    assignment_kind=None,
                    source_transaction_id=None,
                    sequence=sequence,
                )
            )
    return events


def _looks_like_assignment_follow_on(txn: StoredStockTransaction) -> bool:
    description = (txn.description or "").lower()
    if "option" in description and ("assigned" in description or "assignment" in description):
        return True

    try:
        import json

        raw = json.loads(txn.raw_json or "{}")
    except (TypeError, json.JSONDecodeError):
        return False

    raw_description = (raw.get("Description") or "").lower()
    return "option" in raw_description and (
        "assigned" in raw_description or "assignment" in raw_description
    )


def _is_unique_assignment_match(
    key: Tuple[str, dt.date, int],
    txn: StoredStockTransaction,
    key_counts: Mapping[Tuple[str, dt.date, int], int],
    assignment_prices: Optional[Mapping[Tuple[str, dt.date, int], Decimal]],
) -> bool:
    if assignment_prices is None:
        return False

    if key_counts.get(key, 0) != 1:
        return False

    expected_price = assignment_prices.get(key)
    if expected_price is None:
        return False

    try:
        trade_price = Decimal(txn.price).quantize(PER_SHARE_QUANTIZER, rounding=ROUND_HALF_UP)
        trade_amount = Decimal(txn.amount).quantize(CURRENCY_QUANTIZER, rounding=ROUND_HALF_UP)
    except (InvalidOperation, TypeError):
        return False

    if trade_price != expected_price:
        return False

    expected_total = _currency(expected_price * Decimal(abs(key[2])))
    if key[2] > 0:
        expected_total = -expected_total

    return trade_amount == expected_total


def _create_long_lot(event: ShareEvent, quantity: int) -> Optional[LotPosition]:
    if quantity <= 0:
        return None
    cost_per_share = event.purchase_price_per_share
    credit_per_share = event.additional_credit_per_share
    return LotPosition(
        symbol=event.symbol,
        direction="long",
        opened_at=event.date,
        remaining_quantity=quantity,
        cost_per_share=cost_per_share,
        credit_per_share=credit_per_share,
        premium_per_share=event.premium_per_share,
        fee_per_share=event.fee_per_share,
        option_type=event.option_type,
        strike_price=event.strike_price,
        expiration=event.expiration,
        assignment_kind=event.assignment_kind,
        source_transaction_id=event.source_transaction_id,
    )


def _create_short_lot(event: ShareEvent, quantity: int) -> Optional[LotPosition]:
    if quantity <= 0:
        return None
    credit_per_share = event.sale_price_per_share + event.additional_credit_per_share
    return LotPosition(
        symbol=event.symbol,
        direction="short",
        opened_at=event.date,
        remaining_quantity=quantity,
        cost_per_share=event.purchase_price_per_share,
        credit_per_share=credit_per_share,
        premium_per_share=event.premium_per_share,
        fee_per_share=event.fee_per_share,
        option_type=event.option_type,
        strike_price=event.strike_price,
        expiration=event.expiration,
        assignment_kind=event.assignment_kind,
        source_transaction_id=event.source_transaction_id,
    )


def _close_long_lot(lot: LotPosition, event: ShareEvent, quantity: int) -> StockLotRecord:
    share_price_total = _currency(lot.cost_per_share * quantity)
    share_price_per_share = _per_share(lot.cost_per_share)
    open_premium_total = _currency(lot.premium_per_share * quantity)
    open_fee_total = _currency(lot.fee_per_share * quantity)

    realized_per_share_raw = (
        lot.credit_per_share
        + event.sale_price_per_share
        + event.additional_credit_per_share
        - lot.cost_per_share
        - event.fee_per_share
    )
    net_credit_per_share = _per_share(realized_per_share_raw)
    net_credit_total = _currency(realized_per_share_raw * quantity)

    return StockLotRecord(
        symbol=lot.symbol,
        opened_at=lot.opened_at,
        closed_at=event.date,
        share_quantity=quantity,
        direction="long",
        option_type=lot.option_type,
        strike_price=lot.strike_price,
        expiration=lot.expiration,
        share_price_total=share_price_total,
        share_price_per_share=share_price_per_share,
        open_premium_total=open_premium_total,
        open_premium_per_share=_per_share(lot.premium_per_share),
        open_fee_total=open_fee_total,
        net_credit_total=net_credit_total,
        net_credit_per_share=net_credit_per_share,
        assignment_kind=lot.assignment_kind,
        source_transaction_id=lot.source_transaction_id,
        status="closed",
    )


def _close_short_lot(lot: LotPosition, event: ShareEvent, quantity: int) -> StockLotRecord:
    share_price_total = _currency(event.purchase_price_per_share * quantity)
    share_price_per_share = _per_share(event.purchase_price_per_share)
    open_premium_total = _currency(lot.premium_per_share * quantity)
    open_fee_total = _currency(lot.fee_per_share * quantity)

    realized_per_share_raw = (
        lot.credit_per_share
        + event.sale_price_per_share
        + event.additional_credit_per_share
        - event.purchase_price_per_share
        - lot.cost_per_share
        - event.fee_per_share
    )
    net_credit_per_share = _per_share(realized_per_share_raw)
    net_credit_total = _currency(realized_per_share_raw * quantity)

    return StockLotRecord(
        symbol=lot.symbol,
        opened_at=lot.opened_at,
        closed_at=event.date,
        share_quantity=-quantity,
        direction="short",
        option_type=lot.option_type,
        strike_price=lot.strike_price,
        expiration=lot.expiration,
        share_price_total=share_price_total,
        share_price_per_share=share_price_per_share,
        open_premium_total=open_premium_total,
        open_premium_per_share=_per_share(lot.premium_per_share),
        open_fee_total=open_fee_total,
        net_credit_total=net_credit_total,
        net_credit_per_share=net_credit_per_share,
        assignment_kind=lot.assignment_kind,
        source_transaction_id=lot.source_transaction_id,
        status="closed",
    )


def _lot_to_open_record(lot: LotPosition) -> StockLotRecord:
    quantity = lot.remaining_quantity
    share_quantity = quantity if lot.direction == "long" else -quantity

    share_price_total = _currency(lot.cost_per_share * quantity)
    share_price_per_share = _per_share(lot.cost_per_share)
    net_credit_total = _currency(lot.credit_per_share * quantity)
    net_credit_per_share = _per_share(lot.credit_per_share)
    open_premium_total = _currency(lot.premium_per_share * quantity)
    open_fee_total = _currency(lot.fee_per_share * quantity)

    return StockLotRecord(
        symbol=lot.symbol,
        opened_at=lot.opened_at,
        closed_at=None,
        share_quantity=share_quantity,
        direction=lot.direction,
        option_type=lot.option_type,
        strike_price=lot.strike_price,
        expiration=lot.expiration,
        share_price_total=share_price_total,
        share_price_per_share=share_price_per_share,
        open_premium_total=open_premium_total,
        open_premium_per_share=_per_share(lot.premium_per_share),
        open_fee_total=open_fee_total,
        net_credit_total=net_credit_total,
        net_credit_per_share=net_credit_per_share,
        assignment_kind=lot.assignment_kind,
        source_transaction_id=lot.source_transaction_id,
        status="open",
    )


def _build_assignment_records_from_leg(leg: MatchedLeg) -> List[AssignmentStockLotRecord]:
    records: List[AssignmentStockLotRecord] = []
    for lot in leg.lots:
        if not lot.close_portions:
            continue
        for close_portion in lot.close_portions:
            if not close_portion.fill.is_assignment:
                continue
            maybe_record = _lot_to_assignment_record(lot, close_portion)
            if maybe_record:
                records.append(maybe_record)
    return _merge_assignment_records(records)


def _lot_to_assignment_record(
    lot: MatchedLegLot,
    assignment_portion,
) -> Optional[AssignmentStockLotRecord]:
    option_type = lot.contract.option_type.upper()
    if option_type not in {"CALL", "PUT"}:
        return None

    if lot.direction != "short":
        return None

    portion_contracts = Decimal(assignment_portion.quantity)
    share_count = portion_contracts * SHARES_PER_CONTRACT
    if share_count <= 0:
        return None

    raw_txn = assignment_portion.fill.transaction.raw or {}
    source_transaction_id = raw_txn.get("__transaction_id")
    if source_transaction_id is None:
        return None

    strike_price = lot.contract.strike
    share_price_total = strike_price * share_count

    lot_contracts = Decimal(lot.quantity)
    if lot_contracts <= 0:
        return None
    lot_total_shares = lot_contracts * SHARES_PER_CONTRACT
    if lot_total_shares == 0:
        return None
    ratio = portion_contracts / lot_contracts

    open_premium_total = (lot.open_premium * ratio).quantize(
        CURRENCY_QUANTIZER, rounding=ROUND_HALF_UP
    )
    open_fee_total = (lot.open_fees * ratio).quantize(CURRENCY_QUANTIZER, rounding=ROUND_HALF_UP)
    net_credit_total = ((lot.open_credit_net or Decimal("0")) * ratio).quantize(
        CURRENCY_QUANTIZER, rounding=ROUND_HALF_UP
    )

    open_premium_per_share = _per_share(open_premium_total / share_count)
    net_credit_per_share = _per_share(net_credit_total / share_count)

    if option_type == "PUT":
        share_quantity = int(share_count)
        direction = "long"
        assignment_kind = "put_assignment"
    else:
        share_quantity = -int(share_count)
        direction = "short"
        assignment_kind = "call_assignment"

    opened_at = assignment_portion.activity_date

    return AssignmentStockLotRecord(
        symbol=lot.contract.symbol,
        opened_at=opened_at,
        share_quantity=share_quantity,
        direction=direction,
        option_type=option_type,
        strike_price=strike_price,
        expiration=lot.contract.expiration,
        share_price_total=share_price_total,
        share_price_per_share=strike_price,
        open_premium_total=open_premium_total,
        open_premium_per_share=open_premium_per_share,
        open_fee_total=open_fee_total,
        net_credit_total=net_credit_total,
        net_credit_per_share=net_credit_per_share,
        assignment_kind=assignment_kind,
        source_transaction_id=int(source_transaction_id),
    )


def _per_share(value: Decimal) -> Decimal:
    return value.quantize(PER_SHARE_QUANTIZER, rounding=ROUND_HALF_UP)


def _currency(value: Decimal) -> Decimal:
    return value.quantize(CURRENCY_QUANTIZER, rounding=ROUND_HALF_UP)


def _safe_int_quantity(quantity: Decimal) -> Optional[int]:
    if quantity == 0:
        return None
    if quantity != quantity.to_integral_value():
        return None
    return int(quantity)


def _merge_assignment_records(
    records: Sequence[AssignmentStockLotRecord],
) -> List[AssignmentStockLotRecord]:
    merged: dict[tuple[str, dt.date, str, str, Decimal], _AssignmentAccumulator] = {}
    for record in records:
        key = (
            record.symbol,
            record.opened_at,
            record.direction,
            record.assignment_kind,
            record.share_price_per_share,
        )
        bucket = merged.get(key)
        if bucket is None:
            merged[key] = _AssignmentAccumulator(
                symbol=record.symbol,
                opened_at=record.opened_at,
                direction=record.direction,
                option_type=record.option_type,
                strike_price=record.strike_price,
                expiration=record.expiration,
                share_quantity=record.share_quantity,
                share_price_total=record.share_price_total,
                open_premium_total=record.open_premium_total,
                open_fee_total=record.open_fee_total,
                net_credit_total=record.net_credit_total,
                assignment_kind=record.assignment_kind,
                source_transaction_id=record.source_transaction_id,
            )
        else:
            bucket.share_quantity += record.share_quantity
            bucket.share_price_total += record.share_price_total
            bucket.open_premium_total += record.open_premium_total
            bucket.open_fee_total += record.open_fee_total
            bucket.net_credit_total += record.net_credit_total

    merged_records: List[AssignmentStockLotRecord] = []
    for acc in merged.values():
        share_quantity = int(acc.share_quantity)
        share_count = abs(share_quantity)
        if share_count == 0:
            continue
        share_count_decimal = Decimal(share_count)
        share_price_total = _currency(acc.share_price_total)
        open_premium_total = _currency(acc.open_premium_total)
        open_fee_total = _currency(acc.open_fee_total)
        net_credit_total = _currency(acc.net_credit_total)

        share_price_per_share = _per_share(share_price_total / share_count_decimal)
        open_premium_per_share = _per_share(open_premium_total / share_count_decimal)
        net_credit_per_share = _per_share(net_credit_total / share_count_decimal)

        merged_records.append(
            AssignmentStockLotRecord(
                symbol=acc.symbol,
                opened_at=acc.opened_at,
                share_quantity=share_quantity,
                direction=acc.direction,
                option_type=acc.option_type,
                strike_price=acc.strike_price,
                expiration=acc.expiration,
                share_price_total=share_price_total,
                share_price_per_share=share_price_per_share,
                open_premium_total=open_premium_total,
                open_premium_per_share=open_premium_per_share,
                open_fee_total=open_fee_total,
                net_credit_total=net_credit_total,
                net_credit_per_share=net_credit_per_share,
                assignment_kind=acc.assignment_kind,
                source_transaction_id=acc.source_transaction_id,
            )
        )

    return merged_records
