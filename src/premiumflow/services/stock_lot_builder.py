"""Services for deriving and persisting stock lots created by assignments."""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal
from typing import List, Optional

from ..persistence import AssignmentStockLotRecord, SQLiteRepository
from .leg_matching import MatchedLeg, MatchedLegLot
from .transaction_loader import fetch_normalized_transactions, match_legs_from_transactions

SHARES_PER_CONTRACT = Decimal("100")


def rebuild_assignment_stock_lots(
    repository: SQLiteRepository,
    *,
    account_name: str,
    account_number: Optional[str],
) -> None:
    """Rebuild assignment-driven stock lots for the specified account."""

    transactions = fetch_normalized_transactions(
        repository,
        account_name=account_name,
        account_number=account_number,
    )
    matched_legs = match_legs_from_transactions(transactions)
    records: List[AssignmentStockLotRecord] = []
    for leg in matched_legs:
        if leg.lots:
            records.extend(_build_assignment_records_from_leg(leg))

    repository.replace_assignment_stock_lots(
        account_name=account_name,
        account_number=account_number,
        records=records,
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
    return records


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

    lot_contracts = Decimal(lot.quantity) if lot.quantity else Decimal("1")
    ratio = (portion_contracts / lot_contracts).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    open_premium_total = (lot.open_premium * ratio).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )
    open_fee_total = (lot.open_fees * ratio).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    net_credit_total = ((lot.open_credit_net or Decimal("0")) * ratio).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )

    open_premium_per_share = _per_share(open_premium_total, share_count)
    net_credit_per_share = _per_share(net_credit_total, share_count)

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


def _per_share(value: Decimal, share_count: Decimal) -> Decimal:
    if share_count == 0:
        return Decimal("0")
    return (value / share_count).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
