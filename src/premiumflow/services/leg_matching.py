"""FIFO leg matching service.

Transforms :class:`~premiumflow.core.legs.LegFill` sequences into matched lots that pair opening
and closing fills per contract. Provides summaries that downstream layers (CLI, web) can consume
without reimplementing the matching algorithm.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Deque, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

from ..core.legs import LegContract, LegFill, build_leg_fills
from ..core.parser import NormalizedOptionTransaction
from ..persistence import StoredTransaction

Money = Decimal
LegKey = Tuple[str, Optional[str], str]  # (account_name, account_number, leg_id)


def _quantize(value: Decimal | int | float) -> Decimal:
    """Normalise monetary values to cents while preserving sign."""
    if not isinstance(value, Decimal):
        value = Decimal(value)
    return value.quantize(Decimal("0.01"))


@dataclass(frozen=True)
class LotFillPortion:
    """Represents a quantity slice of a :class:`LegFill`."""

    fill: LegFill
    quantity: int
    premium: Money
    fees: Money

    @property
    def activity_date(self) -> date:
        return self.fill.activity_date

    def split(self, quantity: int) -> Tuple["LotFillPortion", Optional["LotFillPortion"]]:
        """Split this portion into two, returning the requested quantity and the remainder."""
        if quantity <= 0 or quantity > self.quantity:
            raise ValueError("split quantity must be between 1 and the existing quantity")

        ratio = Decimal(quantity) / Decimal(self.quantity)
        first = LotFillPortion(
            fill=self.fill,
            quantity=quantity,
            premium=_quantize(self.premium * ratio),
            fees=_quantize(self.fees * ratio),
        )

        remaining_qty = self.quantity - quantity
        if remaining_qty == 0:
            return first, None

        remainder = LotFillPortion(
            fill=self.fill,
            quantity=remaining_qty,
            premium=_quantize(self.premium - first.premium),
            fees=_quantize(self.fees - first.fees),
        )
        return first, remainder


def _portion_from_fill(fill: LegFill, quantity: int) -> LotFillPortion:
    """Create a portion representing ``quantity`` contracts from a fill."""
    if quantity <= 0 or quantity > fill.quantity:
        raise ValueError("quantity must be between 1 and the fill quantity")

    ratio = Decimal(quantity) / Decimal(fill.quantity)
    # Use gross_notional (price * quantity * 100) apportioned by ratio; sign by trans code
    gross_premium = fill.gross_notional * ratio
    if fill.trans_code in {"STO", "STC"}:
        premium = _quantize(gross_premium)  # credits are positive
    else:
        premium = _quantize(-gross_premium)  # debits are negative
    fees = _quantize(fill.fees * ratio)
    return LotFillPortion(fill=fill, quantity=quantity, premium=premium, fees=fees)


@dataclass(frozen=True)
class MatchedLegLot:
    """A FIFO-matched lot derived from opening and closing fills."""

    contract: LegContract
    account_name: str
    account_number: Optional[str]
    direction: str  # "long" or "short"
    quantity: int
    open_portions: Tuple[LotFillPortion, ...]
    close_portions: Tuple[LotFillPortion, ...]
    opened_at: date
    closed_at: Optional[date]
    status: str  # "open" or "closed"
    open_premium: Money
    close_premium: Money
    total_fees: Money
    realized_premium: Optional[Money]

    @property
    def is_open(self) -> bool:
        return self.status == "open"

    @property
    def is_closed(self) -> bool:
        return self.status == "closed"

    @property
    def open_fees(self) -> Money:
        """Total fees paid when opening this lot (sum of fees from open portions)."""
        return _quantize(sum(p.fees for p in self.open_portions))

    @property
    def close_fees(self) -> Money:
        """Total fees paid when closing this lot (sum of fees from close portions)."""
        return _quantize(sum(p.fees for p in self.close_portions))

    @property
    def open_credit_gross(self) -> Money:
        """Gross credit received when opening this lot (before fees)."""
        return self.open_premium

    @property
    def open_credit_net(self) -> Money:
        """Net credit received when opening this lot (gross credit minus opening fees)."""
        return _quantize(self.open_premium - self.open_fees)

    @property
    def close_cost(self) -> Money:
        """
        Cost paid to close this lot (before fees).

        Returns the absolute value of close_premium when closing is a debit (cost to close).
        Returns 0 if the lot is still open or if closing was a credit.
        """
        if not self.is_closed or self.close_premium >= 0:
            return _quantize(0)
        return _quantize(abs(self.close_premium))

    @property
    def close_cost_total(self) -> Money:
        """
        Total cost paid to close this lot (cost plus closing fees).

        Returns the sum of close_cost and close_fees when closing is a debit.
        Returns 0 if the lot is still open or if closing was a credit.
        """
        if not self.is_closed or self.close_premium >= 0:
            return _quantize(0)
        return _quantize(self.close_cost + self.close_fees)

    @property
    def close_quantity(self) -> int:
        """Quantity of contracts closed in this lot. Returns 0 if lot is still open."""
        return self.quantity if self.is_closed else 0

    @property
    def credit_remaining(self) -> Money:
        """
        Remaining potential credit for open lots.

        Returns open_premium for open lots (the credit that could be retained if expired).
        Returns 0 for closed lots.
        """
        return _quantize(self.open_premium) if self.is_open else _quantize(0)

    @property
    def quantity_remaining(self) -> int:
        """Remaining quantity of open contracts in this lot. Returns 0 if lot is fully closed."""
        return self.quantity if self.is_open else 0

    @property
    def net_premium(self) -> Optional[Money]:
        """
        Net profit/loss for this lot after all fees (realized_premium - total_fees).

        This represents the overall P/L for a closed lot, accounting for both opening and closing
        fees. Returns None for open lots (since realized_premium is None).

        Note: This differs from open_credit_net, which only considers the opening transaction.
        net_premium is the complete trade P/L: (open_premium + close_premium) - (open_fees + close_fees).
        """
        if self.realized_premium is None:
            return None
        return _quantize(self.realized_premium - self.total_fees)


@dataclass(frozen=True)
class MatchedLeg:
    """Collection of FIFO lots for a single contract/account combination."""

    contract: LegContract
    account_name: str
    account_number: Optional[str]
    lots: Tuple[MatchedLegLot, ...]
    net_contracts: int
    open_quantity: int
    realized_premium: Money
    open_premium: Money
    total_fees: Money

    @property
    def days_to_expiration(self) -> int:
        return self.contract.days_to_expiration()

    @property
    def is_open(self) -> bool:
        return self.open_quantity != 0

    @property
    def opened_at(self) -> Optional[date]:
        """Earliest date any lot in this leg was opened."""
        if not self.lots:
            return None
        return min(lot.opened_at for lot in self.lots)

    @property
    def closed_at(self) -> Optional[date]:
        """Latest date any lot in this leg was closed. Returns None if leg is fully open."""
        closed_dates = [lot.closed_at for lot in self.lots if lot.closed_at is not None]
        if not closed_dates:
            return None
        return max(closed_dates)

    @property
    def opened_quantity(self) -> int:
        """Total quantity of contracts opened across all lots (including those later closed)."""
        return sum(lot.quantity for lot in self.lots)

    @property
    def closed_quantity(self) -> int:
        """Total quantity of contracts closed across all lots."""
        return sum(lot.close_quantity for lot in self.lots)

    @property
    def open_credit_gross(self) -> Money:
        """Total gross credit received when opening all lots (before fees)."""
        return _quantize(sum(lot.open_credit_gross for lot in self.lots))

    @property
    def close_cost(self) -> Money:
        """Total cost paid to close all lots (before fees)."""
        return _quantize(sum(lot.close_cost for lot in self.lots))

    @property
    def open_fees(self) -> Money:
        """Total fees paid when opening all lots."""
        return _quantize(sum(lot.open_fees for lot in self.lots))

    @property
    def close_fees(self) -> Money:
        """Total fees paid when closing all lots."""
        return _quantize(sum(lot.close_fees for lot in self.lots))

    def resolution(self) -> Optional[str]:
        """
        Return the transaction code for the final method of closure.

        Returns the transaction code (e.g., "BTC", "STC", "OASGN", "OEXP") from the chronologically
        final closing transaction. Assignment and expiration are prioritized over BTC/STC when they
        occur on the same date, as they represent the final resolution by definition.

        Returns None for open legs or when no lots are closed.

        Display formatting should be handled in the CLI/formatter layer.
        """
        # Return None if leg is still open (even if partially closed) or no lots were ever closed
        if self.is_open or not self.closed_quantity:
            return None

        closed_lots = [lot for lot in self.lots if lot.is_closed]
        if not closed_lots:
            return None

        # Collect all closing portions with their sort keys and dates
        all_portions: List[Tuple[LotFillPortion, Tuple[date, date, date, int], date]] = []
        for lot in closed_lots:
            for portion in lot.close_portions:
                sort_key = portion.fill.sort_key()
                activity_date = portion.activity_date
                all_portions.append((portion, sort_key, activity_date))

        if not all_portions:
            return None

        # Find the latest activity date
        latest_date = max(activity_date for _, _, activity_date in all_portions)

        # Among portions with the latest date, prioritize assignment/expiration over BTC/STC
        latest_date_portions = [
            (portion, sort_key)
            for portion, sort_key, activity_date in all_portions
            if activity_date == latest_date
        ]

        # Check for assignment/expiration first (they are final by definition)
        assignment_or_expiration = [
            (portion, sort_key)
            for portion, sort_key in latest_date_portions
            if portion.fill.is_assignment or portion.fill.is_expiration
        ]

        if assignment_or_expiration:
            # Among assignment/expiration, use the latest sort_key
            latest_portion, _ = max(assignment_or_expiration, key=lambda x: x[1])
            return latest_portion.fill.trans_code

        # Otherwise, use the latest sort_key among all latest-date portions
        latest_portion, _ = max(latest_date_portions, key=lambda x: x[1])
        return latest_portion.fill.trans_code


class _LotBuilder:
    """Mutable builder that accumulates open and closing portions before finalising."""

    def __init__(
        self,
        *,
        contract: LegContract,
        account_name: str,
        account_number: Optional[str],
        direction: str,
        open_portions: List[LotFillPortion],
    ) -> None:
        self.contract = contract
        self.account_name = account_name
        self.account_number = account_number
        self.direction = direction
        self.open_portions = open_portions
        self.close_portions: List[LotFillPortion] = []

    @property
    def quantity(self) -> int:
        return sum(portion.quantity for portion in self.open_portions)

    def split(self, quantity: int) -> Tuple["_LotBuilder", Optional["_LotBuilder"]]:
        """Split this builder into two: one with ``quantity`` contracts, the other the remainder."""
        if quantity <= 0 or quantity > self.quantity:
            raise ValueError("split quantity must be between 1 and the current quantity")

        remaining = quantity
        matched_portions: List[LotFillPortion] = []
        updated_portions: List[LotFillPortion] = []

        for portion in self.open_portions:
            if remaining == 0:
                updated_portions.append(portion)
                continue

            take = min(portion.quantity, remaining)
            head, tail = portion.split(take)
            matched_portions.append(head)
            if tail is not None:
                updated_portions.append(tail)
            remaining -= take

        matched_builder = _LotBuilder(
            contract=self.contract,
            account_name=self.account_name,
            account_number=self.account_number,
            direction=self.direction,
            open_portions=matched_portions,
        )

        if not updated_portions:
            return matched_builder, None

        remainder_builder = _LotBuilder(
            contract=self.contract,
            account_name=self.account_name,
            account_number=self.account_number,
            direction=self.direction,
            open_portions=updated_portions,
        )
        return matched_builder, remainder_builder

    def add_close_portion(self, portion: LotFillPortion) -> None:
        self.close_portions.append(portion)

    def to_lot(self, *, status: str) -> MatchedLegLot:
        open_premium = sum(portion.premium for portion in self.open_portions)
        close_premium = sum(portion.premium for portion in self.close_portions)
        total_fees = sum(portion.fees for portion in (*self.open_portions, *self.close_portions))
        opened_at = min(portion.activity_date for portion in self.open_portions)
        closed_at = (
            max(portion.activity_date for portion in self.close_portions)
            if self.close_portions
            else None
        )
        realized = None
        if status == "closed":
            realized = _quantize(open_premium + close_premium)
        return MatchedLegLot(
            contract=self.contract,
            account_name=self.account_name,
            account_number=self.account_number,
            direction=self.direction,
            quantity=self.quantity,
            open_portions=tuple(self.open_portions),
            close_portions=tuple(self.close_portions),
            opened_at=opened_at,
            closed_at=closed_at,
            status=status,
            open_premium=_quantize(open_premium),
            close_premium=_quantize(close_premium),
            total_fees=_quantize(total_fees),
            realized_premium=realized,
        )


def _group_leg_fills(fills: Iterable[LegFill]) -> Dict[LegKey, List[LegFill]]:
    grouped: Dict[LegKey, List[LegFill]] = {}
    for fill in fills:
        key = (fill.account_name, fill.account_number, fill.contract.leg_id)
        grouped.setdefault(key, []).append(fill)

    for bucket in grouped.values():
        bucket.sort(key=lambda item: item.sort_key())
    return grouped


def _direction_for_fill(fill: LegFill) -> str:
    return "long" if fill.signed_quantity > 0 else "short"


def _queue_for_direction(
    directions: Dict[str, Deque[_LotBuilder]], direction: str
) -> Deque[_LotBuilder]:
    if direction not in directions:
        directions[direction] = deque()
    return directions[direction]


def _consume_closing_fill(
    builder_queue: Deque[_LotBuilder],
    closing_portion: LotFillPortion,
) -> Iterator[MatchedLegLot]:
    remaining_portion: Optional[LotFillPortion] = closing_portion
    while remaining_portion is not None:
        if not builder_queue:
            raise ValueError("Encountered closing fill without a corresponding open position.")
        builder = builder_queue.popleft()
        qty_to_close = min(builder.quantity, remaining_portion.quantity)
        matched_builder, remainder_builder = builder.split(qty_to_close)

        used_close, leftover_close = remaining_portion.split(qty_to_close)
        matched_builder.add_close_portion(used_close)
        yield matched_builder.to_lot(status="closed")

        if remainder_builder is not None:
            builder_queue.appendleft(remainder_builder)
        remaining_portion = leftover_close


def match_leg_fills(fills: Sequence[LegFill]) -> MatchedLeg:
    """Return FIFO-matched lots for a single contract/account combination."""
    if not fills:
        raise ValueError("match_leg_fills requires at least one LegFill.")

    contract = fills[0].contract
    account_name = fills[0].account_name
    account_number = fills[0].account_number

    directions: Dict[str, Deque[_LotBuilder]] = {}
    matched_lots: List[MatchedLegLot] = []

    for fill in sorted(fills, key=lambda item: item.sort_key()):
        base_portion = _portion_from_fill(fill, fill.quantity)
        direction = _direction_for_fill(fill)

        if fill.is_opening and not fill.is_closing:
            queue = _queue_for_direction(directions, direction)
            queue.append(
                _LotBuilder(
                    contract=contract,
                    account_name=account_name,
                    account_number=account_number,
                    direction=direction,
                    open_portions=[base_portion],
                )
            )
            continue

        if fill.is_closing and not fill.is_opening:
            target_direction = "short" if fill.signed_quantity > 0 else "long"
            queue = _queue_for_direction(directions, target_direction)
            matched_lots.extend(_consume_closing_fill(queue, base_portion))
            continue

        # Defensive fallback: treat ambiguous fills as closing according to signed quantity.
        queue = _queue_for_direction(directions, "short" if direction == "long" else "long")
        matched_lots.extend(_consume_closing_fill(queue, base_portion))

    # Remaining open builders become open lots.
    for queue in directions.values():
        while queue:
            builder = queue.popleft()
            matched_lots.append(builder.to_lot(status="open"))

    lots_tuple = tuple(matched_lots)
    net_contracts = sum(
        lot.quantity if lot.direction == "long" else -lot.quantity
        for lot in lots_tuple
        if lot.is_open
    )
    open_quantity = sum(lot.quantity for lot in lots_tuple if lot.is_open)
    realized_premium = _quantize(
        sum(
            lot.realized_premium or Decimal("0")
            for lot in lots_tuple
            if lot.realized_premium is not None
        )
    )
    open_premium = _quantize(sum(lot.open_premium for lot in lots_tuple if lot.is_open))
    total_fees = _quantize(sum(lot.total_fees for lot in lots_tuple))

    return MatchedLeg(
        contract=contract,
        account_name=account_name,
        account_number=account_number,
        lots=lots_tuple,
        net_contracts=net_contracts,
        open_quantity=open_quantity,
        realized_premium=realized_premium,
        open_premium=open_premium,
        total_fees=total_fees,
    )


def match_legs(fills: Iterable[LegFill]) -> Dict[LegKey, MatchedLeg]:
    """Group fills by leg and return FIFO-matched results for each grouping."""
    results: Dict[LegKey, MatchedLeg] = {}
    for key, bucket in _group_leg_fills(fills).items():
        results[key] = match_leg_fills(bucket)
    return results


def _stored_to_normalized(stored: StoredTransaction) -> NormalizedOptionTransaction:
    """Convert a StoredTransaction to a NormalizedOptionTransaction."""
    import json

    raw_dict = json.loads(stored.raw_json)
    # Preserve account metadata in raw dict for group_fills_by_account to extract
    raw_dict["Account Name"] = stored.account_name
    if stored.account_number:
        raw_dict["Account Number"] = stored.account_number

    return NormalizedOptionTransaction(
        activity_date=date.fromisoformat(stored.activity_date),
        process_date=date.fromisoformat(stored.process_date) if stored.process_date else None,
        settle_date=date.fromisoformat(stored.settle_date) if stored.settle_date else None,
        instrument=stored.instrument,
        description=stored.description,
        trans_code=stored.trans_code,
        quantity=stored.quantity,
        price=Decimal(stored.price),
        amount=Decimal(stored.amount) if stored.amount else None,
        strike=Decimal(stored.strike),
        option_type=stored.option_type,
        expiration=date.fromisoformat(stored.expiration),
        action=stored.action,
        raw=raw_dict,
    )


def group_fills_by_account(
    transactions: Iterable[NormalizedOptionTransaction],
) -> List[LegFill]:
    """
    Convert transactions to LegFill objects, grouping by account.

    Transactions are expected to have account information available (e.g., from StoredTransaction
    or ParsedImportResult). For transactions from stored sources, use _stored_to_normalized first
    to preserve account metadata.

    Returns a flat list of LegFill objects with account information preserved.
    """
    from collections import defaultdict

    # Group transactions by account (extract from raw dict if available)
    grouped: Dict[Tuple[str, Optional[str]], List[NormalizedOptionTransaction]] = defaultdict(list)

    for txn in transactions:
        # Extract account info from raw dict if available
        account_name = txn.raw.get("Account Name", "") if txn.raw else ""
        account_number = txn.raw.get("Account Number") if txn.raw else None
        if not account_name:
            # Fallback: if no account info in raw, we can't group properly
            # This shouldn't happen in practice, but handle gracefully
            account_name = "Unknown Account"
        key = (account_name, account_number)
        grouped[key].append(txn)

    # Convert each account's transactions to LegFill objects
    all_fills: List[LegFill] = []
    for (account_name, account_number), txns in grouped.items():
        fills = build_leg_fills(
            txns,
            account_name=account_name,
            account_number=account_number,
        )
        all_fills.extend(fills)

    return all_fills


def match_legs_with_errors(
    fills: Iterable[LegFill],
) -> Tuple[Dict[LegKey, MatchedLeg], List[str]]:
    """
    Match legs with error handling, returning matched results and any errors encountered.

    Returns a tuple of (matched_legs_dict, errors_list) where errors are descriptive strings.
    """
    errors: List[str] = []
    matched: Dict[LegKey, MatchedLeg] = {}

    # Group fills by leg key first
    grouped = _group_leg_fills(fills)

    for key, bucket in grouped.items():
        try:
            matched[key] = match_leg_fills(bucket)
        except ValueError as exc:
            # Capture matching errors (e.g., closing fill without corresponding open)
            account_name, account_number, leg_id = key
            account_label = (
                account_name if not account_number else f"{account_name} ({account_number})"
            )
            errors.append(f"{account_label} - {leg_id}: {str(exc)}")
        except Exception as exc:
            # Capture any other unexpected errors
            account_name, account_number, leg_id = key
            account_label = (
                account_name if not account_number else f"{account_name} ({account_number})"
            )
            errors.append(f"{account_label} - {leg_id}: Unexpected error: {str(exc)}")

    return matched, errors
