"""FIFO leg matching service.

Transforms :class:`~premiumflow.core.legs.LegFill` sequences into matched lots that pair opening
and closing fills per contract. Provides summaries that downstream layers (CLI, web) can consume
without reimplementing the matching algorithm.
"""

from __future__ import annotations

import json
from collections import defaultdict, deque
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
    # Use gross_notional (price * quantity * 100) with appropriate sign for premium
    # This ensures premium is always gross (before fees), not net
    gross_premium = fill.gross_notional * ratio
    if fill.trans_code in {"STO", "STC"}:
        premium = _quantize(gross_premium)  # Positive for credits
    else:
        premium = _quantize(-gross_premium)  # Negative for debits
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
        """Fees associated with opening this lot."""
        return _quantize(sum(portion.fees for portion in self.open_portions))

    @property
    def close_fees(self) -> Money:
        """Fees associated with closing this lot."""
        return _quantize(sum(portion.fees for portion in self.close_portions))

    @property
    def open_credit_gross(self) -> Money:
        """Gross credit received when opening (before fees)."""
        return self.open_premium

    @property
    def open_credit_net(self) -> Money:
        """Net credit received when opening (after fees)."""
        return _quantize(self.open_premium - self.open_fees)

    @property
    def close_cost(self) -> Money:
        """Cost paid to close (before fees). Returns 0 if not closed or if close was a credit."""
        if not self.is_closed or self.close_premium >= 0:
            return _quantize(0)
        return _quantize(abs(self.close_premium))

    @property
    def close_cost_total(self) -> Money:
        """Total cost paid to close (cost + fees). Returns 0 if not closed or if close was a credit."""
        if not self.is_closed or self.close_premium >= 0:
            return _quantize(0)
        return _quantize(self.close_cost + self.close_fees)

    @property
    def close_quantity(self) -> int:
        """Quantity of contracts closed. Returns 0 if lot is still open."""
        return self.quantity if self.is_closed else 0

    @property
    def credit_remaining(self) -> Money:
        """Remaining potential credit for open lots. Returns 0 if fully closed."""
        return _quantize(self.open_premium) if self.is_open else _quantize(0)

    @property
    def quantity_remaining(self) -> int:
        """Remaining quantity of open contracts. Returns 0 if fully closed."""
        return self.quantity if self.is_open else 0

    @property
    def net_premium(self) -> Optional[Money]:
        """Net P/L after fees. Returns None if lot is still open."""
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
    def net_premium(self) -> Money:
        """Net P/L after fees for all closed lots in this leg."""
        return _quantize(self.realized_premium - self.total_fees)

    @property
    def opened_at(self) -> Optional[date]:
        """Earliest date any lot was opened."""
        if not self.lots:
            return None
        return min(lot.opened_at for lot in self.lots)

    @property
    def closed_at(self) -> Optional[date]:
        """Latest date any lot was closed."""
        closed_dates = [lot.closed_at for lot in self.lots if lot.closed_at is not None]
        if not closed_dates:
            return None
        return max(closed_dates)

    @property
    def opened_quantity(self) -> int:
        """Total contracts opened across all lots."""
        return sum(lot.quantity for lot in self.lots)

    @property
    def closed_quantity(self) -> int:
        """Total contracts closed across all lots."""
        return sum(lot.close_quantity for lot in self.lots)

    @property
    def open_credit_gross(self) -> Money:
        """Total gross credit received when opening (before fees)."""
        return _quantize(sum(lot.open_premium for lot in self.lots))

    @property
    def close_cost(self) -> Money:
        """Total cost paid to close (before fees)."""
        return _quantize(sum(lot.close_cost for lot in self.lots))

    @property
    def open_fees(self) -> Money:
        """Total fees associated with opening."""
        return _quantize(sum(lot.open_fees for lot in self.lots))

    @property
    def close_fees(self) -> Money:
        """Total fees associated with closing."""
        return _quantize(sum(lot.close_fees for lot in self.lots))

    def resolution(self) -> str:
        """
        Return resolution for fully closed legs. Returns '--' if leg is still open.

        Resolution describes how the leg was closed (e.g., 'Buy to close', 'Expiration').
        """
        if self.is_open:
            return "--"  # Unresolved - leg is still open

        # Helper to get resolution label from a portion
        def _portion_resolution(portion) -> str:
            code = portion.fill.trans_code
            _CLOSE_LABELS = {
                "BTC": "Buy to close",
                "STC": "Sell to close",
                "OEXP": "Expiration",
                "OASGN": "Assignment",
            }
            return _CLOSE_LABELS.get(code, code)

        # Helper to get resolutions from a lot
        def _lot_resolutions(lot: MatchedLegLot) -> List[str]:
            if not lot.close_portions:
                return []
            labels = {_portion_resolution(portion) for portion in lot.close_portions}
            if not labels:
                return []
            if len(labels) == 1:
                return [next(iter(labels))]
            return ["Mixed"]

        resolutions: List[str] = []
        for lot in self.lots:
            # Only include resolutions from fully closed lots
            if lot.is_closed:
                resolutions.extend(_lot_resolutions(lot))

        if not resolutions:
            return "--"

        unique = set(resolutions)
        if len(unique) == 1:
            return next(iter(unique))
        return "Mixed"


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


def _stored_to_normalized(txn: StoredTransaction) -> NormalizedOptionTransaction:
    """Convert a persisted transaction back into its normalized form."""

    def _decimal_from_text(value: Optional[str]) -> Optional[Decimal]:
        if value is None:
            return None
        return Decimal(value)

    return NormalizedOptionTransaction(
        activity_date=date.fromisoformat(txn.activity_date),
        process_date=date.fromisoformat(txn.process_date) if txn.process_date else None,
        settle_date=date.fromisoformat(txn.settle_date) if txn.settle_date else None,
        instrument=txn.instrument,
        description=txn.description,
        trans_code=txn.trans_code,
        quantity=txn.quantity,
        price=Decimal(txn.price),
        amount=_decimal_from_text(txn.amount),
        strike=Decimal(txn.strike),
        option_type=txn.option_type,
        expiration=date.fromisoformat(txn.expiration),
        action=txn.action,
        raw=json.loads(txn.raw_json),
    )


def group_fills_by_account(
    transactions: Sequence[StoredTransaction],
) -> List[LegFill]:
    """Build leg fills grouped by account metadata to preserve account labels."""
    grouped: Dict[Tuple[str, Optional[str]], List[StoredTransaction]] = defaultdict(list)
    for txn in transactions:
        grouped[(txn.account_name, txn.account_number)].append(txn)

    fills: List[LegFill] = []
    for (account_name, account_number), records in grouped.items():
        normalized = [_stored_to_normalized(record) for record in records]
        fills.extend(
            build_leg_fills(
                normalized,
                account_name=account_name,
                account_number=account_number,
            )
        )
    return fills


def match_legs_with_errors(
    fills: Sequence[LegFill],
) -> Tuple[
    Dict[LegKey, MatchedLeg],
    List[Tuple[LegKey, Exception, List[LegFill]]],
]:
    """Run FIFO matching per leg while capturing legs that fail to reconcile."""
    grouped: Dict[LegKey, List[LegFill]] = defaultdict(list)
    for fill in fills:
        key = (fill.account_name, fill.account_number, fill.contract.leg_id)
        grouped[key].append(fill)

    results: Dict[LegKey, MatchedLeg] = {}
    errors: List[Tuple[LegKey, Exception, List[LegFill]]] = []

    for key, bucket in grouped.items():
        bucket.sort(key=lambda fill: fill.sort_key())
        try:
            results[key] = match_leg_fills(bucket)
        except Exception as exc:  # noqa: BLE001 - surface all matching issues
            errors.append((key, exc, bucket))

    return results, errors


def match_legs(fills: Iterable[LegFill]) -> Dict[LegKey, MatchedLeg]:
    """Group fills by leg and return FIFO-matched results for each grouping."""
    results: Dict[LegKey, MatchedLeg] = {}
    for key, bucket in _group_leg_fills(fills).items():
        results[key] = match_leg_fills(bucket)
    return results
