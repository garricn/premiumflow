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

from ..core.legs import LegContract, LegFill

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
    premium = _quantize(fill.effective_premium * ratio)
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
