"""Domain models for normalized option legs and fills.

The helpers in this module build on :class:`~premiumflow.core.parser.NormalizedOptionTransaction`
to expose structured contract metadata, per-fill cash metrics, and aggregate views that keep
track of open quantity, retained premium, and days-to-expiration. Matching lots across fills
is handled in follow-up work (see issue #136); the classes here stay focused on describing
contracts and summarizing the raw fills for a single leg.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from .parser import NormalizedOptionTransaction

CONTRACT_MULTIPLIER = Decimal("100")
EASTERN_TZ = ZoneInfo("US/Eastern")

_OPENING_CODES = {"BTO", "STO"}
_CLOSING_CODES = {"BTC", "STC", "OASGN", "OEXP"}
_ASSIGNMENT_CODES = {"OASGN"}
_EXPIRATION_CODES = {"OEXP"}
_DISPLAY_PREFIXES = (
    "Option Expiration for ",
    "Option Assignment for ",
    "Option Exercise for ",
    "Assignment of ",
)


def _compute_signed_quantity(
    trans_code: str, quantity: int, net_before: int, action: str
) -> int:
    """Return the signed quantity delta contributed by the transaction."""
    code = (trans_code or "").upper()
    result = 0
    if code == "BTO":
        result = quantity
    elif code == "STO":
        result = -quantity
    elif code == "BTC":
        result = quantity
    elif code == "STC":
        result = -quantity
    elif code in {"OASGN", "OEXP"}:
        if net_before < 0:
            result = quantity
        elif net_before > 0:
            result = -quantity
        else:
            # Fallback when no prior context: default to closing short for OASGN, long for OEXP.
            result = quantity if code == "OASGN" else -quantity
    else:
        normalized_action = (action or "").upper()
        result = quantity if normalized_action == "BUY" else -quantity
    return result


def _strike_to_cents(value: Decimal) -> int:
    """Convert a strike price to an integer number of cents."""
    normalized = value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return int((normalized * Decimal("100")).to_integral_value(rounding=ROUND_HALF_UP))


def _normalize_display(description: str) -> str:
    """Strip broker-specific prefixes from contract descriptions."""
    cleaned = (description or "").strip()
    for prefix in _DISPLAY_PREFIXES:
        if cleaned.startswith(prefix):
            return cleaned[len(prefix) :].strip()
    return cleaned


@dataclass(frozen=True)
class LegContract:
    """Identifies a single option contract (symbol/expiration/strike/type)."""

    leg_id: str
    symbol: str
    expiration: date
    option_type: str
    strike: Decimal
    display_name: str

    @classmethod
    def from_transaction(cls, txn: NormalizedOptionTransaction) -> "LegContract":
        """Derive contract metadata from a normalized transaction."""
        symbol = (txn.instrument or "").strip().upper()
        option_type = txn.option_type.upper()
        option_code = "C" if option_type.startswith("C") else "P"
        strike_cents = _strike_to_cents(txn.strike)
        leg_id = f"{symbol}-{txn.expiration.isoformat()}-{option_code}-{strike_cents}"
        display_name = _normalize_display(txn.description)
        return cls(
            leg_id=leg_id,
            symbol=symbol,
            expiration=txn.expiration,
            option_type=option_type,
            strike=txn.strike,
            display_name=display_name,
        )

    def days_to_expiration(self, *, as_of: Optional[date | datetime] = None) -> int:
        """Return the non-negative number of days from ``as_of`` to expiration."""
        if as_of is None:
            as_of_date = datetime.now(EASTERN_TZ).date()
        elif isinstance(as_of, datetime):
            as_of_date = as_of.astimezone(EASTERN_TZ).date()
        else:
            as_of_date = as_of
        delta = (self.expiration - as_of_date).days
        return max(delta, 0)


@dataclass(frozen=True)
class LegFill:
    """Wraps a normalized transaction with shared contract/account context."""

    contract: LegContract
    account_name: str
    account_number: Optional[str]
    transaction: NormalizedOptionTransaction
    _signed_quantity: int = field(repr=False)
    _sequence: int = field(repr=False, default=0)

    @property
    def quantity(self) -> int:
        return self.transaction.quantity

    @property
    def trans_code(self) -> str:
        return (self.transaction.trans_code or "").upper()

    @property
    def is_opening(self) -> bool:
        return self.trans_code in _OPENING_CODES

    @property
    def is_closing(self) -> bool:
        return self.trans_code in _CLOSING_CODES

    @property
    def is_assignment(self) -> bool:
        return self.trans_code in _ASSIGNMENT_CODES

    @property
    def is_expiration(self) -> bool:
        if self.trans_code in _EXPIRATION_CODES:
            return True
        description = (self.transaction.description or "").lower()
        return description.startswith("option expiration for")

    @property
    def signed_quantity(self) -> int:
        """Quantity signed to reflect net position impact."""
        return self._signed_quantity

    @property
    def gross_notional(self) -> Decimal:
        value = self.transaction.price * Decimal(self.quantity) * CONTRACT_MULTIPLIER
        return value.quantize(Decimal("0.01"))

    @property
    def effective_premium(self) -> Decimal:
        if self.transaction.amount is not None:
            return self.transaction.amount
        notional = self.gross_notional
        if self.trans_code in {"STO", "STC"}:
            return notional
        return -notional

    @property
    def fees(self) -> Decimal:
        amount = self.transaction.amount
        if amount is None:
            return Decimal("0.00")
        # Broker reports already include fees in ``Amount``; subtract from gross notional to recover
        # the effective fee value (e.g., Robinhood regulatory fees). Always positive.
        delta = abs(self.gross_notional - abs(amount))
        return delta.quantize(Decimal("0.01"))

    @property
    def activity_date(self) -> date:
        return self.transaction.activity_date

    def sort_key(self) -> Tuple[date, date, date, int]:
        """Stable sort key matching the parser's chronological ordering."""
        txn = self.transaction
        activity = txn.activity_date
        process = txn.process_date or activity
        settle = txn.settle_date or activity
        return activity, process, settle, self._sequence


@dataclass(frozen=True)
class OptionLeg:
    """Aggregate view of all fills that belong to a single contract."""

    contract: LegContract
    account_name: str
    account_number: Optional[str]
    fills: Tuple[LegFill, ...]
    opening_quantity: int
    closing_quantity: int
    gross_open_premium: Decimal
    gross_close_premium: Decimal
    total_fees: Decimal

    @property
    def net_contracts(self) -> int:
        return sum(fill.signed_quantity for fill in self.fills)

    @property
    def open_quantity(self) -> int:
        remaining = self.opening_quantity - self.closing_quantity
        return max(remaining, 0)

    @property
    def is_open(self) -> bool:
        return self.open_quantity > 0

    @property
    def net_pnl(self) -> Decimal:
        return (self.gross_open_premium + self.gross_close_premium).quantize(Decimal("0.01"))

    @property
    def realized_pnl(self) -> Optional[Decimal]:
        """
        Net profit/loss realized so far.

        When the leg is fully closed this equals ``net_pnl``. For partially closed legs the
        value reflects aggregate cash impact to date; precise realized/open splits are delegated
        to the FIFO matching service in issue #136.
        """
        if self.closing_quantity == 0:
            return None
        value = self.net_pnl
        if self.is_open:
            return None
        return value

    def days_to_expiration(self, *, as_of: Optional[date | datetime] = None) -> int:
        return self.contract.days_to_expiration(as_of=as_of)

    @property
    def first_fill_date(self) -> date:
        return self.fills[0].activity_date

    @property
    def last_fill_date(self) -> date:
        return self.fills[-1].activity_date


def build_leg_fills(
    transactions: Sequence[NormalizedOptionTransaction],
    *,
    account_name: str,
    account_number: Optional[str],
) -> List[LegFill]:
    """Convert normalized transactions into :class:`LegFill` instances."""
    indexed = list(enumerate(transactions))
    indexed.sort(
        key=lambda item: (
            item[1].activity_date,
            item[1].process_date or item[1].activity_date,
            item[1].settle_date or item[1].activity_date,
            (
                0
                if (item[1].trans_code or "").upper() in _OPENING_CODES
                else 1 if (item[1].trans_code or "").upper() in _CLOSING_CODES else 2
            ),
            item[0],
        )
    )

    fills: List[LegFill] = []
    running_net: Dict[str, int] = {}
    for order_index, (_original_index, txn) in enumerate(indexed):
        contract = LegContract.from_transaction(txn)
        leg_key = contract.leg_id
        net_before = running_net.get(leg_key, 0)
        signed_quantity = _compute_signed_quantity(
            txn.trans_code, txn.quantity, net_before, txn.action
        )
        running_net[leg_key] = net_before + signed_quantity
        fills.append(
            LegFill(
                contract=contract,
                account_name=account_name,
                account_number=account_number,
                transaction=txn,
                _signed_quantity=signed_quantity,
                _sequence=order_index,
            )
        )
    return fills


def aggregate_legs(
    fills: Iterable[LegFill],
) -> Dict[Tuple[str, Optional[str], str], OptionLeg]:
    """
    Group leg fills by account and contract, returning aggregate summaries.

    The dictionary key is ``(account_name, account_number, contract.leg_id)`` (with ``account_name``
    trimmed) so callers can safely combine fills from multiple imports before handing off to FIFO
    matching.
    """
    grouped: Dict[Tuple[str, Optional[str], str], List[LegFill]] = {}
    for fill in fills:
        account_key = (fill.account_name or "").strip()
        key = (account_key, fill.account_number, fill.contract.leg_id)
        grouped.setdefault(key, []).append(fill)

    aggregates: Dict[Tuple[str, Optional[str], str], OptionLeg] = {}
    for key, bucket in grouped.items():
        account_name, account_number, _ = key
        bucket.sort(key=lambda item: item.sort_key())

        opening_quantity = sum(fill.quantity for fill in bucket if fill.is_opening)
        closing_quantity = sum(fill.quantity for fill in bucket if fill.is_closing)
        gross_open = sum(
            (fill.effective_premium for fill in bucket if fill.is_opening), Decimal("0.00")
        )
        gross_close = sum(
            (fill.effective_premium for fill in bucket if fill.is_closing), Decimal("0.00")
        )
        total_fees = sum((fill.fees for fill in bucket), Decimal("0.00"))

        aggregates[key] = OptionLeg(
            contract=bucket[0].contract,
            account_name=account_name or bucket[0].account_name,
            account_number=account_number,
            fills=tuple(bucket),
            opening_quantity=opening_quantity,
            closing_quantity=closing_quantity,
            gross_open_premium=gross_open.quantize(Decimal("0.01")),
            gross_close_premium=gross_close.quantize(Decimal("0.01")),
            total_fees=total_fees.quantize(Decimal("0.01")),
        )

    return aggregates
