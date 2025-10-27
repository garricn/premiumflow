"""Core data models for roll chain analysis.

The original project used Pydantic models, but the tests that accompany this kata run
in an isolated environment without network access.  To keep the public API stable
while avoiding the heavy Pydantic dependency, the models have been reimplemented
with ``dataclasses`` and a thin layer of manual validation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, List, Optional


def _coerce_decimal(value: Any, field_name: str) -> Decimal:
    """Convert ``value`` to :class:`~decimal.Decimal` with helpful errors."""

    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            raise ValueError(f"{field_name} cannot be empty")
        try:
            return Decimal(cleaned)
        except (InvalidOperation, ValueError) as exc:
            raise ValueError(f"{field_name} must be a numeric value") from exc
    raise TypeError(f"{field_name} must be a Decimal-compatible value")


def _coerce_int(value: Any, field_name: str) -> int:
    """Convert ``value`` to :class:`int`.

    Strings that contain commas or Robinhood-style parentheses for negative values are
    also supported to make it easier to construct objects from CSV exports.
    """

    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        cleaned = value.strip().replace(",", "")
        if cleaned.startswith("(") and cleaned.endswith(")"):
            cleaned = f"-{cleaned[1:-1]}"
        if not cleaned:
            return 0
        try:
            return int(Decimal(cleaned))
        except (InvalidOperation, ValueError) as exc:
            raise ValueError(f"{field_name} must be an integer") from exc
    raise TypeError(f"{field_name} must be an int-compatible value")


def _coerce_datetime(value: Any, field_name: str) -> datetime:
    """Coerce ``value`` to :class:`~datetime.datetime`.

    ``datetime`` instances are returned unchanged.  Strings are parsed using
    ``datetime.fromisoformat`` first and fall back to ``%m/%d/%Y`` which matches the
    transaction export format used in the tests.
    """

    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError(f"{field_name} cannot be empty")
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            pass
        for fmt in ("%m/%d/%Y", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
        raise ValueError(f"{field_name} must be a datetime or ISO date string")
    raise TypeError(f"{field_name} must be datetime-compatible")


def _normalize_option_type(value: Any) -> str:
    normalized = (value or "").strip().upper()
    if normalized not in {"C", "P"}:
        raise ValueError('option_type must be "C" or "P"')
    return normalized


def _normalize_action(value: Any) -> str:
    normalized = (value or "").strip().upper()
    if normalized not in {"BUY", "SELL"}:
        raise ValueError('action must be "BUY" or "SELL"')
    return normalized


@dataclass
class Transaction:
    """Represents a single options transaction."""

    symbol: str
    strike: Decimal
    option_type: str
    expiration: str
    quantity: int
    price: Decimal
    action: str
    date: datetime
    fees: Decimal = Decimal("0.04")

    def __post_init__(self) -> None:
        self.symbol = (self.symbol or "").strip().upper()
        if not self.symbol:
            raise ValueError("symbol cannot be empty")

        self.option_type = _normalize_option_type(self.option_type)
        self.action = _normalize_action(self.action)
        self.expiration = (self.expiration or "").strip()
        if not self.expiration:
            raise ValueError("expiration cannot be empty")

        self.strike = _coerce_decimal(self.strike, "strike")
        self.price = _coerce_decimal(self.price, "price")
        self.fees = _coerce_decimal(self.fees, "fees")
        self.quantity = _coerce_int(self.quantity, "quantity")
        self.date = _coerce_datetime(self.date, "date")

    @property
    def net_quantity(self) -> int:
        """Net quantity (positive for buy, negative for sell)."""

        return self.quantity if self.action == "BUY" else -self.quantity

    @property
    def total_fees(self) -> Decimal:
        """Total fees for this transaction."""

        return self.fees * abs(self.quantity)

    @property
    def position_spec(self) -> str:
        """Position specification string for lookup."""

        return f"{self.symbol} ${self.strike} {self.option_type} {self.expiration}"


@dataclass
class RollChain:
    """Represents a roll chain of connected transactions."""

    transactions: List[Transaction] = field(default_factory=list)
    symbol: str = ""
    strike: Decimal = Decimal("0")
    option_type: str = "C"
    expiration: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.transactions, Iterable):
            raise TypeError("transactions must be an iterable of Transaction objects")

        converted: List[Transaction] = []
        for txn in self.transactions:
            if isinstance(txn, Transaction):
                converted.append(txn)
            elif isinstance(txn, dict):
                converted.append(Transaction(**txn))
            else:
                raise TypeError("transactions must contain Transaction instances or dicts")

        if len(converted) < 2:
            raise ValueError("Roll chain must have at least 2 transactions")

        self.transactions = converted

        # Normalise summary attributes, falling back to the first transaction when possible.
        self.symbol = (self.symbol or converted[0].symbol).strip().upper()
        self.option_type = _normalize_option_type(self.option_type or converted[0].option_type)
        self.expiration = (self.expiration or converted[0].expiration).strip()
        if not self.expiration:
            raise ValueError("expiration cannot be empty")

        base_strike = self.strike or converted[0].strike
        self.strike = _coerce_decimal(base_strike, "strike")

    @property
    def net_quantity(self) -> int:
        """Net quantity across all transactions."""

        return sum(t.net_quantity for t in self.transactions)

    @property
    def total_credits(self) -> Decimal:
        """Total credits received."""

        return sum(t.price * abs(t.quantity) for t in self.transactions if t.action == "SELL")

    @property
    def total_debits(self) -> Decimal:
        """Total debits paid."""

        return sum(t.price * abs(t.quantity) for t in self.transactions if t.action == "BUY")

    @property
    def net_pnl(self) -> Decimal:
        """Net profit/loss."""

        return self.total_credits - self.total_debits

    @property
    def total_fees(self) -> Decimal:
        """Total fees across all transactions."""

        return sum(t.total_fees for t in self.transactions)

    @property
    def net_pnl_after_fees(self) -> Decimal:
        """Net P&L after accounting for fees."""

        return self.net_pnl - self.total_fees

    @property
    def breakeven_price(self) -> Optional[Decimal]:
        """Breakeven price for the position."""

        if self.net_quantity == 0:
            return None
        return self.strike + (self.net_pnl_after_fees / abs(self.net_quantity))

    @property
    def is_closed(self) -> bool:
        """Whether the position is closed (net quantity = 0)."""

        return self.net_quantity == 0

    @property
    def is_open(self) -> bool:
        """Whether the position is open (net quantity != 0)."""

        return self.net_quantity != 0
