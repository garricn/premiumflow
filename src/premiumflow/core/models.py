"""
Core data models for roll chain analysis.

This module defines the Pydantic models for transactions and roll chains.
"""

from datetime import datetime
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator


class Transaction(BaseModel):
    """Represents a single options transaction."""

    symbol: str = Field(..., description="Stock symbol (e.g., 'TSLA')")
    strike: Decimal = Field(..., description="Strike price")
    option_type: str = Field(..., description="'C' for call, 'P' for put")
    expiration: str = Field(..., description="Expiration date (YYYY-MM-DD)")
    quantity: int = Field(..., description="Number of contracts")
    price: Decimal = Field(..., description="Price per contract")
    action: str = Field(..., description="'BUY' or 'SELL'")
    date: datetime = Field(..., description="Transaction date")

    @field_validator("option_type")
    @classmethod
    def validate_option_type(cls, v):
        if v.upper() not in ["C", "P"]:
            raise ValueError('option_type must be "C" or "P"')
        return v.upper()

    @field_validator("action")
    @classmethod
    def validate_action(cls, v):
        if v.upper() not in ["BUY", "SELL"]:
            raise ValueError('action must be "BUY" or "SELL"')
        return v.upper()

    @property
    def net_quantity(self) -> int:
        """Net quantity (positive for buy, negative for sell)."""
        return self.quantity if self.action == "BUY" else -self.quantity

    @property
    def position_spec(self) -> str:
        """Position specification string for lookup."""
        return f"{self.symbol} ${self.strike} {self.option_type} {self.expiration}"


class RollChain(BaseModel):
    """Represents a roll chain of connected transactions."""

    transactions: List[Transaction] = Field(..., description="List of transactions in the chain")
    symbol: str = Field(..., description="Stock symbol")
    strike: Decimal = Field(..., description="Strike price")
    option_type: str = Field(..., description="'C' for call, 'P' for put")
    expiration: str = Field(..., description="Expiration date")

    @field_validator("transactions")
    @classmethod
    def validate_transactions(cls, v):
        if len(v) < 2:
            raise ValueError("Roll chain must have at least 2 transactions")
        return v

    @property
    def net_quantity(self) -> int:
        """Net quantity across all transactions."""
        return sum(t.net_quantity for t in self.transactions)

    @property
    def total_credits(self) -> Decimal:
        """Total credits received."""
        return sum(
            (t.price * abs(t.quantity) for t in self.transactions if t.action == "SELL"),
            Decimal("0"),
        )

    @property
    def total_debits(self) -> Decimal:
        """Total debits paid."""
        return sum(
            (t.price * abs(t.quantity) for t in self.transactions if t.action == "BUY"),
            Decimal("0"),
        )

    @property
    def net_pnl(self) -> Decimal:
        """Net profit/loss."""
        return self.total_credits - self.total_debits

    @property
    def breakeven_price(self) -> Optional[Decimal]:
        """Breakeven price for the position."""
        if self.net_quantity == 0:
            return None
        return self.strike + (self.net_pnl / abs(self.net_quantity))

    @property
    def is_closed(self) -> bool:
        """Whether the position is closed (net quantity = 0)."""
        return self.net_quantity == 0

    @property
    def is_open(self) -> bool:
        """Whether the position is open (net quantity != 0)."""
        return self.net_quantity != 0
