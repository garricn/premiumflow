"""Services for managing cost basis overrides for transfer transactions."""

from __future__ import annotations

from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Optional, Sequence

from ..persistence import (
    SQLiteRepository,
    StoredTransferBasisItem,
    TransferBasisStatus,
)

__all__ = [
    "CostBasisError",
    "CostBasisNotFoundError",
    "get_due_transfer_basis_items",
    "list_transfer_basis_items",
    "resolve_transfer_basis_override",
    "snooze_transfer_basis_item",
    "reopen_transfer_basis_item",
    "list_resolved_transfer_basis_items",
]

_CURRENCY_QUANTIZER = Decimal("0.01")
_PER_SHARE_QUANTIZER = Decimal("0.0001")


class CostBasisError(RuntimeError):
    """Base class for cost basis override errors."""


class CostBasisNotFoundError(CostBasisError):
    """Raised when no matching transfer basis entry can be found."""


def list_transfer_basis_items(
    repository: SQLiteRepository,
    *,
    account_name: Optional[str] = None,
    account_number: Optional[str] = None,
    statuses: Optional[Sequence[TransferBasisStatus]] = None,
    due_only: bool = False,
) -> list[StoredTransferBasisItem]:
    """Return transfer basis entries for the requested filters."""

    return repository.list_transfer_basis_items(
        account_name=account_name,
        account_number=account_number,
        statuses=statuses,
        due_only=due_only,
    )


def get_due_transfer_basis_items(
    repository: SQLiteRepository,
    *,
    account_name: Optional[str] = None,
    account_number: Optional[str] = None,
) -> list[StoredTransferBasisItem]:
    """Return pending or snoozed entries whose reminders are due."""

    return list_transfer_basis_items(
        repository,
        account_name=account_name,
        account_number=account_number,
        statuses=("pending", "snoozed"),
        due_only=True,
    )


def list_resolved_transfer_basis_items(
    repository: SQLiteRepository,
    *,
    account_name: Optional[str] = None,
    account_number: Optional[str] = None,
) -> list[StoredTransferBasisItem]:
    """Return resolved transfer basis overrides."""

    return list_transfer_basis_items(
        repository,
        account_name=account_name,
        account_number=account_number,
        statuses=("resolved",),
        due_only=False,
    )


def resolve_transfer_basis_override(  # noqa: PLR0913
    repository: SQLiteRepository,
    *,
    account_name: str,
    account_number: Optional[str],
    instrument: str,
    activity_date: date,
    shares: Decimal,
    basis_total: Optional[Decimal] = None,
    basis_per_share: Optional[Decimal] = None,
    trans_code: Optional[str] = None,
) -> StoredTransferBasisItem:
    """Resolve a pending transfer basis entry with the supplied override values."""

    normalized_symbol = _normalize_symbol(instrument)
    normalized_shares = _require_positive(shares, "shares")
    resolved_total, resolved_per_share = _resolve_basis_values(
        normalized_shares, basis_total, basis_per_share
    )

    matches = repository.find_transfer_basis_items(
        account_name=account_name,
        account_number=account_number,
        instrument=normalized_symbol,
        activity_date=activity_date,
        shares=normalized_shares,
        include_resolved=False,
        trans_code=trans_code,
    )
    if not matches:
        raise CostBasisNotFoundError(
            "No pending transfer basis entry matches the provided filters."
        )
    if len(matches) > 1 and not trans_code:
        raise CostBasisError(
            "Multiple entries match the provided filters. Specify --trans-code to disambiguate."
        )

    entry = matches[0]
    updated = repository.resolve_transfer_basis_item(
        entry.id,
        basis_total=resolved_total,
        basis_per_share=resolved_per_share,
    )
    if not updated:
        raise CostBasisError("Failed to resolve transfer basis entry.")

    refreshed = repository.get_transfer_basis_item_by_id(entry.id)
    if refreshed is None:
        raise CostBasisError("Resolved transfer basis entry could not be reloaded.")
    return refreshed


def snooze_transfer_basis_item(
    repository: SQLiteRepository,
    *,
    item_id: int,
    remind_after: datetime,
) -> StoredTransferBasisItem:
    """Snooze a transfer basis entry until the provided UTC timestamp."""

    updated = repository.snooze_transfer_basis_item(
        item_id,
        remind_after=remind_after,
    )
    if not updated:
        raise CostBasisNotFoundError("Transfer basis entry not found.")
    refreshed = repository.get_transfer_basis_item_by_id(item_id)
    if refreshed is None:
        raise CostBasisError("Snoozed transfer basis entry could not be reloaded.")
    return refreshed


def reopen_transfer_basis_item(
    repository: SQLiteRepository,
    *,
    item_id: int,
) -> StoredTransferBasisItem:
    """Return a transfer basis entry to pending state."""

    updated = repository.reopen_transfer_basis_item(item_id)
    if not updated:
        raise CostBasisNotFoundError("Transfer basis entry not found.")
    refreshed = repository.get_transfer_basis_item_by_id(item_id)
    if refreshed is None:
        raise CostBasisError("Reopened transfer basis entry could not be reloaded.")
    return refreshed


def _resolve_basis_values(
    shares: Decimal,
    basis_total: Optional[Decimal],
    basis_per_share: Optional[Decimal],
) -> tuple[Decimal, Decimal]:
    if basis_total is None and basis_per_share is None:
        raise CostBasisError("Provide either basis_total or basis_per_share.")

    shares = _require_positive(shares, "shares")
    normalized_total = _quantize_currency(basis_total) if basis_total is not None else None
    normalized_per_share = (
        _quantize_per_share(basis_per_share) if basis_per_share is not None else None
    )

    if normalized_total is not None and normalized_per_share is not None:
        expected_total = _quantize_currency(normalized_per_share * shares)
        if abs(expected_total - normalized_total) > _CURRENCY_QUANTIZER:
            raise CostBasisError(
                "basis_total does not match basis_per_share × shares within one cent."
            )
        return normalized_total, normalized_per_share

    if normalized_total is not None:
        return normalized_total, _quantize_per_share(normalized_total / shares)

    assert normalized_per_share is not None  # Required when total is missing
    return _quantize_currency(normalized_per_share * shares), normalized_per_share


def _normalize_symbol(symbol: str) -> str:
    stripped = (symbol or "").strip().upper()
    if not stripped:
        raise CostBasisError("Ticker symbol cannot be blank.")
    return stripped


def _require_positive(value: Decimal, label: str) -> Decimal:
    if value <= Decimal("0"):
        raise CostBasisError(f"{label} must be greater than zero.")
    return value


def _quantize_currency(value: Decimal) -> Decimal:
    return value.quantize(_CURRENCY_QUANTIZER, rounding=ROUND_HALF_UP)


def _quantize_per_share(value: Decimal) -> Decimal:
    return value.quantize(_PER_SHARE_QUANTIZER, rounding=ROUND_HALF_UP)
