"""Import and apply external tax lot snapshots (e.g., Robinhood unrealized PDF)."""

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable, List, Optional

from ..persistence import (
    SQLiteRepository,
    StoredExternalTaxLot,
    StoredTransferBasisItem,
)
from .cost_basis import CostBasisError, resolve_transfer_basis_override

__all__ = [
    "ExternalTaxLotImportError",
    "ExternalTaxLotImportResult",
    "ExternalTaxLotRecord",
    "import_external_tax_lot_snapshot",
]


class ExternalTaxLotImportError(RuntimeError):
    """Base class for external tax lot import errors."""


class PdfExtractionError(ExternalTaxLotImportError):
    """Raised when the PDF could not be converted into text."""


class ParsingError(ExternalTaxLotImportError):
    """Raised when the extracted text could not be parsed."""


@dataclass(frozen=True)
class ExternalTaxLotRecord:
    """Structured representation of a tax lot row from the unrealized report."""

    open_date: date
    hold_date: date
    security_id: Optional[str]
    ticker: str
    description: str
    shares: Decimal
    price: Optional[Decimal]
    book_cost: Optional[Decimal]
    wash_adjustment: Optional[Decimal]
    tax_cost: Decimal
    lot_status: Optional[str]


@dataclass(frozen=True)
class ExternalTaxLotImportResult:
    """Summary of an external tax lot import run."""

    total_snapshot_lots: int
    stored_snapshot_lots: int
    resolved_transfer_items: int
    unresolved_transfer_items: List[StoredTransferBasisItem]
    ambiguous_transfer_items: List[tuple[StoredTransferBasisItem, List[StoredExternalTaxLot]]]
    resolution_errors: List[tuple[StoredTransferBasisItem, str]]


PDF_LINE_PATTERN = re.compile(r"^\s*(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}/\d{1,2}/\d{4})\s*(.*)$")
CUSIP_PATTERN = re.compile(r"^[0-9A-Z]{6,}$")
NUMERIC_TOKEN_PATTERN = re.compile(r"^[+-]?\d+(?:\.\d+)?\.?$")
SHARE_TOLERANCE = Decimal("0.0001")


def import_external_tax_lot_snapshot(
    repository: SQLiteRepository,
    *,
    pdf_path: Path,
    account_name: str,
    account_number: Optional[str],
    snapshot_label: Optional[str] = None,
) -> ExternalTaxLotImportResult:
    """Parse, persist, and apply a PDF tax lot snapshot for the specified account."""

    pdf_path = pdf_path.resolve()
    if not pdf_path.exists():
        raise ExternalTaxLotImportError(f"PDF path does not exist: {pdf_path}")

    snapshot_label = snapshot_label or pdf_path.stem

    text = _extract_pdf_text(pdf_path)
    lots = list(_parse_tax_lot_text(text.splitlines()))

    repository.replace_external_tax_lots(
        account_name=account_name,
        account_number=account_number,
        snapshot_label=snapshot_label,
        lots=[
            {
                "security_id": record.security_id,
                "ticker": record.ticker,
                "description": record.description,
                "open_date": record.open_date.isoformat(),
                "hold_date": record.hold_date.isoformat(),
                "shares": record.shares,
                "price": record.price,
                "book_cost": record.book_cost,
                "wash_adjustment": record.wash_adjustment,
                "tax_cost": record.tax_cost,
                "lot_status": record.lot_status,
            }
            for record in lots
        ],
    )

    resolved_count, unresolved, ambiguous, errors = _apply_snapshot_to_transfer_basis(
        repository,
        account_name=account_name,
        account_number=account_number,
    )

    return ExternalTaxLotImportResult(
        total_snapshot_lots=len(lots),
        stored_snapshot_lots=len(lots),
        resolved_transfer_items=resolved_count,
        unresolved_transfer_items=unresolved,
        ambiguous_transfer_items=ambiguous,
        resolution_errors=errors,
    )


def _extract_pdf_text(pdf_path: Path) -> str:
    """Extract text content from the PDF using pdftotext."""

    try:
        result = subprocess.run(
            ["pdftotext", "-layout", str(pdf_path), "-"],
            check=True,
            capture_output=True,
        )
    except FileNotFoundError as exc:  # pragma: no cover - depends on environment
        raise PdfExtractionError(
            "pdftotext command not found. Install poppler utils or adjust PATH."
        ) from exc
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.decode("utf-8", errors="replace")
        raise PdfExtractionError(f"Failed to extract text from PDF: {stderr}") from exc

    return result.stdout.decode("utf-8", errors="replace")


def _parse_tax_lot_text(lines: Iterable[str]) -> Iterable[ExternalTaxLotRecord]:
    """Yield `ExternalTaxLotRecord` objects from the extracted text lines."""

    for raw_line in lines:
        match = PDF_LINE_PATTERN.match(raw_line)
        if not match:
            continue

        open_date_str, hold_date_str, remainder = match.groups()
        try:
            open_dt = _parse_date(open_date_str)
            hold_dt = _parse_date(hold_date_str)
        except ValueError as exc:
            raise ParsingError(f"Invalid date in row: {raw_line.strip()}") from exc

        security_id = _extract_security_id(remainder)
        parts = _split_remainder(remainder)

        description, shares_decimal, price_value, status, value_tokens = _parse_parts(
            parts, security_id
        )
        book_cost, wash_adjustment, tax_cost = _parse_value_tokens(value_tokens, raw_line)
        ticker = _extract_ticker(description)

        yield ExternalTaxLotRecord(
            open_date=open_dt,
            hold_date=hold_dt,
            security_id=security_id,
            ticker=ticker,
            description=description,
            shares=shares_decimal,
            price=price_value,
            book_cost=book_cost,
            wash_adjustment=wash_adjustment,
            tax_cost=tax_cost,
            lot_status=status,
        )


def _apply_snapshot_to_transfer_basis(
    repository: SQLiteRepository,
    *,
    account_name: str,
    account_number: Optional[str],
) -> tuple[
    int,
    List[StoredTransferBasisItem],
    List[tuple[StoredTransferBasisItem, List[StoredExternalTaxLot]]],
    List[tuple[StoredTransferBasisItem, str]],
]:
    """Resolve pending transfer basis entries using the imported external tax lots."""

    pending_items = repository.list_transfer_basis_items(
        account_name=account_name,
        account_number=account_number,
        statuses=("pending", "snoozed"),
        due_only=False,
    )

    resolved_count = 0
    unresolved: List[StoredTransferBasisItem] = []
    ambiguous: List[tuple[StoredTransferBasisItem, List[StoredExternalTaxLot]]] = []
    errors: List[tuple[StoredTransferBasisItem, str]] = []

    external_lots = repository.list_external_tax_lots(
        account_name=account_name,
        account_number=account_number,
    )
    lot_inventory = _build_lot_inventory(external_lots)

    for item in sorted(pending_items, key=lambda entry: (entry.instrument.upper(), entry.shares)):
        ticker = item.instrument.strip().upper()
        inventory = lot_inventory.get(ticker)
        if not inventory:
            unresolved.append(item)
            continue

        basis_total = Decimal("0")
        remaining_target = item.shares
        updated_inventory: List[_LotSlice] = []

        for lot in inventory:
            if remaining_target <= SHARE_TOLERANCE:
                updated_inventory.append(lot)
                continue

            take = min(lot.shares, remaining_target)
            if take <= SHARE_TOLERANCE:
                updated_inventory.append(lot)
                continue

            basis_fraction = (
                lot.basis * (take / lot.shares) if lot.shares > SHARE_TOLERANCE else Decimal("0")
            )
            basis_total += basis_fraction
            remaining_target -= take

            remaining_shares = lot.shares - take
            remaining_basis = lot.basis - basis_fraction
            if remaining_shares > SHARE_TOLERANCE and remaining_basis > Decimal("0"):
                updated_inventory.append(
                    _LotSlice(
                        shares=remaining_shares,
                        basis=remaining_basis,
                    )
                )

        if remaining_target > SHARE_TOLERANCE:
            unresolved.append(item)
            lot_inventory[ticker] = updated_inventory
            continue

        lot_inventory[ticker] = updated_inventory

        try:
            resolve_transfer_basis_override(
                repository,
                account_name=account_name,
                account_number=account_number,
                instrument=item.instrument,
                activity_date=date.fromisoformat(item.activity_date),
                shares=item.shares,
                basis_total=basis_total,
                basis_per_share=None,
                trans_code=item.trans_code,
            )
        except CostBasisError as exc:
            errors.append((item, str(exc)))
            continue

        resolved_count += 1

        if not lot_inventory[ticker]:
            del lot_inventory[ticker]

    return resolved_count, unresolved, ambiguous, errors


def _build_lot_inventory(lots: Iterable[StoredExternalTaxLot]) -> dict[str, List["_LotSlice"]]:
    inventory: dict[str, List[_LotSlice]] = {}
    for lot in sorted(lots, key=lambda entry: (entry.ticker.upper(), entry.open_date)):
        shares = lot.shares
        basis = abs(lot.tax_cost)
        if shares <= SHARE_TOLERANCE or basis <= Decimal("0"):
            continue
        ticker = lot.ticker.strip().upper()
        inventory.setdefault(ticker, []).append(_LotSlice(shares=shares, basis=basis))
    return inventory


@dataclass
class _LotSlice:
    shares: Decimal
    basis: Decimal


def _parse_date(value: str) -> date:
    month, day, year = value.split("/")
    return date(int(year), int(month), int(day))


def _parse_decimal(token: str) -> Decimal:
    stripped = token.strip()
    negative = stripped.startswith("(") and stripped.endswith(")")
    cleaned = stripped.strip("()").replace(",", "")
    if cleaned.endswith("."):
        cleaned = cleaned[:-1]
    if not cleaned:
        raise ParsingError(f"Empty numeric token: {token!r}")
    try:
        value = Decimal(cleaned)
    except (InvalidOperation, ValueError) as exc:
        raise ParsingError(f"Invalid numeric token: {token!r}") from exc
    return -value if negative else value


def _parse_decimal_or_none(token: Optional[str]) -> Optional[Decimal]:
    if token is None:
        return None
    stripped = token.strip()
    if not stripped:
        return None
    try:
        return _parse_decimal(stripped)
    except ParsingError:
        return None


def _extract_ticker(description: str) -> str:
    if "(" in description and description.endswith(")"):
        return description.rsplit("(", 1)[-1].rstrip(")").strip() or description.split()[0]
    parts = description.split()
    return parts[-1] if parts else "UNKNOWN"


def _extract_security_id(remainder: str) -> Optional[str]:
    candidate = remainder[:12].strip()
    if candidate and CUSIP_PATTERN.match(candidate):
        return candidate
    return None


def _split_remainder(remainder: str) -> List[str]:
    return [part for part in re.split(r"\s{2,}", remainder.rstrip()) if part]


def _parse_parts(
    parts: List[str],
    security_id: Optional[str],
) -> tuple[str, Decimal, Optional[Decimal], Optional[str], List[str]]:
    idx = 0
    if security_id and parts and parts[0].lstrip("0") == security_id.lstrip("0"):
        idx += 1

    description_tokens: List[str] = []
    shares_decimal: Optional[Decimal] = None
    while idx < len(parts):
        token = parts[idx]
        if NUMERIC_TOKEN_PATTERN.match(token):
            shares_decimal = _parse_decimal(token)
            idx += 1
            break
        description_tokens.append(token)
        idx += 1

    if shares_decimal is None:
        raise ParsingError("Could not determine share quantity in tax lot row.")

    description = " ".join(description_tokens).strip() or "(unknown)"

    price_value: Optional[Decimal] = None
    if idx < len(parts):
        price_value = _parse_decimal_or_none(parts[idx])
        idx += 1

    status: Optional[str] = None
    value_tokens: List[str] = []
    for token in parts[idx:]:
        if token.endswith(" st") or token.endswith(" lt"):
            value_str, status_token = token.rsplit(" ", 1)
            if value_str:
                value_tokens.append(value_str)
            status = status or status_token
        elif token in {"st", "lt"}:
            status = token
        else:
            value_tokens.append(token)

    return description, shares_decimal, price_value, status, value_tokens


def _parse_value_tokens(
    tokens: List[str],
    raw_line: str,
) -> tuple[Optional[Decimal], Optional[Decimal], Decimal]:
    parsed_values = [_parse_decimal_or_none(token) for token in tokens if token]
    parsed_values = [value for value in parsed_values if value is not None]

    if len(parsed_values) == 3:
        book_cost, wash_adjustment, tax_cost = parsed_values
    elif len(parsed_values) == 2:
        book_cost, tax_cost = parsed_values[0], parsed_values[1]
        wash_adjustment = None
    elif len(parsed_values) == 1:
        book_cost = None
        wash_adjustment = None
        tax_cost = parsed_values[0]
    else:
        raise ParsingError(f"Missing tax cost value for row: {raw_line.strip()}")

    if tax_cost is None:
        raise ParsingError(f"Missing tax cost value for row: {raw_line.strip()}")

    return book_cost, wash_adjustment, tax_cost
