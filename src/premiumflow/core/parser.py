"""
CSV parsing functionality for roll chain analysis.

This module handles parsing transaction data from CSV files.
"""

import csv
import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional

ALLOWED_OPTION_CODES = {"STO", "STC", "BTO", "BTC", "OASGN", "OEXP"}
CONTRACT_MULTIPLIER = Decimal("100")


class ImportValidationError(ValueError):
    """Raised when CSV input fails import validation."""


@dataclass
class NormalizedOptionTransaction:
    """Normalized representation of an option row for downstream processing."""

    activity_date: date
    process_date: Optional[date]
    settle_date: Optional[date]
    instrument: str
    description: str
    trans_code: str
    quantity: int
    price: Decimal
    amount: Optional[Decimal]
    strike: Decimal
    option_type: str
    expiration: date
    action: str
    raw: Dict[str, str]

    @property
    def symbol(self) -> str:
        return self.instrument


@dataclass
class ParsedImportResult:
    """Container for normalized import data and account metadata."""

    account_name: str
    account_number: Optional[str]
    transactions: List[NormalizedOptionTransaction]


def parse_date(date_str: str) -> datetime:
    """Parse date string in M/D/YYYY format."""
    return datetime.strptime(date_str, "%m/%d/%Y")


def is_options_transaction(row: Dict[str, str]) -> bool:
    """
    Determine if a transaction is options-related.

    Options transactions have:
    - Trans codes like BTC, STO, OASGN
    - Descriptions containing Call/Put with strike prices
    """
    trans_code = (row.get("Trans Code") or "").strip()
    description = (row.get("Description") or "").strip()

    # Check for options-specific transaction codes
    options_codes = {"BTC", "STO", "OASGN", "OEXP"}
    if trans_code in options_codes:
        return True

    # Check for Call/Put in description
    if "Call" in description or "Put" in description:
        return True

    return False


def is_call_option(description: str) -> bool:
    """Check if the option is a Call."""
    return "Call" in description


def is_put_option(description: str) -> bool:
    """Check if the option is a Put."""
    return "Put" in description


def format_position_spec(symbol: str, strike: float, option_type: str, expiration: str) -> str:
    """Format position specification for lookup (legacy compatibility)."""
    return f"{symbol} ${strike} {option_type} {expiration}"


def parse_lookup_input(lookup_input: str) -> tuple:
    """Parse lookup input string (legacy compatibility)."""
    # Pattern: TICKER $STRIKE TYPE DATE
    pattern = r"(\w+)\s+\$(\d+(?:\.\d+)?)\s+([CP])\s+(\d{4}-\d{2}-\d{2})"
    match = re.match(pattern, lookup_input.strip())

    if not match:
        raise ValueError(f"Invalid lookup format: {lookup_input}")

    symbol, strike, option_type, expiration = match.groups()
    return symbol, float(strike), option_type, expiration


def load_option_transactions(
    csv_file: str,
    *,
    account_name: str,
    account_number: Optional[str] = None,
) -> ParsedImportResult:
    """
    Validate and normalize option transactions from a CSV file.

    Parameters
    ----------
    csv_file:
        Path to the Robinhood-style CSV export.
    account_name:
        Required CLI-supplied account label; must contain non-whitespace characters.
    account_number:
        Optional account identifier. When provided, must contain non-whitespace characters.

    Returns
    -------
    ParsedImportResult
        Aggregated account metadata and the list of normalized option rows
        (filtered to ``ALLOWED_OPTION_CODES``).

    Raises
    ------
    ImportValidationError
        When the CSV header is missing, account metadata is invalid, or a row
        fails validation. Errors include 1-based row numbers.
    """

    normalized: List[NormalizedOptionTransaction] = []
    normalized_account_name, normalized_account_number = _validate_account_metadata(
        account_name, account_number
    )
    with open(csv_file, "r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ImportValidationError("CSV file is empty or missing a header row.")

        for index, row in enumerate(reader, start=2):  # header counted as row 1
            if _row_is_blank(row):
                continue  # skip blank lines

            try:
                normalized_row = _normalize_row(row, index)
            except ImportValidationError as exc:
                raise ImportValidationError(f"Row {index}: {exc}") from exc

            if normalized_row is not None:
                normalized.append(normalized_row)

    return ParsedImportResult(
        account_name=normalized_account_name,
        account_number=normalized_account_number,
        transactions=normalized,
    )


def _normalize_row(row: Dict[str, str], row_number: int) -> Optional[NormalizedOptionTransaction]:
    """Normalize a CSV row; returns None for non-option transactions."""

    trans_code = _parse_trans_code(row, row_number)
    if not trans_code or trans_code not in ALLOWED_OPTION_CODES:
        return None

    activity_date = _parse_date_field(row, "Activity Date", row_number)
    process_date = _parse_optional_date_field(row, "Process Date", row_number)
    settle_date = _parse_optional_date_field(row, "Settle Date", row_number)
    instrument = _require_field(row, "Instrument", row_number)
    description = _normalize_description(row, row_number)
    amount = _parse_money(row, "Amount", row_number, allow_negative=True, required=False)
    quantity = _parse_quantity(row, "Quantity", row_number)

    price_value = _parse_money(
        row,
        "Price",
        row_number,
        allow_negative=False,
        required=False,
    )
    if price_value is None:
        price = _infer_price_from_amount(amount, quantity, trans_code, row_number)
    else:
        price = price_value

    option_type, strike, expiration = _parse_option_details(description, row_number)
    action = "SELL" if trans_code in {"STO", "STC"} else "BUY"

    return NormalizedOptionTransaction(
        activity_date=activity_date,
        process_date=process_date,
        settle_date=settle_date,
        instrument=instrument,
        description=description,
        trans_code=trans_code,
        quantity=abs(quantity),
        price=price,
        amount=amount,
        strike=strike,
        option_type=option_type,
        expiration=expiration,
        action=action,
        raw=dict(row),
    )


def _parse_trans_code(row: Dict[str, str], row_number: int) -> Optional[str]:
    trans_code_raw = row.get("Trans Code")
    if trans_code_raw is None:
        raise ImportValidationError('Missing required column "Trans Code".')
    trans_code = trans_code_raw.strip().upper()
    if not trans_code:
        return None
    return trans_code


def _validate_account_metadata(
    account_name: str, account_number: Optional[str]
) -> tuple[str, Optional[str]]:
    if account_name is None:
        raise ImportValidationError("--account-name is required.")
    normalized_name = account_name.strip()
    if not normalized_name:
        raise ImportValidationError("--account-name is required.")

    if account_number is None:
        return normalized_name, None

    normalized_number = account_number.strip()
    if not normalized_number:
        raise ImportValidationError("--account-number cannot be blank.")

    return normalized_name, normalized_number


def _require_field(row: Dict[str, str], field: str, row_number: int) -> str:
    value = row.get(field)
    if value is None:
        raise ImportValidationError(f'Missing required column "{field}".')

    stripped = value.strip()
    if not stripped:
        raise ImportValidationError(f'Column "{field}" cannot be blank.')
    return stripped


def _normalize_description(row: Dict[str, str], row_number: int) -> str:
    raw = _require_field(row, "Description", row_number)
    return " ".join(raw.split())


def _parse_date_field(row: Dict[str, str], field: str, row_number: int) -> date:
    value = _require_field(row, field, row_number)
    try:
        return datetime.strptime(value, "%m/%d/%Y").date()
    except ValueError as exc:
        raise ImportValidationError(f'Invalid date in "{field}": {value}') from exc


def _parse_optional_date_field(row: Dict[str, str], field: str, row_number: int) -> Optional[date]:
    value = row.get(field)
    if not value or not value.strip():
        return None
    try:
        return datetime.strptime(value.strip(), "%m/%d/%Y").date()
    except ValueError as exc:
        raise ImportValidationError(f'Invalid date in "{field}": {value}') from exc


def _parse_quantity(row: Dict[str, str], field: str, row_number: int) -> int:
    value = _require_field(row, field, row_number)
    cleaned = value.replace(",", "").strip()
    negative = cleaned.startswith("(") and cleaned.endswith(")")
    if negative:
        cleaned = cleaned[1:-1]

    try:
        quantity = int(cleaned)
    except ValueError as exc:
        raise ImportValidationError(f'Invalid integer in "{field}": {value}') from exc

    return -quantity if negative else quantity


def _parse_money(
    row: Dict[str, str],
    field: str,
    row_number: int,
    *,
    allow_negative: bool,
    required: bool = True,
    allow_parenthesized_positive: bool = False,
) -> Optional[Decimal]:
    raw_value = row.get(field)
    if raw_value is None:
        if required:
            raise ImportValidationError(f'Missing required column "{field}".')
        return None

    stripped = raw_value.strip()
    if not stripped:
        if required:
            raise ImportValidationError(f'Column "{field}" cannot be blank.')
        return None

    negative = stripped.startswith("(") and stripped.endswith(")")
    if negative:
        stripped = stripped[1:-1]

    cleaned = stripped.replace("$", "").replace(",", "")

    try:
        value = Decimal(cleaned)
    except InvalidOperation as exc:
        raise ImportValidationError(f'Invalid decimal in "{field}": {raw_value}') from exc

    if negative:
        value = -value

    if not allow_negative and value < 0:
        if negative and allow_parenthesized_positive:
            return abs(value)
        raise ImportValidationError(f'Column "{field}" must be non-negative.')

    return value


def _infer_price_from_amount(
    amount: Optional[Decimal], quantity: int, trans_code: str, row_number: int
) -> Decimal:
    """
    Derive a per-contract price when broker exports omit the explicit Price field.

    Robinhood occasionally omits prices for assignments/exercises but still
    provides an ``Amount``. Infer the price by dividing the absolute cash flow
    by the contract count and standard multiplier. This preserves downstream
    calculations that expect a per-contract price while accepting the raw CSV.
    """

    if amount is None:
        if trans_code in {"OASGN", "OEXP"}:
            return Decimal("0.00")
        raise ImportValidationError('Column "Price" cannot be blank.')

    contracts = abs(quantity)
    if contracts == 0:
        raise ImportValidationError(
            'Column "Price" cannot be inferred because "Quantity" evaluates to zero.'
        )

    inferred = abs(amount) / (Decimal(contracts) * CONTRACT_MULTIPLIER)
    return inferred.quantize(Decimal("0.01"))


def _row_is_blank(row: Dict[str, str]) -> bool:
    if not row:
        return True

    for value in row.values():
        if isinstance(value, list):
            if any(item and item.strip() for item in value):
                return False
            continue
        if value and value.strip():
            return False
    return True


def _parse_option_details(description: str, row_number: int) -> tuple[str, Decimal, date]:
    option_type: Optional[str] = None
    lowered = description.lower()

    if " call" in lowered or lowered.endswith("call"):
        option_type = "CALL"
    elif " put" in lowered or lowered.endswith("put"):
        option_type = "PUT"
    else:
        raise ImportValidationError("Description must include 'Call' or 'Put'.")

    strike_match = re.search(r"\$(\d+(?:\.\d+)?)", description)
    if not strike_match:
        raise ImportValidationError("Unable to determine strike price from description.")
    strike = Decimal(strike_match.group(1))

    expiration_match = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", description)
    if not expiration_match:
        raise ImportValidationError("Unable to determine expiration date from description.")
    month, day_str, year_str = expiration_match.groups()
    try:
        expiration = date(int(year_str), int(month), int(day_str))
    except ValueError as exc:
        raise ImportValidationError("Expiration date in description is invalid.") from exc

    return option_type, strike, expiration
