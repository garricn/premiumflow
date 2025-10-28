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

from .models import Transaction

ALLOWED_OPTION_CODES = {"STO", "STC", "BTO", "BTC", "OASGN"}


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
    fees: Decimal
    raw: Dict[str, str]

    @property
    def symbol(self) -> str:
        return self.instrument


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
    options_codes = {"BTC", "STO", "OASGN"}
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


def parse_transaction_row(row: Dict[str, str]) -> Transaction:
    """Parse a CSV row into a Transaction object."""
    # Extract symbol from Instrument field
    symbol = row.get("Instrument", "").strip()

    # Parse description to extract strike, type, and expiration
    description = row.get("Description", "")

    # Extract strike price using regex
    strike_match = re.search(r"\$(\d+(?:\.\d+)?)", description)
    if not strike_match:
        raise ValueError(f"Could not extract strike price from: {description}")
    strike = Decimal(strike_match.group(1))

    # Determine option type
    option_type = "C" if is_call_option(description) else "P"

    # Extract expiration date from description
    # Format: "SYMBOL MM/DD/YYYY Call/Put $STRIKE"
    # Example: "TSLA 10/17/2025 Call $200.00" -> "2025-10-17"
    expiration_match = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", description)
    if not expiration_match:
        raise ValueError(f"Could not extract expiration date from: {description}")

    month, day, year = expiration_match.groups()
    # Format as YYYY-MM-DD
    expiration = f"{year}-{month.zfill(2)}-{day.zfill(2)}"

    # Parse quantity and price
    quantity_str = (row.get("Quantity") or "0").replace(",", "")
    quantity = int(quantity_str or "0")
    price_raw = (row.get("Price") or "0").strip()
    price_str = price_raw.replace("$", "").replace(",", "")
    if not price_str:
        price_str = "0"
    try:
        price = Decimal(price_str)
    except InvalidOperation:
        price = Decimal("0")

    # Determine action based on transaction code
    trans_code = row.get("Trans Code", "").strip()
    if trans_code in ["STO", "STC"]:
        action = "SELL"  # Both are sell transactions
    elif trans_code in ["BTO", "BTC"]:
        action = "BUY"  # Both are buy transactions
    else:
        action = "BUY"  # Default assumption for unknown codes

    # Parse date
    date = parse_date(row.get("Activity Date", ""))

    return Transaction(
        symbol=symbol,
        strike=strike,
        option_type=option_type,
        expiration=expiration,
        quantity=quantity,
        price=price,
        action=action,
        date=date,
    )


def parse_csv_file(csv_file: str) -> List[Transaction]:
    """Parse a CSV file and return a list of Transaction objects."""
    transactions = []

    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Skip empty rows
            if not row.get("Activity Date"):
                continue

            if is_options_transaction(row):
                try:
                    transaction = parse_transaction_row(row)
                    transactions.append(transaction)
                except (ValueError, KeyError) as e:
                    print(f"Warning: Skipping invalid transaction: {e}")
                    continue

    return transactions


def get_options_transactions(csv_file: str) -> List[Dict[str, str]]:
    """Extract all options transactions from CSV file (legacy function)."""
    options_txns = []

    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Skip empty rows
            if not row.get("Activity Date"):
                continue

            if is_options_transaction(row):
                options_txns.append(row)

    return options_txns


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
    csv_file: str, *, regulatory_fee: Decimal
) -> List[NormalizedOptionTransaction]:
    """
    Validate and normalize option transactions from a CSV file.

    Returns only rows whose transaction code is in ``ALLOWED_OPTION_CODES``.
    """

    normalized: List[NormalizedOptionTransaction] = []
    reg_fee = _coerce_regulatory_fee(regulatory_fee)

    with open(csv_file, "r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ImportValidationError("CSV file is empty or missing a header row.")

        for index, row in enumerate(reader, start=2):  # header counted as row 1
            if not row or not any(value and value.strip() for value in row.values()):
                continue  # skip blank lines

            try:
                normalized_row = _normalize_row(row, reg_fee, index)
            except ImportValidationError as exc:
                raise ImportValidationError(f"Row {index}: {exc}") from exc

            if normalized_row is not None:
                normalized.append(normalized_row)

    return normalized


def _normalize_row(
    row: Dict[str, str], regulatory_fee: Decimal, row_number: int
) -> Optional[NormalizedOptionTransaction]:
    """Normalize a CSV row; returns None for non-option transactions."""

    trans_code_raw = row.get("Trans Code")
    if trans_code_raw is None:
        raise ImportValidationError('Missing required column "Trans Code".')
    trans_code = trans_code_raw.strip().upper()
    if not trans_code:
        raise ImportValidationError('Column "Trans Code" cannot be blank.')
    if trans_code not in ALLOWED_OPTION_CODES:
        return None

    activity_date = _parse_date_field(row, "Activity Date", row_number)
    process_date = _parse_optional_date_field(row, "Process Date", row_number)
    settle_date = _parse_optional_date_field(row, "Settle Date", row_number)
    instrument = _require_field(row, "Instrument", row_number)
    description = _normalize_description(row, row_number)
    quantity = _parse_quantity(row, "Quantity", row_number)
    price_value = _parse_money(row, "Price", row_number, allow_negative=False)
    if price_value is None:
        raise ImportValidationError('Column "Price" cannot be blank.')
    price = price_value
    amount = _parse_money(row, "Amount", row_number, allow_negative=True, required=False)

    option_type, strike, expiration = _parse_option_details(description, row_number)
    action = "SELL" if trans_code in {"STO", "STC"} else "BUY"

    commission = _parse_money(row, "Commission", row_number, allow_negative=True, required=False)
    if commission is not None:
        fees = abs(commission)
    else:
        fees = regulatory_fee * abs(quantity)

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
        fees=fees,
        raw=dict(row),
    )


def _coerce_regulatory_fee(value: Decimal) -> Decimal:
    try:
        fee = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ImportValidationError(f"Invalid regulatory fee value: {value!r}") from exc

    if fee < 0:
        raise ImportValidationError("Regulatory fee must be non-negative.")
    return fee


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
        raise ImportValidationError(f'Column "{field}" must be non-negative.')

    return value


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
