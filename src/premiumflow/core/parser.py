"""
CSV parsing functionality for roll chain analysis.

This module handles parsing transaction data from CSV files.
"""

import csv
from datetime import datetime
from typing import List, Dict, Any
from .models import Transaction


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
    from decimal import Decimal, InvalidOperation
    import re

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
    import re

    # Pattern: TICKER $STRIKE TYPE DATE
    pattern = r"(\w+)\s+\$(\d+(?:\.\d+)?)\s+([CP])\s+(\d{4}-\d{2}-\d{2})"
    match = re.match(pattern, lookup_input.strip())

    if not match:
        raise ValueError(f"Invalid lookup format: {lookup_input}")

    symbol, strike, option_type, expiration = match.groups()
    return symbol, float(strike), option_type, expiration
