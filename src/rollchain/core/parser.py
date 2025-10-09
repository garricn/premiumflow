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
    return datetime.strptime(date_str, '%m/%d/%Y')


def is_options_transaction(row: Dict[str, str]) -> bool:
    """
    Determine if a transaction is options-related.
    
    Options transactions have:
    - Trans codes like BTC, STO, OASGN
    - Descriptions containing Call/Put with strike prices
    """
    trans_code = (row.get('Trans Code') or '').strip().upper()
    description = (row.get('Description') or '').strip()
    
    # Check for options-specific transaction codes
    options_codes = {'BTC', 'BTO', 'STC', 'STO', 'OASGN'}
    if trans_code in options_codes:
        return True
    
    # Check for Call/Put in description
    if 'Call' in description or 'Put' in description:
        return True
    
    return False


def is_call_option(description: str) -> bool:
    """Check if the option is a Call."""
    return 'Call' in description


def is_put_option(description: str) -> bool:
    """Check if the option is a Put."""
    return 'Put' in description


def parse_transaction_row(row: Dict[str, str]) -> Transaction:
    """Parse a CSV row into a Transaction object."""
    from decimal import Decimal
    import re
    
    # Extract symbol from Instrument field
    symbol = row.get('Instrument', '').strip()
    
    # Parse description to extract strike, type, and expiration
    description = row.get('Description', '')
    
    # Extract strike price using regex
    strike_match = re.search(r'\$(\d+(?:\.\d+)?)', description)
    if not strike_match:
        raise ValueError(f"Could not extract strike price from: {description}")
    strike = Decimal(strike_match.group(1))
    
    # Determine option type
    option_type = 'C' if is_call_option(description) else 'P'
    
    # Extract expiration date from the description. Most brokerage CSV exports
    # format the option description as "TICKER MM/DD/YYYY Call $STRIKE" (or Put).
    exp_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', description)
    if not exp_match:
        raise ValueError(f"Could not extract expiration from: {description}")

    expiration_date = datetime.strptime(exp_match.group(1), "%m/%d/%Y")
    expiration = expiration_date.strftime("%Y-%m-%d")
    
    # Parse quantity and price
    quantity = int(row.get('Quantity', '0'))
    price_str = row.get('Price', '0').replace('$', '').replace(',', '')
    price = Decimal(price_str)
    
    # Determine action based on transaction code
    trans_code = row.get('Trans Code', '').strip().upper()
    action_map = {
        'STO': 'SELL',
        'BTO': 'BUY',
        'STC': 'SELL',
        'BTC': 'BUY',
        'OASGN': 'SELL',
    }
    action = action_map.get(trans_code)
    if action is None:
        # Default to BUY to maintain previous behaviour for unknown codes while
        # still providing a sensible fallback.
        action = 'BUY'
    
    # Parse date
    date = parse_date(row.get('Activity Date', ''))
    
    return Transaction(
        symbol=symbol,
        strike=strike,
        option_type=option_type,
        expiration=expiration,
        quantity=quantity,
        price=price,
        action=action,
        date=date
    )


def parse_csv_file(csv_file: str) -> List[Transaction]:
    """Parse a CSV file and return a list of Transaction objects."""
    transactions = []
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Skip empty rows
            if not row.get('Activity Date'):
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
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Skip empty rows
            if not row.get('Activity Date'):
                continue
                
            if is_options_transaction(row):
                options_txns.append(row)
    
    return options_txns


def format_position_spec(
    symbol: str,
    strike: float = None,
    option_type: str = None,
    expiration: str = None
) -> str:
    """Format position specification for lookup (legacy compatibility).

    This function supports both the legacy single-argument form where a full
    description string is provided (e.g. "TSLA 11/21/2025 Call $550.00") and
    the newer explicit component form. In either case, the output matches the
    lookup specification expected by ``parse_lookup_input``.
    """
    import re

    # Legacy form: a single description string containing all the details.
    if strike is None and option_type is None and expiration is None:
        description = symbol
        pattern = (
            r"^(?P<symbol>\w+)\s+"
            r"(?P<expiration>\d{1,2}/\d{1,2}/\d{4}|\d{4}-\d{2}-\d{2})\s+"
            r"(?P<option_type>Call|Put|C|P)\s+"
            r"\$(?P<strike>\d+(?:\.\d+)?)"
        )
        match = re.match(pattern, description.strip(), re.IGNORECASE)
        if not match:
            raise ValueError(f"Invalid position description: {description}")

        symbol = match.group('symbol').upper()
        option_type_match = match.group('option_type').upper()[0]
        option_type_formatted = 'CALL' if option_type_match == 'C' else 'PUT'
        expiration = match.group('expiration')
        strike_value = match.group('strike')
        try:
            strike_float = float(strike_value)
        except ValueError:
            pass
        else:
            if strike_float.is_integer():
                strike_value = str(int(strike_float))
    else:
        if strike is None or option_type is None or expiration is None:
            raise ValueError("format_position_spec requires strike, option_type, and expiration")

        symbol = symbol.upper()
        option_type_input = str(option_type).strip().upper()
        if option_type_input not in {'C', 'P', 'CALL', 'PUT'}:
            raise ValueError("option_type must be 'C' or 'P'")
        option_type_formatted = 'CALL' if option_type_input.startswith('C') else 'PUT'

        expiration = str(expiration).strip()
        strike_str = str(strike).strip()
        try:
            strike_float = float(strike_str)
        except ValueError:
            strike_value = strike_str
        else:
            strike_value = str(int(strike_float)) if strike_float.is_integer() else strike_str

    return f"{symbol} ${strike_value} {option_type_formatted} {expiration}"


def parse_lookup_input(lookup_input: str) -> Dict[str, Any]:
    """Parse lookup input string (legacy compatibility).

    The parser accepts either ``C``/``P`` or ``CALL``/``PUT`` (case-insensitive)
    and supports both ``YYYY-MM-DD`` and ``MM/DD/YYYY`` expiration formats.
    A dictionary matching the legacy return signature is returned for backwards
    compatibility with existing consumers.
    """
    import re

    pattern = (
        r"^(?P<ticker>\w+)\s+"
        r"\$(?P<strike>\d+(?:\.\d+)?)\s+"
        r"(?P<option_type>CALL|PUT|C|P)\s+"
        r"(?P<expiration>\d{4}-\d{2}-\d{2}|\d{1,2}/\d{1,2}/\d{4})$"
    )

    match = re.match(pattern, lookup_input.strip(), re.IGNORECASE)
    if not match:
        raise ValueError(f"Invalid lookup format: {lookup_input}")

    from datetime import datetime
    from decimal import Decimal

    ticker = match.group('ticker').upper()
    strike_value = match.group('strike')
    strike_decimal = Decimal(strike_value)
    if strike_decimal == strike_decimal.to_integral():
        strike_value = str(int(strike_decimal))
    option_type_input = match.group('option_type').upper()
    option_type = 'CALL' if option_type_input.startswith('C') else 'PUT'
    expiration_input = match.group('expiration')
    expiration_display = expiration_input
    expiration_iso = None
    if '-' in expiration_input:
        dt = datetime.strptime(expiration_input, "%Y-%m-%d")
        expiration_display = f"{dt.month}/{dt.day}/{dt.year}"
        expiration_iso = expiration_input
    else:
        expiration_iso = datetime.strptime(expiration_input, "%m/%d/%Y").strftime("%Y-%m-%d")

    return {
        'ticker': ticker,
        'symbol': ticker,
        'strike': strike_value,
        'option_type': option_type,
        'option_type_code': option_type[0],
        'expiration': expiration_display,
        'expiration_iso': expiration_iso,
    }
