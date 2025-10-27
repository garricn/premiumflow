"""Option contract parsing utilities."""

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Optional


@dataclass(frozen=True)
class OptionDescriptor:
    symbol: str
    expiration: str
    option_type: str
    strike: Decimal


_PATTERN = re.compile(
    r"^\s*(?P<symbol>[A-Za-z]+)\s+"
    r"(?P<expiration>\d{1,2}/\d{1,2}/\d{4})\s+"
    r"(?P<option_type>Call|Put)\s+\$?(?P<strike>[\d,]+(?:\.\d+)?)\s*$"
)


def parse_option_description(description: Optional[str]) -> Optional[OptionDescriptor]:
    if not description:
        return None

    match = _PATTERN.match(description)
    if not match:
        return None

    strike_text = match.group("strike").replace(",", "")
    try:
        strike = Decimal(strike_text)
    except InvalidOperation:
        return None

    return OptionDescriptor(
        symbol=match.group("symbol").upper(),
        expiration=match.group("expiration"),
        option_type=match.group("option_type").capitalize(),
        strike=strike,
    )
