"""Target price calculation utilities."""

from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from typing import Iterable, List, Optional, Sequence


def parse_decimal(value: Optional[str]) -> Optional[Decimal]:
    """Parse a currency-like string into a Decimal."""
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None

    negative = False
    if text.startswith("(") and text.endswith(")"):
        negative = True
        text = text[1:-1]

    text = text.replace("$", "").replace(",", "").strip()
    if text.startswith("-"):
        negative = True
        text = text[1:]

    if not text:
        return None

    try:
        amount = Decimal(text)
    except InvalidOperation:
        return None

    if negative:
        amount = -amount
    return amount


def calculate_target_percents(bounds: Sequence[Decimal]) -> List[Decimal]:
    """Normalize target bounds into an ordered, de-duplicated percent list."""
    if not bounds:
        return []

    lower = bounds[0]
    upper = bounds[-1]
    if lower == upper:
        return [lower]

    midpoint = (lower + upper) / Decimal("2")
    percents: List[Decimal] = [lower, midpoint, upper]
    ordered_unique: List[Decimal] = []
    for value in percents:
        if value not in ordered_unique:
            ordered_unique.append(value)
    return ordered_unique


def compute_target_close_prices(
    trans_code: Optional[str],
    price_text: Optional[str],
    percents: Iterable[Decimal],
) -> Optional[List[Decimal]]:
    """Compute close prices for a trade given percent targets."""
    code = (trans_code or "").strip().upper()
    if code not in {"STO", "BTO"}:
        return None

    price = parse_decimal(price_text)
    if price is None:
        return None

    results: List[Decimal] = []
    for percent in percents:
        if code == "STO":
            target_price = price * (Decimal("1") - percent)
            target_price = max(target_price, Decimal("0"))
        else:
            target_price = price * (Decimal("1") + percent)
        results.append(target_price.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

    if code == "STO":
        results.sort(reverse=True)
    else:
        results.sort()
    return results
