"""Shared CLI helper utilities."""

from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Dict, List, Optional, Tuple


def is_open_chain(chain: Dict[str, Any]) -> bool:
    """Determine whether a detected chain is still open."""
    status = (chain.get("status") or "").upper()
    if status in {"OPEN", "CLOSED"}:
        return status == "OPEN"

    transactions: List[Dict[str, Any]] = chain.get("transactions") or []
    if not transactions:
        return False
    last_code = (transactions[-1].get("Trans Code") or "").strip().upper()
    return last_code in {"STO", "BTO"}


def parse_target_range(target: str) -> Tuple[Decimal, Decimal]:
    """Parse target range string into decimal bounds."""
    try:
        parts = target.split("-")
        if len(parts) != 2:
            raise ValueError(f"Invalid target range format: {target}")
        lower = Decimal(parts[0].strip())
        upper = Decimal(parts[1].strip())
        if lower >= upper:
            raise ValueError(f"Lower bound must be less than upper bound: {target}")
        return lower, upper
    except (ValueError, IndexError) as exc:
        raise ValueError(f"Invalid target range format: {target}") from exc


def format_percent(value: Decimal) -> str:
    """Format a decimal as a percentage string."""
    percent = (value * Decimal("100")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    text = f"{percent:,.2f}"
    if text.endswith(".00"):
        text = text[:-3]
    elif text.endswith("0"):
        text = text[:-1]
    return f"{text}%"


def filter_open_chains(chains: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filter a list of chains to only include those with open positions."""
    return [chain for chain in chains if is_open_chain(chain)]


def format_expiration_date(expiration: str) -> str:
    """Format expiration date from YYYY-MM-DD to MM/DD/YYYY."""
    try:
        parts = expiration.split("-")
        if len(parts) != 3:
            return expiration
        year_text, month_text, day_text = parts
        return f"{int(month_text):02d}/{int(day_text):02d}/{year_text}"
    except (ValueError, IndexError):
        return expiration


def create_target_label(target_percents: List[Decimal]) -> str:
    """Create a target label string from target percentages."""
    return "Target (" + ", ".join(format_percent(value) for value in target_percents) + ")"


def format_account_label(account_name: Optional[str], account_number: Optional[str]) -> str:
    """Format account name with optional account number for display."""
    if not account_name:
        return "All Accounts"
    if not account_number:
        return account_name
    return f"{account_name} ({account_number})"
