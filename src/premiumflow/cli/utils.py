"""
Shared CLI utilities and helper functions.

This module contains common utilities used across CLI commands.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, Iterable, List

import click
from rich.table import Table

from ..services.options import parse_option_description
from ..services.targets import compute_target_close_prices
from ..services.display import format_option_display, format_target_close_prices
from ..services.cli_helpers import parse_target_range as _parse_target_range


def parse_target_range(target: str) -> tuple[Decimal, Decimal]:
    """Parse target range string with Click error handling."""
    try:
        return _parse_target_range(target)
    except ValueError as e:
        raise click.BadParameter(str(e)) from e


def prepare_transactions_for_display(
    transactions: Iterable[Dict[str, Any]],
    target_percents: List[Decimal],
) -> List[Dict[str, str]]:
    """Prepare transactions for display formatting."""
    rows: List[Dict[str, str]] = []
    for txn in transactions:
        parsed_option = parse_option_description(txn.get('Description', ''))
        formatted_desc, expiration = format_option_display(parsed_option, txn.get('Description', ''))
        target_prices = compute_target_close_prices(
            txn.get('Trans Code'),
            txn.get('Price'),
            target_percents,
        )

        rows.append(
            {
                "date": txn.get('Activity Date', ''),
                "symbol": (txn.get('Instrument') or '').strip(),
                "expiration": expiration,
                "code": txn.get('Trans Code', ''),
                "quantity": txn.get('Quantity', ''),
                "price": txn.get('Price', ''),
                "description": formatted_desc,
                "target_close": format_target_close_prices(target_prices),
            }
        )

    return rows


def create_transactions_table(transactions: List[Dict[str, str]]) -> Table:
    """Create a Rich table for displaying transactions."""
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Date", style="dim", width=10)
    table.add_column("Symbol", style="cyan", width=8)
    table.add_column("Expiration", style="dim", width=10)
    table.add_column("Code", style="yellow", width=6)
    table.add_column("Quantity", justify="right", width=8)
    table.add_column("Price", justify="right", width=8)
    table.add_column("Description", width=30)
    table.add_column("Target Close", justify="right", width=12)

    for txn in transactions:
        table.add_row(
            txn["date"],
            txn["symbol"],
            txn["expiration"],
            txn["code"],
            txn["quantity"],
            txn["price"],
            txn["description"],
            txn["target_close"],
        )

    return table