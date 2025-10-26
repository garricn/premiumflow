"""
Trace command for options CLI.

Provides the CLI entry point that displays the history of a roll chain.
"""

from __future__ import annotations

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from ..core.parser import get_options_transactions
from ..services.analysis import calculate_target_price_range
from ..services.chain_builder import detect_roll_chains
from ..services.display import (
    ensure_display_name,
    format_breakeven,
    format_currency,
    format_net_pnl,
    format_price_range,
    format_realized_pnl,
)
from .utils import parse_target_range


def _render_chain_summary(chain: dict, target_bounds: tuple) -> Panel:
    """Create a summary panel for a roll chain."""
    summary_lines = [
        f"Rolls: {chain.get('roll_count', 0)}",
        f"Start: {chain.get('start_date', 'N/A')} → End: {chain.get('end_date', 'N/A')}",
        f"Total Credits: {format_currency(chain.get('total_credits'))}",
        f"Total Debits: {format_currency(chain.get('total_debits'))}",
    ]

    if chain.get("status") == "CLOSED":
        summary_lines.append(f"Net P&L (after fees): {format_net_pnl(chain)}")
    else:
        summary_lines.append(f"Realized P&L (after fees): {format_realized_pnl(chain)}")
        summary_lines.append(f"Breakeven to close: {format_breakeven(chain)}")
        summary_lines.append(
            f"Target Price: {format_price_range(calculate_target_price_range(chain, target_bounds))}"
        )

    title = f"{ensure_display_name(chain)} ({chain.get('status', 'UNKNOWN')})"
    return Panel("\n".join(summary_lines), title=title, border_style="blue")


def _render_transactions_table(chain: dict) -> Table:
    """Create the transactions table for a roll chain."""
    table = Table(title="Transactions")
    table.add_column("Date", style="cyan")
    table.add_column("Code", style="magenta")
    table.add_column("Qty", justify="right")
    table.add_column("Price", justify="right")
    table.add_column("Amount", justify="right")
    table.add_column("Description", style="yellow", overflow="fold")

    for txn in chain.get("transactions", []):
        table.add_row(
            txn.get("Activity Date", ""),
            txn.get("Trans Code", ""),
            txn.get("Quantity", ""),
            txn.get("Price", ""),
            txn.get("Amount", ""),
            txn.get("Description", ""),
        )

    return table


@click.command()
@click.argument('display_name')
@click.argument('csv_file', type=click.Path(exists=True), required=False, default="all_transactions.csv")
@click.option(
    '--target',
    default='0.5-0.7',
    show_default=True,
    help='Target profit range as fraction of net credit (e.g. 0.5-0.7)',
)
def trace(display_name, csv_file, target):
    """Trace the full history of a roll chain by display name."""
    console = Console()
    target_bounds = parse_target_range(target)

    try:
        console.print(f"[blue]Tracing {display_name} in {csv_file}[/blue]")
        raw_transactions = get_options_transactions(csv_file)
        chains = detect_roll_chains(raw_transactions)

        display_key = display_name.strip().lower()
        matched = [
            chain for chain in chains
            if ensure_display_name(chain).lower() == display_key
        ]

        if not matched:
            console.print(f"[yellow]No roll chains found for {display_name}[/yellow]")
            return

        matched.sort(key=lambda chain: chain.get("start_date", ""))

        for index, chain in enumerate(matched, start=1):
            console.print(f"\n[bold]Chain {index}[/bold]")
            console.print(_render_chain_summary(chain, target_bounds))
            console.print(_render_transactions_table(chain))

    except click.ClickException:
        raise
    except Exception as exc:
        console.print(f"[red]Error: {exc}[/red]")
        raise click.Abort()
