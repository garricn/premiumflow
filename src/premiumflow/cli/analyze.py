"""
Analyze command for premiumflow CLI.

This module provides the analyze command for detecting and analyzing roll chains
from CSV transaction data.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Tuple

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from ..core.parser import load_option_transactions
from ..services.analysis import (
    calculate_target_price_range,
    is_open_chain,
)
from ..services.chain_builder import detect_roll_chains
from ..services.cli_helpers import parse_target_range as _parse_target_range
from ..services.display import (
    ensure_display_name,
    format_breakeven,
    format_currency,
    format_net_pnl,
    format_percent,
    format_price_range,
    format_realized_pnl,
)
from ..services.targets import calculate_target_percents
from ..services.transactions import normalized_to_csv_dicts


def parse_target_range(target: str) -> Tuple[Decimal, Decimal]:
    """Parse target range string with Click error handling."""
    try:
        return _parse_target_range(target)
    except ValueError as e:
        raise click.BadParameter(str(e)) from e


@click.command()
@click.argument("csv_file", type=click.Path(exists=True))
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "summary", "raw"]),
    default="table",
    help="Output format",
)
@click.option("--open-only", is_flag=True, help="Only display roll chains with open positions")
@click.option(
    "--target",
    default="0.5-0.7",
    show_default=True,
    help="Target profit range as fraction of net credit (e.g. 0.5-0.7)",
)
def analyze(csv_file, output_format, open_only, target):
    """Analyze roll chains from a CSV file."""
    console = Console()

    # Parse target range first to get proper Click error handling
    target_bounds = parse_target_range(target)

    try:
        # Parse CSV file
        console.print(f"[blue]Parsing {csv_file}...[/blue]")
        parsed = load_option_transactions(
            csv_file,
            account_name=Path(csv_file).stem or "Analysis Account",
        )
        transactions = parsed.transactions
        console.print(f"[green]Found {len(transactions)} options transactions[/green]")

        # Convert normalized transactions for chain detection
        raw_transactions = normalized_to_csv_dicts(transactions)

        # Detect roll chains
        console.print("[blue]Detecting roll chains...[/blue]")
        chains = detect_roll_chains(raw_transactions)
        console.print(f"[green]Found {len(chains)} roll chains[/green]")

        if open_only:
            chains = [chain for chain in chains if is_open_chain(chain)]
            console.print(f"[cyan]Open chains: {len(chains)}[/cyan]")
        target_percents = calculate_target_percents(target_bounds)
        target_label = (
            "Target (" + ", ".join(format_percent(value) for value in target_percents) + ")"
        )

        # Display results
        if output_format == "table":
            table = Table(title="Roll Chains Analysis")

            table.add_column("Display", style="cyan", no_wrap=True)
            table.add_column("Expiration", style="magenta", no_wrap=True)
            table.add_column("Status", style="yellow", no_wrap=True)
            table.add_column("Credits", justify="right", no_wrap=True)
            table.add_column("Debits", justify="right", no_wrap=True)
            table.add_column("P&L", justify="right", no_wrap=True)
            table.add_column("Breakeven", justify="right", no_wrap=True)
            table.add_column(target_label, justify="right", no_wrap=True)

            for _, chain in enumerate(chains, 1):
                table.add_row(
                    ensure_display_name(chain),
                    chain.get("expiration", "") or "N/A",
                    chain.get("status", "UNKNOWN"),
                    format_currency(chain.get("total_credits")),
                    format_currency(chain.get("total_debits")),
                    format_net_pnl(chain),
                    format_breakeven(chain),
                    format_price_range(calculate_target_price_range(chain, target_bounds)),
                )

            console.print(table)
        elif output_format == "summary":
            for i, chain in enumerate(chains, 1):
                console.print(f"\n[bold]Chain {i}:[/bold]")
                credits = format_currency(chain.get("total_credits"))
                debits = format_currency(chain.get("total_debits"))
                body_lines = [
                    f"Display: {ensure_display_name(chain)}",
                    f"Expiration: {chain.get('expiration', '') or 'N/A'}",
                    f"Status: {chain.get('status', 'UNKNOWN')} (Rolls: {chain.get('roll_count', 0)})",
                    f"Period: {chain.get('start_date', 'N/A')} → {chain.get('end_date', 'N/A')}",
                    f"Credits: {credits}",
                    f"Debits: {debits}",
                ]

                if chain.get("status") == "CLOSED":
                    body_lines.append(f"Net P&L: {format_net_pnl(chain)}")
                else:
                    body_lines.append(f"Realized P&L: {format_realized_pnl(chain)}")
                    body_lines.append(f"Breakeven to close: {format_breakeven(chain)}")
                    body_lines.append(
                        f"Target Price: {format_price_range(calculate_target_price_range(chain, target_bounds))}"
                    )

                console.print(
                    Panel(
                        "\n".join(body_lines),
                        title=ensure_display_name(chain),
                        border_style="blue",
                    )
                )
        else:  # raw
            for i, chain in enumerate(chains, 1):
                console.print(f"\nChain {i}: {chain}")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort() from e
