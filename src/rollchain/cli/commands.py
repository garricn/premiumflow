"""
Command-line interface for rollchain.

This module provides the CLI commands using Click.
"""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Iterable, List, Optional, Tuple

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from ..core.parser import get_options_transactions, parse_csv_file, parse_lookup_input
from ..services.chain_builder import detect_roll_chains
from ..services.options import OptionDescriptor, parse_option_description
from ..services.targets import calculate_target_percents, compute_target_close_prices
from ..services.transactions import (
    filter_open_positions,
    filter_transactions_by_option_type,
    filter_transactions_by_ticker,
)
from ..services.analysis import (
    is_open_chain,
    calculate_realized_pnl,
    calculate_target_price_range,
)
from ..services.display import (
    format_currency,
    format_breakeven,
    format_percent,
    format_price_range,
    format_target_close_prices,
    ensure_display_name,
    format_option_display,
    format_net_pnl,
    format_realized_pnl,
)
from ..services.json_serializer import (
    serialize_decimal,
    serialize_transaction,
    serialize_chain,
    build_ingest_payload,
)
from ..services.cli_helpers import parse_target_range as _parse_target_range


def parse_target_range(target: str) -> Tuple[Decimal, Decimal]:
    """Parse target range string with Click error handling."""
    try:
        return _parse_target_range(target)
    except ValueError as e:
        raise click.BadParameter(str(e)) from e


def prepare_transactions_for_display(
    transactions: Iterable[Dict[str, Any]],
    target_percents: List[Decimal],
) -> List[Dict[str, str]]:
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


@click.group()
@click.version_option(version="0.1.0")
def main():
    """RollChain - Options trading roll chain analysis tool."""
    pass


@main.command()
@click.argument('csv_file', type=click.Path(exists=True))
@click.option('--format', 'output_format',
              type=click.Choice(['table', 'summary', 'raw']),
              default='table',
              help='Output format')
@click.option('--open-only', is_flag=True,
              help='Only display roll chains with open positions')
@click.option('--target', default='0.5-0.7', show_default=True,
              help='Target profit range as fraction of net credit (e.g. 0.5-0.7)')
def analyze(csv_file, output_format, open_only, target):
    """Analyze roll chains from a CSV file."""
    console = Console()
    
    # Parse target range first to get proper Click error handling
    target_bounds = parse_target_range(target)
    
    try:
        # Parse CSV file
        console.print(f"[blue]Parsing {csv_file}...[/blue]")
        transactions = parse_csv_file(csv_file)
        console.print(f"[green]Found {len(transactions)} options transactions[/green]")
        
        # Get raw transaction data for chain detection
        raw_transactions = get_options_transactions(csv_file)
        
        # Detect roll chains
        console.print("[blue]Detecting roll chains...[/blue]")
        chains = detect_roll_chains(raw_transactions)
        console.print(f"[green]Found {len(chains)} roll chains[/green]")

        if open_only:
            chains = [chain for chain in chains if is_open_chain(chain)]
            console.print(f"[cyan]Open chains: {len(chains)}[/cyan]")
        target_percents = calculate_target_percents(target_bounds)
        target_label = "Target (" + ", ".join(format_percent(value) for value in target_percents) + ")"
        
        # Display results
        if output_format == 'table':
            table = Table(title="Roll Chains Analysis")

            table.add_column("Display", style="cyan", no_wrap=True)
            table.add_column("Expiration", style="magenta", no_wrap=True)
            table.add_column("Status", style="yellow", no_wrap=True)
            table.add_column("Credits", justify="right", no_wrap=True)
            table.add_column("Debits", justify="right", no_wrap=True)
            table.add_column("P&L", justify="right", no_wrap=True)
            table.add_column("Breakeven", justify="right", no_wrap=True)
            table.add_column(target_label, justify="right", no_wrap=True)

            for idx, chain in enumerate(chains, 1):
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
        elif output_format == 'summary':
            for i, chain in enumerate(chains, 1):
                console.print(f"\n[bold]Chain {i}:[/bold]")
                credits = format_currency(chain.get("total_credits"))
                debits = format_currency(chain.get("total_debits"))
                fees = format_currency(chain.get("total_fees"))
                body_lines = [
                    f"Display: {ensure_display_name(chain)}",
                    f"Expiration: {chain.get('expiration', '') or 'N/A'}",
                    f"Status: {chain.get('status', 'UNKNOWN')} (Rolls: {chain.get('roll_count', 0)})",
                    f"Period: {chain.get('start_date', 'N/A')} → {chain.get('end_date', 'N/A')}",
                    f"Credits: {credits}",
                    f"Debits: {debits}",
                    f"Fees: {fees}",
                ]

                if chain.get("status") == "CLOSED":
                    body_lines.append(f"Net P&L (after fees): {format_net_pnl(chain)}")
                else:
                    body_lines.append(f"Realized P&L (after fees): {format_realized_pnl(chain)}")
                    body_lines.append(f"Breakeven to close: {format_breakeven(chain)}")
                    body_lines.append(f"Target Price: {format_price_range(calculate_target_price_range(chain, target_bounds))}")

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
        raise click.Abort()


@main.command()
@click.option('--options/--no-options', 'options_only', default=True, help='Filter to options transactions (default behaviour)')
@click.option('--ticker', 'ticker_symbol', help='Filter transactions by ticker symbol')
@click.option('--strategy', type=click.Choice(['calls', 'puts']), help='Filter transactions by strategy')
@click.option('--file', 'csv_file', type=click.Path(exists=True), default='all_transactions.csv', show_default=True,
              help='CSV file to ingest')
@click.option('--open-only', is_flag=True, help='Show only open option positions (no closing trades)')
@click.option('--target', default='0.5-0.7', show_default=True,
              help='Target profit range as fraction of entry price / credit (e.g. 0.5-0.7)')
@click.option('--json-output', 'json_output', is_flag=True, help='Emit JSON instead of table output')
@click.pass_context
def ingest(ctx, options_only, ticker_symbol, strategy, csv_file, open_only, target, json_output):
    """Ingest and display raw options transactions from CSV."""
    console = Console()
    
    # Parse target range first to get proper Click error handling
    target_bounds = parse_target_range(target)
    
    try:
        emit_text = not json_output

        if emit_text:
            console.print(f"[blue]Ingesting {csv_file}...[/blue]")
        transactions = get_options_transactions(csv_file)
        target_percents = calculate_target_percents(target_bounds)
        target_label = "Target (" + ", ".join(format_percent(value) for value in target_percents) + ")"

        calls_only = strategy == 'calls'
        puts_only = strategy == 'puts'

        try:
            filtered_by_ticker = filter_transactions_by_ticker(transactions, ticker_symbol)
            filtered_transactions = filter_transactions_by_option_type(
                filtered_by_ticker,
                calls_only=calls_only,
                puts_only=puts_only,
            )
        except ValueError as exc:
            ctx.fail(str(exc))

        chain_source_transactions = list(filtered_transactions)

        if ticker_symbol:
            ticker_key = ticker_symbol.strip().upper()
            if not filtered_by_ticker:
                if json_output:
                    empty_payload = build_ingest_payload(
                        csv_file=csv_file,
                        transactions=[],
                        display_rows=[],
                        chains=[],
                        target_percents=target_percents,
                        options_only=options_only,
                        ticker=ticker_symbol,
                        strategy=strategy,
                        open_only=open_only,
                    )
                    console.print_json(data=empty_payload)
                else:
                    console.print(f"[yellow]No options transactions found for ticker {ticker_key}[/yellow]")
                return
            if emit_text:
                console.print(f"[green]Filtered to {len(filtered_by_ticker)} {ticker_key} options transactions[/green]")
        else:
            if emit_text:
                console.print(f"[green]Found {len(filtered_transactions)} options transactions[/green]")

        if open_only:
            filtered_transactions = filter_open_positions(filtered_transactions)
            if emit_text:
                console.print(f"[cyan]Open positions: {len(filtered_transactions)}[/cyan]")

        display_rows = prepare_transactions_for_display(filtered_transactions, target_percents)

        if not filtered_transactions and emit_text:
            console.print("[yellow]No transactions match the provided filters.[/yellow]")
            return

        if json_output:
            chains_for_json = detect_roll_chains(chain_source_transactions)
            payload = build_ingest_payload(
                csv_file=csv_file,
                transactions=filtered_transactions,
                display_rows=display_rows,
                chains=chains_for_json,
                target_percents=target_percents,
                options_only=options_only,
                ticker=ticker_symbol,
                strategy=strategy,
                open_only=open_only,
            )
            console.print_json(data=payload)
            return

        # Display transactions in a table
        from rich.table import Table
        table = Table(title="Options Transactions", expand=True)

        table.add_column("Date", style="cyan")
        table.add_column("Symbol", style="magenta")
        table.add_column("Expiration", style="magenta")
        table.add_column("Code", style="green")
        table.add_column("Quantity", justify="right")
        table.add_column("Price", justify="right")
        table.add_column(target_label, justify="right")
        table.add_column("Description", style="yellow")
        
        for row in display_rows:
            table.add_row(
                row["date"],
                row["symbol"],
                row["expiration"],
                row["code"],
                row["quantity"],
                row["price"],
                row["target_close"],
                row["description"],
            )
        
        console.print(table)
        
    except click.ClickException:
        raise
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()


@main.command()
@click.argument('position_spec')
@click.option('--file', 'csv_file', type=click.Path(exists=True), default='all_transactions.csv', show_default=True,
              help='CSV file to search')
def lookup(position_spec, csv_file):
    """Look up a specific position in the CSV data."""
    console = Console()
    
    try:
        console.print(f"[blue]Looking up position: {position_spec}[/blue]")
        try:
            symbol, strike, option_type, expiration = parse_lookup_input(position_spec)
        except ValueError as exc:
            raise click.BadParameter(str(exc)) from exc

        transactions = get_options_transactions(csv_file)
        target_symbol = symbol.upper()
        target_option = 'Call' if option_type.upper() == 'C' else 'Put'
        strike_decimal = Decimal(str(strike))
        expiration_parts = expiration.split('-')
        year_text, month_text, day_text = expiration_parts
        expiration_display = f"{int(month_text):02d}/{int(day_text):02d}/{year_text}"

        matches = []
        for txn in transactions:
            descriptor = parse_option_description(txn.get('Description', ''))
            if not descriptor:
                continue
            if descriptor.symbol != target_symbol:
                continue
            if descriptor.option_type != target_option:
                continue
            if descriptor.strike != strike_decimal:
                continue
            if descriptor.expiration != expiration_display:
                continue
            matches.append(txn)
        
        if matches:
            console.print(f"[green]Found {len(matches)} matching transactions[/green]")
            
            from rich.table import Table
            table = Table(title=f"Position: {position_spec}")
            
            table.add_column("Date", style="cyan")
            table.add_column("Symbol", style="magenta")
            table.add_column("Code", style="green")
            table.add_column("Quantity", justify="right")
            table.add_column("Price", justify="right")
            table.add_column("Description", style="yellow")
            
            for txn in matches:
                table.add_row(
                    txn.get('Activity Date', ''),
                    txn.get('Instrument', ''),
                    txn.get('Trans Code', ''),
                    txn.get('Quantity', ''),
                    txn.get('Price', ''),
                    txn.get('Description', '')
                )
            
            console.print(table)
        else:
            console.print(f"[yellow]No transactions found for position: {position_spec}[/yellow]")
        
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()


@main.command()
@click.argument('display_name')
@click.argument('csv_file', type=click.Path(exists=True), required=False, default="all_transactions.csv")
@click.option('--target', default='0.5-0.7', show_default=True,
              help='Target profit range as fraction of net credit (e.g. 0.5-0.7)')
def trace(display_name, csv_file, target):
    """Trace the full history of a roll chain by display name."""
    console = Console()

    # Parse target range first to get proper Click error handling
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
            title = f"{ensure_display_name(chain)} ({chain.get('status', 'UNKNOWN')})"
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
                summary_lines.append(f"Target Price: {format_price_range(calculate_target_price_range(chain, target_bounds))}")

            console.print(
                Panel(
                    "\n".join(summary_lines),
                    title=f"Chain {index}: {title}",
                    border_style="blue",
                )
            )

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

            console.print(table)
            console.print()

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()


if __name__ == '__main__':
    main()
