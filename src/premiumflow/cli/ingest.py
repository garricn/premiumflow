"""
Import command for PremiumFlow CLI.

This module provides the primary ``import`` command used to display and
serialize raw options transactions extracted from CSV input. A deprecated
``ingest`` alias is kept temporarily for backward compatibility.
"""

from __future__ import annotations

from typing import Iterable

import click
from rich.console import Console
from rich.table import Table

from ..core.parser import get_options_transactions
from ..services.chain_builder import detect_roll_chains
from ..services.display import format_percent
from ..services.json_serializer import build_ingest_payload
from ..services.targets import calculate_target_percents
from ..services.transactions import (
    filter_open_positions,
    filter_transactions_by_option_type,
    filter_transactions_by_ticker,
)
from .utils import parse_target_range, prepare_transactions_for_display


def _emit_transactions_table(table_rows: Iterable[dict[str, str]], target_label: str) -> Table:
    """Create a Rich table for the import command."""
    table = Table(title="Options Transactions", expand=True)
    table.add_column("Date", style="cyan")
    table.add_column("Symbol", style="magenta")
    table.add_column("Expiration", style="magenta")
    table.add_column("Code", style="green")
    table.add_column("Quantity", justify="right")
    table.add_column("Price", justify="right")
    table.add_column(target_label, justify="right")
    table.add_column("Description", style="yellow")

    for row in table_rows:
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

    return table


def _apply_import_options(func):
    """Attach the shared options used by both import and ingest commands."""

    option_decorators = [
        click.option(
            "--options/--no-options",
            "options_only",
            default=True,
            help="Filter to options transactions (default behaviour)",
        ),
        click.option("--ticker", "ticker_symbol", help="Filter transactions by ticker symbol"),
        click.option(
            "--strategy",
            type=click.Choice(["calls", "puts"]),
            help="Filter transactions by strategy",
        ),
        click.option(
            "--file",
            "csv_file",
            type=click.Path(exists=True),
            default="all_transactions.csv",
            show_default=True,
            help="CSV file to import",
        ),
        click.option(
            "--open-only", is_flag=True, help="Show only open option positions (no closing trades)"
        ),
        click.option(
            "--target",
            default="0.5-0.7",
            show_default=True,
            help="Target profit range as fraction of entry price / credit (e.g. 0.5-0.7)",
        ),
        click.option(
            "--json-output", "json_output", is_flag=True, help="Emit JSON instead of table output"
        ),
    ]

    func = click.pass_context(func)
    for decorator in reversed(option_decorators):
        func = decorator(func)
    return func


def _run_import(
    ctx: click.Context,
    *,
    options_only,
    ticker_symbol,
    strategy,
    csv_file,
    open_only,
    target,
    json_output,
    console_label: str,
) -> None:
    """Shared implementation used by both import and ingest commands."""

    console = Console()
    target_bounds = parse_target_range(target)

    try:
        emit_text = not json_output

        if emit_text:
            console.print(f"[blue]{console_label} {csv_file}...[/blue]")

        transactions = get_options_transactions(csv_file)
        target_percents = calculate_target_percents(target_bounds)
        target_label = (
            "Target (" + ", ".join(format_percent(value) for value in target_percents) + ")"
        )

        calls_only = strategy == "calls"
        puts_only = strategy == "puts"

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
                    console.print(
                        f"[yellow]No options transactions found for ticker {ticker_key}[/yellow]"
                    )
                return
            if emit_text:
                console.print(
                    f"[green]Filtered to {len(filtered_by_ticker)} {ticker_key} options transactions[/green]"
                )
        else:
            if emit_text:
                console.print(
                    f"[green]Found {len(filtered_transactions)} options transactions[/green]"
                )

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

        transactions_table = _emit_transactions_table(display_rows, target_label)
        console.print(transactions_table)

    except click.ClickException:
        raise
    except Exception as exc:
        console.print(f"[red]Error: {exc}[/red]")
        raise click.Abort() from exc


@click.command(name="import")
@_apply_import_options
def import_transactions(
    ctx, options_only, ticker_symbol, strategy, csv_file, open_only, target, json_output
):
    """Import and display raw options transactions from CSV."""

    _run_import(
        ctx,
        options_only=options_only,
        ticker_symbol=ticker_symbol,
        strategy=strategy,
        csv_file=csv_file,
        open_only=open_only,
        target=target,
        json_output=json_output,
        console_label="Importing",
    )


@click.command(name="ingest")
@_apply_import_options
def ingest(ctx, options_only, ticker_symbol, strategy, csv_file, open_only, target, json_output):
    """Deprecated alias for ``premiumflow import``."""

    click.echo(
        "[deprecated] 'premiumflow ingest' is deprecated; use 'premiumflow import' instead.",
        err=True,
    )

    _run_import(
        ctx,
        options_only=options_only,
        ticker_symbol=ticker_symbol,
        strategy=strategy,
        csv_file=csv_file,
        open_only=open_only,
        target=target,
        json_output=json_output,
        console_label="Importing",
    )
