"""
Import command for PremiumFlow CLI.

This module provides the primary ``import`` command used to display and
serialize raw options transactions extracted from CSV input. A deprecated
``ingest`` alias is kept temporarily for backward compatibility.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Iterable, List, Optional

import click
from rich.console import Console
from rich.table import Table

from ..core.parser import (
    ImportValidationError,
    NormalizedOptionTransaction,
    load_option_transactions,
)
from ..services.chain_builder import detect_roll_chains
from ..services.display import format_currency
from ..services.json_serializer import build_ingest_payload
from ..services.transactions import normalized_to_csv_dicts


def _transaction_key_from_txn(txn: NormalizedOptionTransaction) -> tuple:
    return (
        (txn.instrument or "").strip().upper(),
        txn.option_type,
        txn.expiration,
        txn.strike,
        (txn.description or "").strip(),
    )


def _filter_by_ticker(
    transactions: Iterable[NormalizedOptionTransaction],
    ticker_symbol: Optional[str],
) -> List[NormalizedOptionTransaction]:
    if not ticker_symbol:
        return list(transactions)
    ticker_key = ticker_symbol.strip().upper()
    return [txn for txn in transactions if (txn.instrument or "").strip().upper() == ticker_key]


def _filter_by_strategy(
    transactions: Iterable[NormalizedOptionTransaction], strategy: Optional[str]
) -> List[NormalizedOptionTransaction]:
    transactions = list(transactions)
    if strategy == "calls":
        return [txn for txn in transactions if txn.option_type == "CALL"]
    if strategy == "puts":
        return [txn for txn in transactions if txn.option_type == "PUT"]
    return transactions


def _filter_open_transactions(
    transactions: Iterable[NormalizedOptionTransaction],
) -> tuple[List[NormalizedOptionTransaction], int]:
    transactions = list(transactions)
    net_by_key: dict[tuple, int] = {}
    for txn in transactions:
        key = _transaction_key_from_txn(txn)
        delta = txn.quantity if txn.action == "BUY" else -txn.quantity
        net_by_key[key] = net_by_key.get(key, 0) + delta

    open_keys = {key for key, net in net_by_key.items() if net != 0}
    if not open_keys:
        return [], 0

    filtered = [txn for txn in transactions if _transaction_key_from_txn(txn) in open_keys]
    return filtered, len(open_keys)


def _sort_transactions(
    transactions: Iterable[NormalizedOptionTransaction],
) -> List[NormalizedOptionTransaction]:
    indexed = list(enumerate(transactions))
    indexed.sort(
        key=lambda item: (
            item[1].activity_date,
            item[1].process_date or item[1].activity_date,
            item[1].settle_date or item[1].activity_date,
            item[0],
        )
    )
    return [txn for _, txn in indexed]


def _build_transaction_table(
    account_name: str,
    transactions: Iterable[NormalizedOptionTransaction],
) -> Table:
    table = Table(title=f"Options Transactions – {account_name}", expand=True)
    table.add_column("Date", style="cyan")
    table.add_column("Symbol", style="magenta", no_wrap=True)
    table.add_column("Expiration", style="magenta")
    table.add_column("Strike", justify="right")
    table.add_column("Type", style="magenta")
    table.add_column("Action", style="green")
    table.add_column("Code", style="green")
    table.add_column("Quantity", justify="right")
    table.add_column("Price", justify="right")
    table.add_column("Amount", justify="right")
    table.add_column("Description", style="yellow")

    for txn in transactions:
        table.add_row(
            txn.activity_date.isoformat(),
            (txn.instrument or "").strip(),
            txn.expiration.isoformat(),
            format_currency(txn.strike),
            txn.option_type,
            txn.action,
            txn.trans_code,
            str(txn.quantity),
            format_currency(txn.price),
            format_currency(txn.amount) if txn.amount is not None else "--",
            txn.description,
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
            "--account-name",
            required=True,
            help="Human-readable account label to attach to this import (required).",
        ),
        click.option(
            "--account-number",
            help="Optional account identifier to echo in output.",
        ),
        click.option(
            "--regulatory-fee",
            default="0.04",
            show_default=True,
            help="Per-contract regulatory fee (USD) when commission data is absent.",
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
    account_name,
    account_number,
    regulatory_fee,
    json_output,
    console_label: str,
) -> None:
    """Shared implementation used by both import and ingest commands."""

    console = Console()

    try:
        regulatory_fee_value = Decimal(str(regulatory_fee))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise click.BadParameter("--regulatory-fee must be a decimal value.") from exc

    try:
        parsed = load_option_transactions(
            csv_file,
            account_name=account_name,
            account_number=account_number,
            regulatory_fee=regulatory_fee_value,
        )
    except ImportValidationError as exc:
        ctx.fail(str(exc))
        return

    emit_text = not json_output
    if emit_text:
        console.print(f"[blue]{console_label} {csv_file}...[/blue]")

    transactions = list(parsed.transactions)
    filtered_transactions = _filter_by_ticker(transactions, ticker_symbol)

    if ticker_symbol:
        ticker_key = ticker_symbol.strip().upper()
        if not filtered_transactions:
            if json_output:
                payload = build_ingest_payload(
                    csv_file=csv_file,
                    account_name=parsed.account_name,
                    account_number=parsed.account_number,
                    transactions=[],
                    chains=[],
                    options_only=options_only,
                    ticker=ticker_symbol,
                    strategy=strategy,
                    open_only=open_only,
                )
                console.print_json(data=payload)
            else:
                console.print(
                    f"[yellow]No options transactions found for ticker {ticker_key}[/yellow]"
                )
            return
        if emit_text:
            console.print(
                f"[green]Filtered to {len(filtered_transactions)} {ticker_key} options transactions[/green]"
            )
    else:
        if emit_text:
            console.print(f"[green]Found {len(filtered_transactions)} options transactions[/green]")

    filtered_transactions = _filter_by_strategy(filtered_transactions, strategy)
    filtered_transactions = _sort_transactions(filtered_transactions)
    chain_source_transactions = normalized_to_csv_dicts(filtered_transactions)

    open_position_count = 0
    if open_only:
        filtered_transactions, open_position_count = _filter_open_transactions(
            filtered_transactions
        )
        filtered_transactions = _sort_transactions(filtered_transactions)
        if emit_text:
            console.print(f"[cyan]Open positions: {open_position_count}[/cyan]")
        chain_source_transactions = normalized_to_csv_dicts(filtered_transactions)

    chains_for_json = detect_roll_chains(chain_source_transactions)

    if json_output:
        payload = build_ingest_payload(
            csv_file=csv_file,
            account_name=parsed.account_name,
            account_number=parsed.account_number,
            transactions=filtered_transactions,
            chains=chains_for_json,
            options_only=options_only,
            ticker=ticker_symbol,
            strategy=strategy,
            open_only=open_only,
        )
        console.print_json(data=payload)
        return

    if not filtered_transactions:
        console.print("[yellow]No transactions match the provided filters.[/yellow]")
        return

    account_line = f"[green]Account:[/green] {parsed.account_name}"
    if parsed.account_number:
        account_line += f" ({parsed.account_number})"
    console.print(account_line)

    table = _build_transaction_table(parsed.account_name, filtered_transactions)
    console.print(table)


@click.command(name="import")
@_apply_import_options
def import_transactions(
    ctx,
    options_only,
    ticker_symbol,
    strategy,
    csv_file,
    open_only,
    account_name,
    account_number,
    regulatory_fee,
    json_output,
):
    """Import and display raw options transactions from CSV."""

    _run_import(
        ctx,
        options_only=options_only,
        ticker_symbol=ticker_symbol,
        strategy=strategy,
        csv_file=csv_file,
        open_only=open_only,
        account_name=account_name,
        account_number=account_number,
        regulatory_fee=regulatory_fee,
        json_output=json_output,
        console_label="Importing",
    )


@click.command(name="ingest")
@_apply_import_options
def ingest(
    ctx,
    options_only,
    ticker_symbol,
    strategy,
    csv_file,
    open_only,
    account_name,
    account_number,
    regulatory_fee,
    json_output,
):
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
        account_name=account_name,
        account_number=account_number,
        regulatory_fee=regulatory_fee,
        json_output=json_output,
        console_label="Importing",
    )
