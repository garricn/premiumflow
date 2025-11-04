"""
Import command for PremiumFlow CLI.

This module provides the primary ``import`` command used to display and
serialize raw options transactions extracted from CSV input.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Dict, Iterable, List, Literal, Optional, Sequence, Tuple

import click
from rich.console import Console
from rich.table import Table

from ..core.parser import (
    ImportValidationError,
    NormalizedOptionTransaction,
    load_option_transactions,
)
from ..persistence import (
    DuplicateImportError,
    SQLiteRepository,
    StoreResult,
    store_import_result,
)
from ..services.chain_builder import detect_roll_chains
from ..services.cli_helpers import format_account_label
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
    """Attach the shared options used by the CLI import command and its subcommands."""

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
            type=click.Path(path_type=Path),
            default=Path("all_transactions.csv"),
            show_default=True,
            help="CSV file to import",
        ),
        click.option(
            "--open-only", is_flag=True, help="Show only open option positions (no closing trades)"
        ),
        click.option(
            "--account-name",
            help="Human-readable account label to attach to this import (required when importing).",
        ),
        click.option(
            "--account-number",
            help="Optional account identifier to echo in output.",
        ),
        click.option(
            "--skip-existing",
            is_flag=True,
            help="Skip persistence when this file has already been imported for the account.",
        ),
        click.option(
            "--replace-existing",
            is_flag=True,
            help="Replace persisted data when this file has already been imported.",
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
    skip_existing,
    replace_existing,
    json_output,
    console_label: str,
) -> None:
    """Shared implementation used by the CLI import command."""

    console = Console()

    if not account_name or not account_name.strip():
        ctx.fail("--account-name is required when importing transactions.")
        return

    account_name = account_name.strip()
    account_number = account_number.strip() if account_number else None

    csv_file = Path(csv_file)

    if not csv_file.exists():
        ctx.fail(f"CSV file not found: {csv_file}")
        return

    if skip_existing and replace_existing:
        ctx.fail("--skip-existing and --replace-existing cannot be used together.")
        return

    duplicate_strategy: Literal["error", "skip", "replace"] = (
        "skip" if skip_existing else "replace" if replace_existing else "error"
    )

    try:
        parsed = load_option_transactions(
            str(csv_file),
            account_name=account_name,
            account_number=account_number,
        )
    except ImportValidationError as exc:
        ctx.fail(str(exc))
        return

    emit_text = not json_output
    if emit_text:
        console.print(f"[blue]{console_label} {csv_file}...[/blue]")

    try:
        store_result = store_import_result(
            parsed,
            source_path=str(csv_file),
            options_only=bool(options_only),
            ticker=(ticker_symbol.strip().upper() if ticker_symbol else None),
            strategy=strategy,
            open_only=bool(open_only),
            duplicate_strategy=duplicate_strategy,
        )
    except DuplicateImportError as exc:
        ctx.fail(str(exc))
        return
    except sqlite3.Error as exc:  # pragma: no cover - storage warning only
        store_result = StoreResult(import_id=-1, status="skipped")
        if emit_text:
            console.print(
                f"[yellow]Warning: Failed to persist import data ({exc}). Continuing without storage.[/yellow]"
            )

    if emit_text and store_result.status == "skipped" and duplicate_strategy == "skip":
        console.print("[yellow]Import already persisted; skipping new storage.[/yellow]")
    elif emit_text and store_result.status == "replaced":
        console.print("[cyan]Existing persisted import replaced with new data.[/cyan]")

    transactions = list(parsed.transactions)
    filtered_transactions = _filter_by_ticker(transactions, ticker_symbol)

    if ticker_symbol:
        ticker_key = ticker_symbol.strip().upper()
        if not filtered_transactions:
            if json_output:
                payload = build_ingest_payload(
                    csv_file=str(csv_file),
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
            csv_file=str(csv_file),
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


@click.group(name="import", invoke_without_command=True)
@_apply_import_options
def import_group(
    ctx,
    options_only,
    ticker_symbol,
    strategy,
    csv_file,
    open_only,
    account_name,
    account_number,
    skip_existing,
    replace_existing,
    json_output,
):
    """Import and manage stored option CSV ingests."""

    if ctx.invoked_subcommand is not None:
        ctx.ensure_object(dict)
        return

    _run_import(
        ctx,
        options_only=options_only,
        ticker_symbol=ticker_symbol,
        strategy=strategy,
        csv_file=csv_file,
        open_only=open_only,
        account_name=account_name,
        account_number=account_number,
        skip_existing=skip_existing,
        replace_existing=replace_existing,
        json_output=json_output,
        console_label="Importing",
    )


def _activity_ranges_for(
    repo: SQLiteRepository, import_ids: Sequence[int]
) -> Dict[int, Tuple[Optional[str], Optional[str]]]:
    if not import_ids:
        return {}
    return repo.fetch_import_activity_ranges(import_ids)


@import_group.command("list")
@click.option("--account-name", help="Filter imports by account name.")
@click.option("--account-number", help="Filter imports by account number.")
@click.option("--limit", type=int, help="Maximum number of rows to display.")
@click.option(
    "--offset", type=int, default=0, show_default=True, help="Rows to skip before listing."
)
@click.option(
    "--order",
    type=click.Choice(["asc", "desc"], case_sensitive=False),
    default="desc",
    show_default=True,
    help="Sort order for imports based on imported_at.",
)
def list_imports_command(account_name, account_number, limit, offset, order):
    """List stored imports with optional filters."""

    repo = SQLiteRepository()
    order = (order or "desc").lower()
    imports = repo.list_imports(
        account_name=account_name,
        account_number=account_number,
        limit=limit,
        offset=offset,
        order=order,
    )

    console = Console()
    if not imports:
        console.print("[yellow]No stored imports match the provided filters.[/yellow]")
        return

    ranges = _activity_ranges_for(repo, [item.id for item in imports])

    table = Table(title="Stored Imports", expand=True)
    table.add_column("ID", justify="right")
    table.add_column("Account")
    table.add_column("Rows", justify="right")
    table.add_column("Imported At")
    table.add_column("Activity Start")
    table.add_column("Activity End")
    table.add_column("Options Only", justify="center")
    table.add_column("Open Only", justify="center")
    table.add_column("Ticker", justify="center")
    table.add_column("Strategy", justify="center")
    table.add_column("Source")

    for import_record in imports:
        first_date, last_date = ranges.get(import_record.id, (None, None))
        table.add_row(
            str(import_record.id),
            format_account_label(import_record.account_name, import_record.account_number),
            str(import_record.row_count),
            import_record.imported_at,
            first_date or "—",
            last_date or "—",
            "Yes" if import_record.options_only else "No",
            "Yes" if import_record.open_only else "No",
            import_record.ticker or "—",
            import_record.strategy or "—",
            Path(import_record.source_path).name,
        )

    console.print(table)


@import_group.command("delete")
@click.argument("import_id", type=int)
@click.option("--yes", "confirm_delete", is_flag=True, help="Delete without confirmation.")
def delete_import_command(import_id: int, confirm_delete: bool) -> None:
    """Delete a stored import by identifier."""

    repo = SQLiteRepository()

    if not confirm_delete:
        if not click.confirm(f"Delete import {import_id}? This cannot be undone."):
            click.echo("Aborted.")
            return

    deleted = repo.delete_import(import_id)
    if deleted:
        click.echo(f"Deleted import {import_id}.")
    else:
        raise click.ClickException(f"No import found with id {import_id}.")
