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
    ParsedImportResult,
    load_option_transactions,
)
from ..services.cash_flows import CashFlowSummary, summarize_cash_flows
from ..services.chain_builder import detect_roll_chains
from ..services.display import format_currency, format_percent, format_target_close_prices
from ..services.json_serializer import build_ingest_payload
from ..services.targets import calculate_target_percents, compute_target_close_prices
from .utils import parse_target_range


def _format_money_string(value: Decimal) -> str:
    quantized = value.quantize(Decimal("0.01"))
    if quantized < 0:
        return f"(${abs(quantized):,.2f})"
    return f"${quantized:,.2f}"


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


def _create_parsed_result(
    parsed: ParsedImportResult, transactions: Iterable[NormalizedOptionTransaction]
) -> ParsedImportResult:
    return ParsedImportResult(
        account_name=parsed.account_name,
        account_number=parsed.account_number,
        regulatory_fee=parsed.regulatory_fee,
        transactions=list(transactions),
    )


def _transactions_to_csv_dicts(
    transactions: Iterable[NormalizedOptionTransaction],
) -> List[dict[str, str]]:
    rows: List[dict[str, str]] = []
    for txn in transactions:
        notional = txn.price * txn.quantity
        signed_amount = notional if txn.action == "SELL" else -notional
        rows.append(
            {
                "Activity Date": txn.activity_date.strftime("%m/%d/%Y"),
                "Process Date": txn.process_date.strftime("%m/%d/%Y") if txn.process_date else "",
                "Settle Date": txn.settle_date.strftime("%m/%d/%Y") if txn.settle_date else "",
                "Instrument": txn.instrument,
                "Description": txn.description,
                "Trans Code": txn.trans_code,
                "Quantity": str(txn.quantity),
                "Price": _format_money_string(txn.price),
                "Amount": _format_money_string(signed_amount),
                "Commission": _format_money_string(txn.fees) if txn.fees else "",
            }
        )
    return rows


def _build_cash_flow_table(
    summary: CashFlowSummary,
    target_percents: List[Decimal],
    target_label: str,
) -> Table:
    table = Table(title=f"Options Transactions – {summary.account_name}", expand=True)
    table.add_column("Date", style="cyan")
    table.add_column("Symbol", style="magenta", no_wrap=True)
    table.add_column("Expiration", style="magenta")
    table.add_column("Code", style="green")
    table.add_column("Quantity", justify="right")
    table.add_column("Price", justify="right")
    table.add_column("Credit", justify="right")
    table.add_column("Debit", justify="right")
    table.add_column("Fee", justify="right")
    table.add_column("Net Premium", justify="right")
    table.add_column("Net P&L", justify="right")
    table.add_column(target_label, justify="right")
    table.add_column("Description", style="yellow")

    for row in summary.rows:
        txn = row.transaction
        target_prices = compute_target_close_prices(
            txn.trans_code,
            format(txn.price, "f"),
            target_percents,
        )

        table.add_row(
            txn.activity_date.isoformat(),
            (txn.instrument or "").strip(),
            txn.expiration.isoformat(),
            txn.trans_code,
            str(txn.quantity),
            format_currency(txn.price),
            format_currency(row.credit),
            format_currency(row.debit),
            format_currency(row.fee),
            format_currency(row.running_net_premium),
            format_currency(row.running_net_pnl),
            format_target_close_prices(target_prices),
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
            "--target",
            default="0.5-0.7",
            show_default=True,
            help="Target profit range as fraction of entry price / credit (e.g. 0.5-0.7)",
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
    target,
    account_name,
    account_number,
    regulatory_fee,
    json_output,
    console_label: str,
) -> None:
    """Shared implementation used by both import and ingest commands."""

    console = Console()
    target_bounds = parse_target_range(target)
    target_percents = calculate_target_percents(target_bounds)
    target_label = (
        "Target (" + ", ".join(format_percent(value) for value in target_percents) + ")"
        if target_percents
        else "Target"
    )

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
            empty_summary = summarize_cash_flows(_create_parsed_result(parsed, []))
            if json_output:
                payload = build_ingest_payload(
                    csv_file=csv_file,
                    summary=empty_summary,
                    chains=[],
                    target_percents=target_percents,
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
    chain_source_transactions = _transactions_to_csv_dicts(filtered_transactions)

    open_position_count = 0
    if open_only:
        filtered_transactions, open_position_count = _filter_open_transactions(
            filtered_transactions
        )
        if emit_text:
            console.print(f"[cyan]Open positions: {open_position_count}[/cyan]")

    filtered_summary = summarize_cash_flows(_create_parsed_result(parsed, filtered_transactions))
    chains_for_json = detect_roll_chains(chain_source_transactions)

    if json_output:
        payload = build_ingest_payload(
            csv_file=csv_file,
            summary=filtered_summary,
            chains=chains_for_json,
            target_percents=target_percents,
            options_only=options_only,
            ticker=ticker_symbol,
            strategy=strategy,
            open_only=open_only,
        )
        console.print_json(data=payload)
        return

    if not filtered_summary.rows:
        console.print("[yellow]No transactions match the provided filters.[/yellow]")
        return

    account_line = f"[green]Account:[/green] {filtered_summary.account_name}"
    if filtered_summary.account_number:
        account_line += f" ({filtered_summary.account_number})"
    account_line += f" · Reg Fee: {format_currency(filtered_summary.regulatory_fee)}"
    console.print(account_line)

    table = _build_cash_flow_table(filtered_summary, target_percents, target_label)
    console.print(table)

    totals = filtered_summary.totals
    console.print(
        "[bold magenta]Totals:[/bold magenta] "
        f"Credits {format_currency(totals.credits)} · "
        f"Debits {format_currency(totals.debits)} · "
        f"Fees {format_currency(totals.fees)} · "
        f"Net Premium {format_currency(totals.net_premium)} · "
        f"Net P&L {format_currency(totals.net_pnl)}"
    )


@click.command(name="import")
@_apply_import_options
def import_transactions(
    ctx,
    options_only,
    ticker_symbol,
    strategy,
    csv_file,
    open_only,
    target,
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
        target=target,
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
    target,
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
        target=target,
        account_name=account_name,
        account_number=account_number,
        regulatory_fee=regulatory_fee,
        json_output=json_output,
        console_label="Importing",
    )
