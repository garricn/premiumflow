"""
Command-line interface for rollchain.

This module provides the CLI commands using Click.
"""

from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Iterable, List, Optional, Tuple

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from ..core.parser import get_options_transactions, parse_csv_file
from ..services.chain_builder import detect_roll_chains
from ..services.options import OptionDescriptor, parse_option_description
from ..services.targets import calculate_target_percents, compute_target_close_prices
from ..services.transactions import (
    filter_open_positions,
    filter_transactions_by_option_type,
    filter_transactions_by_ticker,
)


def _is_open_chain(chain: Dict[str, Any]) -> bool:
    """Determine whether a detected chain is still open."""
    status = (chain.get("status") or "").upper()
    if status in {"OPEN", "CLOSED"}:
        return status == "OPEN"

    transactions: List[Dict[str, Any]] = chain.get("transactions") or []
    if not transactions:
        return False
    last_code = (transactions[-1].get("Trans Code") or "").strip().upper()
    return last_code in {"STO", "BTO"}


def _format_currency(value: Decimal | None) -> str:
    if value is None:
        return "--"
    quantized = value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    sign = "-" if quantized < 0 else ""
    quantized = abs(quantized)
    return f"{sign}${quantized:,.2f}"


def _format_breakeven(chain: Dict[str, Any]) -> str:
    if chain.get("status") != "OPEN":
        return "--"
    breakeven = chain.get("breakeven_price")
    if breakeven is None:
        return "--"
    direction = chain.get("breakeven_direction") or ""
    return f"{_format_currency(breakeven)} {direction}".strip()


def _calculate_realized_pnl(chain: Dict[str, Any]) -> Decimal:
    total_credits = chain.get("total_credits") or Decimal("0")
    total_debits = chain.get("total_debits") or Decimal("0")
    total_fees = chain.get("total_fees") or Decimal("0")
    return total_credits - total_debits - total_fees


def _format_realized_pnl(chain: Dict[str, Any]) -> str:
    return _format_currency(_calculate_realized_pnl(chain))


def _format_net_pnl(chain: Dict[str, Any]) -> str:
    if chain.get("status") != "CLOSED":
        return _format_realized_pnl(chain)
    return _format_currency(chain.get("net_pnl_after_fees"))


def _parse_target_range(target: str) -> Tuple[Decimal, Decimal]:
    try:
        lower_str, upper_str = target.split('-', 1)
        lower = Decimal(lower_str.strip())
        upper = Decimal(upper_str.strip())
    except (ValueError, ArithmeticError):  # split or Decimal conversion
        raise click.BadParameter("Target range must be in the form LOWER-UPPER, e.g. 0.5-0.7")

    if lower < Decimal('0') or upper > Decimal('1') or lower > upper:
        raise click.BadParameter("Target bounds must satisfy 0 <= lower <= upper <= 1")
    return lower, upper


def _format_percent(value: Decimal) -> str:
    percent = (value * Decimal('100')).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    text = f"{percent:,.2f}"
    if text.endswith(".00"):
        text = text[:-3]
    elif text.endswith("0"):
        text = text[:-1]
    return f"{text}%"


def _calculate_target_price_range(chain: Dict[str, Any], bounds: Tuple[Decimal, Decimal]) -> Optional[Tuple[Decimal, Decimal]]:
    breakeven = chain.get('breakeven_price')
    net_contracts = chain.get('net_contracts', 0)
    if breakeven is None or not net_contracts:
        return None

    realized = _calculate_realized_pnl(chain)
    contracts = abs(net_contracts)
    if contracts == 0:
        return None

    per_share_realized = realized / (Decimal(contracts) * Decimal('100'))
    per_share_realized = per_share_realized.quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)
    if per_share_realized <= Decimal('0'):
        return None

    lower_shift = (per_share_realized * bounds[0]).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    upper_shift = (per_share_realized * bounds[1]).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

    breakeven = Decimal(breakeven)
    if net_contracts < 0:
        low_price = breakeven - upper_shift
        high_price = breakeven - lower_shift
    else:
        low_price = breakeven + lower_shift
        high_price = breakeven + upper_shift

    return low_price, high_price


def _format_price_range(value_pair: Optional[Tuple[Decimal, Decimal]]) -> str:
    if not value_pair:
        return "--"
    low, high = value_pair
    return f"{_format_currency(low)} - {_format_currency(high)}"


def _format_target_close_prices(price_list: Optional[List[Decimal]]) -> str:
    if not price_list:
        return "--"
    return ", ".join(_format_currency(value) for value in price_list)


def _ensure_display_name(chain: Dict[str, Any]) -> str:
    display = chain.get("display_name")
    if display:
        return display
    symbol = chain.get("symbol") or ""
    strike = chain.get("strike")
    option_label = chain.get("option_label") or ""
    if isinstance(strike, Decimal):
        if strike == strike.to_integral_value():
            strike_text = f"{int(strike)}"
        else:
            strike_text = f"{strike.normalize()}"
    else:
        strike_text = str(strike or "")
    return " ".join(filter(None, [symbol, f"${strike_text}", option_label])).strip() or symbol


def _parse_option_description(description: str) -> Optional[Tuple[str, str, str, Decimal]]:
    if not description:
        return None
    import re

    pattern = re.compile(
        r'^\s*(?P<symbol>[A-Za-z]+)\s+'
        r'(?P<expiration>\d{1,2}/\d{1,2}/\d{4})\s+'
        r'(?P<option_type>Call|Put)\s+\$?(?P<strike>[\d,]+(?:\.\d+)?)\s*$'
    )
    match = pattern.match(description)
    if not match:
        return None

    symbol = match.group('symbol').upper()
    expiration = match.group('expiration')
    option_type = match.group('option_type').capitalize()
    strike_text = match.group('strike').replace(',', '')
    try:
        strike = Decimal(strike_text)
    except InvalidOperation:
        return None
    return symbol, expiration, option_type, strike


def _format_option_display(parsed: Optional[OptionDescriptor], fallback: str) -> Tuple[str, str]:
    if not parsed:
        return fallback, ""
    strike_text = f"{parsed.strike.quantize(Decimal('0.01')):,.2f}"
    return f"{parsed.symbol} ${strike_text} {parsed.option_type}", parsed.expiration


def prepare_transactions_for_display(
    transactions: Iterable[Dict[str, Any]],
    target_percents: List[Decimal],
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for txn in transactions:
        parsed_option = parse_option_description(txn.get('Description', ''))
        formatted_desc, expiration = _format_option_display(parsed_option, txn.get('Description', ''))
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
                "target_close": _format_target_close_prices(target_prices),
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
            chains = [chain for chain in chains if _is_open_chain(chain)]
            console.print(f"[cyan]Open chains: {len(chains)}[/cyan]")

        target_bounds = _parse_target_range(target)
        target_percents = calculate_target_percents(target_bounds)
        target_label = "Target (" + ", ".join(_format_percent(value) for value in target_percents) + ")"
        
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
                    _ensure_display_name(chain),
                    chain.get("expiration", "") or "N/A",
                    chain.get("status", "UNKNOWN"),
                    _format_currency(chain.get("total_credits")),
                    _format_currency(chain.get("total_debits")),
                    _format_net_pnl(chain),
                    _format_breakeven(chain),
                    _format_price_range(_calculate_target_price_range(chain, target_bounds)),
                )

            console.print(table)
        elif output_format == 'summary':
            for i, chain in enumerate(chains, 1):
                console.print(f"\n[bold]Chain {i}:[/bold]")
                credits = _format_currency(chain.get("total_credits"))
                debits = _format_currency(chain.get("total_debits"))
                fees = _format_currency(chain.get("total_fees"))
                body_lines = [
                    f"Display: {_ensure_display_name(chain)}",
                    f"Expiration: {chain.get('expiration', '') or 'N/A'}",
                    f"Status: {chain.get('status', 'UNKNOWN')} (Rolls: {chain.get('roll_count', 0)})",
                    f"Period: {chain.get('start_date', 'N/A')} → {chain.get('end_date', 'N/A')}",
                    f"Credits: {credits}",
                    f"Debits: {debits}",
                    f"Fees: {fees}",
                ]

                if chain.get("status") == "CLOSED":
                    body_lines.append(f"Net P&L (after fees): {_format_net_pnl(chain)}")
                else:
                    body_lines.append(f"Realized P&L (after fees): {_format_realized_pnl(chain)}")
                    body_lines.append(f"Breakeven to close: {_format_breakeven(chain)}")
                    body_lines.append(f"Target Price: {_format_price_range(_calculate_target_price_range(chain, target_bounds))}")

                console.print(
                    Panel(
                        "\n".join(body_lines),
                        title=_ensure_display_name(chain),
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
@click.argument('csv_file', type=click.Path(exists=True))
@click.option('--ticker', 'ticker_symbol', help='Filter transactions by ticker symbol')
@click.option('--calls-only', is_flag=True, default=False, help='Show only call option transactions')
@click.option('--puts-only', is_flag=True, default=False, help='Show only put option transactions')
@click.option('--open-only', is_flag=True,
              help='Show only open option positions (no closing trades)')
@click.option('--target', default='0.5-0.7', show_default=True,
              help='Target profit range as fraction of entry price / credit (e.g. 0.5-0.7)')
@click.pass_context
def ingest(ctx, csv_file, ticker_symbol, calls_only, puts_only, open_only, target):
    """Ingest and display raw options transactions from CSV."""
    console = Console()
    
    try:
        console.print(f"[blue]Ingesting {csv_file}...[/blue]")
        transactions = get_options_transactions(csv_file)
        target_bounds = _parse_target_range(target)
        target_percents = calculate_target_percents(target_bounds)
        target_label = "Target (" + ", ".join(_format_percent(value) for value in target_percents) + ")"

        try:
            filtered_by_ticker = filter_transactions_by_ticker(transactions, ticker_symbol)
            filtered_transactions = filter_transactions_by_option_type(
                filtered_by_ticker,
                calls_only=calls_only,
                puts_only=puts_only,
            )
        except ValueError as exc:
            ctx.fail(str(exc))

        if ticker_symbol:
            ticker_key = ticker_symbol.strip().upper()
            if not filtered_by_ticker:
                console.print(f"[yellow]No options transactions found for ticker {ticker_key}[/yellow]")
                return
            console.print(f"[green]Filtered to {len(filtered_by_ticker)} {ticker_key} options transactions[/green]")
        else:
            console.print(f"[green]Found {len(filtered_transactions)} options transactions[/green]")

        if open_only:
            filtered_transactions = filter_open_positions(filtered_transactions)
            console.print(f"[cyan]Open positions: {len(filtered_transactions)}[/cyan]")

        if not filtered_transactions:
            console.print("[yellow]No transactions match the provided filters.[/yellow]")
            return
        
        display_rows = prepare_transactions_for_display(filtered_transactions, target_percents)

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
@click.argument('csv_file', type=click.Path(exists=True))
def lookup(position_spec, csv_file):
    """Look up a specific position in the CSV data."""
    console = Console()
    
    try:
        console.print(f"[blue]Looking up position: {position_spec}[/blue]")
        
        # This is a simplified lookup - in a real implementation,
        # you'd parse the position spec and find matching transactions
        transactions = get_options_transactions(csv_file)
        
        # Simple text search for now
        matches = []
        for txn in transactions:
            if position_spec.lower() in txn.get('Description', '').lower():
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

    try:
        console.print(f"[blue]Tracing {display_name} in {csv_file}[/blue]")
        raw_transactions = get_options_transactions(csv_file)
        chains = detect_roll_chains(raw_transactions)

        display_key = display_name.strip().lower()
        target_bounds = _parse_target_range(target)

        matched = [
            chain for chain in chains
            if _ensure_display_name(chain).lower() == display_key
        ]

        if not matched:
            console.print(f"[yellow]No roll chains found for {display_name}[/yellow]")
            return

        matched.sort(key=lambda chain: chain.get("start_date", ""))

        for index, chain in enumerate(matched, start=1):
            title = f"{_ensure_display_name(chain)} ({chain.get('status', 'UNKNOWN')})"
            summary_lines = [
                f"Rolls: {chain.get('roll_count', 0)}",
                f"Start: {chain.get('start_date', 'N/A')} → End: {chain.get('end_date', 'N/A')}",
                f"Total Credits: {_format_currency(chain.get('total_credits'))}",
                f"Total Debits: {_format_currency(chain.get('total_debits'))}",
            ]

            if chain.get("status") == "CLOSED":
                summary_lines.append(f"Net P&L (after fees): {_format_net_pnl(chain)}")
            else:
                summary_lines.append(f"Realized P&L (after fees): {_format_realized_pnl(chain)}")
                summary_lines.append(f"Breakeven to close: {_format_breakeven(chain)}")
                summary_lines.append(f"Target Price: {_format_price_range(_calculate_target_price_range(chain, target_bounds))}")

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
