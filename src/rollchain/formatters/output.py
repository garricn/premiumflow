"""
Output formatting for roll chain analysis.

This module handles formatting and displaying analysis results.
"""

from decimal import Decimal
from typing import List, Dict, Any
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from ..core.models import Transaction, RollChain


def format_position_spec(symbol: str, strike: Decimal, option_type: str, expiration: str) -> str:
    """Format position specification for lookup."""
    return f"{symbol} ${strike} {option_type} {expiration}"


def format_transaction_summary(transaction: Transaction) -> str:
    """Format a single transaction for display."""
    return (
        f"{transaction.date.strftime('%m/%d/%Y'):12} | "
        f"{transaction.symbol:6} | "
        f"{transaction.action:6} | "
        f"{transaction.quantity:8} | "
        f"{transaction.price:12} | "
        f"{transaction.position_spec}"
    )


def format_roll_chain_summary(chain: RollChain) -> str:
    """Format a roll chain summary for display."""
    summary = f"""
Roll Chain: {chain.symbol} ${chain.strike} {chain.option_type} {chain.expiration}
Status: {'CLOSED' if chain.is_closed else 'OPEN'}
Net Quantity: {chain.net_quantity}
Total Credits: ${chain.total_credits:,.2f}
Total Debits: ${chain.total_debits:,.2f}
Net P&L: ${chain.net_pnl:,.2f}
Total Fees: ${chain.total_fees:,.2f}
Net P&L (after fees): ${chain.net_pnl_after_fees:,.2f}
Breakeven: ${chain.breakeven_price:,.2f if chain.breakeven_price else 'N/A'}
Transactions: {len(chain.transactions)}
"""
    return summary.strip()


def create_roll_chain_table(chains: List[RollChain]) -> Table:
    """Create a Rich table for displaying roll chains."""
    table = Table(title="Roll Chains Analysis")
    
    table.add_column("Symbol", style="cyan")
    table.add_column("Strike", style="magenta")
    table.add_column("Type", style="green")
    table.add_column("Status", style="yellow")
    table.add_column("Net Qty", justify="right")
    table.add_column("Net P&L", justify="right", style="green")
    table.add_column("Breakeven", justify="right")
    table.add_column("Txs", justify="right")
    
    for chain in chains:
        status = "CLOSED" if chain.is_closed else "OPEN"
        pnl_color = "green" if chain.net_pnl_after_fees >= 0 else "red"
        
        table.add_row(
            chain.symbol,
            f"${chain.strike}",
            chain.option_type,
            status,
            str(chain.net_quantity),
            f"[{pnl_color}]${chain.net_pnl_after_fees:,.2f}[/{pnl_color}]",
            f"${chain.breakeven_price:,.2f}" if chain.breakeven_price else "N/A",
            str(len(chain.transactions))
        )
    
    return table


def display_roll_chains(chains: List[RollChain], console: Console = None):
    """Display roll chains using Rich formatting."""
    if console is None:
        console = Console()
    
    if not chains:
        console.print("[yellow]No roll chains found.[/yellow]")
        return
    
    table = create_roll_chain_table(chains)
    console.print(table)
    
    # Show detailed summary for each chain
    for i, chain in enumerate(chains, 1):
        console.print(f"\n[bold]Chain {i}:[/bold]")
        console.print(Panel(
            format_roll_chain_summary(chain),
            title=f"{chain.symbol} ${chain.strike} {chain.option_type}",
            border_style="blue"
        ))
