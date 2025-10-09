"""
Command-line interface for rollchain.

This module provides the CLI commands using Click.
"""

import click
from rich.console import Console
from rich.panel import Panel
from ..core.parser import parse_csv_file, get_options_transactions
from ..services.chain_builder import detect_roll_chains
from ..formatters.output import display_roll_chains


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
def analyze(csv_file, output_format):
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
        
        # Display results
        if output_format == 'table':
            # Convert dict chains to a simple table format
            from rich.table import Table
            table = Table(title="Roll Chains Analysis")
            
            table.add_column("Symbol", style="cyan")
            table.add_column("Strike", style="magenta")
            table.add_column("Type", style="green")
            table.add_column("Transactions", justify="right")
            
            for chain in chains:
                table.add_row(
                    chain.get('symbol', ''),
                    f"${chain.get('strike', 0)}",
                    chain.get('option_type', ''),
                    str(len(chain.get('transactions', [])))
                )
            
            console.print(table)
        elif output_format == 'summary':
            for i, chain in enumerate(chains, 1):
                console.print(f"\n[bold]Chain {i}:[/bold]")
                console.print(Panel(
                    f"Symbol: {chain.get('symbol', '')}\n"
                    f"Strike: ${chain.get('strike', 0)}\n"
                    f"Type: {chain.get('option_type', '')}\n"
                    f"Transactions: {len(chain.get('transactions', []))}",
                    title=f"Chain {i}",
                    border_style="blue"
                ))
        else:  # raw
            for i, chain in enumerate(chains, 1):
                console.print(f"\nChain {i}: {chain}")
        
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()


@main.command()
@click.argument('csv_file', type=click.Path(exists=True))
def ingest(csv_file):
    """Ingest and display raw options transactions from CSV."""
    console = Console()
    
    try:
        console.print(f"[blue]Ingesting {csv_file}...[/blue]")
        transactions = get_options_transactions(csv_file)
        
        console.print(f"[green]Found {len(transactions)} options transactions[/green]")
        
        # Display transactions in a table
        from rich.table import Table
        table = Table(title="Options Transactions")
        
        table.add_column("Date", style="cyan")
        table.add_column("Symbol", style="magenta")
        table.add_column("Code", style="green")
        table.add_column("Quantity", justify="right")
        table.add_column("Price", justify="right")
        table.add_column("Description", style="yellow")
        
        for txn in transactions:
            table.add_row(
                txn.get('Activity Date', ''),
                txn.get('Instrument', ''),
                txn.get('Trans Code', ''),
                txn.get('Quantity', ''),
                txn.get('Price', ''),
                txn.get('Description', '')
            )
        
        console.print(table)
        
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


if __name__ == '__main__':
    main()
