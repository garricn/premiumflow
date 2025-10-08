#!/usr/bin/env python3
"""
Roll - Trading transaction analysis tool
"""

import csv
import sys
import os
from datetime import datetime
from typing import List, Dict, Any
import argparse


def parse_date(date_str: str) -> datetime:
    """Parse date string in M/D/YYYY format."""
    return datetime.strptime(date_str, '%m/%d/%Y')


def is_options_transaction(row: Dict[str, str]) -> bool:
    """
    Determine if a transaction is options-related.
    
    Options transactions have:
    - Trans codes like BTC, STO, OASGN
    - Descriptions containing Call/Put with strike prices
    """
    trans_code = (row.get('Trans Code') or '').strip()
    description = (row.get('Description') or '').strip()
    
    # Check for options-specific transaction codes
    options_codes = {'BTC', 'STO', 'OASGN'}
    if trans_code in options_codes:
        return True
    
    # Check for Call/Put in description
    if 'Call' in description or 'Put' in description:
        return True
    
    return False


def format_options_transaction(row: Dict[str, str]) -> str:
    """Format an options transaction for display."""
    activity_date = row.get('Activity Date', '')
    instrument = row.get('Instrument', '')
    description = row.get('Description', '')
    trans_code = row.get('Trans Code', '')
    quantity = row.get('Quantity', '')
    price = row.get('Price', '')
    amount = row.get('Amount', '')
    
    return (f"{activity_date:12} | {instrument:6} | {trans_code:6} | "
            f"{quantity:8} | {price:12} | {amount:15} | {description}")


def get_options_transactions(csv_file: str) -> List[Dict[str, str]]:
    """Extract all options transactions from CSV file."""
    options_txns = []
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Skip empty rows
            if not row.get('Activity Date'):
                continue
                
            if is_options_transaction(row):
                options_txns.append(row)
    
    return options_txns


def is_call_option(description: str) -> bool:
    """Check if the option is a Call."""
    return 'Call' in description


def is_put_option(description: str) -> bool:
    """Check if the option is a Put."""
    return 'Put' in description


def group_by_ticker(transactions: List[Dict[str, str]]) -> Dict[str, List[Dict[str, str]]]:
    """Group transactions by ticker symbol."""
    grouped = {}
    for txn in transactions:
        ticker = txn.get('Instrument', '').strip()
        if ticker not in grouped:
            grouped[ticker] = []
        grouped[ticker].append(txn)
    return grouped


def deduplicate_transactions(transactions: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Remove duplicate transactions with identical details."""
    seen = set()
    unique_txns = []
    
    for txn in transactions:
        # Create a unique key for this transaction
        key = (
            txn.get('Activity Date', ''),
            txn.get('Instrument', ''),
            txn.get('Trans Code', ''),
            txn.get('Quantity', ''),
            txn.get('Price', ''),
            txn.get('Description', '')
        )
        
        if key not in seen:
            seen.add(key)
            unique_txns.append(txn)
    
    return unique_txns


def detect_roll_chains(transactions: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    """
    Detect roll chains - sequences of connected positions.
    A roll chain: Open -> Close+Open -> Close+Open -> ... -> Close
    Minimum: 3 transactions (Open, Close+Open, Close)
    """
    import re
    from typing import Any
    
    # First detect individual rolls
    rolls = detect_rolls(transactions)
    
    # Deduplicate and sort transactions
    unique_txns = deduplicate_transactions(transactions)
    unique_txns.sort(key=lambda x: parse_date(x.get('Activity Date', '')))
    
    # Group by ticker
    by_ticker = {}
    for txn in unique_txns:
        ticker = txn.get('Instrument', '').strip()
        if ticker not in by_ticker:
            by_ticker[ticker] = []
        by_ticker[ticker].append(txn)
    
    chains = []
    
    # For each ticker, build roll chains
    for ticker, txns in by_ticker.items():
        # Track which transactions are part of chains
        used_txns = set()
        
        # Start with each opening position (STO/BTO)
        for i, open_txn in enumerate(txns):
            if open_txn.get('Trans Code') not in ['STO', 'BTO']:
                continue
            
            txn_id = id(open_txn)
            if txn_id in used_txns:
                continue
            
            # Try to build a chain starting from this opening
            chain = build_chain(open_txn, txns, rolls, used_txns)
            
            if chain and len(chain['transactions']) >= 3:  # Minimum: Open, Roll, Close
                chains.append(chain)
                # Mark all transactions in this chain as used
                for txn in chain['transactions']:
                    used_txns.add(id(txn))
    
    return chains


def build_chain(initial_open, all_txns, rolls, used_txns):
    """Build a roll chain starting from an initial opening position."""
    import re
    
    chain_txns = [initial_open]
    current_position = initial_open.get('Description', '')
    ticker = initial_open.get('Instrument', '').strip()
    
    # Find all subsequent rolls and closes for this position
    open_date = parse_date(initial_open.get('Activity Date', ''))
    
    while True:
        # Look for a roll or close of the current position
        next_txn = None
        is_roll = False
        
        # Check if there's a roll involving the current position
        for roll in rolls:
            if (roll['ticker'] == ticker and 
                roll.get('btc_desc') == current_position and
                id(get_txn_by_desc_date(all_txns, current_position, roll['date'], 'BTC')) not in used_txns):
                # Found a roll closing this position
                btc_txn = get_txn_by_desc_date(all_txns, current_position, roll['date'], 'BTC')
                sto_txn = get_txn_by_desc_date(all_txns, roll['sto_desc'], roll['date'], 'STO')
                
                if btc_txn and sto_txn:
                    chain_txns.append(btc_txn)
                    chain_txns.append(sto_txn)
                    current_position = roll['sto_desc']
                    is_roll = True
                    break
        
        if is_roll:
            continue
        
        # No roll found, look for a simple close
        for txn in all_txns:
            if (txn.get('Description', '') == current_position and
                txn.get('Trans Code') in ['BTC', 'STC'] and
                parse_date(txn.get('Activity Date', '')) > open_date and
                id(txn) not in used_txns):
                chain_txns.append(txn)
                next_txn = txn
                break
        
        break  # End of chain
    
    if len(chain_txns) < 2:
        return None
    
    # Calculate chain statistics
    total_credits = 0
    total_debits = 0
    roll_count = 0
    
    for txn in chain_txns:
        amount = float(txn.get('Amount', '0').replace('$', '').replace(',', '').replace('(', '-').replace(')', ''))
        if txn.get('Trans Code') in ['STO', 'BTO']:
            total_credits += abs(amount)
        elif txn.get('Trans Code') in ['BTC', 'STC']:
            total_debits += abs(amount)
    
    # Count rolls (pairs of BTC+STO in the chain)
    for i in range(len(chain_txns) - 1):
        if (chain_txns[i].get('Trans Code') in ['BTC', 'STC'] and
            i + 1 < len(chain_txns) and
            chain_txns[i + 1].get('Trans Code') in ['STO', 'BTO']):
            roll_count += 1
    
    net_pnl = total_credits - total_debits
    
    # Determine if chain is open or closed
    # Chain is OPEN if last transaction is an opening (STO/BTO)
    # Chain is CLOSED if last transaction is a closing (BTC/STC)
    last_txn_code = chain_txns[-1].get('Trans Code', '')
    status = 'OPEN' if last_txn_code in ['STO', 'BTO'] else 'CLOSED'
    
    return {
        'ticker': ticker,
        'start_date': chain_txns[0].get('Activity Date', ''),
        'end_date': chain_txns[-1].get('Activity Date', ''),
        'transactions': chain_txns,
        'roll_count': roll_count,
        'total_credits': total_credits,
        'total_debits': total_debits,
        'net_pnl': net_pnl,
        'initial_position': chain_txns[0].get('Description', ''),
        'final_position': chain_txns[-1].get('Description', '') if len(chain_txns) > 1 else None,
        'status': status
    }


def get_txn_by_desc_date(txns, description, date, trans_code):
    """Find a transaction by description, date, and transaction code."""
    for txn in txns:
        if (txn.get('Description', '') == description and
            txn.get('Activity Date', '') == date and
            txn.get('Trans Code', '') == trans_code):
            return txn
    return None


def detect_rolls(transactions: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Detect rolls based on the correct definition:
    A roll = BTC + STO (or STC + BTO) on the SAME day, same quantity, same option type.
    The close+open pair is what defines a roll.
    """
    rolls = []
    
    # Deduplicate transactions first
    unique_txns = deduplicate_transactions(transactions)
    
    # Group by ticker and date
    by_ticker_date = {}
    for txn in unique_txns:
        ticker = txn.get('Instrument', '').strip()
        date = txn.get('Activity Date', '')
        key = (ticker, date)
        if key not in by_ticker_date:
            by_ticker_date[key] = []
        by_ticker_date[key].append(txn)
    
    # Look for same-day BTC+STO pairs (or STC+BTO for long positions)
    # We need to look at ALL transactions to find when positions were opened
    for (ticker, date), txns in by_ticker_date.items():
        if len(txns) < 2:
            continue
            
        # Get close and open transactions
        close_txns = [txn for txn in txns if txn.get('Trans Code') in ['BTC', 'STC']]
        open_txns = [txn for txn in txns if txn.get('Trans Code') in ['STO', 'BTO']]
        
        # Track which transactions have been matched to avoid duplicates
        matched_close = set()
        matched_open = set()
        
        # Match close+open pairs
        import re
        
        # For each close transaction, find when that position was originally opened
        # This helps us match the right BTC with the right STO when there are multiples
        # The BTC leg of a roll MUST match an earlier position by ticker, expiry, strategy, and strike
        close_with_open_dates = []
        for i, close_txn in enumerate(close_txns):
            close_desc = close_txn.get('Description', '')
            
            # Find the original STO/BTO for this position
            # Must match: Ticker, Expiration, Strategy (Call/Put), Strike
            # All of this is encoded in the Description field
            open_date = None
            for txn in unique_txns:
                if (txn.get('Instrument', '').strip() == ticker and
                    txn.get('Trans Code') in ['STO', 'BTO'] and
                    txn.get('Description', '') == close_desc and  # Exact match on contract details
                    parse_date(txn.get('Activity Date', '')) < parse_date(date)):
                    # Found an earlier open for this exact position
                    if open_date is None or parse_date(txn.get('Activity Date', '')) > parse_date(open_date):
                        open_date = txn.get('Activity Date', '')
            
            # Only include BTCs that have a matching earlier position
            # If open_date is None, this BTC doesn't match any earlier position
            if open_date is not None:
                close_with_open_dates.append((i, close_txn, open_date))
        
        # Sort by open date (oldest positions first) - these are more likely to be rolled
        close_with_open_dates.sort(key=lambda x: parse_date(x[2]))
        
        for i, close_txn, open_date in close_with_open_dates:
            if i in matched_close:
                continue
                
            close_qty = int(close_txn.get('Quantity', '0'))
            close_desc = close_txn.get('Description', '')
            close_code = close_txn.get('Trans Code', '')
            
            # Extract strike from close transaction
            close_strike = re.search(r'\$(\d+\.?\d*)', close_desc)
            
            for j, open_txn in enumerate(open_txns):
                if j in matched_open:
                    continue
                    
                open_qty = int(open_txn.get('Quantity', '0'))
                open_desc = open_txn.get('Description', '')
                open_code = open_txn.get('Trans Code', '')
                
                # Extract strike from open transaction
                open_strike = re.search(r'\$(\d+\.?\d*)', open_desc)
                
                # Match quantities and option type
                # BTC must pair with STO (short positions)
                # STC must pair with BTO (long positions)
                is_valid_pair = (
                    (close_code == 'BTC' and open_code == 'STO') or
                    (close_code == 'STC' and open_code == 'BTO')
                )
                
                same_quantity = (close_qty == open_qty and close_qty > 0)
                same_type = (
                    ('Call' in close_desc and 'Call' in open_desc) or 
                    ('Put' in close_desc and 'Put' in open_desc)
                )
                
                # A roll should have different strikes or different expirations
                different_position = False
                if close_strike and open_strike:
                    different_position = (close_strike.group(1) != open_strike.group(1))
                
                # If strikes are the same, check expiration dates
                if not different_position and close_strike and open_strike:
                    close_exp = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', close_desc)
                    open_exp = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', open_desc)
                    if close_exp and open_exp:
                        different_position = (close_exp.group(1) != open_exp.group(1))
                
                if is_valid_pair and same_quantity and same_type and different_position:
                    # Calculate net credit/debit
                    close_amount = float(close_txn.get('Amount', '0').replace('$', '').replace(',', '').replace('(', '-').replace(')', ''))
                    open_amount = float(open_txn.get('Amount', '0').replace('$', '').replace(',', '').replace('(', '-').replace(')', ''))
                    net_credit = open_amount + close_amount
                    
                    roll_info = {
                        'type': 'ROLL',
                        'ticker': ticker,
                        'date': date,
                        'quantity': close_qty,
                        'close_code': close_code,
                        'open_code': open_code,
                        'btc_desc': close_desc,  # Keep field names for backward compatibility
                        'sto_desc': open_desc,
                        'btc_price': close_txn.get('Price', ''),
                        'sto_price': open_txn.get('Price', ''),
                        'net_credit': net_credit,
                        'btc_amount': close_amount,
                        'sto_amount': open_amount,
                        'original_open_date': open_date  # Track when the original position was opened
                    }
                    rolls.append(roll_info)
                    
                    # Mark these transactions as matched
                    matched_close.add(i)
                    matched_open.add(j)
                    break  # Move to next close transaction
    
    return rolls




def print_ticker_group(ticker: str, transactions: List[Dict[str, str]]):
    """Print a group of transactions for a specific ticker."""
    print(f"\n  {ticker} ({len(transactions)} transactions)")
    print(f"  {'-' * 138}")
    print(f"  {'Date':12} | {'Action':6} | {'Quantity':8} | "
          f"{'Price':12} | {'Amount':15} | Description{' ' * 32} | {'Chain':14}")
    print(f"  {'-' * 138}")
    
    # Detect roll chains for this ticker
    chains = detect_roll_chains(transactions)
    
    # Create a mapping of transaction ID to chain ID with status
    txn_to_chain = {}
    for chain_num, chain in enumerate(chains, 1):
        chain_id = f"RC-{chain_num:03d}"
        status = chain['status']
        chain_display = f"{chain_id} ({status})"
        for txn in chain['transactions']:
            txn_to_chain[id(txn)] = chain_display
    
    # Print transactions with chain ID
    for txn in transactions:
        activity_date = txn.get('Activity Date', '')
        description = txn.get('Description', '')
        trans_code = txn.get('Trans Code', '')
        quantity = txn.get('Quantity', '')
        price = txn.get('Price', '')
        amount = txn.get('Amount', '')
        
        # Get chain ID with status if this transaction is part of a chain
        chain_info = txn_to_chain.get(id(txn), '')
        
        print(f"  {activity_date:12} | {trans_code:6} | "
              f"{quantity:8} | {price:12} | {amount:15} | {description:47} | {chain_info:14}")
    
    # Print roll chain summary
    ticker_chains = [chain for chain in chains if chain['ticker'] == ticker]
    if ticker_chains:
        print(f"\n  🔗 ROLL CHAINS DETECTED ({len(ticker_chains)} chains):")
        for i, chain in enumerate(ticker_chains, 1):
            chain_id = f"RC-{i:03d}"
            status = chain['status']
            status_emoji = "🔒" if status == "CLOSED" else "🔓"
            
            # Calculate total fees for the chain
            total_fees = len(chain['transactions']) * 0.04  # $0.04 per contract leg
            
            # Get the current position (last transaction in the chain)
            last_txn = chain['transactions'][-1]
            current_position = format_position_spec(last_txn.get('Description', ''))
            
            # Display header with current position
            print(f"\n    {chain_id} ({status_emoji} {status}): {current_position}")
            print(f"    {chain['start_date']} → {chain['end_date']} | Rolls: {chain['roll_count']}")
            
            # Calculate realized P&L for each position leg (STO/BTO -> BTC/STC pairs)
            realized_legs = []
            txns = chain['transactions']
            i = 0
            while i < len(txns):
                txn = txns[i]
                if txn.get('Trans Code') in ['STO', 'BTO']:
                    # Opening transaction - look for corresponding close
                    open_desc = txn.get('Description', '')
                    open_amount = float(txn.get('Amount', '0').replace('$', '').replace(',', '').replace('(', '-').replace(')', ''))
                    
                    # Find the close for this position
                    for j in range(i + 1, len(txns)):
                        close_txn = txns[j]
                        if (close_txn.get('Trans Code') in ['BTC', 'STC'] and 
                            close_txn.get('Description', '') == open_desc):
                            close_amount = float(close_txn.get('Amount', '0').replace('$', '').replace(',', '').replace('(', '-').replace(')', ''))
                            realized_pnl = open_amount + close_amount  # close_amount is negative
                            realized_legs.append({
                                'position': open_desc,
                                'open_date': txn.get('Activity Date', ''),
                                'close_date': close_txn.get('Activity Date', ''),
                                'pnl': realized_pnl
                            })
                            break
                i += 1
            
            # Create summary table
            print(f"    {'-' * 80}")
            print(f"    {'Metric':25} | {'Value':>15}")
            print(f"    {'-' * 80}")
            print(f"    {'Credits Received':25} | ${chain['total_credits']:>14,.2f}")
            print(f"    {'Debits Paid':25} | ${chain['total_debits']:>14,.2f}")
            print(f"    {'Fees':25} | ${total_fees:>14.2f}")
            
            if status == "CLOSED":
                # For closed chains, show net realized P&L
                net_pnl_after_fees = chain['net_pnl'] - total_fees
                net_text = f"${net_pnl_after_fees:,.2f} profit" if net_pnl_after_fees > 0 else f"${abs(net_pnl_after_fees):,.2f} loss"
                print(f"    {'Net Realized P&L':25} | {net_text:>15}")
            else:
                # For open chains, show target breakeven price
                net_so_far = chain['total_credits'] - chain['total_debits'] - total_fees
                
                # Get the last transaction (the open position)
                last_txn = chain['transactions'][-1]
                qty = int(last_txn.get('Quantity', '1'))
                last_txn_code = last_txn.get('Trans Code', '')
                
                # Breakeven price per share (each contract = 100 shares)
                breakeven_per_share = net_so_far / (qty * 100)
                
                # Determine the breakeven direction based on transaction type
                if last_txn_code in ['STO', 'BTO']:
                    # Short position (STO/BTO) - need to close at lower price
                    breakeven_text = f"${breakeven_per_share:.2f} or less"
                elif last_txn_code in ['STC', 'BTC']:
                    # Long position (STC/BTC) - need to close at higher price  
                    breakeven_text = f"${breakeven_per_share:.2f} or more"
                else:
                    # Fallback
                    breakeven_text = f"${breakeven_per_share:.2f}"
                
                print(f"    {'Net So Far':25} | ${net_so_far:>14,.2f}")
                print(f"    {'Breakeven Price':25} | {breakeven_text:>15}")
            
            # Display realized P&L for each leg in table format
            if realized_legs:
                print(f"    {'-' * 80}")
                print(f"    {'Realized P&L by Position':>50}")
                print(f"    {'-' * 80}")
                print(f"    {'Period':20} | {'P&L':>15}")
                print(f"    {'-' * 80}")
                for leg in realized_legs:
                    period = f"{leg['open_date']} → {leg['close_date']}"
                    pnl_text = f"${leg['pnl']:,.2f} gain" if leg['pnl'] > 0 else f"${abs(leg['pnl']):,.2f} loss"
                    print(f"    {period:20} | {pnl_text:>15}")
                print(f"    {'-' * 80}")
            
            # Show transaction table
            print(f"    {'-' * 130}")
            print(f"    {'#':3} | {'Date':12} | {'Action':6} | {'Qty':3} | {'Price':12} | {'Amount':15} | {'Fees':8} | Contract")
            print(f"    {'-' * 130}")
            for j, txn in enumerate(chain['transactions'], 1):
                desc = txn.get('Description', '')
                date = txn.get('Activity Date', '')
                code = txn.get('Trans Code', '')
                qty = txn.get('Quantity', '')
                price = txn.get('Price', '')
                amount = txn.get('Amount', '')
                # Calculate fees: $0.04 per contract
                qty_num = int(qty) if qty else 0
                fees = qty_num * 0.04
                print(f"    {j:3} | {date:12} | {code:6} | {qty:3} | {price:12} | {amount:15} | ${fees:6.2f} | {desc}")


def injest_options(csv_file: str, ticker_filter: str = None, strategy_filter: str = None):
    """Process and display options transactions sorted by date."""
    options_txns = get_options_transactions(csv_file)
    
    # Filter by ticker if specified
    if ticker_filter:
        ticker_filter = ticker_filter.upper()
        options_txns = [txn for txn in options_txns if txn.get('Instrument', '').strip().upper() == ticker_filter]
        if not options_txns:
            print(f"No options transactions found for ticker: {ticker_filter}")
            return
    
    # Sort by activity date (most recent first)
    options_txns.sort(key=lambda x: parse_date(x['Activity Date']), reverse=True)
    
    # Separate calls and puts
    calls = [txn for txn in options_txns if is_call_option(txn.get('Description', ''))]
    puts = [txn for txn in options_txns if is_put_option(txn.get('Description', ''))]
    
    # Filter by strategy if specified
    if strategy_filter:
        strategy_filter = strategy_filter.lower()
        if strategy_filter == 'calls':
            options_txns = calls
            calls = calls
            puts = []
        elif strategy_filter == 'puts':
            options_txns = puts
            calls = []
            puts = puts
        else:
            print(f"Invalid strategy filter. Use 'calls' or 'puts'")
            return
    
    # Group by ticker
    calls_by_ticker = group_by_ticker(calls)
    puts_by_ticker = group_by_ticker(puts)
    
    # Print header with filters
    header_parts = ["OPTIONS TRANSACTIONS"]
    if ticker_filter:
        header_parts.append(ticker_filter)
    if strategy_filter:
        header_parts.append(strategy_filter.upper())
    
    header = " - ".join(header_parts)
    
    print(f"\n{'=' * 120}")
    print(f"{header}")
    print(f"{'=' * 120}")
    
    # Print CALLS section (if not filtered to puts only)
    if calls:
        print(f"\n{'=' * 120}")
        print(f"CALLS ({len(calls)} transactions)")
        print(f"{'=' * 120}")
        
        # Sort tickers alphabetically
        for ticker in sorted(calls_by_ticker.keys()):
            print_ticker_group(ticker, calls_by_ticker[ticker])
    
    # Print PUTS section (if not filtered to calls only)
    if puts:
        print(f"\n{'=' * 120}")
        print(f"PUTS ({len(puts)} transactions)")
        print(f"{'=' * 120}")
        
        # Sort tickers alphabetically
        for ticker in sorted(puts_by_ticker.keys()):
            print_ticker_group(ticker, puts_by_ticker[ticker])
    
    print(f"\n{'=' * 120}")
    print(f"Total options transactions: {len(options_txns)} (Calls: {len(calls)}, Puts: {len(puts)})")
    print(f"{'=' * 120}")


def format_position_spec(description: str) -> str:
    """
    Format a transaction description into position specification format.
    Example: "TSLA 11/21/2025 Call $550.00" -> "TSLA $550 CALL 11/21/2025"
    """
    import re
    
    # Extract components using regex
    # Pattern: TICKER DATE Call/Put $STRIKE
    pattern = r'(\w+)\s+(\d{1,2}/\d{1,2}/\d{4})\s+(Call|Put)\s+\$(\d+(?:\.\d+)?)'
    match = re.match(pattern, description)
    
    if match:
        ticker = match.group(1)
        expiration = match.group(2)
        option_type = match.group(3).upper()
        strike = match.group(4)
        
        # Format strike to remove trailing zeros
        strike_float = float(strike)
        if strike_float == int(strike_float):
            strike = str(int(strike_float))
        else:
            strike = strike
        
        return f"{ticker} ${strike} {option_type} {expiration}"
    
    return description


def parse_lookup_input(lookup_str: str) -> Dict[str, str]:
    """
    Parse lookup input in format: "TICKER $STRIKE TYPE MM/DD/YY"
    Example: "IREN $70 CALL 2/20/26"
    """
    parts = lookup_str.strip().split()
    if len(parts) != 4:
        raise ValueError("Lookup format must be: TICKER $STRIKE TYPE MM/DD/YY")
    
    ticker = parts[0].upper()
    strike_str = parts[1]
    option_type = parts[2].upper()
    expiration = parts[3]
    
    # Validate strike format
    if not strike_str.startswith('$'):
        raise ValueError("Strike must start with $")
    strike = strike_str[1:]  # Remove $ sign
    
    # Validate option type
    if option_type not in ['CALL', 'PUT']:
        raise ValueError("Option type must be CALL or PUT")
    
    return {
        'ticker': ticker,
        'strike': strike,
        'option_type': option_type,
        'expiration': expiration
    }


def find_chain_by_position(csv_file: str, lookup_spec: Dict[str, str]) -> Dict[str, Any]:
    """
    Find roll chain containing the specified position.
    Returns chain info if found, None otherwise.
    """
    # Read all options transactions
    options_txns = []
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if is_options_transaction(row):
                options_txns.append(row)
    
    # Group by ticker and strategy
    calls_by_ticker = {}
    puts_by_ticker = {}
    
    for txn in options_txns:
        instrument = (txn.get('Instrument') or '').strip()
        description = (txn.get('Description') or '').strip()
        
        if 'Call' in description:
            if instrument not in calls_by_ticker:
                calls_by_ticker[instrument] = []
            calls_by_ticker[instrument].append(txn)
        elif 'Put' in description:
            if instrument not in puts_by_ticker:
                puts_by_ticker[instrument] = []
            puts_by_ticker[instrument].append(txn)
    
    # Check the appropriate ticker group
    ticker = lookup_spec['ticker']
    option_type = lookup_spec['option_type']
    
    if option_type == 'CALL':
        ticker_txns = calls_by_ticker.get(ticker, [])
    else:
        ticker_txns = puts_by_ticker.get(ticker, [])
    
    if not ticker_txns:
        return None
    
    # Detect roll chains for this ticker
    chains = detect_roll_chains(ticker_txns)
    
    # Look for matching position in any chain
    for chain in chains:
        for txn in chain['transactions']:
            description = txn.get('Description', '')
            
            # Check if this transaction matches our lookup criteria
            if (lookup_spec['ticker'] in description and 
                f"${lookup_spec['strike']}" in description and
                lookup_spec['option_type'].title() in description and
                lookup_spec['expiration'] in description):
                return chain
    
    return None


def lookup_chain(csv_file: str, lookup_str: str):
    """Look up a roll chain by position specification."""
    try:
        lookup_spec = parse_lookup_input(lookup_str)
    except ValueError as e:
        print(f"Error: {e}")
        print("Format: TICKER $STRIKE TYPE MM/DD/YY")
        print("Example: IREN $70 CALL 2/20/26")
        return
    
    chain = find_chain_by_position(csv_file, lookup_spec)
    
    if chain:
        print(f"\n🔍 FOUND ROLL CHAIN for {lookup_str}")
        print(f"{'=' * 80}")
        
        # Display chain summary
        status = chain['status']
        status_emoji = "🔒" if status == "CLOSED" else "🔓"
        
        print(f"\nChain: {chain['start_date']} → {chain['end_date']} | Status: {status_emoji} {status} | Rolls: {chain['roll_count']}")
        
        # Calculate fees
        total_fees = sum(int(txn.get('Quantity', '0')) * 0.04 for txn in chain['transactions'])
        
        # Create summary table
        print(f"\n{'Metric':25} | {'Value':>15}")
        print(f"{'-' * 45}")
        print(f"{'Credits Received':25} | ${chain['total_credits']:>14,.2f}")
        print(f"{'Debits Paid':25} | ${chain['total_debits']:>14,.2f}")
        print(f"{'Fees':25} | ${total_fees:>14.2f}")
        
        if status == "CLOSED":
            net_pnl_after_fees = chain['net_pnl'] - total_fees
            net_text = f"${net_pnl_after_fees:,.2f} profit" if net_pnl_after_fees > 0 else f"${abs(net_pnl_after_fees):,.2f} loss"
            print(f"{'Net Realized P&L':25} | {net_text:>15}")
        else:
            net_so_far = chain['total_credits'] - chain['total_debits'] - total_fees
            last_txn = chain['transactions'][-1]
            qty = int(last_txn.get('Quantity', '1'))
            last_txn_code = last_txn.get('Trans Code', '')
            
            breakeven_per_share = net_so_far / (qty * 100)
            
            if last_txn_code in ['STO', 'BTO']:
                breakeven_text = f"${breakeven_per_share:.2f} or less"
            elif last_txn_code in ['STC', 'BTC']:
                breakeven_text = f"${breakeven_per_share:.2f} or more"
            else:
                breakeven_text = f"${breakeven_per_share:.2f}"
            
            print(f"{'Net So Far':25} | ${net_so_far:>14,.2f}")
            print(f"{'Breakeven Price':25} | {breakeven_text:>15}")
        
        # Show transaction table
        print(f"\n{'Transactions:'}")
        print(f"{'-' * 130}")
        print(f"{'#':3} | {'Date':12} | {'Action':6} | {'Qty':3} | {'Price':12} | {'Amount':15} | {'Fees':8} | Contract")
        print(f"{'-' * 130}")
        for i, txn in enumerate(chain['transactions'], 1):
            desc = txn.get('Description', '')
            date = txn.get('Activity Date', '')
            code = txn.get('Trans Code', '')
            qty = txn.get('Quantity', '')
            price = txn.get('Price', '')
            amount = txn.get('Amount', '')
            qty_num = int(qty) if qty else 0
            fees = qty_num * 0.04
            print(f"{i:3} | {date:12} | {code:6} | {qty:3} | {price:12} | {amount:15} | ${fees:6.2f} | {desc}")
        
    else:
        print(f"\n❌ NO ROLL CHAIN FOUND for {lookup_str}")
        print("This position is not part of any detected roll chain.")


def main():
    parser = argparse.ArgumentParser(description='Roll - Trading transaction analysis tool')
    parser.add_argument('command', choices=['injest', 'lookup'], help='Command to execute')
    parser.add_argument('--options', action='store_true', help='Show options transactions')
    parser.add_argument('--ticker', type=str, help='Filter by ticker symbol (e.g., TSLA)')
    parser.add_argument('--strategy', choices=['calls', 'puts'], help='Filter by strategy: calls or puts')
    parser.add_argument('--file', help='CSV file to process (default: all_transactions.csv)')
    parser.add_argument('position', nargs='?', help='Position to lookup (e.g., "IREN $70 CALL 2/20/26")')
    
    args = parser.parse_args()
    
    # Use provided file or default
    csv_file = args.file if args.file else 'all_transactions.csv'
    
    # Check if file exists
    if not os.path.exists(csv_file):
        print(f"Error: File '{csv_file}' not found")
        sys.exit(1)
    
    if args.command == 'injest':
        if args.options:
            injest_options(csv_file, args.ticker, args.strategy)
        else:
            print("Please specify --options flag")
            sys.exit(1)
    elif args.command == 'lookup':
        if not args.position:
            print("Error: Position required for lookup command")
            print("Usage: roll lookup \"TICKER $STRIKE TYPE MM/DD/YY\"")
            print("Example: roll lookup \"IREN $70 CALL 2/20/26\"")
            sys.exit(1)
        lookup_chain(csv_file, args.position)
    else:
        print(f"Unknown command: {args.command}")
        sys.exit(1)


if __name__ == '__main__':
    main()

