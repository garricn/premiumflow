"""
Roll chain detection and building functionality.

This module handles detecting and building roll chains from transaction data.
"""

from typing import List, Dict, Any, Set
from decimal import Decimal
from ..core.models import Transaction, RollChain
from ..core.parser import parse_date


def detect_rolls(transactions: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    """Detect individual roll transactions (BTC + STO on same day)."""
    import re
    
    rolls = []
    by_date = {}
    
    # Group transactions by date
    for txn in transactions:
        date = txn.get('Activity Date', '')
        if date not in by_date:
            by_date[date] = []
        by_date[date].append(txn)
    
    # Look for rolls on each date
    for date, txns in by_date.items():
        btc_txns = [t for t in txns if t.get('Trans Code') == 'BTC']
        sto_txns = [t for t in txns if t.get('Trans Code') == 'STO']
        
        for btc in btc_txns:
            for sto in sto_txns:
                if btc.get('Instrument') == sto.get('Instrument'):
                    # Check if this looks like a roll (same ticker, different strikes)
                    btc_desc = btc.get('Description', '')
                    sto_desc = sto.get('Description', '')
                    
                    # Extract strikes
                    btc_strike = re.search(r'\$(\d+(?:\.\d+)?)', btc_desc)
                    sto_strike = re.search(r'\$(\d+(?:\.\d+)?)', sto_desc)
                    
                    if btc_strike and sto_strike:
                        btc_strike_val = Decimal(btc_strike.group(1))
                        sto_strike_val = Decimal(sto_strike.group(1))
                        
                        # Different strikes = roll
                        if btc_strike_val != sto_strike_val:
                            rolls.append({
                                'date': date,
                                'ticker': btc.get('Instrument', ''),
                                'btc_desc': btc_desc,
                                'sto_desc': sto_desc,
                                'btc_strike': btc_strike_val,
                                'sto_strike': sto_strike_val
                            })
    
    return rolls


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


def group_by_ticker(transactions: List[Dict[str, str]]) -> Dict[str, List[Dict[str, str]]]:
    """Group transactions by ticker symbol."""
    grouped = {}
    for txn in transactions:
        ticker = txn.get('Instrument', '').strip()
        if ticker not in grouped:
            grouped[ticker] = []
        grouped[ticker].append(txn)
    return grouped


def get_txn_by_desc_date(txns: List[Dict[str, str]], desc: str, date: str, trans_code: str) -> Dict[str, str]:
    """Find transaction by description, date, and transaction code."""
    for txn in txns:
        if (txn.get('Description') == desc and 
            txn.get('Activity Date') == date and 
            txn.get('Trans Code') == trans_code):
            return txn
    return None


def build_chain(initial_open, all_txns, rolls, used_txns):
    """Build a roll chain starting from an initial opening position."""
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
        
        if not is_roll:
            # Look for a simple close (BTC without corresponding STO)
            for txn in all_txns:
                if (txn.get('Instrument') == ticker and
                    txn.get('Trans Code') == 'BTC' and
                    txn.get('Description') == current_position and
                    id(txn) not in used_txns):
                    chain_txns.append(txn)
                    break
            break
    
    if len(chain_txns) >= 3:  # Minimum: Open, Roll, Close
        # Extract position details from first transaction
        first_desc = chain_txns[0].get('Description', '')
        import re
        strike_match = re.search(r'\$(\d+(?:\.\d+)?)', first_desc)
        strike = Decimal(strike_match.group(1)) if strike_match else Decimal('0')
        
        option_type = 'C' if 'Call' in first_desc else 'P'
        expiration = "2024-01-19"  # Simplified - should be parsed from description
        
        return {
            'transactions': chain_txns,
            'symbol': ticker,
            'strike': strike,
            'option_type': option_type,
            'expiration': expiration
        }
    
    return None


def detect_roll_chains(transactions: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    """
    Detect roll chains - sequences of connected positions.
    A roll chain: Open -> Close+Open -> Close+Open -> ... -> Close
    Minimum: 3 transactions (Open, Close+Open, Close)
    """
    # First detect individual rolls
    rolls = detect_rolls(transactions)
    
    # Deduplicate and sort transactions
    unique_txns = deduplicate_transactions(transactions)
    unique_txns.sort(key=lambda x: parse_date(x.get('Activity Date', '')))
    
    # Group by ticker
    by_ticker = group_by_ticker(unique_txns)
    
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
