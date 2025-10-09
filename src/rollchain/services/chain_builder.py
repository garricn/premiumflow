"""
Roll chain detection and building functionality.

This module handles detecting and building roll chains from transaction data.
"""

from collections import defaultdict, deque
from datetime import datetime
from decimal import Decimal, InvalidOperation
import re
from typing import Any, Dict, List, Optional, Set

from ..core.parser import parse_date


FEE_PER_CONTRACT = Decimal("0.04")
_CONTRACT_PATTERN = re.compile(
    r"(?P<symbol>\w+)\s+"
    r"(?P<expiration>\d{1,2}/\d{1,2}/\d{4})\s+"
    r"(?P<option_type>Call|Put)\s+\$(?P<strike>\d+(?:\.\d+)?)",
    re.IGNORECASE,
)


def _clean_number(value: Optional[str]) -> str:
    if not value:
        return "0"
    cleaned = value.replace(",", "").strip()
    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = f"-{cleaned[1:-1]}"
    return cleaned or "0"


def _parse_amount(value: Optional[str]) -> Decimal:
    cleaned = _clean_number((value or "").replace("$", ""))
    try:
        return Decimal(cleaned)
    except (ValueError, InvalidOperation):
        return Decimal("0")


def _parse_quantity(value: Optional[str]) -> int:
    cleaned = _clean_number(value)
    try:
        return int(Decimal(cleaned))
    except (ValueError, InvalidOperation):
        return 0


def _format_strike(strike: Decimal) -> str:
    if strike == strike.to_integral_value():
        return f"{int(strike)}"
    return f"{strike.normalize()}"


def _extract_contract_details(description: str) -> Dict[str, Any]:
    match = _CONTRACT_PATTERN.search(description or "")
    if not match:
        return {}

    strike = Decimal(match.group("strike"))
    option_label = match.group("option_type").title()
    symbol = match.group("symbol").upper()

    return {
        "symbol": symbol,
        "expiration": match.group("expiration"),
        "option_type": "C" if option_label == "Call" else "P",
        "strike": strike,
        "option_label": option_label,
        "display_name": f"{symbol} ${_format_strike(strike)} {option_label}",
    }


def detect_rolls(transactions: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    """Detect individual roll transactions (BTC + STO on same day)."""
    import re

    rolls = []
    open_codes = {'STO', 'BTO'}
    close_codes = {'BTC', 'STC'}

    open_positions: Dict[tuple, deque] = defaultdict(deque)
    close_origin_dates: Dict[int, List[datetime]] = {}

    sorted_txns = sorted(
        (
            (parse_date(txn.get('Activity Date', '')), idx, txn)
            for idx, txn in enumerate(transactions)
        ),
        key=lambda item: (item[0], item[1])
    )

    for activity_dt, _, txn in sorted_txns:
        trans_code = (txn.get('Trans Code') or '').strip().upper()
        key = (
            (txn.get('Instrument') or '').strip(),
            (txn.get('Description') or '').strip(),
        )
        qty = max(abs(_parse_quantity(txn.get('Quantity'))), 1)

        if trans_code in open_codes:
            for _ in range(qty):
                open_positions[key].append(activity_dt)
        elif trans_code in close_codes:
            assigned = close_origin_dates.setdefault(id(txn), [])
            for _ in range(qty):
                if open_positions[key]:
                    assigned.append(open_positions[key].popleft())

    by_date: Dict[str, List[Dict[str, str]]] = {}
    for txn in transactions:
        date = txn.get('Activity Date', '')
        by_date.setdefault(date, []).append(txn)

    for date, txns in by_date.items():
        btc_txns = [t for t in txns if (t.get('Trans Code') or '').strip().upper() in close_codes]
        sto_txns = [t for t in txns if (t.get('Trans Code') or '').strip().upper() in open_codes]

        close_entries = []
        for btc in btc_txns:
            origin_dates = close_origin_dates.get(id(btc), [])
            origin_dates.sort()
            open_date = origin_dates[0] if origin_dates else None
            close_entries.append((open_date, parse_date(btc.get('Activity Date', '')), btc))

        close_entries.sort(key=lambda entry: (entry[0] or parse_date('12/31/2999'), entry[1]))

        used_open_indices: Set[int] = set()

        for open_date, _, btc in close_entries:
            if open_date is None:
                continue

            btc_desc = btc.get('Description', '') or ''
            btc_details = _extract_contract_details(btc_desc)
            if not btc_details:
                continue

            btc_qty = abs(_parse_quantity(btc.get('Quantity')))
            if not btc_qty:
                continue

            for idx, sto in enumerate(sto_txns):
                if idx in used_open_indices:
                    continue

                if (btc.get('Instrument') or '').strip() != (sto.get('Instrument') or '').strip():
                    continue

                sto_desc = sto.get('Description', '') or ''
                sto_details = _extract_contract_details(sto_desc)
                if not sto_details:
                    continue

                if btc_details['option_label'] != sto_details['option_label']:
                    continue

                sto_qty = abs(_parse_quantity(sto.get('Quantity')))
                if btc_qty != sto_qty or not sto_qty:
                    continue

                same_contract = (
                    btc_details['strike'] == sto_details['strike'] and
                    btc_details['expiration'] == sto_details['expiration']
                )
                if same_contract:
                    continue

                used_open_indices.add(idx)
                rolls.append({
                    'date': date,
                    'ticker': btc.get('Instrument', ''),
                    'btc_desc': btc_desc,
                    'sto_desc': sto_desc,
                    'btc_strike': btc_details['strike'],
                    'sto_strike': sto_details['strike'],
                    'btc_expiration': btc_details['expiration'],
                    'sto_expiration': sto_details['expiration'],
                    'option_label': btc_details['option_label'],
                    'quantity': btc_qty,
                })
                break

    return rolls


def _format_amount(amount: Decimal) -> str:
    """Format Decimal amount using Robinhood-style parentheses for negatives."""
    if amount < 0:
        return f"(${abs(amount):,.2f})"
    return f"${amount:,.2f}"


def deduplicate_transactions(transactions: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Group partial fills (same contract/price) so we operate on net legs."""
    aggregated: Dict[tuple, Dict[str, str]] = {}

    for txn in transactions:
        key = (
            txn.get('Activity Date', ''),
            txn.get('Instrument', ''),
            (txn.get('Trans Code') or '').strip().upper(),
            txn.get('Price', ''),
            txn.get('Description', '')
        )

        if key not in aggregated:
            aggregated[key] = dict(txn)
            continue

        existing = aggregated[key]
        existing_qty = _parse_quantity(existing.get('Quantity'))
        incoming_qty = _parse_quantity(txn.get('Quantity'))
        combined_qty = existing_qty + incoming_qty
        existing['Quantity'] = str(combined_qty)

        existing_amount = _parse_amount(existing.get('Amount'))
        incoming_amount = _parse_amount(txn.get('Amount'))
        combined_amount = existing_amount + incoming_amount
        existing['Amount'] = _format_amount(combined_amount)

    return list(aggregated.values())


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
    current_position = initial_open.get("Description", "")
    ticker = initial_open.get("Instrument", "").strip()

    while True:
        is_roll = False

        # Check if there's a roll involving the current position
        for roll in rolls:
            if (
                roll["ticker"] == ticker
                and roll.get("btc_desc") == current_position
                and id(get_txn_by_desc_date(all_txns, current_position, roll["date"], "BTC"))
                not in used_txns
            ):
                btc_txn = get_txn_by_desc_date(all_txns, current_position, roll["date"], "BTC")
                sto_txn = get_txn_by_desc_date(all_txns, roll["sto_desc"], roll["date"], "STO")

                if btc_txn and sto_txn:
                    chain_txns.append(btc_txn)
                    chain_txns.append(sto_txn)
                    current_position = roll["sto_desc"]
                    is_roll = True
                    break

        if not is_roll:
            # Look for a simple close (BTC without corresponding STO)
            for txn in all_txns:
                if (
                    txn.get("Instrument") == ticker
                    and txn.get("Trans Code") in {"BTC", "STC", "OASGN"}
                    and txn.get("Description") == current_position
                    and id(txn) not in used_txns
                ):
                    chain_txns.append(txn)
                    break
            break

    if len(chain_txns) < 2:
        return None

    total_credits = Decimal("0")
    total_debits = Decimal("0")
    total_fees = Decimal("0")
    net_contracts = 0

    for txn in chain_txns:
        amount = _parse_amount(txn.get("Amount"))
        qty = _parse_quantity(txn.get("Quantity"))
        code = (txn.get("Trans Code") or "").strip().upper()

        if code in {"STO", "BTO"}:
            total_credits += abs(amount)
        elif code in {"BTC", "STC", "OASGN"}:
            total_debits += abs(amount)

        total_fees += abs(qty) * FEE_PER_CONTRACT

        if code == "STO":
            net_contracts -= qty
        elif code == "BTO":
            net_contracts += qty
        elif code == "BTC":
            net_contracts += qty
        elif code == "STC":
            net_contracts -= qty
        elif code == "OASGN":
            net_contracts += qty

    net_pnl = total_credits - total_debits
    net_pnl_after_fees = net_pnl - total_fees
    status = "OPEN" if net_contracts != 0 else "CLOSED"

    first_txn = chain_txns[0]
    last_txn = chain_txns[-1]

    final_position_desc = last_txn.get("Description", "")
    contract_details = _extract_contract_details(final_position_desc)

    breakeven_price = None
    breakeven_direction = None
    open_contracts = abs(net_contracts)
    if status == "OPEN" and open_contracts > 0:
        breakeven_price = (total_credits - total_debits - total_fees) / (open_contracts * 100)
        breakeven_direction = "or less" if net_contracts < 0 else "or more"

    roll_count = 0
    for idx in range(len(chain_txns) - 1):
        current_code = (chain_txns[idx].get("Trans Code") or "").strip().upper()
        next_code = (chain_txns[idx + 1].get("Trans Code") or "").strip().upper()
        if current_code in {"BTC", "STC"} and next_code in {"STO", "BTO"}:
            roll_count += 1

    chain_data: Dict[str, Any] = {
        "transactions": chain_txns,
        "symbol": ticker,
        "ticker": ticker,
        "start_date": first_txn.get("Activity Date", ""),
        "end_date": last_txn.get("Activity Date", ""),
        "status": status,
        "roll_count": roll_count,
        "total_credits": total_credits,
        "total_debits": total_debits,
        "total_fees": total_fees,
        "net_pnl": net_pnl,
        "net_pnl_after_fees": net_pnl_after_fees,
        "initial_position": first_txn.get("Description", ""),
        "final_position": final_position_desc,
        "net_contracts": net_contracts,
        "breakeven_price": breakeven_price,
        "breakeven_direction": breakeven_direction,
    }

    if contract_details:
        chain_data.update(
            {
                "strike": contract_details["strike"],
                "option_type": contract_details["option_type"],
                "option_label": contract_details["option_label"],
                "expiration": contract_details["expiration"],
                "display_name": contract_details["display_name"],
            }
        )
    else:
        chain_data.update(
            {
                "strike": Decimal("0"),
                "option_type": "C" if "CALL" in final_position_desc.upper() else "P",
                "option_label": "Call" if "CALL" in final_position_desc.upper() else "Put",
                "expiration": "",
                "display_name": final_position_desc or ticker,
            }
        )

    if status == "CLOSED":
        chain_data["breakeven_price"] = None
        chain_data["breakeven_direction"] = None

    return chain_data


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
