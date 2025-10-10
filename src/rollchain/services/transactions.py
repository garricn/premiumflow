"""Transaction filtering helpers."""

from typing import Any, Dict, Iterable, List, Optional, Tuple


def filter_transactions_by_ticker(
    transactions: Iterable[Dict[str, Any]],
    ticker_symbol: Optional[str],
) -> List[Dict[str, Any]]:
    if not ticker_symbol:
        return list(transactions)

    ticker_key = ticker_symbol.strip().upper()
    return [
        txn
        for txn in transactions
        if (txn.get('Instrument') or '').strip().upper() == ticker_key
    ]


def filter_transactions_by_option_type(
    transactions: Iterable[Dict[str, Any]],
    *,
    calls_only: bool = False,
    puts_only: bool = False,
) -> List[Dict[str, Any]]:
    if calls_only and puts_only:
        raise ValueError("Cannot combine --calls-only and --puts-only")

    transactions = list(transactions)
    if calls_only:
        return [
            txn for txn in transactions
            if 'call' in (txn.get('Description') or '').lower()
        ]
    if puts_only:
        return [
            txn for txn in transactions
            if 'put' in (txn.get('Description') or '').lower()
        ]
    return transactions


def filter_open_positions(transactions: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    transactions = list(transactions)
    closing_codes = {"STC", "BTC"}
    opening_codes = {"STO", "BTO"}

    def _txn_key(txn: Dict[str, Any]) -> Tuple[str, str]:
        return (
            (txn.get('Instrument') or '').strip().upper(),
            (txn.get('Description') or '').strip(),
        )

    closing_positions = {
        _txn_key(txn)
        for txn in transactions
        if (txn.get('Trans Code') or '').strip().upper() in closing_codes
    }

    return [
        txn for txn in transactions
        if (txn.get('Trans Code') or '').strip().upper() in opening_codes
        and _txn_key(txn) not in closing_positions
    ]
