"""Transaction filtering helpers."""

from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ..core.parser import NormalizedOptionTransaction


def filter_transactions_by_ticker(
    transactions: Iterable[Dict[str, Any]],
    ticker_symbol: Optional[str],
) -> List[Dict[str, Any]]:
    if not ticker_symbol:
        return list(transactions)

    ticker_key = ticker_symbol.strip().upper()
    return [
        txn for txn in transactions if (txn.get("Instrument") or "").strip().upper() == ticker_key
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
        return [txn for txn in transactions if "call" in (txn.get("Description") or "").lower()]
    if puts_only:
        return [txn for txn in transactions if "put" in (txn.get("Description") or "").lower()]
    return transactions


def _txn_key(txn: Dict[str, Any]) -> Tuple[str, str]:
    """Extract transaction key (instrument + description) for grouping."""
    return (
        (txn.get("Instrument") or "").strip().upper(),
        (txn.get("Description") or "").strip(),
    )


def _parse_quantity(quantity_str: str) -> int:
    """Parse quantity string, handling negative values in parentheses."""
    if not quantity_str:
        return 0
    cleaned = quantity_str.replace(",", "").strip()
    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = f"-{cleaned[1:-1]}"
    try:
        return int(float(cleaned))
    except (ValueError, TypeError):
        return 0


def _aggregate_position_quantities(
    transactions: List[Dict[str, Any]],
) -> Dict[Tuple[str, str], int]:
    """Build a dict of net quantities for each position based on opening/closing codes."""
    closing_codes = {"STC", "BTC"}
    opening_codes = {"STO", "BTO"}
    position_quantities: Dict[Tuple[str, str], int] = {}

    for txn in transactions:
        trans_code = (txn.get("Trans Code") or "").strip().upper()
        if trans_code not in opening_codes and trans_code not in closing_codes:
            continue

        key = _txn_key(txn)
        quantity = _parse_quantity(txn.get("Quantity", "0"))

        if key not in position_quantities:
            position_quantities[key] = 0

        if trans_code in opening_codes:
            # Opening: BTO adds positive (long), STO adds negative (short)
            if trans_code == "BTO":
                position_quantities[key] += quantity
            elif trans_code == "STO":
                position_quantities[key] -= quantity
        elif trans_code in closing_codes:
            # Closing: STC subtracts (closes long), BTC adds (closes short)
            if trans_code == "STC":
                position_quantities[key] -= quantity
            elif trans_code == "BTC":
                position_quantities[key] += quantity

    return position_quantities


def _build_aggregated_transaction(
    txn: Dict[str, Any],
    net_quantity: int,
    trans_code: str,
) -> Dict[str, Any]:
    """Create aggregated transaction with net quantity and recalculated amount."""
    aggregated_txn = dict(txn)
    aggregated_txn["Quantity"] = str(net_quantity)

    price_str = txn.get("Price", "0").replace("$", "").replace(",", "")
    try:
        price = float(price_str)
        if price > 0:
            total_amount = net_quantity * price
            aggregated_txn["Amount"] = (
                f"(${total_amount:.2f})" if trans_code == "BTO" else f"${total_amount:.2f}"
            )
    except (ValueError, TypeError):
        pass

    return aggregated_txn


def filter_open_positions(
    transactions: Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Filter transactions to show only positions that are still open (net quantity != 0)."""
    transactions = list(transactions)
    opening_codes = {"STO", "BTO"}

    # Calculate net quantities for all positions
    position_quantities = _aggregate_position_quantities(transactions)

    # Collect aggregated transactions for open positions
    open_positions = []
    processed_positions = set()

    for txn in transactions:
        trans_code = (txn.get("Trans Code") or "").strip().upper()
        if trans_code not in opening_codes:
            continue

        key = _txn_key(txn)
        net_quantity = position_quantities.get(key, 0)

        if net_quantity != 0 and key not in processed_positions:
            aggregated_txn = _build_aggregated_transaction(txn, net_quantity, trans_code)
            open_positions.append(aggregated_txn)
            processed_positions.add(key)

    return open_positions


def _format_money_string(value: Decimal) -> str:
    quantized = value.quantize(Decimal("0.01"))
    if quantized < 0:
        return f"(${abs(quantized):,.2f})"
    return f"${quantized:,.2f}"


def normalized_to_csv_dicts(
    transactions: Iterable[NormalizedOptionTransaction],
) -> List[Dict[str, str]]:
    """Convert normalized transactions into CSV-style dicts.

    Values are serialized as strings (for example, Price ``$3.00`` or Amount ``($200.00)``) to match
    the legacy CSV format consumed by chain detection and display helpers. Numeric strings preserve
    two decimal places so downstream formatting stays consistent.
    """

    rows: List[Dict[str, str]] = []
    for txn in transactions:
        if txn.amount is not None:
            signed_amount = txn.amount
        else:
            notional = txn.price * Decimal(txn.quantity) * Decimal("100")
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
            }
        )
    return rows
