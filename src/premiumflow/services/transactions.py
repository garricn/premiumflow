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


def filter_open_positions(transactions: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filter transactions to show only positions that are still open (net quantity > 0)."""
    transactions = list(transactions)
    closing_codes = {"STC", "BTC"}
    opening_codes = {"STO", "BTO"}

    def _txn_key(txn: Dict[str, Any]) -> Tuple[str, str]:
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

    # Group transactions by position (instrument + description)
    position_quantities = {}

    for txn in transactions:
        trans_code = (txn.get("Trans Code") or "").strip().upper()
        if trans_code not in opening_codes and trans_code not in closing_codes:
            continue

        key = _txn_key(txn)
        quantity = _parse_quantity(txn.get("Quantity", "0"))

        if key not in position_quantities:
            position_quantities[key] = 0

        if trans_code in opening_codes:
            # Opening transactions: BTO adds positive quantity (long), STO adds negative quantity (short)
            if trans_code == "BTO":
                position_quantities[key] += quantity  # Long position: positive quantity
            elif trans_code == "STO":
                position_quantities[key] -= quantity  # Short position: negative quantity
        elif trans_code in closing_codes:
            # Closing transactions: STC subtracts quantity (closes long), BTC adds quantity (closes short)
            if trans_code == "STC":
                position_quantities[key] -= quantity  # Close long: subtract quantity
            elif trans_code == "BTC":
                position_quantities[key] += quantity  # Close short: add quantity

    # Return aggregated opening transactions for positions that are still open (net quantity != 0)
    open_positions = []
    processed_positions = set()

    for txn in transactions:
        trans_code = (txn.get("Trans Code") or "").strip().upper()
        if trans_code in opening_codes:
            key = _txn_key(txn)
            net_quantity = position_quantities.get(key, 0)

            if net_quantity != 0 and key not in processed_positions:
                # Create aggregated entry for this position
                aggregated_txn = dict(txn)  # Copy the transaction
                aggregated_txn["Quantity"] = str(net_quantity)  # Set net quantity

                # Recalculate amount based on net quantity and average price
                # For simplicity, we'll use the last transaction's price
                # In a more sophisticated implementation, you might want to calculate weighted average
                price_str = txn.get("Price", "0").replace("$", "").replace(",", "")
                try:
                    price = float(price_str)
                    if price > 0:
                        total_amount = net_quantity * price
                        aggregated_txn["Amount"] = (
                            f"(${total_amount:.2f})"
                            if trans_code == "BTO"
                            else f"${total_amount:.2f}"
                        )
                except (ValueError, TypeError):
                    pass  # Keep original amount if price parsing fails

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
    the legacy CSV format consumed by chain detection and display helpers.
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
                "Commission": _format_money_string(txn.fees) if txn.fees else "",
            }
        )
    return rows
