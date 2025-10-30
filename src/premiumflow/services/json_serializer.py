"""JSON serialization utilities for roll chain data."""

from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence

from ..core.parser import NormalizedOptionTransaction
from .display import ensure_display_name


def is_open_chain(chain: Dict[str, Any]) -> bool:
    """Determine whether a detected chain is still open."""
    status = (chain.get("status") or "").upper()
    if status in {"OPEN", "CLOSED"}:
        return status == "OPEN"

    transactions: List[Dict[str, Any]] = chain.get("transactions") or []
    if not transactions:
        return False
    last_code = (transactions[-1].get("Trans Code") or "").strip().upper()
    return last_code in {"STO", "BTO"}


def serialize_decimal(value: Any) -> Any:
    """Serialize Decimal values to JSON-compatible format."""
    if isinstance(value, Decimal):
        normalized = value.normalize()
        return format(normalized, "f")
    return value


def serialize_transaction(txn: Dict[str, Any]) -> Dict[str, Any]:
    """Serialize a transaction dictionary for JSON output."""
    return {
        "activity_date": txn.get("Activity Date", ""),
        "instrument": (txn.get("Instrument") or "").strip(),
        "description": txn.get("Description", ""),
        "trans_code": (txn.get("Trans Code") or "").strip(),
        "quantity": txn.get("Quantity", ""),
        "price": txn.get("Price", ""),
        "amount": txn.get("Amount", ""),
    }


def serialize_chain(chain: Dict[str, Any], chain_id: str) -> Dict[str, Any]:
    """Serialize a chain dictionary for JSON output."""
    serialized_transactions = [
        {
            "activity_date": leg.get("Activity Date", ""),
            "trans_code": (leg.get("Trans Code") or "").strip(),
            "quantity": leg.get("Quantity", ""),
            "price": leg.get("Price", ""),
            "amount": leg.get("Amount", ""),
            "description": leg.get("Description", ""),
        }
        for leg in chain.get("transactions", [])
    ]

    return {
        "chain_id": chain_id,
        "display_name": ensure_display_name(chain),
        "symbol": chain.get("symbol"),
        "status": chain.get("status"),
        "start_date": chain.get("start_date"),
        "end_date": chain.get("end_date"),
        "roll_count": chain.get("roll_count"),
        "strike": serialize_decimal(chain.get("strike")),
        "option_type": chain.get("option_type"),
        "expiration": chain.get("expiration"),
        "total_credits": serialize_decimal(chain.get("total_credits")),
        "total_debits": serialize_decimal(chain.get("total_debits")),
        "total_fees": serialize_decimal(chain.get("total_fees")),
        "net_pnl": serialize_decimal(chain.get("net_pnl")),
        "net_pnl_after_fees": serialize_decimal(chain.get("net_pnl_after_fees")),
        "breakeven_price": serialize_decimal(chain.get("breakeven_price")),
        "breakeven_direction": chain.get("breakeven_direction"),
        "net_contracts": chain.get("net_contracts"),
        "transactions": serialized_transactions,
    }


def build_ingest_payload(
    *,
    csv_file: str,
    account_name: str,
    account_number: Optional[str],
    transactions: Sequence[NormalizedOptionTransaction],
    chains: List[Dict[str, Any]],
    options_only: bool,
    ticker: Optional[str],
    strategy: Optional[str],
    open_only: bool,
) -> Dict[str, Any]:
    """Build the complete payload for JSON output in ingest command."""
    transactions_payload = [serialize_normalized_transaction(txn) for txn in transactions]

    if open_only:
        # Filter chains to only include open ones
        chains = [chain for chain in chains if is_open_chain(chain)]

    filtered_chains: List[Dict[str, Any]] = []
    for idx, chain in enumerate(chains, start=1):
        filtered_chains.append(serialize_chain(chain, f"chain-{idx}"))

    return {
        "source_file": csv_file,
        "filters": {
            "options_only": options_only,
            "ticker": ticker,
            "strategy": strategy,
            "open_only": open_only,
        },
        "account": {
            "name": account_name,
            "number": account_number,
        },
        "transactions": transactions_payload,
        "chains": filtered_chains,
    }


def serialize_normalized_transaction(
    txn: NormalizedOptionTransaction,
) -> Dict[str, Any]:
    """Serialize a normalized transaction object to JSON-friendly structure."""
    return {
        "activity_date": txn.activity_date.isoformat(),
        "process_date": txn.process_date.isoformat() if txn.process_date else None,
        "settle_date": txn.settle_date.isoformat() if txn.settle_date else None,
        "instrument": txn.instrument,
        "symbol": txn.instrument,
        "description": txn.description,
        "trans_code": txn.trans_code,
        "action": txn.action,
        "quantity": txn.quantity,
        "price": serialize_decimal(txn.price),
        "amount": serialize_decimal(txn.amount) if txn.amount is not None else None,
        "strike": serialize_decimal(txn.strike),
        "option_type": txn.option_type,
        "expiration": txn.expiration.isoformat(),
    }
