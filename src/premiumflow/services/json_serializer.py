"""JSON serialization utilities for roll chain data."""

from decimal import Decimal
from typing import Any, Dict, List, Optional

from .cash_flows import CashFlowRow, CashFlowSummary
from .display import ensure_display_name
from .targets import compute_target_close_prices


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


def _serialize_cash_flow_row(
    row: CashFlowRow,
    target_percents: List[Decimal],
) -> Dict[str, Any]:
    txn = row.transaction
    target_prices = compute_target_close_prices(
        txn.trans_code,
        format(txn.price, "f"),
        target_percents,
    )

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
        "credit": serialize_decimal(row.credit),
        "debit": serialize_decimal(row.debit),
        "fee": serialize_decimal(row.fee),
        "running": {
            "credits": serialize_decimal(row.running_credits),
            "debits": serialize_decimal(row.running_debits),
            "fees": serialize_decimal(row.running_fees),
            "net_premium": serialize_decimal(row.running_net_premium),
            "net_pnl": serialize_decimal(row.running_net_pnl),
        },
        "targets": [serialize_decimal(value) for value in target_prices] if target_prices else [],
        "expiration": txn.expiration.isoformat(),
        "strike": serialize_decimal(txn.strike),
        "option_type": txn.option_type,
    }


def build_ingest_payload(
    *,
    csv_file: str,
    summary: CashFlowSummary,
    chains: List[Dict[str, Any]],
    target_percents: List[Decimal],
    options_only: bool,
    ticker: Optional[str],
    strategy: Optional[str],
    open_only: bool,
) -> Dict[str, Any]:
    """Build the complete payload for JSON output in ingest command."""
    transactions_payload = [_serialize_cash_flow_row(row, target_percents) for row in summary.rows]

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
        "target_percents": [str(value) for value in target_percents],
        "account": {
            "name": summary.account_name,
            "number": summary.account_number,
        },
        "regulatory_fee": serialize_decimal(summary.regulatory_fee),
        "cash_flow": {
            "credits": serialize_decimal(summary.totals.credits),
            "debits": serialize_decimal(summary.totals.debits),
            "fees": serialize_decimal(summary.totals.fees),
            "net_premium": serialize_decimal(summary.totals.net_premium),
            "net_pnl": serialize_decimal(summary.totals.net_pnl),
        },
        "transactions": transactions_payload,
        "chains": filtered_chains,
    }
