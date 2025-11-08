"""JSON serialization utilities for roll chain data."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence

from ..core.parser import NormalizedOptionTransaction
from .display import ensure_display_name
from .leg_matching import LotFillPortion, MatchedLeg, MatchedLegLot

if TYPE_CHECKING:
    from .cash_flow import CashFlowPnlReport, PeriodMetrics


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
        "net_pnl": serialize_decimal(chain.get("net_pnl")),
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


def _decimal_to_string(value: Decimal) -> str:
    """Convert Decimal to string with 2 decimal places."""
    return format(value.quantize(Decimal("0.01")), "f")


def serialize_leg_portion(portion: LotFillPortion) -> Dict[str, Any]:
    """Serialize a LotFillPortion to JSON-friendly structure."""
    fill = portion.fill
    return {
        "quantity": portion.quantity,
        "premium": _decimal_to_string(portion.premium),
        "fees": _decimal_to_string(portion.fees),
        "activity_date": portion.activity_date.isoformat(),
        "trans_code": fill.trans_code,
        "description": fill.transaction.description,
    }


def serialize_leg_lot(lot: MatchedLegLot) -> Dict[str, Any]:
    """Serialize a MatchedLegLot to JSON-friendly structure."""
    return {
        "contract": {
            "leg_id": lot.contract.leg_id,
            "symbol": lot.contract.symbol,
            "expiration": lot.contract.expiration.isoformat(),
            "option_type": lot.contract.option_type,
            "strike": _decimal_to_string(lot.contract.strike),
            "display_name": lot.contract.display_name,
        },
        "account_name": lot.account_name,
        "account_number": lot.account_number,
        "direction": lot.direction,
        "quantity": lot.quantity,
        "opened_at": lot.opened_at.isoformat(),
        "closed_at": lot.closed_at.isoformat() if lot.closed_at else None,
        "status": lot.status,
        "open_portions": [serialize_leg_portion(p) for p in lot.open_portions],
        "close_portions": [serialize_leg_portion(p) for p in lot.close_portions],
        "open_premium": _decimal_to_string(lot.open_premium),
        "close_premium": _decimal_to_string(lot.close_premium),
        "total_fees": _decimal_to_string(lot.total_fees),
        "realized_pnl": (
            _decimal_to_string(lot.realized_pnl) if lot.realized_pnl is not None else None
        ),
        "open_fees": _decimal_to_string(lot.open_fees),
        "close_fees": _decimal_to_string(lot.close_fees),
        "open_credit_gross": _decimal_to_string(lot.open_credit_gross),
        "open_credit_net": _decimal_to_string(lot.open_credit_net),
        "close_cost": _decimal_to_string(lot.close_cost),
        "close_cost_total": _decimal_to_string(lot.close_cost_total),
        "close_quantity": lot.close_quantity,
        "credit_remaining": _decimal_to_string(lot.credit_remaining),
        "quantity_remaining": lot.quantity_remaining,
        "net_pnl": _decimal_to_string(lot.net_pnl) if lot.net_pnl is not None else None,
    }


def serialize_leg(leg: MatchedLeg) -> Dict[str, Any]:
    """Serialize a MatchedLeg to JSON-friendly structure."""
    return {
        "contract": {
            "leg_id": leg.contract.leg_id,
            "symbol": leg.contract.symbol,
            "expiration": leg.contract.expiration.isoformat(),
            "option_type": leg.contract.option_type,
            "strike": _decimal_to_string(leg.contract.strike),
            "display_name": leg.contract.display_name,
        },
        "account_name": leg.account_name,
        "account_number": leg.account_number,
        "lots": [serialize_leg_lot(lot) for lot in leg.lots],
        "net_contracts": leg.net_contracts,
        "open_quantity": leg.open_quantity,
        "realized_pnl": _decimal_to_string(leg.realized_pnl),
        "open_premium": _decimal_to_string(leg.open_premium),
        "total_fees": _decimal_to_string(leg.total_fees),
        "days_to_expiration": leg.days_to_expiration,
        "is_open": leg.is_open,
        "status": "open" if leg.is_open else "closed",
        "opened_at": leg.opened_at.isoformat() if leg.opened_at else None,
        "closed_at": leg.closed_at.isoformat() if leg.closed_at else None,
        "opened_quantity": leg.opened_quantity,
        "closed_quantity": leg.closed_quantity,
        "open_credit_gross": _decimal_to_string(leg.open_credit_gross),
        "close_cost": _decimal_to_string(leg.close_cost),
        "open_fees": _decimal_to_string(leg.open_fees),
        "close_fees": _decimal_to_string(leg.close_fees),
        "resolution": leg.resolution(),
    }


def serialize_period_metrics(metrics: PeriodMetrics) -> Dict[str, Any]:
    """Serialize a PeriodMetrics object to JSON-friendly structure."""
    return {
        "period_key": metrics.period_key,
        "period_label": metrics.period_label,
        "credits": _decimal_to_string(metrics.credits),
        "debits": _decimal_to_string(metrics.debits),
        "net_cash_flow": _decimal_to_string(metrics.net_cash_flow),
        "realized_profits_gross": _decimal_to_string(metrics.realized_profits_gross),
        "realized_losses_gross": _decimal_to_string(metrics.realized_losses_gross),
        "realized_pnl_gross": _decimal_to_string(metrics.realized_pnl_gross),
        "realized_profits_net": _decimal_to_string(metrics.realized_profits_net),
        "realized_losses_net": _decimal_to_string(metrics.realized_losses_net),
        "realized_pnl_net": _decimal_to_string(metrics.realized_pnl_net),
        "assignment_realized_gross": _decimal_to_string(metrics.assignment_realized_gross),
        "assignment_realized_net": _decimal_to_string(metrics.assignment_realized_net),
        "unrealized_exposure": _decimal_to_string(metrics.unrealized_exposure),
        "opening_fees": _decimal_to_string(metrics.opening_fees),
        "closing_fees": _decimal_to_string(metrics.closing_fees),
        "total_fees": _decimal_to_string(metrics.total_fees),
        "realized_breakdowns": {
            view: {
                "profits_gross": _decimal_to_string(totals.profits_gross),
                "losses_gross": _decimal_to_string(totals.losses_gross),
                "net_gross": _decimal_to_string(totals.net_gross),
                "profits_net": _decimal_to_string(totals.profits_net),
                "losses_net": _decimal_to_string(totals.losses_net),
                "net_net": _decimal_to_string(totals.net_net),
            }
            for view, totals in metrics.realized_breakdowns.items()
        },
    }


def serialize_cash_flow_pnl_report(report: CashFlowPnlReport) -> Dict[str, Any]:
    """Serialize a CashFlowPnlReport object to JSON-friendly structure."""
    return {
        "account_name": report.account_name,
        "account_number": report.account_number,
        "period_type": report.period_type,
        "periods": [serialize_period_metrics(p) for p in report.periods],
        "totals": serialize_period_metrics(report.totals),
    }
