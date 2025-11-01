"""
Cash-flow aggregation helpers for import processing.

These utilities consume normalized option transactions (produced by
``premiumflow.core.parser.load_option_transactions``) and compute per-row cash
flow metrics alongside overall totals for credits, debits, net premium, and net
P&L.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import List, Optional

from ..core.parser import NormalizedOptionTransaction, ParsedImportResult

CONTRACT_MULTIPLIER = Decimal("100")
ZERO = Decimal("0")


@dataclass(frozen=True)
class CashFlowTotals:
    """Aggregate cash-flow totals."""

    credits: Decimal
    debits: Decimal
    net_premium: Decimal
    net_pnl: Decimal


@dataclass(frozen=True)
class CashFlowRow:
    """
    Cash-flow metrics for a single normalized transaction.

    ``credit``/``debit`` reflect the absolute cash exchanged for the row
    (``price * quantity``) depending on whether the action is SELL/BUY. Running
    totals capture the cumulative view after applying the transaction.
    """

    transaction: NormalizedOptionTransaction
    credit: Decimal
    debit: Decimal
    running_credits: Decimal
    running_debits: Decimal
    running_net_premium: Decimal
    running_net_pnl: Decimal


@dataclass(frozen=True)
class CashFlowSummary:
    """Complete cash-flow report for an import."""

    account_name: str
    account_number: Optional[str]
    rows: List[CashFlowRow]
    totals: CashFlowTotals


def summarize_cash_flows(parsed: ParsedImportResult) -> CashFlowSummary:
    """
    Aggregate cash-flow metrics for normalized option transactions.

    Parameters
    ----------
    parsed:
        Result produced by ``load_option_transactions`` containing account
        metadata and normalized rows.

    Returns
    -------
    CashFlowSummary
        Per-transaction cash-flow metrics with running totals and aggregate
        totals suitable for CLI and JSON output.
    """

    running_credits = ZERO
    running_debits = ZERO

    rows: List[CashFlowRow] = []

    for txn in parsed.transactions:
        cash_value = _calculate_cash_value(txn)
        if cash_value >= ZERO:
            credit = cash_value
            debit = ZERO
        else:
            credit = ZERO
            debit = -cash_value

        running_credits += credit
        running_debits += debit

        running_net_premium = running_credits - running_debits
        running_net_pnl = running_net_premium

        rows.append(
            CashFlowRow(
                transaction=txn,
                credit=credit,
                debit=debit,
                running_credits=running_credits,
                running_debits=running_debits,
                running_net_premium=running_net_premium,
                running_net_pnl=running_net_pnl,
            )
        )

    net_premium = running_credits - running_debits
    totals = CashFlowTotals(
        credits=running_credits,
        debits=running_debits,
        net_premium=net_premium,
        net_pnl=net_premium,
    )

    return CashFlowSummary(
        account_name=parsed.account_name,
        account_number=parsed.account_number,
        rows=rows,
        totals=totals,
    )


def _calculate_cash_value(txn: NormalizedOptionTransaction) -> Decimal:
    """
    Determine the gross cash value of a transaction.

    Prefer broker-supplied ``Amount`` values when available since they already
    include the contract multiplier and reflect the actual cash movement. When
    ``Amount`` is missing (e.g., synthetic fixtures), fall back to
    ``price * quantity * 100`` to approximate the same behaviour.
    """

    if txn.amount is not None:
        return txn.amount

    base_value = txn.price * Decimal(txn.quantity) * CONTRACT_MULTIPLIER
    return base_value if txn.action == "SELL" else -base_value
