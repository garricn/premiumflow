"""
Cash-flow aggregation helpers for import processing.

These utilities consume normalized option transactions (produced by
``premiumflow.core.parser.load_option_transactions``) and compute per-row cash
flow metrics alongside overall totals for credits, debits, fees, net premium,
and net P&L.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import List, Optional

from ..core.parser import NormalizedOptionTransaction, ParsedImportResult


@dataclass(frozen=True)
class CashFlowTotals:
    """Aggregate cash-flow totals."""

    credits: Decimal
    debits: Decimal
    fees: Decimal
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
    fee: Decimal
    running_credits: Decimal
    running_debits: Decimal
    running_fees: Decimal
    running_net_premium: Decimal
    running_net_pnl: Decimal


@dataclass(frozen=True)
class CashFlowSummary:
    """Complete cash-flow report for an import."""

    account_name: str
    account_number: Optional[str]
    regulatory_fee: Decimal
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

    running_credits = Decimal("0")
    running_debits = Decimal("0")
    running_fees = Decimal("0")

    rows: List[CashFlowRow] = []

    for txn in parsed.transactions:
        notional = txn.price * txn.quantity
        if txn.action == "SELL":
            credit = notional
            debit = Decimal("0")
        else:
            credit = Decimal("0")
            debit = notional

        running_credits += credit
        running_debits += debit
        running_fees += txn.fees

        running_net_premium = running_credits - running_debits
        running_net_pnl = running_net_premium - running_fees

        rows.append(
            CashFlowRow(
                transaction=txn,
                credit=credit,
                debit=debit,
                fee=txn.fees,
                running_credits=running_credits,
                running_debits=running_debits,
                running_fees=running_fees,
                running_net_premium=running_net_premium,
                running_net_pnl=running_net_pnl,
            )
        )

    totals = CashFlowTotals(
        credits=running_credits,
        debits=running_debits,
        fees=running_fees,
        net_premium=running_credits - running_debits,
        net_pnl=(running_credits - running_debits) - running_fees,
    )

    return CashFlowSummary(
        account_name=parsed.account_name,
        account_number=parsed.account_number,
        regulatory_fee=parsed.regulatory_fee,
        rows=rows,
        totals=totals,
    )
