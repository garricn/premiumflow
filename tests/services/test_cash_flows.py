from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import List, Optional

from premiumflow.core.parser import NormalizedOptionTransaction, ParsedImportResult
from premiumflow.services.cash_flows import summarize_cash_flows


def _make_transaction(
    *,
    trans_code: str,
    action: str,
    quantity: int,
    price: str,
    amount: Optional[str] = None,
) -> NormalizedOptionTransaction:
    return NormalizedOptionTransaction(
        activity_date=date(2025, 10, 7),
        process_date=date(2025, 10, 7),
        settle_date=date(2025, 10, 8),
        instrument="TSLA",
        description="TSLA 10/25/2025 Call $200.00",
        trans_code=trans_code,
        quantity=quantity,
        price=Decimal(price),
        strike=Decimal("200"),
        option_type="CALL",
        expiration=date(2025, 10, 25),
        action=action,
        amount=Decimal(amount) if amount is not None else None,
        raw={},
    )


def _make_parsed_result(transactions: List[NormalizedOptionTransaction]) -> ParsedImportResult:
    return ParsedImportResult(
        account_name="Robinhood IRA",
        account_number="RH-12345",
        transactions=transactions,
    )


def test_summarize_cash_flows_basic_flow():
    transactions = [
        _make_transaction(
            trans_code="STO",
            action="SELL",
            quantity=2,
            price="1.20",
            amount="240",
        ),
        _make_transaction(
            trans_code="BTC",
            action="BUY",
            quantity=1,
            price="0.80",
            amount="-80",
        ),
    ]
    parsed = _make_parsed_result(transactions)

    summary = summarize_cash_flows(parsed)

    assert summary.account_name == "Robinhood IRA"
    assert summary.account_number == "RH-12345"
    assert summary.totals.credits == Decimal("240")
    assert summary.totals.debits == Decimal("80")
    assert summary.totals.net_premium == Decimal("160")
    assert summary.totals.net_pnl == Decimal("160")

    assert len(summary.rows) == 2
    first, second = summary.rows

    assert first.credit == Decimal("240")
    assert first.debit == Decimal("0")
    assert first.running_credits == Decimal("240")
    assert first.running_debits == Decimal("0")
    assert first.running_net_premium == Decimal("240")
    assert first.running_net_pnl == Decimal("240")

    assert second.credit == Decimal("0")
    assert second.debit == Decimal("80")
    assert second.running_credits == Decimal("240")
    assert second.running_debits == Decimal("80")
    assert second.running_net_premium == Decimal("160")
    assert second.running_net_pnl == Decimal("160")


def test_summarize_cash_flows_handles_assignment_debit():
    transactions = [
        _make_transaction(
            trans_code="STO",
            action="SELL",
            quantity=1,
            price="1.00",
            amount="100",
        ),
        _make_transaction(
            trans_code="OASGN",
            action="BUY",
            quantity=1,
            price="0.50",
            amount="-50",
        ),
    ]
    parsed = _make_parsed_result(transactions)

    summary = summarize_cash_flows(parsed)

    assert summary.totals.credits == Decimal("100")
    assert summary.totals.debits == Decimal("50")
    assert summary.totals.net_premium == Decimal("50")
    assert summary.totals.net_pnl == Decimal("50")

    assert summary.rows[1].debit == Decimal("50")
    assert summary.rows[1].credit == Decimal("0")


def test_summarize_cash_flows_empty_input():
    parsed = _make_parsed_result([])

    summary = summarize_cash_flows(parsed)

    assert summary.totals == summary.totals.__class__(
        credits=Decimal("0"),
        debits=Decimal("0"),
        net_premium=Decimal("0"),
        net_pnl=Decimal("0"),
    )
    assert summary.rows == []


def test_summarize_cash_flows_handles_assignment_credit():
    transactions = [
        _make_transaction(
            trans_code="STO",
            action="SELL",
            quantity=1,
            price="1.00",
            amount="100",
        ),
        _make_transaction(
            trans_code="OASGN",
            action="BUY",
            quantity=1,
            price="0.50",
            amount="150",
        ),
    ]
    summary = summarize_cash_flows(_make_parsed_result(transactions))

    assert summary.totals.credits == Decimal("250")
    assert summary.totals.debits == Decimal("0")
    assert summary.totals.net_premium == Decimal("250")
    assert summary.totals.net_pnl == Decimal("250")
