from __future__ import annotations

from datetime import date
from decimal import Decimal

from premiumflow.core.legs import (
    LegContract,
    aggregate_legs,
    build_leg_fills,
)
from premiumflow.core.parser import NormalizedOptionTransaction


def _make_transaction(
    *,
    activity_date: date,
    description: str,
    trans_code: str,
    quantity: int,
    price: str,
    amount: str,
) -> NormalizedOptionTransaction:
    return NormalizedOptionTransaction(
        activity_date=activity_date,
        process_date=activity_date,
        settle_date=activity_date,
        instrument="TMC",
        description=description,
        trans_code=trans_code,
        quantity=quantity,
        price=Decimal(price),
        amount=Decimal(amount),
        strike=Decimal("7.00"),
        option_type="CALL",
        expiration=date(2025, 10, 17),
        action="SELL" if trans_code in {"STO", "STC"} else "BUY",
        raw={},
    )


def test_leg_contract_id_for_standard_description():
    txn = _make_transaction(
        activity_date=date(2025, 10, 7),
        description="TMC 10/17/2025 Call $7.00",
        trans_code="STO",
        quantity=2,
        price="1.20",
        amount="240",
    )

    contract = LegContract.from_transaction(txn)

    assert contract.leg_id == "TMC-2025-10-17-C-700"
    assert contract.display_name == "TMC 10/17/2025 Call $7.00"


def test_leg_contract_id_strips_option_expiration_prefix():
    txn = _make_transaction(
        activity_date=date(2025, 10, 17),
        description="Option Expiration for TMC 10/17/2025 Call $7.00",
        trans_code="OEXP",
        quantity=2,
        price="0.00",
        amount="0",
    )

    contract = LegContract.from_transaction(txn)

    assert contract.leg_id == "TMC-2025-10-17-C-700"
    assert contract.display_name == "TMC 10/17/2025 Call $7.00"
    assert contract.days_to_expiration(as_of=date(2025, 10, 10)) == 7


def test_leg_fill_exposes_cash_metrics_and_flags():
    txn = _make_transaction(
        activity_date=date(2025, 10, 7),
        description="TMC 10/17/2025 Call $7.00",
        trans_code="STO",
        quantity=2,
        price="1.20",
        amount="240",
    )
    fill = build_leg_fills([txn], account_name="Robinhood IRA", account_number="RH-12345")[0]

    assert fill.is_opening is True
    assert fill.is_closing is False
    assert fill.signed_quantity == -2
    assert fill.gross_notional == Decimal("240.00")
    assert fill.effective_premium == Decimal("240")
    assert fill.fees == Decimal("0.00")
    assert fill.is_assignment is False
    assert fill.is_expiration is False


def test_leg_fill_fees_handle_debit_transactions():
    txn = _make_transaction(
        activity_date=date(2025, 10, 12),
        description="TMC 10/17/2025 Call $7.00",
        trans_code="BTC",
        quantity=1,
        price="0.50",
        amount="-50.65",
    )
    fill = build_leg_fills([txn], account_name="Robinhood IRA", account_number="RH-12345")[0]

    assert fill.is_closing is True
    assert fill.fees == Decimal("0.65")


def test_aggregate_legs_groups_fills_and_computes_totals():
    fills = build_leg_fills(
        [
            _make_transaction(
                activity_date=date(2025, 10, 7),
                description="TMC 10/17/2025 Call $7.00",
                trans_code="STO",
                quantity=2,
                price="1.20",
                amount="240",
            ),
            _make_transaction(
                activity_date=date(2025, 10, 12),
                description="TMC 10/17/2025 Call $7.00",
                trans_code="BTC",
                quantity=1,
                price="0.50",
                amount="-50",
            ),
            _make_transaction(
                activity_date=date(2025, 10, 17),
                description="Option Expiration for TMC 10/17/2025 Call $7.00",
                trans_code="OEXP",
                quantity=1,
                price="0.00",
                amount="0",
            ),
        ],
        account_name="Robinhood IRA",
        account_number="RH-12345",
    )

    summary = aggregate_legs(fills)
    leg_id = "TMC-2025-10-17-C-700"
    key = ("Robinhood IRA", "RH-12345", leg_id)
    assert key in summary
    leg = summary[key]

    assert leg.opening_quantity == 2
    assert leg.closing_quantity == 2
    assert leg.open_quantity == 0
    assert leg.is_open is False
    assert leg.gross_open_premium == Decimal("240.00")
    assert leg.gross_close_premium == Decimal("-50.00")
    assert leg.net_premium == Decimal("190.00")
    assert leg.realized_premium == Decimal("190.00")
    assert leg.total_fees == Decimal("0.00")
    assert leg.days_to_expiration(as_of=date(2025, 10, 16)) == 1
    assert [fill.transaction.trans_code for fill in leg.fills] == ["STO", "BTC", "OEXP"]


def test_long_position_expiration_zeroes_out_net_contracts():
    fills = build_leg_fills(
        [
            _make_transaction(
                activity_date=date(2025, 9, 1),
                description="TMC 10/17/2025 Call $7.00",
                trans_code="BTO",
                quantity=1,
                price="1.50",
                amount="-150",
            ),
            _make_transaction(
                activity_date=date(2025, 10, 17),
                description="Option Expiration for TMC 10/17/2025 Call $7.00",
                trans_code="OEXP",
                quantity=1,
                price="0.00",
                amount="0",
            ),
        ],
        account_name="Robinhood IRA",
        account_number="RH-12345",
    )

    assert [fill.signed_quantity for fill in fills] == [1, -1]

    summary = aggregate_legs(fills)
    leg_id = "TMC-2025-10-17-C-700"
    key = ("Robinhood IRA", "RH-12345", leg_id)
    assert key in summary
    leg = summary[key]

    assert leg.net_contracts == 0
    assert leg.open_quantity == 0


def test_realized_premium_is_none_for_partially_closed_leg():
    fills = build_leg_fills(
        [
            _make_transaction(
                activity_date=date(2025, 10, 1),
                description="TMC 10/17/2025 Call $7.00",
                trans_code="STO",
                quantity=2,
                price="1.00",
                amount="200",
            ),
            _make_transaction(
                activity_date=date(2025, 10, 5),
                description="TMC 10/17/2025 Call $7.00",
                trans_code="BTC",
                quantity=1,
                price="0.30",
                amount="-30",
            ),
        ],
        account_name="Robinhood IRA",
        account_number="RH-12345",
    )

    summary = aggregate_legs(fills)
    leg_id = "TMC-2025-10-17-C-700"
    key = ("Robinhood IRA", "RH-12345", leg_id)
    leg = summary[key]

    assert leg.is_open is True
    assert leg.realized_premium is None


def test_build_leg_fills_sorts_transactions_before_signing():
    fills = build_leg_fills(
        [
            _make_transaction(
                activity_date=date(2025, 10, 17),
                description="Option Expiration for TMC 10/17/2025 Call $7.00",
                trans_code="OEXP",
                quantity=1,
                price="0.00",
                amount="0",
            ),
            _make_transaction(
                activity_date=date(2025, 10, 7),
                description="TMC 10/17/2025 Call $7.00",
                trans_code="STO",
                quantity=1,
                price="1.20",
                amount="120",
            ),
        ],
        account_name="Robinhood IRA",
        account_number="RH-12345",
    )

    assert [fill.transaction.trans_code for fill in fills] == ["STO", "OEXP"]
    assert [fill.signed_quantity for fill in fills] == [-1, 1]


def test_build_leg_fills_orders_open_before_close_on_same_timestamp():
    """
    When opening and closing transactions share identical timestamps (activity/process/settle),
    openings should be ordered before closings to avoid interleaving close events ahead of opens.
    """
    same_day = date(2025, 10, 17)

    fills = build_leg_fills(
        [
            # Closing event first in input, but with identical timestamps
            _make_transaction(
                activity_date=same_day,
                description="TMC 10/17/2025 Call $7.00",
                trans_code="BTC",
                quantity=1,
                price="0.50",
                amount="-50",
            ),
            # Opening event second in input, identical timestamps
            _make_transaction(
                activity_date=same_day,
                description="TMC 10/17/2025 Call $7.00",
                trans_code="STO",
                quantity=1,
                price="1.20",
                amount="120",
            ),
        ],
        account_name="Robinhood IRA",
        account_number="RH-12345",
    )

    # Despite input order (BTC then STO), we expect STO to appear first due to action priority.
    assert [fill.transaction.trans_code for fill in fills] == ["STO", "BTC"]
    assert [fill.signed_quantity for fill in fills] == [-1, 1]
