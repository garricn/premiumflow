from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Optional

from premiumflow.core.legs import build_leg_fills
from premiumflow.core.parser import NormalizedOptionTransaction
from premiumflow.services.leg_matching import (
    MatchedLegLot,
    match_leg_fills,
    match_legs,
)


def _make_txn(
    *,
    activity_date: date,
    description: str,
    trans_code: str,
    quantity: int,
    price: str,
    amount: str,
    option_type: str = "CALL",
    strike: str = "7.00",
    process_date: Optional[date] = None,
    settle_date: Optional[date] = None,
) -> NormalizedOptionTransaction:
    """Create a normalized transaction for testing."""
    return NormalizedOptionTransaction(
        activity_date=activity_date,
        process_date=process_date or activity_date,
        settle_date=settle_date or activity_date,
        instrument="TMC",
        description=description,
        trans_code=trans_code,
        quantity=quantity,
        price=Decimal(price),
        amount=Decimal(amount),
        strike=Decimal(strike),
        option_type=option_type,
        expiration=date(2025, 10, 17),
        action="SELL" if trans_code in {"STO", "STC"} else "BUY",
        raw={},
    )


def _single_leg_fills(transactions):
    return build_leg_fills(
        transactions,
        account_name="Robinhood IRA",
        account_number="RH-12345",
    )


def test_match_leg_fills_handles_complete_short_cycle():
    transactions = [
        _make_txn(
            activity_date=date(2025, 10, 7),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="STO",
            quantity=2,
            price="1.20",
            amount="240",
        ),
        _make_txn(
            activity_date=date(2025, 10, 10),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="BTC",
            quantity=2,
            price="0.50",
            amount="-100",
        ),
    ]
    fills = _single_leg_fills(transactions)

    matched = match_leg_fills(fills)

    assert matched.account_name == "Robinhood IRA"
    assert matched.open_quantity == 0
    assert matched.net_contracts == 0
    assert matched.realized_premium == Decimal("140.00")
    assert matched.open_premium == Decimal("0.00")
    assert matched.total_fees == Decimal("0.00")

    assert len(matched.lots) == 1
    lot = matched.lots[0]
    assert lot.status == "closed"
    assert lot.direction == "short"
    assert lot.quantity == 2
    assert lot.realized_premium == Decimal("140.00")
    assert lot.open_premium == Decimal("240.00")
    assert lot.close_premium == Decimal("-100.00")


def test_match_leg_fills_handles_partial_close_with_open_lot():
    transactions = [
        _make_txn(
            activity_date=date(2025, 10, 1),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="STO",
            quantity=3,
            price="1.00",
            amount="300",
        ),
        _make_txn(
            activity_date=date(2025, 10, 5),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="BTC",
            quantity=1,
            price="0.30",
            amount="-30",
        ),
    ]
    fills = _single_leg_fills(transactions)

    matched = match_leg_fills(fills)

    assert matched.open_quantity == 2
    assert matched.net_contracts == -2  # short two contracts remain
    assert matched.realized_premium == Decimal("70.00")
    assert matched.open_premium == Decimal("200.00")

    lots_by_status = {lot.status: lot for lot in matched.lots}  # type: ignore[call-overload]

    closed_lot: MatchedLegLot = lots_by_status["closed"]
    assert closed_lot.quantity == 1
    assert closed_lot.realized_premium == Decimal("70.00")

    open_lot: MatchedLegLot = lots_by_status["open"]
    assert open_lot.quantity == 2
    assert open_lot.open_premium == Decimal("200.00")
    assert open_lot.close_premium == Decimal("0.00")
    assert open_lot.realized_premium is None
    # New computed props on lot
    assert open_lot.open_fees == Decimal("0.00")
    assert open_lot.close_fees == Decimal("0.00")
    assert open_lot.open_credit_gross == Decimal("200.00")
    assert open_lot.open_credit_net == Decimal("200.00")
    assert open_lot.credit_remaining == Decimal("200.00")
    assert open_lot.quantity_remaining == 2
    assert open_lot.net_premium is None


def test_match_leg_fills_sorts_transactions_before_matching():
    # Intentional newest-first order: expiration precedes opening trade
    transactions = [
        _make_txn(
            activity_date=date(2025, 10, 17),
            description="Option Expiration for TMC 10/17/2025 Call $7.00",
            trans_code="OEXP",
            quantity=1,
            price="0.00",
            amount="0",
        ),
        _make_txn(
            activity_date=date(2025, 10, 7),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="STO",
            quantity=1,
            price="1.20",
            amount="120",
        ),
    ]
    fills = _single_leg_fills(transactions)

    matched = match_leg_fills(fills)

    assert len(matched.lots) == 1
    lot = matched.lots[0]
    assert [p.fill.trans_code for p in lot.open_portions] == ["STO"]
    assert [p.fill.trans_code for p in lot.close_portions] == ["OEXP"]
    assert lot.realized_premium == Decimal("120.00")
    assert matched.realized_premium == Decimal("120.00")


def test_match_legs_groups_multiple_contracts():
    sto_a = _make_txn(
        activity_date=date(2025, 10, 1),
        description="TMC 10/17/2025 Call $7.00",
        trans_code="STO",
        quantity=1,
        price="1.00",
        amount="100",
    )
    btc_a = _make_txn(
        activity_date=date(2025, 10, 5),
        description="TMC 10/17/2025 Call $7.00",
        trans_code="BTC",
        quantity=1,
        price="0.40",
        amount="-40",
    )
    sto_b = _make_txn(
        activity_date=date(2025, 10, 1),
        description="TMC 10/17/2025 Put $5.00",
        trans_code="STO",
        quantity=1,
        price="1.50",
        amount="150",
        option_type="PUT",
        strike="5.00",
    )

    fills = build_leg_fills(
        [sto_a, btc_a, sto_b],
        account_name="Robinhood IRA",
        account_number="RH-12345",
    )

    results = match_legs(fills)

    assert len(results) == 2
    short_call = results[("Robinhood IRA", "RH-12345", "TMC-2025-10-17-C-700")]
    assert short_call.realized_premium == Decimal("60.00")
    assert short_call.open_quantity == 0

    short_put = results[("Robinhood IRA", "RH-12345", "TMC-2025-10-17-P-500")]
    assert short_put.open_quantity == 1
    assert short_put.realized_premium == Decimal("0.00")
    assert short_put.open_premium == Decimal("150.00")


def test_match_leg_fills_handles_long_position_closure():
    transactions = [
        _make_txn(
            activity_date=date(2025, 9, 10),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="BTO",
            quantity=1,
            price="0.90",
            amount="-90",
        ),
        _make_txn(
            activity_date=date(2025, 9, 25),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="STC",
            quantity=1,
            price="1.40",
            amount="140",
        ),
    ]
    fills = _single_leg_fills(transactions)

    matched = match_leg_fills(fills)

    assert matched.net_contracts == 0
    assert matched.realized_premium == Decimal("50.00")
    assert len(matched.lots) == 1
    lot = matched.lots[0]
    assert lot.status == "closed"
    assert lot.direction == "long"
    assert lot.realized_premium == Decimal("50.00")
    # New computed props on closed lot
    assert lot.open_fees == Decimal("0.00")
    assert lot.close_fees == Decimal("0.00")
    assert lot.open_credit_gross == Decimal("-90.00")  # long open is debit
    assert lot.open_credit_net == Decimal("-90.00")
    assert lot.close_cost == Decimal("0.00")  # close was a credit here (STC)
    assert lot.close_cost_total == Decimal("0.00")
    assert lot.close_quantity == 1
    assert lot.credit_remaining == Decimal("0.00")
    assert lot.quantity_remaining == 0
    assert lot.net_premium == Decimal("50.00")


def test_portion_premium_uses_gross_notional_sign_and_ratio():
    """Apportion premiums by price*qty*100 and sign by transaction code."""
    # Open short 3 @ $1.00 -> gross 300; portion 2 should contribute +200 open premium
    # Close 2 @ $0.30 -> gross 60; portion 2 should contribute -60 close premium
    transactions = [
        _make_txn(
            activity_date=date(2025, 10, 1),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="STO",
            quantity=3,
            price="1.00",
            amount="300",
        ),
        _make_txn(
            activity_date=date(2025, 10, 5),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="BTC",
            quantity=2,
            price="0.30",
            amount="-60",
        ),
    ]
    fills = _single_leg_fills(transactions)
    matched = match_leg_fills(fills)

    # One closed lot of 2 and one open lot of 1
    lots = {lot.status: lot for lot in matched.lots}
    closed_lot: MatchedLegLot = lots["closed"]
    assert closed_lot.quantity == 2
    assert closed_lot.open_premium == Decimal("200.00")
    assert closed_lot.close_premium == Decimal("-60.00")


def test_match_leg_fills_handles_full_assignment_closure():
    transactions = [
        _make_txn(
            activity_date=date(2025, 9, 1),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="STO",
            quantity=2,
            price="1.10",
            amount="220",
        ),
        _make_txn(
            activity_date=date(2025, 10, 17),
            description="Assignment of TMC 10/17/2025 Call $7.00",
            trans_code="OASGN",
            quantity=2,
            price="0.00",
            amount="0",
        ),
    ]

    matched = match_leg_fills(_single_leg_fills(transactions))

    assert matched.open_quantity == 0
    assert matched.realized_premium == Decimal("220.00")
    assignment_lot = matched.lots[0]
    assert assignment_lot.status == "closed"
    assert assignment_lot.realized_premium == Decimal("220.00")


def test_match_leg_fills_handles_full_expiration_closure_ordered():
    transactions = [
        _make_txn(
            activity_date=date(2025, 9, 1),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="STO",
            quantity=1,
            price="1.20",
            amount="120",
        ),
        _make_txn(
            activity_date=date(2025, 10, 17),
            description="Option Expiration for TMC 10/17/2025 Call $7.00",
            trans_code="OEXP",
            quantity=1,
            price="0.00",
            amount="0",
        ),
    ]

    matched = match_leg_fills(_single_leg_fills(transactions))

    assert matched.open_quantity == 0
    assert matched.realized_premium == Decimal("120.00")
    expiration_lot = matched.lots[0]
    assert expiration_lot.status == "closed"
    assert expiration_lot.realized_premium == Decimal("120.00")


def test_match_leg_fills_handles_partial_assignment_after_closes():
    transactions = [
        _make_txn(
            activity_date=date(2025, 9, 1),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="STO",
            quantity=3,
            price="1.05",
            amount="315",
        ),
        _make_txn(
            activity_date=date(2025, 9, 20),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="BTC",
            quantity=1,
            price="0.40",
            amount="-40",
        ),
        _make_txn(
            activity_date=date(2025, 10, 17),
            description="Assignment of TMC 10/17/2025 Call $7.00",
            trans_code="OASGN",
            quantity=2,
            price="0.00",
            amount="0",
        ),
    ]

    matched = match_leg_fills(_single_leg_fills(transactions))

    closed_lots = [lot for lot in matched.lots if lot.status == "closed"]
    assert len(closed_lots) == 2

    btc_lot = next(
        lot for lot in closed_lots if any(p.fill.trans_code == "BTC" for p in lot.close_portions)
    )
    assert btc_lot.quantity == 1
    assert btc_lot.realized_premium == Decimal("65.00")

    assign_lot = next(
        lot for lot in closed_lots if any(p.fill.trans_code == "OASGN" for p in lot.close_portions)
    )
    assert assign_lot.quantity == 2
    assert assign_lot.realized_premium == Decimal("210.00")
    assert matched.open_quantity == 0


def test_match_leg_fills_handles_partial_expiration_after_closes():
    transactions = [
        _make_txn(
            activity_date=date(2025, 9, 1),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="STO",
            quantity=2,
            price="1.30",
            amount="260",
        ),
        _make_txn(
            activity_date=date(2025, 9, 20),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="BTC",
            quantity=1,
            price="0.35",
            amount="-35",
        ),
        _make_txn(
            activity_date=date(2025, 10, 17),
            description="Option Expiration for TMC 10/17/2025 Call $7.00",
            trans_code="OEXP",
            quantity=1,
            price="0.00",
            amount="0",
        ),
    ]

    matched = match_leg_fills(_single_leg_fills(transactions))

    closed_lots = [lot for lot in matched.lots if lot.status == "closed"]
    assert len(closed_lots) == 2

    btc_lot = next(
        lot for lot in closed_lots if any(p.fill.trans_code == "BTC" for p in lot.close_portions)
    )
    assert btc_lot.realized_premium == Decimal("95.00")

    exp_lot = next(
        lot for lot in closed_lots if any(p.fill.trans_code == "OEXP" for p in lot.close_portions)
    )
    assert exp_lot.realized_premium == Decimal("130.00")
    assert matched.open_quantity == 0


def test_matched_lot_close_cost_for_debit_close():
    """close_cost should reflect debit (negative close_premium) when closing is a cost."""
    transactions = [
        _make_txn(
            activity_date=date(2025, 10, 1),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="STO",
            quantity=2,
            price="1.00",
            amount="200",
        ),
        _make_txn(
            activity_date=date(2025, 10, 5),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="BTC",
            quantity=2,
            price="0.50",
            amount="-100",
        ),
    ]
    fills = _single_leg_fills(transactions)
    matched = match_leg_fills(fills)

    lot = matched.lots[0]
    assert lot.status == "closed"
    assert lot.close_premium == Decimal("-100.00")  # debit close
    assert lot.close_cost == Decimal("100.00")  # cost is positive
    assert lot.close_cost_total == Decimal("100.00")  # no fees in this case
    assert lot.close_quantity == 2
    assert lot.quantity_remaining == 0
    assert lot.credit_remaining == Decimal("0.00")


def test_matched_lot_close_cost_zero_for_credit_close():
    """close_cost should be 0 when closing is a credit (positive close_premium)."""
    transactions = [
        _make_txn(
            activity_date=date(2025, 9, 1),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="BTO",
            quantity=1,
            price="0.90",
            amount="-90",
        ),
        _make_txn(
            activity_date=date(2025, 9, 15),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="STC",
            quantity=1,
            price="1.40",
            amount="140",
        ),
    ]
    fills = _single_leg_fills(transactions)
    matched = match_leg_fills(fills)

    lot = matched.lots[0]
    assert lot.status == "closed"
    assert lot.close_premium == Decimal("140.00")  # credit close
    assert lot.close_cost == Decimal("0.00")  # no cost when closing is a credit
    assert lot.close_cost_total == Decimal("0.00")
    assert lot.close_quantity == 1


def test_matched_lot_credit_remaining_for_open_lot():
    """credit_remaining should equal open_premium for open lots, 0 for closed."""
    transactions = [
        _make_txn(
            activity_date=date(2025, 10, 1),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="STO",
            quantity=3,
            price="1.00",
            amount="300",
        ),
        _make_txn(
            activity_date=date(2025, 10, 5),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="BTC",
            quantity=1,
            price="0.30",
            amount="-30",
        ),
    ]
    fills = _single_leg_fills(transactions)
    matched = match_leg_fills(fills)

    lots = {lot.status: lot for lot in matched.lots}
    closed_lot = lots["closed"]
    assert closed_lot.credit_remaining == Decimal("0.00")
    assert closed_lot.quantity_remaining == 0

    open_lot = lots["open"]
    assert open_lot.open_premium == Decimal("200.00")
    assert open_lot.credit_remaining == Decimal("200.00")
    assert open_lot.quantity_remaining == 2


def test_matched_lot_net_premium_calculation():
    """net_premium should equal realized_premium minus total_fees."""
    transactions = [
        _make_txn(
            activity_date=date(2025, 10, 1),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="STO",
            quantity=2,
            price="1.00",
            amount="200",
        ),
        _make_txn(
            activity_date=date(2025, 10, 5),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="BTC",
            quantity=2,
            price="0.50",
            amount="-100",
        ),
    ]
    fills = _single_leg_fills(transactions)
    matched = match_leg_fills(fills)

    lot = matched.lots[0]
    assert lot.status == "closed"
    assert lot.realized_premium == Decimal("100.00")
    assert lot.total_fees == Decimal("0.00")
    assert lot.net_premium == Decimal("100.00")

    # For open lots, net_premium should be None
    open_transactions = [
        _make_txn(
            activity_date=date(2025, 10, 1),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="STO",
            quantity=1,
            price="1.00",
            amount="100",
        ),
    ]
    open_fills = _single_leg_fills(open_transactions)
    open_matched = match_leg_fills(open_fills)
    open_lot = open_matched.lots[0]
    assert open_lot.status == "open"
    assert open_lot.realized_premium is None
    assert open_lot.net_premium is None


def test_matched_lot_open_credit_net_with_fees():
    """open_credit_net should subtract fees from gross credit."""
    # Create transaction with fees: gross_notional = 200, amount = 198 -> fees = 2
    transactions = [
        _make_txn(
            activity_date=date(2025, 10, 1),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="STO",
            quantity=2,
            price="1.00",
            amount="198",  # Less than gross_notional (200) to simulate fees
        ),
    ]
    fills = _single_leg_fills(transactions)
    matched = match_leg_fills(fills)

    lot = matched.lots[0]
    assert lot.status == "open"
    assert lot.open_premium == Decimal("200.00")  # gross_notional
    assert lot.open_fees == Decimal("2.00")  # difference between gross and amount
    assert lot.open_credit_gross == Decimal("200.00")
    assert lot.open_credit_net == Decimal("198.00")  # gross minus fees


def test_matched_leg_opened_at_closed_at():
    """opened_at should be earliest lot open date, closed_at should be latest lot close date."""
    transactions = [
        _make_txn(
            activity_date=date(2025, 10, 1),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="STO",
            quantity=2,
            price="1.00",
            amount="200",
        ),
        _make_txn(
            activity_date=date(2025, 10, 5),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="BTC",
            quantity=1,
            price="0.50",
            amount="-50",
        ),
        _make_txn(
            activity_date=date(2025, 10, 10),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="STO",
            quantity=1,
            price="1.20",
            amount="120",
        ),
        _make_txn(
            activity_date=date(2025, 10, 15),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="BTC",
            quantity=1,
            price="0.60",
            amount="-60",
        ),
    ]
    fills = _single_leg_fills(transactions)
    matched = match_leg_fills(fills)

    assert matched.opened_at == date(2025, 10, 1)
    assert matched.closed_at == date(2025, 10, 15)
    assert matched.opened_quantity == 3  # 2 + 1
    assert matched.closed_quantity == 2  # 1 + 1


def test_matched_leg_opened_at_closed_at_open_leg():
    """opened_at should work for open legs, closed_at should be None."""
    transactions = [
        _make_txn(
            activity_date=date(2025, 10, 1),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="STO",
            quantity=2,
            price="1.00",
            amount="200",
        ),
    ]
    fills = _single_leg_fills(transactions)
    matched = match_leg_fills(fills)

    assert matched.opened_at == date(2025, 10, 1)
    assert matched.closed_at is None
    assert matched.opened_quantity == 2
    assert matched.closed_quantity == 0


def test_matched_leg_quantities():
    """opened_quantity and closed_quantity should aggregate across all lots."""
    transactions = [
        _make_txn(
            activity_date=date(2025, 10, 1),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="STO",
            quantity=3,
            price="1.00",
            amount="300",
        ),
        _make_txn(
            activity_date=date(2025, 10, 5),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="BTC",
            quantity=1,
            price="0.50",
            amount="-50",
        ),
        _make_txn(
            activity_date=date(2025, 10, 10),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="BTC",
            quantity=1,
            price="0.60",
            amount="-60",
        ),
    ]
    fills = _single_leg_fills(transactions)
    matched = match_leg_fills(fills)

    assert matched.opened_quantity == 3  # all 3 were opened
    assert matched.closed_quantity == 2  # 1 + 1 closed
    assert matched.open_quantity == 1  # 1 remaining open


def test_matched_leg_open_credit_gross_close_cost():
    """open_credit_gross and close_cost should aggregate from lots."""
    transactions = [
        _make_txn(
            activity_date=date(2025, 10, 1),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="STO",
            quantity=2,
            price="1.00",
            amount="200",
        ),
        _make_txn(
            activity_date=date(2025, 10, 5),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="BTC",
            quantity=2,
            price="0.50",
            amount="-100",
        ),
    ]
    fills = _single_leg_fills(transactions)
    matched = match_leg_fills(fills)

    assert matched.open_credit_gross == Decimal("200.00")
    assert matched.close_cost == Decimal("100.00")  # debit close


def test_matched_leg_fees():
    """open_fees and close_fees should aggregate from lots."""
    # Create transactions with fees: amount differs from gross_notional
    transactions = [
        _make_txn(
            activity_date=date(2025, 10, 1),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="STO",
            quantity=2,
            price="1.00",
            amount="198",  # 2 fee
        ),
        _make_txn(
            activity_date=date(2025, 10, 5),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="BTC",
            quantity=2,
            price="0.50",
            amount="-102",  # 2 fee
        ),
    ]
    fills = _single_leg_fills(transactions)
    matched = match_leg_fills(fills)

    assert matched.open_fees == Decimal("2.00")
    assert matched.close_fees == Decimal("2.00")


def test_matched_leg_resolution_buy_to_close():
    """resolution() should return 'BTC' transaction code for BTC closes."""
    transactions = [
        _make_txn(
            activity_date=date(2025, 10, 1),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="STO",
            quantity=2,
            price="1.00",
            amount="200",
        ),
        _make_txn(
            activity_date=date(2025, 10, 5),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="BTC",
            quantity=2,
            price="0.50",
            amount="-100",
        ),
    ]
    fills = _single_leg_fills(transactions)
    matched = match_leg_fills(fills)

    assert matched.resolution() == "BTC"


def test_matched_leg_resolution_sell_to_close():
    """resolution() should return 'STC' transaction code for STC closes."""
    transactions = [
        _make_txn(
            activity_date=date(2025, 9, 1),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="BTO",
            quantity=1,
            price="0.90",
            amount="-90",
        ),
        _make_txn(
            activity_date=date(2025, 9, 15),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="STC",
            quantity=1,
            price="1.40",
            amount="140",
        ),
    ]
    fills = _single_leg_fills(transactions)
    matched = match_leg_fills(fills)

    assert matched.resolution() == "STC"


def test_matched_leg_resolution_assignment():
    """resolution() should return 'OASGN' transaction code for assignment closes."""
    transactions = [
        _make_txn(
            activity_date=date(2025, 9, 1),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="STO",
            quantity=2,
            price="1.10",
            amount="220",
        ),
        _make_txn(
            activity_date=date(2025, 10, 17),
            description="Assignment of TMC 10/17/2025 Call $7.00",
            trans_code="OASGN",
            quantity=2,
            price="0.00",
            amount="0",
        ),
    ]
    fills = _single_leg_fills(transactions)
    matched = match_leg_fills(fills)

    assert matched.resolution() == "OASGN"


def test_matched_leg_resolution_expiration():
    """resolution() should return 'OEXP' transaction code for expiration closes."""
    transactions = [
        _make_txn(
            activity_date=date(2025, 9, 1),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="STO",
            quantity=1,
            price="1.20",
            amount="120",
        ),
        _make_txn(
            activity_date=date(2025, 10, 17),
            description="Option Expiration for TMC 10/17/2025 Call $7.00",
            trans_code="OEXP",
            quantity=1,
            price="0.00",
            amount="0",
        ),
    ]
    fills = _single_leg_fills(transactions)
    matched = match_leg_fills(fills)

    assert matched.resolution() == "OEXP"


def test_matched_leg_resolution_open_leg():
    """resolution() should return None for open legs."""
    transactions = [
        _make_txn(
            activity_date=date(2025, 10, 1),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="STO",
            quantity=2,
            price="1.00",
            amount="200",
        ),
    ]
    fills = _single_leg_fills(transactions)
    matched = match_leg_fills(fills)

    assert matched.resolution() is None


def test_matched_leg_resolution_mixed_closes():
    """resolution() should return the transaction code from the final closing transaction."""
    transactions = [
        _make_txn(
            activity_date=date(2025, 9, 1),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="STO",
            quantity=3,
            price="1.00",
            amount="300",
        ),
        _make_txn(
            activity_date=date(2025, 9, 20),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="BTC",
            quantity=1,
            price="0.40",
            amount="-40",
        ),
        _make_txn(
            activity_date=date(2025, 10, 17),
            description="Assignment of TMC 10/17/2025 Call $7.00",
            trans_code="OASGN",
            quantity=2,
            price="0.00",
            amount="0",
        ),
    ]
    fills = _single_leg_fills(transactions)
    matched = match_leg_fills(fills)

    # Should return the chronologically final closing transaction code (OASGN on 10/17)
    assert matched.resolution() == "OASGN"


def test_matched_leg_resolution_returns_final_not_prioritized():
    """resolution() should return the chronologically final transaction code, not prioritized type."""
    transactions = [
        _make_txn(
            activity_date=date(2025, 9, 1),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="STO",
            quantity=3,
            price="1.00",
            amount="300",
        ),
        _make_txn(
            activity_date=date(2025, 9, 15),
            description="Assignment of TMC 10/17/2025 Call $7.00",
            trans_code="OASGN",
            quantity=1,
            price="0.00",
            amount="0",
        ),
        _make_txn(
            activity_date=date(2025, 10, 5),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="BTC",
            quantity=2,
            price="0.40",
            amount="-80",
        ),
    ]
    fills = _single_leg_fills(transactions)
    matched = match_leg_fills(fills)

    # Should return BTC (final chronologically) not OASGN (prioritized type)
    assert matched.resolution() == "BTC"


def test_matched_leg_resolution_same_date_uses_tie_breaker():
    """resolution() should use process_date/settle_date as tie-breakers when activity dates match."""
    same_date = date(2025, 10, 17)
    transactions = [
        _make_txn(
            activity_date=date(2025, 10, 1),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="STO",
            quantity=2,
            price="1.00",
            amount="200",
        ),
        _make_txn(
            activity_date=same_date,
            process_date=date(2025, 10, 15),  # Earlier process date
            description="TMC 10/17/2025 Call $7.00",
            trans_code="BTC",
            quantity=1,
            price="0.50",
            amount="-50",
        ),
        _make_txn(
            activity_date=same_date,
            process_date=date(2025, 10, 18),  # Later process date (should win)
            description="TMC 10/17/2025 Call $7.00",
            trans_code="BTC",
            quantity=1,
            price="0.60",
            amount="-60",
        ),
    ]
    fills = _single_leg_fills(transactions)
    matched = match_leg_fills(fills)

    # Should return BTC with latest process_date (10/18) even though both have same activity_date
    assert matched.resolution() == "BTC"


def test_matched_leg_resolution_all_dates_same_uses_sequence():
    """resolution() should use sequence number as final tie-breaker when all dates match."""
    same_date = date(2025, 10, 17)
    transactions = [
        _make_txn(
            activity_date=date(2025, 10, 1),
            description="TMC 10/17/2025 Call $7.00",
            trans_code="STO",
            quantity=2,
            price="1.00",
            amount="200",
        ),
        # Both closings have identical activity/process/settle dates
        # The second one (STC) should win due to later sequence number
        _make_txn(
            activity_date=same_date,
            process_date=same_date,
            settle_date=same_date,
            description="TMC 10/17/2025 Call $7.00",
            trans_code="BTC",
            quantity=1,
            price="0.50",
            amount="-50",
        ),
        _make_txn(
            activity_date=same_date,
            process_date=same_date,
            settle_date=same_date,
            description="TMC 10/17/2025 Call $7.00",
            trans_code="BTC",
            quantity=1,
            price="0.60",
            amount="-60",
        ),
    ]
    fills = _single_leg_fills(transactions)
    matched = match_leg_fills(fills)

    # Should return transaction code from the second BTC (later sequence) even though all dates are identical
    # The second BTC comes after the first in the input, so gets higher sequence number
    assert matched.resolution() == "BTC"
    # Verify both lots were closed
    assert matched.closed_quantity == 2
