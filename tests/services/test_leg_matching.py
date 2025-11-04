from __future__ import annotations

from datetime import date
from decimal import Decimal

from premiumflow.core.legs import build_leg_fills
from premiumflow.core.parser import NormalizedOptionTransaction
from premiumflow.services.leg_matching import MatchedLegLot, match_leg_fills, match_legs


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
) -> NormalizedOptionTransaction:
    """Create a normalized transaction for testing."""
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
