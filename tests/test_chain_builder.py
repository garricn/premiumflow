"""Unit tests for rollchain.services.chain_builder helpers."""

from decimal import Decimal

from rollchain.services.chain_builder import (
    deduplicate_transactions,
    detect_roll_chains,
    detect_rolls,
)
from rollchain.core.parser import parse_transaction_row


def build_txn(overrides: dict) -> dict:
    """Convenience helper for minimal transaction dicts."""
    base = {
        "Activity Date": "9/1/2025",
        "Process Date": "9/1/2025",
        "Settle Date": "9/3/2025",
        "Instrument": "TEST",
        "Description": "TEST 9/19/2025 Call $100.00",
        "Trans Code": "STO",
        "Quantity": "1",
        "Price": "$1.00",
        "Amount": "$100.00",
    }
    txn = dict(base)
    txn.update(overrides)
    return txn


def test_deduplicate_transactions_merges_partial_fills():
    """Multiple fills at the same price should aggregate into a single leg."""
    txns = [
        build_txn({
            "Activity Date": "9/10/2025",
            "Instrument": "PLTR",
            "Trans Code": "BTC",
            "Quantity": "1",
            "Price": "$1.50",
            "Amount": "($150.00)",
            "Description": "PLTR 10/17/2025 Call $185.00",
        }),
        build_txn({
            "Activity Date": "9/10/2025",
            "Instrument": "PLTR",
            "Trans Code": "BTC",
            "Quantity": "1",
            "Price": "$1.50",
            "Amount": "($150.00)",
            "Description": "PLTR 10/17/2025 Call $185.00",
        }),
    ]

    merged = deduplicate_transactions(txns)

    assert len(merged) == 1
    merged_txn = merged[0]
    assert merged_txn["Quantity"] == "2"
    assert merged_txn["Amount"] == "($300.00)"


def test_detect_rolls_requires_matching_option_type():
    """Roll detection should ignore mismatched Call/Put pairs on the same day."""
    txns = [
        # Legitimate roll: BTC -> STO, same option type (put) with different strikes.
        build_txn({
            "Activity Date": "8/27/2025",
            "Instrument": "HOOD",
            "Trans Code": "STO",
            "Quantity": "1",
            "Price": "$1.00",
            "Amount": "$100.00",
            "Description": "HOOD 8/29/2025 Put $103.00",
        }),
        build_txn({
            "Activity Date": "8/29/2025",
            "Instrument": "HOOD",
            "Trans Code": "BTC",
            "Quantity": "1",
            "Price": "$1.20",
            "Amount": "($120.00)",
            "Description": "HOOD 8/29/2025 Put $103.00",
        }),
        build_txn({
            "Activity Date": "8/29/2025",
            "Instrument": "HOOD",
            "Trans Code": "STO",
            "Quantity": "1",
            "Price": "$2.00",
            "Amount": "$200.00",
            "Description": "HOOD 10/17/2025 Put $90.00",
        }),
        # Same day close/open but option types don't match (Call vs Put) – should be ignored.
        build_txn({
            "Activity Date": "9/1/2025",
            "Instrument": "HOOD",
            "Trans Code": "STO",
            "Quantity": "1",
            "Price": "$1.10",
            "Amount": "$110.00",
            "Description": "HOOD 10/17/2025 Call $145.00",
        }),
        build_txn({
            "Activity Date": "9/9/2025",
            "Instrument": "HOOD",
            "Trans Code": "BTC",
            "Quantity": "1",
            "Price": "$1.10",
            "Amount": "($110.00)",
            "Description": "HOOD 10/17/2025 Call $145.00",
        }),
        build_txn({
            "Activity Date": "9/9/2025",
            "Instrument": "HOOD",
            "Trans Code": "STO",
            "Quantity": "1",
            "Price": "$2.00",
            "Amount": "$200.00",
            "Description": "HOOD 10/17/2025 Put $100.00",
        }),
    ]

    rolls = [r for r in detect_rolls(txns) if r["ticker"] == "HOOD"]

    assert len(rolls) == 1
    assert rolls[0]["btc_desc"].endswith("Put $103.00")
    assert rolls[0]["sto_desc"].endswith("Put $90.00")


def test_detect_roll_chains_handles_partial_fill_closed_chain():
    """Chains built from partial fills should still close out cleanly."""
    txns = [
        # Initial short position opened across two partial fills.
        build_txn({
            "Activity Date": "9/08/2025",
            "Instrument": "PLTR",
            "Trans Code": "STO",
            "Quantity": "1",
            "Price": "$2.00",
            "Amount": "$200.00",
            "Description": "PLTR 10/17/2025 Call $185.00",
        }),
        build_txn({
            "Activity Date": "9/08/2025",
            "Instrument": "PLTR",
            "Trans Code": "STO",
            "Quantity": "1",
            "Price": "$2.00",
            "Amount": "$200.00",
            "Description": "PLTR 10/17/2025 Call $185.00",
        }),
        # Close existing short via two fills and roll to a later expiry.
        build_txn({
            "Activity Date": "9/19/2025",
            "Instrument": "PLTR",
            "Trans Code": "BTC",
            "Quantity": "1",
            "Price": "$1.50",
            "Amount": "($150.00)",
            "Description": "PLTR 10/17/2025 Call $185.00",
        }),
        build_txn({
            "Activity Date": "9/19/2025",
            "Instrument": "PLTR",
            "Trans Code": "BTC",
            "Quantity": "1",
            "Price": "$1.50",
            "Amount": "($150.00)",
            "Description": "PLTR 10/17/2025 Call $185.00",
        }),
        build_txn({
            "Activity Date": "9/19/2025",
            "Instrument": "PLTR",
            "Trans Code": "STO",
            "Quantity": "1",
            "Price": "$3.00",
            "Amount": "$300.00",
            "Description": "PLTR 11/21/2025 Call $200.00",
        }),
        build_txn({
            "Activity Date": "9/19/2025",
            "Instrument": "PLTR",
            "Trans Code": "STO",
            "Quantity": "1",
            "Price": "$3.00",
            "Amount": "$300.00",
            "Description": "PLTR 11/21/2025 Call $200.00",
        }),
        # Close the rolled position via two fills.
        build_txn({
            "Activity Date": "9/22/2025",
            "Instrument": "PLTR",
            "Trans Code": "BTC",
            "Quantity": "1",
            "Price": "$2.50",
            "Amount": "($250.00)",
            "Description": "PLTR 11/21/2025 Call $200.00",
        }),
        build_txn({
            "Activity Date": "9/22/2025",
            "Instrument": "PLTR",
            "Trans Code": "BTC",
            "Quantity": "1",
            "Price": "$2.50",
            "Amount": "($250.00)",
            "Description": "PLTR 11/21/2025 Call $200.00",
        }),
    ]

    chains = [c for c in detect_roll_chains(txns) if c["symbol"] == "PLTR"]

    assert len(chains) == 1
    chain = chains[0]
    assert chain["status"] == "CLOSED"
    assert chain["net_contracts"] == 0
    assert chain["roll_count"] == 1
    assert len(chain["transactions"]) == 4, "Partial fills should aggregate into four net legs"


def test_detect_roll_chains_does_not_flag_simple_open_close_as_roll():
    """A simple open/close alongside a real roll should not form a second chain."""
    txns = [
        # Simple position: STO then BTC a few days later (no roll)
        build_txn({
            "Activity Date": "9/17/2025",
            "Instrument": "TSLA",
            "Description": "TSLA 11/21/2025 Call $550.00",
            "Trans Code": "STO",
            "Price": "$4.25",
            "Amount": "$425.00",
        }),
        build_txn({
            "Activity Date": "9/22/2025",
            "Instrument": "TSLA",
            "Description": "TSLA 11/21/2025 Call $550.00",
            "Trans Code": "BTC",
            "Price": "$5.25",
            "Amount": "($525.00)",
        }),
        # Legitimate roll chain (515 -> 530)
        build_txn({
            "Activity Date": "9/12/2025",
            "Instrument": "TSLA",
            "Description": "TSLA 10/17/2025 Call $515.00",
            "Trans Code": "STO",
            "Price": "$3.00",
            "Amount": "$300.00",
        }),
        build_txn({
            "Activity Date": "9/22/2025",
            "Instrument": "TSLA",
            "Description": "TSLA 10/17/2025 Call $515.00",
            "Trans Code": "BTC",
            "Price": "$7.30",
            "Amount": "($730.00)",
        }),
        build_txn({
            "Activity Date": "9/22/2025",
            "Instrument": "TSLA",
            "Description": "TSLA 11/21/2025 Call $530.00",
            "Trans Code": "STO",
            "Price": "$5.75",
            "Amount": "$575.00",
        }),
        build_txn({
            "Activity Date": "10/08/2025",
            "Instrument": "TSLA",
            "Description": "TSLA 11/21/2025 Call $530.00",
            "Trans Code": "BTC",
            "Price": "$8.75",
            "Amount": "($875.00)",
        }),
    ]

    chains = [c for c in detect_roll_chains(txns) if c["symbol"] == "TSLA"]

    # BUG: currently returns two chains (one false). Expect only the real roll.
    assert len(chains) == 1


def _make_tsla_txn(date, desc, code, qty, price, amount):
    return {
        "Activity Date": date,
        "Process Date": date,
        "Settle Date": date,
        "Instrument": "TSLA",
        "Description": desc,
        "Trans Code": code,
        "Quantity": str(qty),
        "Price": price,
        "Amount": amount,
    }


def test_detect_roll_chains_handles_fully_closed_partial_fill_chain():
    """Aggregated closes should still leave a flat position marked as closed."""

    transactions = [
        _make_tsla_txn("8/27/2025", "TSLA 08/29/2025 Put $350.00", "STO", 1, "$9.50", "$950.00"),
        _make_tsla_txn("8/28/2025", "TSLA 08/29/2025 Put $350.00", "BTC", 1, "$5.00", "($500.00)"),
        _make_tsla_txn("8/28/2025", "TSLA 10/10/2025 Put $320.00", "STO", 1, "$10.25", "$1,024.95"),
        _make_tsla_txn("8/28/2025", "TSLA 10/10/2025 Put $320.00", "STO", 1, "$11.21", "$1,120.95"),
        _make_tsla_txn("8/29/2025", "TSLA 10/10/2025 Put $320.00", "STO", 1, "$12.15", "$1,214.95"),
        _make_tsla_txn("9/11/2025", "TSLA 10/10/2025 Put $320.00", "BTC", 1, "$4.80", "($480.04)"),
        _make_tsla_txn("9/11/2025", "TSLA 10/10/2025 Put $320.00", "BTC", 1, "$4.80", "($480.04)"),
        _make_tsla_txn("9/11/2025", "TSLA 10/10/2025 Put $320.00", "BTC", 1, "$4.80", "($480.04)"),
    ]

    chains = detect_roll_chains(transactions)
    tsla_chain = next(
        chain for chain in chains
        if chain.get("symbol") == "TSLA" and chain.get("strike") == Decimal("320")
    )

    # Expectation for a fully closed position.
    assert tsla_chain["status"] == "CLOSED"
    assert tsla_chain["net_contracts"] == 0


def test_roll_chain_pnl_calculation_treats_bto_as_debit_and_stc_as_credit():
    """
    Test that roll chain P&L calculation correctly treats BTO as debit and STC as credit.
    
    This test verifies that:
    - BTO (Buy-to-Open) is correctly treated as a debit (cash outflow)
    - STC (Sell-to-Close) is correctly treated as a credit (cash inflow)
    
    This test uses a roll chain with long positions: BTO -> BTC+STO -> STC
    Expected P&L: (STO + STC) - (BTO + BTC) = ($300 + $800) - ($500 + $200) = $400 profit
    """
    transactions = [
        # Initial long position: Buy-to-Open
        build_txn({
            "Activity Date": "9/1/2025",
            "Instrument": "TSLA",
            "Description": "TSLA 10/17/2025 Call $200.00",
            "Trans Code": "BTO",
            "Quantity": "1",
            "Price": "$5.00",
            "Amount": "($500.00)",
        }),
        # Roll: Buy-to-Close old position + Sell-to-Open new position
        build_txn({
            "Activity Date": "9/10/2025",
            "Instrument": "TSLA",
            "Description": "TSLA 10/17/2025 Call $200.00",
            "Trans Code": "BTC",
            "Quantity": "1",
            "Price": "$2.00",
            "Amount": "($200.00)",
        }),
        build_txn({
            "Activity Date": "9/10/2025",
            "Instrument": "TSLA",
            "Description": "TSLA 11/21/2025 Call $210.00",
            "Trans Code": "STO",
            "Quantity": "1",
            "Price": "$3.00",
            "Amount": "$300.00",
        }),
        # Final close: Sell-to-Close
        build_txn({
            "Activity Date": "9/20/2025",
            "Instrument": "TSLA", 
            "Description": "TSLA 11/21/2025 Call $210.00",
            "Trans Code": "STC",
            "Quantity": "1",
            "Price": "$8.00",
            "Amount": "$800.00",
        }),
    ]
    
    chains = detect_roll_chains(transactions)
    
    # Should find one chain
    assert len(chains) == 1
    chain = chains[0]
    
    # Chain should be closed (net contracts = 0)
    assert chain["status"] == "CLOSED"
    assert chain["net_contracts"] == 0
    
    # P&L should be $400 profit: (STO $300 + STC $800) - (BTO $500 + BTC $200) = $400
    # Current bug: treats BTO as credit, STC as debit = ($500 + $300) - ($200 + $800) = -$200
    expected_pnl = Decimal("400.00")  # ($300 + $800) - ($500 + $200)
    assert chain["net_pnl"] == expected_pnl, f"Expected P&L {expected_pnl}, got {chain['net_pnl']}"
    
    # Verify the individual credit/debit calculations
    # Credits should be: STO $300 + STC $800 = $1100
    # Debits should be: BTO $500 + BTC $200 = $700
    assert chain["total_credits"] == Decimal("1100.00"), f"Expected credits $1100, got {chain['total_credits']}"
    assert chain["total_debits"] == Decimal("700.00"), f"Expected debits $700, got {chain['total_debits']}"


def test_build_chain_pnl_calculation_treats_bto_as_debit_and_stc_as_credit():
    """
    Test that build_chain P&L calculation correctly treats BTO as debit and STC as credit.
    
    This test directly calls the build_chain function to verify correct P&L calculation.
    Verifies that BTO (Buy-to-Open) is treated as a debit and STC (Sell-to-Close) 
    is treated as a credit, ensuring accurate P&L calculations.
    """
    from rollchain.services.chain_builder import build_chain
    
    # Create a simple chain: BTO -> STC (long position)
    transactions = [
        build_txn({
            "Activity Date": "9/1/2025",
            "Instrument": "TSLA",
            "Description": "TSLA 10/17/2025 Call $200.00",
            "Trans Code": "BTO",
            "Quantity": "1",
            "Price": "$5.00",
            "Amount": "($500.00)",
        }),
        build_txn({
            "Activity Date": "9/15/2025",
            "Instrument": "TSLA", 
            "Description": "TSLA 10/17/2025 Call $200.00",
            "Trans Code": "STC",
            "Quantity": "1",
            "Price": "$8.00",
            "Amount": "$800.00",
        }),
    ]
    
    # Call build_chain directly with the first transaction as initial_open
    chain = build_chain(transactions[0], transactions, [], set())
    
    # Should find a chain
    assert chain is not None
    
    # P&L should be $300 profit (STC $800 - BTO $500)
    # Current bug: treats BTO as credit, STC as debit = $500 - $800 = -$300
    expected_pnl = Decimal("300.00")  # $800 - $500
    assert chain["net_pnl"] == expected_pnl, f"Expected P&L {expected_pnl}, got {chain['net_pnl']}"
    
    # Verify the individual credit/debit calculations
    # BTO should be counted as debit (cash outflow)
    # STC should be counted as credit (cash inflow)
    assert chain["total_credits"] == Decimal("800.00"), f"Expected credits $800, got {chain['total_credits']}"
    assert chain["total_debits"] == Decimal("500.00"), f"Expected debits $500, got {chain['total_debits']}"


def test_parse_transaction_row_treats_stc_as_sell_action():
    """
    Test that parse_transaction_row correctly treats STC as SELL action.
    
    This test demonstrates the bug where STC (Sell-to-Close) transactions
    are incorrectly parsed as BUY actions instead of SELL actions.
    
    Expected: STC should be parsed as action='SELL'
    Current bug: STC defaults to action='BUY'
    """
    # Create a CSV row for STC transaction
    stc_row = {
        'Instrument': 'TSLA',
        'Description': 'TSLA 10/17/2025 Call $200.00',
        'Trans Code': 'STC',
        'Quantity': '1',
        'Price': '$8.00',
        'Amount': '$800.00',
        'Activity Date': '9/15/2025'
    }
    
    # Parse the transaction
    transaction = parse_transaction_row(stc_row)
    
    # STC should be parsed as SELL action
    # Current bug: defaults to BUY action
    assert transaction.action == 'SELL', f"Expected action='SELL', got action='{transaction.action}'"
    
    # Verify other fields are parsed correctly
    assert transaction.symbol == 'TSLA'
    assert transaction.quantity == 1
    assert transaction.price == Decimal('8.00')


def test_parse_transaction_row_treats_bto_as_buy_action():
    """
    Test that parse_transaction_row correctly treats BTO as BUY action.
    
    This test verifies that BTO (Buy-to-Open) is correctly parsed as BUY action.
    This should already work correctly, but we test it to ensure our fix
    doesn't break existing functionality.
    """
    # Create a CSV row for BTO transaction
    bto_row = {
        'Instrument': 'TSLA',
        'Description': 'TSLA 10/17/2025 Call $200.00',
        'Trans Code': 'BTO',
        'Quantity': '1',
        'Price': '$5.00',
        'Amount': '($500.00)',
        'Activity Date': '9/1/2025'
    }
    
    # Parse the transaction
    transaction = parse_transaction_row(bto_row)
    
    # BTO should be parsed as BUY action
    assert transaction.action == 'BUY', f"Expected action='BUY', got action='{transaction.action}'"
    
    # Verify other fields are parsed correctly
    assert transaction.symbol == 'TSLA'
    assert transaction.quantity == 1
    assert transaction.price == Decimal('5.00')


def test_parse_transaction_row_extracts_expiration_from_description():
    """
    Test that expiration date is parsed from description instead of being hard-coded.
    
    This test demonstrates the bug where all transactions get expiration = "2024-01-19"
    regardless of the actual expiration date in the description.
    
    Scenario: Parse transactions with different expiration dates
    Expected: Each transaction should have its correct expiration date
    Current bug: All transactions get hard-coded "2024-01-19"
    """
    from rollchain.core.parser import parse_transaction_row
    
    # Test case 1: October 2025 expiration
    oct_row = {
        'Instrument': 'TSLA',
        'Description': 'TSLA 10/17/2025 Call $200.00',
        'Trans Code': 'BTO',
        'Quantity': '1',
        'Price': '$5.00',
        'Amount': '($500.00)',
        'Activity Date': '9/1/2025'
    }
    
    oct_transaction = parse_transaction_row(oct_row)
    
    # Should parse expiration from description, not use hard-coded "2024-01-19"
    assert oct_transaction.expiration == "2025-10-17", f"Expected 2025-10-17, got {oct_transaction.expiration}"
    
    # Test case 2: November 2025 expiration  
    nov_row = {
        'Instrument': 'AAPL',
        'Description': 'AAPL 11/21/2025 Put $150.00',
        'Trans Code': 'STO',
        'Quantity': '2',
        'Price': '$3.00',
        'Amount': '$600.00',
        'Activity Date': '9/1/2025'
    }
    
    nov_transaction = parse_transaction_row(nov_row)
    
    # Should parse different expiration date
    assert nov_transaction.expiration == "2025-11-21", f"Expected 2025-11-21, got {nov_transaction.expiration}"
    
    # Test case 3: December 2025 expiration
    dec_row = {
        'Instrument': 'SPY',
        'Description': 'SPY 12/19/2025 Call $500.00',
        'Trans Code': 'BTO',
        'Quantity': '1',
        'Price': '$10.00',
        'Amount': '($1000.00)',
        'Activity Date': '9/1/2025'
    }
    
    dec_transaction = parse_transaction_row(dec_row)
    
    # Should parse yet another expiration date
    assert dec_transaction.expiration == "2025-12-19", f"Expected 2025-12-19, got {dec_transaction.expiration}"


def test_format_roll_chain_summary_handles_none_breakeven_price():
    """
    Test that format_roll_chain_summary handles None breakeven_price without crashing.
    
    This test demonstrates the bug where f-string formatting with conditional
    inside format specifier causes ValueError: invalid format string.
    
    Scenario: Chain with breakeven_price = None
    Expected: Should format summary without crashing
    Current bug: Raises ValueError: invalid format string
    """
    from rollchain.formatters.output import format_roll_chain_summary
    from rollchain.core.models import RollChain, Transaction
    from decimal import Decimal
    
    # Create a chain with None breakeven_price
    chain = RollChain(
        symbol="TSLA",
        option_type="C",
        strike=Decimal("200.00"),
        expiration="2025-10-17",
        net_pnl=Decimal("100.00"),
        total_fees=Decimal("5.00"),
        net_pnl_after_fees=Decimal("95.00"),
        breakeven_price=None,  # This should not crash the formatter
        transactions=[
            Transaction(
                symbol="TSLA",
                option_type="C",
                strike=Decimal("200.00"),
                expiration="2025-10-17",
                action="BUY",
                quantity=1,
                price=Decimal("5.00"),
                date="2025-09-01"
            ),
            Transaction(
                symbol="TSLA",
                option_type="C",
                strike=Decimal("200.00"),
                expiration="2025-10-17",
                action="SELL",
                quantity=1,
                price=Decimal("6.00"),
                date="2025-09-15"
            )
        ]
    )
    
    # This should not raise ValueError: invalid format string
    summary = format_roll_chain_summary(chain)
    
    # Should contain the formatted summary with 'N/A' for breakeven
    assert "Breakeven: N/A" in summary
    assert "Net P&L: $1.00" in summary  # Actual calculated P&L from the transactions
    assert "Transactions: 2" in summary


def test_format_roll_chain_summary_handles_valid_breakeven_price():
    """
    Test that format_roll_chain_summary handles valid breakeven_price correctly.
    
    This test ensures the formatter works correctly when breakeven_price has a value.
    
    Scenario: Chain with breakeven_price = 205.50
    Expected: Should format summary with proper breakeven price
    """
    from rollchain.formatters.output import format_roll_chain_summary
    from rollchain.core.models import RollChain, Transaction
    from decimal import Decimal
    
    # Create a chain with valid breakeven_price (open position)
    chain = RollChain(
        symbol="TSLA",
        option_type="C",
        strike=Decimal("200.00"),
        expiration="2025-10-17",
        transactions=[
            Transaction(
                symbol="TSLA",
                option_type="C",
                strike=Decimal("200.00"),
                expiration="2025-10-17",
                action="BUY",
                quantity=2,  # Buy 2 contracts
                price=Decimal("5.00"),
                date="2025-09-01"
            ),
            Transaction(
                symbol="TSLA",
                option_type="C",
                strike=Decimal("200.00"),
                expiration="2025-10-17",
                action="SELL",
                quantity=1,  # Sell 1 contract (net quantity = 1, open position)
                price=Decimal("6.00"),
                date="2025-09-15"
            )
        ]
    )
    
    # This should format correctly
    summary = format_roll_chain_summary(chain)
    
    # Should contain the formatted summary with proper breakeven price
    # Net quantity = 1, net P&L = 6 - 10 = -4, breakeven = 200 + (-4.12/1) = 195.88
    assert "Breakeven: $195.88" in summary
    assert "Net P&L: $-4.00" in summary
    assert "Transactions: 2" in summary
