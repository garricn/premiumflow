"""Unit tests for rollchain.services.chain_builder helpers."""

from rollchain.services.chain_builder import (
    deduplicate_transactions,
    detect_roll_chains,
    detect_rolls,
)


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
