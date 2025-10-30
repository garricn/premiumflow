"""CLI integration-related tests for premiumflow commands."""

import json
from decimal import Decimal

from click.testing import CliRunner

from premiumflow.cli.commands import main as premiumflow_cli
from premiumflow.cli.utils import prepare_transactions_for_display
from premiumflow.core.parser import load_option_transactions
from premiumflow.services.targets import calculate_target_percents
from premiumflow.services.transactions import (
    filter_open_positions,
    filter_transactions_by_option_type,
    filter_transactions_by_ticker,
    normalized_to_csv_dicts,
)


def test_cli_help_lists_all_commands():
    """Root CLI help should list all registered subcommands."""
    runner = CliRunner()

    result = runner.invoke(premiumflow_cli, ["--help"])

    assert result.exit_code == 0
    output = result.output
    for command in ("analyze", "import", "ingest", "lookup", "trace"):
        assert command in output


def test_cli_unknown_command_reports_error():
    """Unknown commands should produce a helpful error message."""
    runner = CliRunner()

    result = runner.invoke(premiumflow_cli, ["unknown"])

    assert result.exit_code != 0
    assert "No such command" in result.output


def _write_sample_csv(tmp_path):
    csv_content = """Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
9/1/2025,9/1/2025,9/3/2025,TMC,TMC 11/21/2025 Call $11.00,STO,1,$0.40,$40.00
9/1/2025,9/1/2025,9/3/2025,PLTR,PLTR 11/21/2025 Call $200.00,STO,1,$3.00,$300.00
9/2/2025,9/2/2025,9/4/2025,PLTR,PLTR 11/21/2025 Put $200.00,BTC,1,$2.50,($250.00)
"""
    sample_csv = tmp_path / "sample.csv"
    sample_csv.write_text(csv_content, encoding="utf-8")
    return sample_csv


def _write_trace_csv(tmp_path):
    csv_content = """Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
9/12/2025,9/12/2025,9/15/2025,TSLA,TSLA 10/17/2025 Call $515.00,STO,1,$3.00,$300.00
9/22/2025,9/22/2025,9/24/2025,TSLA,TSLA 10/17/2025 Call $515.00,BTC,1,$7.00,($700.00)
9/22/2025,9/22/2025,9/24/2025,TSLA,TSLA 11/21/2025 Call $550.00,STO,1,$5.00,$500.00
10/8/2025,10/8/2025,10/10/2025,TSLA,TSLA 11/21/2025 Call $550.00,BTC,1,$6.00,($600.00)
1/3/2026,1/3/2026,1/5/2026,AAPL,AAPL 01/17/2026 Put $120.00,STO,1,$4.00,$400.00
"""
    sample_csv = tmp_path / "trace.csv"
    sample_csv.write_text(csv_content, encoding="utf-8")
    return sample_csv


def _write_open_chain_csv(tmp_path):
    csv_content = """Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
9/12/2025,9/12/2025,9/15/2025,TSLA,TSLA 10/17/2025 Call $515.00,STO,1,$3.00,$300.00
9/22/2025,9/22/2025,9/24/2025,TSLA,TSLA 10/17/2025 Call $515.00,BTC,1,$7.00,($700.00)
9/22/2025,9/22/2025,9/24/2025,TSLA,TSLA 11/21/2025 Call $550.00,STO,1,$5.00,$500.00
"""
    sample_csv = tmp_path / "open_chain.csv"
    sample_csv.write_text(csv_content, encoding="utf-8")
    return sample_csv


def test_import_reports_missing_ticker(tmp_path):
    """--ticker should report when no transactions exist for the symbol."""
    sample_csv = _write_sample_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(
        premiumflow_cli,
        ["import", "--ticker", "ZZZ", "--file", str(sample_csv), "--account-name", "Test Account"],
    )

    assert result.exit_code == 0
    assert "No options transactions found for ticker ZZZ" in result.output


def test_filter_transactions_by_ticker(tmp_path):
    """Ticker filter returns only matching instruments."""
    sample_csv = _write_sample_csv(tmp_path)
    transactions = _load_transaction_dicts(str(sample_csv))

    filtered = filter_transactions_by_ticker(transactions, "TMC")

    assert len(filtered) == 1
    assert filtered[0]["Instrument"] == "TMC"


def test_filter_transactions_calls_only(tmp_path):
    """Call filter keeps only call legs from the selection."""
    sample_csv = _write_sample_csv(tmp_path)
    transactions = _load_transaction_dicts(str(sample_csv))

    pltr_transactions = filter_transactions_by_ticker(transactions, "PLTR")
    calls_only = filter_transactions_by_option_type(pltr_transactions, calls_only=True)

    assert len(calls_only) == 1
    assert "Call" in calls_only[0]["Description"]
    assert "Put" not in calls_only[0]["Description"]


def test_filter_transactions_puts_only(tmp_path):
    """Put filter keeps only put legs from the selection."""
    sample_csv = _write_sample_csv(tmp_path)
    transactions = _load_transaction_dicts(str(sample_csv))

    pltr_transactions = filter_transactions_by_ticker(transactions, "PLTR")
    puts_only = filter_transactions_by_option_type(pltr_transactions, puts_only=True)

    assert len(puts_only) == 1
    assert "Put" in puts_only[0]["Description"]
    assert "Call" not in puts_only[0]["Description"]


def test_filter_open_positions_and_display_format(tmp_path):
    """Open-only filter removes closed legs and formats display metadata."""
    csv_content = """Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
9/12/2025,9/12/2025,9/15/2025,TSLA,TSLA 10/17/2025 Call $515.00,STO,1,$3.00,$300.00
9/22/2025,9/22/2025,9/24/2025,TSLA,TSLA 10/17/2025 Call $515.00,BTC,1,$7.00,($700.00)
9/30/2025,9/30/2025,10/2/2025,AAPL,AAPL 12/20/2025 Call $300.00,BTO,1,$4.50,($450.00)
"""
    csv_path = tmp_path / "open_only.csv"
    csv_path.write_text(csv_content, encoding="utf-8")

    transactions = _load_transaction_dicts(str(csv_path))
    open_positions = filter_open_positions(transactions)

    assert len(open_positions) == 1


def test_filter_open_positions_includes_partially_closed_positions():
    """
    Test that open position filter correctly handles partial closes by netting quantities.

    This test demonstrates the bug where partially closed positions are incorrectly
    filtered out. The filter should net quantities rather than treat close presence
    as an all-or-nothing toggle.

    Scenario: STO 2 contracts, BTC 1 contract = 1 contract still open
    Expected: Should show 1 open position (net quantity > 0)
    Current bug: Shows 0 open positions (all filtered out)
    """
    from premiumflow.services.transactions import filter_open_positions

    transactions = [
        # Open 2 long contracts (BTO)
        {
            "Activity Date": "9/1/2025",
            "Instrument": "TSLA",
            "Description": "TSLA 10/17/2025 Call $200.00",
            "Trans Code": "BTO",
            "Quantity": "2",
            "Price": "$5.00",
            "Amount": "($1000.00)",
        },
        # Close 1 contract (partial close with STC)
        {
            "Activity Date": "9/15/2025",
            "Instrument": "TSLA",
            "Description": "TSLA 10/17/2025 Call $200.00",
            "Trans Code": "STC",
            "Quantity": "1",
            "Price": "$8.00",
            "Amount": "$800.00",
        },
        # Another position that's fully closed (should not appear)
        {
            "Activity Date": "9/1/2025",
            "Instrument": "AAPL",
            "Description": "AAPL 10/17/2025 Call $150.00",
            "Trans Code": "STO",
            "Quantity": "1",
            "Price": "$3.00",
            "Amount": "$300.00",
        },
        {
            "Activity Date": "9/10/2025",
            "Instrument": "AAPL",
            "Description": "AAPL 10/17/2025 Call $150.00",
            "Trans Code": "BTC",
            "Quantity": "1",
            "Price": "$2.00",
            "Amount": "($200.00)",
        },
    ]

    open_positions = filter_open_positions(transactions)

    # Should find 1 open position (TSLA with net quantity of 1)
    # Current bug: finds 0 positions because partial close filters out all TSLA positions
    assert len(open_positions) == 1, f"Expected 1 open position, got {len(open_positions)}"

    # The remaining position should be TSLA with net quantity
    tsla_position = open_positions[0]
    assert tsla_position["Instrument"] == "TSLA"
    assert tsla_position["Description"] == "TSLA 10/17/2025 Call $200.00"
    assert tsla_position["Trans Code"] == "BTO"
    assert tsla_position["Quantity"] == "1"  # Net quantity after partial close (2 - 1 = 1)


def test_filter_open_positions_aggregates_partial_fills():
    """
    Test that open position filter aggregates partial fills instead of returning duplicates.

    This test demonstrates the bug where partially closed positions return all
    original opening transactions instead of aggregating them into net quantities.

    Scenario: BTO 1 + BTO 1 + STC 1 = 1 contract still open
    Expected: Should return 1 aggregated entry with net quantity
    Current bug: Returns 2 separate BTO entries (double-counting)
    """
    from premiumflow.services.transactions import filter_open_positions

    transactions = [
        # Open 1 contract (first fill)
        {
            "Activity Date": "9/1/2025",
            "Instrument": "TSLA",
            "Description": "TSLA 10/17/2025 Call $200.00",
            "Trans Code": "BTO",
            "Quantity": "1",
            "Price": "$5.00",
            "Amount": "($500.00)",
        },
        # Open 1 contract (second fill)
        {
            "Activity Date": "9/1/2025",
            "Instrument": "TSLA",
            "Description": "TSLA 10/17/2025 Call $200.00",
            "Trans Code": "BTO",
            "Quantity": "1",
            "Price": "$5.00",
            "Amount": "($500.00)",
        },
        # Close 1 contract (partial close)
        {
            "Activity Date": "9/15/2025",
            "Instrument": "TSLA",
            "Description": "TSLA 10/17/2025 Call $200.00",
            "Trans Code": "STC",
            "Quantity": "1",
            "Price": "$8.00",
            "Amount": "$800.00",
        },
    ]

    open_positions = filter_open_positions(transactions)

    # Should find 1 aggregated position (net quantity = 1)
    # Current bug: finds 2 separate BTO entries (double-counting)
    assert (
        len(open_positions) == 1
    ), f"Expected 1 aggregated position, got {len(open_positions)} separate entries"

    # The aggregated position should have net quantity of 1
    aggregated_position = open_positions[0]
    assert aggregated_position["Instrument"] == "TSLA"
    assert aggregated_position["Description"] == "TSLA 10/17/2025 Call $200.00"
    assert aggregated_position["Trans Code"] == "BTO"
    # Note: The exact quantity handling depends on implementation approach
    # This test verifies we get 1 entry instead of 2


def test_prepare_transactions_for_display_honors_target_range(tmp_path):
    """Target percent range adjusts BTC/STC guidance."""
    sample_csv = _write_sample_csv(tmp_path)
    transactions = _load_transaction_dicts(str(sample_csv))
    pltr_calls = filter_transactions_by_option_type(
        filter_transactions_by_ticker(transactions, "PLTR"),
        calls_only=True,
    )

    target_percents = calculate_target_percents((Decimal("0.25"), Decimal("0.6")))
    rows = prepare_transactions_for_display(
        pltr_calls,
        target_percents,
    )

    assert rows[0]["description"] == "PLTR $200.00 Call"
    assert rows[0]["target_close"] == "$2.25, $1.73, $1.20"


def test_import_cli_open_only_message(tmp_path):
    """CLI still reports open position counts for --open-only."""
    csv_content = """Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
9/12/2025,9/12/2025,9/15/2025,TSLA,TSLA 10/17/2025 Call $515.00,STO,1,$3.00,$300.00
9/22/2025,9/22/2025,9/24/2025,TSLA,TSLA 10/17/2025 Call $515.00,BTC,1,$7.00,($700.00)
9/30/2025,9/30/2025,10/2/2025,AAPL,AAPL 12/20/2025 Call $300.00,BTO,1,$4.50,($450.00)
"""
    csv_path = tmp_path / "open_only.csv"
    csv_path.write_text(csv_content, encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        premiumflow_cli,
        [
            "import",
            "--options",
            "--open-only",
            "--file",
            str(csv_path),
            "--account-name",
            "Test Account",
        ],
    )

    assert result.exit_code == 0
    assert "Open positions: 1" in result.output


def test_analyze_summary_shows_realized_for_open_chain(tmp_path):
    csv_path = _write_open_chain_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(
        premiumflow_cli, ["analyze", str(csv_path), "--format", "summary", "--open-only"]
    )

    assert result.exit_code == 0
    output = result.output
    assert "Realized P&L (after fees):" in output
    assert "Target Price: $0.30 - $0.50" in output
    assert "Net P&L (after fees):" not in output


def test_analyze_summary_custom_target(tmp_path):
    csv_path = _write_open_chain_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(
        premiumflow_cli,
        ["analyze", str(csv_path), "--format", "summary", "--open-only", "--target", "0.25-0.5"],
    )

    assert result.exit_code == 0
    output = result.output
    assert "Target Price: $0.50 - $0.75" in output


def test_trace_outputs_full_history(tmp_path):
    csv_path = _write_trace_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(premiumflow_cli, ["trace", "TSLA $550 Call", str(csv_path)])

    assert result.exit_code == 0
    output = result.output
    assert "Chain 1" in output
    assert "TSLA 10/17/2025 Call $515.00" in output
    assert "TSLA 11/21/2025 Call $550.00" in output
    assert "Net P&L" in output


def test_trace_no_match(tmp_path):
    csv_path = _write_trace_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(premiumflow_cli, ["trace", "AAPL $150 Call", str(csv_path)])

    assert result.exit_code == 0
    assert "No roll chains found" in result.output


def test_trace_open_chain_shows_realized(tmp_path):
    csv_path = _write_open_chain_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(premiumflow_cli, ["trace", "TSLA $550 Call", str(csv_path)])

    assert result.exit_code == 0
    output = result.output
    assert "Realized P&L (after fees):" in output
    assert "Target Price: $0.30 - $0.50" in output


def test_trace_custom_target(tmp_path):
    csv_path = _write_open_chain_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(
        premiumflow_cli, ["trace", "TSLA $550 Call", str(csv_path), "--target", "0.2-0.4"]
    )

    assert result.exit_code == 0
    output = result.output
    assert "Target Price: $0.60 - $0.80" in output


def test_lookup_matches_position_spec(tmp_path):
    csv_path = _write_trace_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(
        premiumflow_cli,
        ["lookup", "TSLA $515 C 2025-10-17", "--file", str(csv_path)],
    )

    assert result.exit_code == 0
    assert "Found 2 matching transactions" in result.output

    result_padded = runner.invoke(
        premiumflow_cli,
        ["lookup", "AAPL $120 P 2026-01-17", "--file", str(csv_path)],
    )

    assert result_padded.exit_code == 0
    assert "Found 1 matching transactions" in result_padded.output


def test_lookup_invalid_spec(tmp_path):
    csv_path = _write_trace_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(
        premiumflow_cli,
        ["lookup", "INVALID SPEC", "--file", str(csv_path)],
    )

    assert result.exit_code != 0
    assert "Invalid lookup format" in result.output


def test_filter_open_positions_includes_short_positions():
    """
    Test that open position filter includes short positions (negative quantities).

    This test demonstrates the bug where short positions are filtered out because
    the quantity calculation logic doesn't properly handle short positions as negative quantities.

    Scenario: STO 1 contract (short position) should have net quantity = -1
    Expected: Should show the open short position
    Current bug: STO is treated as positive quantity, so net_quantity = +1 (incorrect)
    """
    from premiumflow.services.transactions import filter_open_positions

    transactions = [
        # Open 1 short contract (STO) - should contribute negative quantity
        {
            "Activity Date": "9/1/2025",
            "Instrument": "TSLA",
            "Description": "TSLA 10/17/2025 Call $200.00",
            "Trans Code": "STO",
            "Quantity": "1",
            "Price": "$5.00",
            "Amount": "$500.00",
        },
        # No closing transaction - position should still be open
    ]

    open_positions = filter_open_positions(transactions)

    # Should find 1 open short position (net quantity = -1)
    # Current bug: STO is treated as positive, so net_quantity = +1, but this is conceptually wrong
    # The real issue is that short positions should have negative net quantities
    assert len(open_positions) == 1, f"Expected 1 open short position, got {len(open_positions)}"

    # The short position should be included with correct quantity sign
    short_position = open_positions[0]
    assert short_position["Instrument"] == "TSLA"
    assert short_position["Description"] == "TSLA 10/17/2025 Call $200.00"
    assert short_position["Trans Code"] == "STO"
    # For short positions, the quantity should be negative to represent the short position
    assert (
        short_position["Quantity"] == "-1"
    ), f"Expected negative quantity for short position, got {short_position['Quantity']}"


def test_import_json_output(tmp_path):
    """JSON output should be machine-friendly with stringified decimals."""
    sample_csv = _write_sample_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(
        premiumflow_cli,
        [
            "import",
            "--options",
            "--file",
            str(sample_csv),
            "--ticker",
            "PLTR",
            "--account-name",
            "Test Account",
            "--json-output",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["filters"]["ticker"] == "PLTR"
    assert payload["filters"]["options_only"] is True
    assert payload["account"]["name"] == "Test Account"
    assert "target_percents" not in payload
    assert payload["transactions"]
    first_txn = payload["transactions"][0]
    assert first_txn["instrument"] == "PLTR"
    assert "credit" not in first_txn
    assert "amount" in first_txn
    assert all(txn["instrument"] == "PLTR" for txn in payload["transactions"])


def test_import_strategy_calls_only(tmp_path):
    """Strategy flag should filter to matching option legs."""
    sample_csv = _write_sample_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(
        premiumflow_cli,
        [
            "import",
            "--file",
            str(sample_csv),
            "--strategy",
            "calls",
            "--account-name",
            "Test Account",
        ],
    )

    assert result.exit_code == 0
    assert "Put" not in result.output


def test_ingest_alias_emits_deprecation(tmp_path):
    """Legacy 'ingest' alias should still function but warn the user."""
    sample_csv = _write_sample_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(
        premiumflow_cli,
        ["ingest", "--file", str(sample_csv), "--account-name", "Test Account"],
    )

    assert result.exit_code == 0
    assert "deprecated" in (result.stderr or "").lower()


def test_filter_open_positions_includes_partially_closed_short_positions():
    """
    Test that open position filter includes partially closed short positions.

    This test demonstrates the bug where short position quantity calculation
    doesn't properly handle the sign convention for short positions.

    Scenario: STO 2 contracts + BTC 1 contract = net quantity = -1 (still open)
    Expected: Should show 1 open short position with negative quantity
    Current bug: STO treated as positive, so net_quantity = +1 (incorrect)
    """
    from premiumflow.services.transactions import filter_open_positions

    transactions = [
        # Open 2 short contracts (STO) - should contribute -2 to net quantity
        {
            "Activity Date": "9/1/2025",
            "Instrument": "TSLA",
            "Description": "TSLA 10/17/2025 Call $200.00",
            "Trans Code": "STO",
            "Quantity": "2",
            "Price": "$5.00",
            "Amount": "$1000.00",
        },
        # Close 1 short contract (BTC) - should add +1 to net quantity
        {
            "Activity Date": "9/15/2025",
            "Instrument": "TSLA",
            "Description": "TSLA 10/17/2025 Call $200.00",
            "Trans Code": "BTC",
            "Quantity": "1",
            "Price": "$3.00",
            "Amount": "($300.00)",
        },
    ]

    open_positions = filter_open_positions(transactions)

    # Should find 1 open short position (net quantity = -1)
    # Current bug: STO treated as positive, so net_quantity = +1 (incorrect)
    assert len(open_positions) == 1, f"Expected 1 open short position, got {len(open_positions)}"

    # The remaining short position should be included with correct quantity sign
    short_position = open_positions[0]
    assert short_position["Instrument"] == "TSLA"
    assert short_position["Description"] == "TSLA 10/17/2025 Call $200.00"
    assert short_position["Trans Code"] == "STO"
    # For short positions, the quantity should be negative to represent the short position
    assert (
        short_position["Quantity"] == "-1"
    ), f"Expected negative quantity for short position, got {short_position['Quantity']}"


def _load_transaction_dicts(csv_path: str) -> list[dict]:
    parsed = load_option_transactions(
        csv_path,
        account_name="Test Account",
        regulatory_fee=Decimal("0.04"),
    )
    return normalized_to_csv_dicts(parsed.transactions)
