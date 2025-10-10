"""CLI integration-related tests for rollchain commands."""

from decimal import Decimal

from click.testing import CliRunner

from rollchain.cli.commands import main as rollchain_cli, prepare_transactions_for_display
from rollchain.services.targets import calculate_target_percents
from rollchain.services.transactions import (
    filter_open_positions,
    filter_transactions_by_option_type,
    filter_transactions_by_ticker,
)
from rollchain.core.parser import get_options_transactions


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


def test_ingest_reports_missing_ticker(tmp_path):
    """--ticker should report when no transactions exist for the symbol."""
    sample_csv = _write_sample_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(rollchain_cli, ['ingest', '--ticker', 'ZZZ', str(sample_csv)])

    assert result.exit_code == 0
    assert "No options transactions found for ticker ZZZ" in result.output


def test_filter_transactions_by_ticker(tmp_path):
    """Ticker filter returns only matching instruments."""
    sample_csv = _write_sample_csv(tmp_path)
    transactions = get_options_transactions(str(sample_csv))

    filtered = filter_transactions_by_ticker(transactions, "TMC")

    assert len(filtered) == 1
    assert filtered[0]["Instrument"] == "TMC"


def test_filter_transactions_calls_only(tmp_path):
    """Call filter keeps only call legs from the selection."""
    sample_csv = _write_sample_csv(tmp_path)
    transactions = get_options_transactions(str(sample_csv))

    pltr_transactions = filter_transactions_by_ticker(transactions, "PLTR")
    calls_only = filter_transactions_by_option_type(pltr_transactions, calls_only=True)

    assert len(calls_only) == 1
    assert "Call" in calls_only[0]["Description"]
    assert "Put" not in calls_only[0]["Description"]


def test_filter_transactions_puts_only(tmp_path):
    """Put filter keeps only put legs from the selection."""
    sample_csv = _write_sample_csv(tmp_path)
    transactions = get_options_transactions(str(sample_csv))

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

    transactions = get_options_transactions(str(csv_path))
    open_positions = filter_open_positions(transactions)

    assert len(open_positions) == 1

    target_percents = calculate_target_percents((Decimal("0.5"), Decimal("0.7")))
    rows = prepare_transactions_for_display(
        open_positions,
        target_percents,
    )
    assert rows[0]["description"] == "AAPL $300.00 Call"
    assert rows[0]["target_close"] == "$6.75, $7.20, $7.65"
    assert rows[0]["expiration"] == "12/20/2025"


def test_prepare_transactions_for_display_honors_target_range(tmp_path):
    """Target percent range adjusts BTC/STC guidance."""
    sample_csv = _write_sample_csv(tmp_path)
    transactions = get_options_transactions(str(sample_csv))
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


def test_ingest_cli_open_only_message(tmp_path):
    """CLI still reports open position counts for --open-only."""
    csv_content = """Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
9/12/2025,9/12/2025,9/15/2025,TSLA,TSLA 10/17/2025 Call $515.00,STO,1,$3.00,$300.00
9/22/2025,9/22/2025,9/24/2025,TSLA,TSLA 10/17/2025 Call $515.00,BTC,1,$7.00,($700.00)
9/30/2025,9/30/2025,10/2/2025,AAPL,AAPL 12/20/2025 Call $300.00,BTO,1,$4.50,($450.00)
"""
    csv_path = tmp_path / "open_only.csv"
    csv_path.write_text(csv_content, encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(rollchain_cli, ['ingest', '--open-only', str(csv_path)])

    assert result.exit_code == 0
    assert "Open positions: 1" in result.output


def test_analyze_summary_shows_realized_for_open_chain(tmp_path):
    csv_path = _write_open_chain_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(rollchain_cli, ['analyze', str(csv_path), '--format', 'summary', '--open-only'])

    assert result.exit_code == 0
    output = result.output
    assert "Realized P&L (after fees):" in output
    assert "Target Price: $0.30 - $0.50" in output
    assert "Net P&L (after fees):" not in output


def test_analyze_summary_custom_target(tmp_path):
    csv_path = _write_open_chain_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(rollchain_cli, ['analyze', str(csv_path), '--format', 'summary', '--open-only', '--target', '0.25-0.5'])

    assert result.exit_code == 0
    output = result.output
    assert "Target Price: $0.50 - $0.75" in output


def test_trace_outputs_full_history(tmp_path):
    csv_path = _write_trace_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(rollchain_cli, ['trace', 'TSLA $550 Call', str(csv_path)])

    assert result.exit_code == 0
    output = result.output
    assert "Chain 1" in output
    assert "TSLA 10/17/2025 Call $515.00" in output
    assert "TSLA 11/21/2025 Call $550.00" in output
    assert "Net P&L" in output


def test_trace_no_match(tmp_path):
    csv_path = _write_trace_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(rollchain_cli, ['trace', 'AAPL $150 Call', str(csv_path)])

    assert result.exit_code == 0
    assert "No roll chains found" in result.output


def test_trace_open_chain_shows_realized(tmp_path):
    csv_path = _write_open_chain_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(rollchain_cli, ['trace', 'TSLA $550 Call', str(csv_path)])

    assert result.exit_code == 0
    output = result.output
    assert "Realized P&L (after fees):" in output
    assert "Target Price: $0.30 - $0.50" in output


def test_trace_custom_target(tmp_path):
    csv_path = _write_open_chain_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(rollchain_cli, ['trace', 'TSLA $550 Call', str(csv_path), '--target', '0.2-0.4'])

    assert result.exit_code == 0
    output = result.output
    assert "Target Price: $0.60 - $0.80" in output
