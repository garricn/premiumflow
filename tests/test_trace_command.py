"""Tests for the trace command module."""

from pathlib import Path

from click.testing import CliRunner

from rollchain.cli.trace import trace


def _write_trace_csv(tmp_path: Path) -> Path:
    csv_content = """Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
9/12/2025,9/12/2025,9/15/2025,TSLA,TSLA 10/17/2025 Call $515.00,STO,1,$3.00,$300.00
9/22/2025,9/22/2025,9/24/2025,TSLA,TSLA 10/17/2025 Call $515.00,BTC,1,$7.00,($700.00)
9/22/2025,9/22/2025,9/24/2025,TSLA,TSLA 11/21/2025 Call $550.00,STO,1,$5.00,$500.00
10/8/2025,10/8/2025,10/10/2025,TSLA,TSLA 11/21/2025 Call $550.00,BTC,1,$6.00,($600.00)
"""
    csv_path = tmp_path / "trace.csv"
    csv_path.write_text(csv_content, encoding="utf-8")
    return csv_path


def _write_open_chain_csv(tmp_path: Path) -> Path:
    csv_content = """Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
9/12/2025,9/12/2025,9/15/2025,TSLA,TSLA 10/17/2025 Call $515.00,STO,1,$3.00,$300.00
9/22/2025,9/22/2025,9/24/2025,TSLA,TSLA 10/17/2025 Call $515.00,BTC,1,$7.00,($700.00)
9/22/2025,9/22/2025,9/24/2025,TSLA,TSLA 11/21/2025 Call $550.00,STO,1,$5.00,$500.00
"""
    csv_path = tmp_path / "open_chain.csv"
    csv_path.write_text(csv_content, encoding="utf-8")
    return csv_path


def test_trace_command_displays_matching_chain(tmp_path):
    """Trace command should print chain summary and transactions."""
    csv_path = _write_trace_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(trace, ['TSLA $550 Call', str(csv_path)])

    assert result.exit_code == 0
    output = result.output
    assert "Chain 1" in output
    assert "TSLA 10/17/2025 Call $515.00" in output
    assert "TSLA 11/21/2025 Call $550.00" in output
    assert "Net P&L (after fees):" in output


def test_trace_command_reports_when_no_match(tmp_path):
    """Trace command warns when no roll chains are found."""
    csv_path = _write_trace_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(trace, ['AAPL $150 Call', str(csv_path)])

    assert result.exit_code == 0
    assert "No roll chains found" in result.output


def test_trace_command_open_chain_shows_target_range(tmp_path):
    """Open chains should show realized P&L and target price guidance."""
    csv_path = _write_open_chain_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(trace, ['TSLA $550 Call', str(csv_path)])

    assert result.exit_code == 0
    output = result.output
    assert "Realized P&L (after fees):" in output
    assert "Target Price: $0.30 - $0.50" in output


def test_trace_command_allows_custom_target(tmp_path):
    """Custom target ranges adjust displayed target price."""
    csv_path = _write_open_chain_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(trace, ['TSLA $550 Call', str(csv_path), '--target', '0.2-0.4'])

    assert result.exit_code == 0
    output = result.output
    assert "Target Price: $0.60 - $0.80" in output


def test_trace_command_invalid_target(tmp_path):
    """Invalid target ranges should surface a Click error."""
    csv_path = _write_trace_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(trace, ['TSLA $550 Call', str(csv_path), '--target', 'invalid'])

    assert result.exit_code != 0
    assert "Invalid target range format" in result.output


def test_trace_command_missing_file():
    """Missing files should raise a Click error."""
    runner = CliRunner()

    result = runner.invoke(trace, ['TSLA $550 Call', 'nonexistent.csv'])

    assert result.exit_code != 0
    assert "Path 'nonexistent.csv' does not exist" in result.output
