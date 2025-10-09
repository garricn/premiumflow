"""CLI integration tests for rollchain commands."""

from click.testing import CliRunner

from rollchain.cli.commands import main as rollchain_cli


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


def test_ingest_filters_by_ticker(tmp_path):
    """--ticker should limit output to the requested symbol."""
    sample_csv = _write_sample_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(rollchain_cli, ['ingest', '--ticker', 'TMC', str(sample_csv)])

    assert result.exit_code == 0
    output = result.output
    assert "TMC" in output
    assert "PLTR" not in output
    assert "Filtered to 1 TMC options transactions" in output


def test_ingest_reports_missing_ticker(tmp_path):
    """--ticker should report when no transactions exist for the symbol."""
    sample_csv = _write_sample_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(rollchain_cli, ['ingest', '--ticker', 'ZZZ', str(sample_csv)])

    assert result.exit_code == 0
    assert "No options transactions found for ticker ZZZ" in result.output


def test_ingest_calls_only(tmp_path):
    """--calls-only filters out puts."""
    sample_csv = _write_sample_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(rollchain_cli, ['ingest', '--ticker', 'PLTR', '--calls-only', str(sample_csv)])

    assert result.exit_code == 0
    output = result.output
    assert "PLTR 11/21/2025 Call" in output
    assert "PLTR 11/21/2025 Put" not in output


def test_ingest_puts_only(tmp_path):
    """--puts-only filters out calls."""
    sample_csv = _write_sample_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(rollchain_cli, ['ingest', '--ticker', 'PLTR', '--puts-only', str(sample_csv)])

    assert result.exit_code == 0
    output = result.output
    assert "PLTR 11/21/2025 Put" in output
    assert "PLTR 11/21/2025 Call" not in output


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
