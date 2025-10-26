"""Tests for the import command module."""

import json
from pathlib import Path

from click.testing import CliRunner

from premiumflow.cli.ingest import import_transactions, ingest


def _write_sample_csv(tmp_path: Path) -> Path:
    csv_content = """Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
9/1/2025,9/1/2025,9/3/2025,TSLA,TSLA 10/17/2025 Call $515.00,STO,1,$3.00,$300.00
9/2/2025,9/2/2025,9/4/2025,AAPL,AAPL 10/17/2025 Put $150.00,BTO,1,$2.00,($200.00)
9/15/2025,9/15/2025,9/17/2025,AAPL,AAPL 10/17/2025 Put $150.00,STC,1,$1.00,$100.00
"""
    csv_path = tmp_path / "sample_ingest.csv"
    csv_path.write_text(csv_content, encoding="utf-8")
    return csv_path


def _write_open_positions_csv(tmp_path: Path) -> Path:
    csv_content = """Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
9/12/2025,9/12/2025,9/15/2025,TSLA,TSLA 10/17/2025 Call $515.00,STO,1,$3.00,$300.00
9/22/2025,9/22/2025,9/24/2025,TSLA,TSLA 10/17/2025 Call $515.00,BTC,1,$7.00,($700.00)
9/22/2025,9/22/2025,9/24/2025,TSLA,TSLA 11/21/2025 Call $550.00,STO,1,$5.00,$500.00
"""
    csv_path = tmp_path / "open_positions.csv"
    csv_path.write_text(csv_content, encoding="utf-8")
    return csv_path


def test_import_command_table_output(tmp_path):
    """Import command renders table output by default."""
    csv_path = _write_sample_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(import_transactions, ['--file', str(csv_path)])

    assert result.exit_code == 0
    assert "Importing" in result.output
    assert "Options Transactions" in result.output
    assert "TSLA" in result.output


def test_import_command_json_output(tmp_path):
    """JSON mode emits serialized payload."""
    csv_path = _write_sample_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(import_transactions, ['--file', str(csv_path), '--json-output'])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["source_file"] == str(csv_path)
    assert payload["filters"]["options_only"] is True
    assert payload["transactions"]  # Should include parsed transactions


def test_import_command_filters_by_ticker(tmp_path):
    """Ticker filter reports when no transactions exist."""
    csv_path = _write_sample_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(import_transactions, ['--file', str(csv_path), '--ticker', 'ZZZ'])

    assert result.exit_code == 0
    assert "No options transactions found for ticker ZZZ" in result.output


def test_import_command_open_only(tmp_path):
    """Open-only flag reports open positions count."""
    csv_path = _write_open_positions_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(import_transactions, ['--file', str(csv_path), '--open-only'])

    assert result.exit_code == 0
    assert "Open positions:" in result.output


def test_import_command_invalid_target_range(tmp_path):
    """Invalid target range raises Click error."""
    csv_path = _write_sample_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(import_transactions, ['--file', str(csv_path), '--target', 'invalid'])
    
    assert result.exit_code != 0
    assert "Invalid target range format" in result.output


def test_ingest_alias_warns(tmp_path):
    """Legacy ingest alias should emit deprecation warning."""
    csv_path = _write_sample_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(ingest, ['--file', str(csv_path)])

    assert result.exit_code == 0
    assert "deprecated" in (result.stderr or "").lower()
