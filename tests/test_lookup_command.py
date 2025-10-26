"""Tests for the lookup command module."""

from pathlib import Path

import pytest
from click.testing import CliRunner

from premiumflow.cli.lookup import lookup


def _write_sample_csv(tmp_path: Path) -> Path:
    csv_content = """Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
9/12/2025,9/12/2025,9/15/2025,TSLA,TSLA 10/17/2025 Call $515.00,STO,1,$3.00,$300.00
9/22/2025,9/22/2025,9/24/2025,TSLA,TSLA 10/17/2025 Call $515.00,BTC,1,$7.00,($700.00)
9/22/2025,9/22/2025,9/24/2025,TSLA,TSLA 11/21/2025 Call $550.00,STO,1,$5.00,$500.00
10/8/2025,10/8/2025,10/10/2025,TSLA,TSLA 11/21/2025 Call $550.00,BTC,1,$6.00,($600.00)
"""
    csv_path = tmp_path / "lookup.csv"
    csv_path.write_text(csv_content, encoding="utf-8")
    return csv_path


def test_lookup_command_finds_matches(tmp_path):
    """Lookup command displays matching transactions."""
    csv_path = _write_sample_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(lookup, ['TSLA $550 C 2025-11-21', '--file', str(csv_path)])

    assert result.exit_code == 0
    assert "Found 2 matching transactions" in result.output
    assert "TSLA 11/21/2025 Call $550.00" in result.output


def test_lookup_command_no_matches(tmp_path):
    """Lookup command reports when no matches are found."""
    csv_path = _write_sample_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(lookup, ['AAPL $150 P 2025-12-19', '--file', str(csv_path)])

    assert result.exit_code == 0
    assert "No transactions found for position" in result.output


def test_lookup_command_invalid_spec(tmp_path):
    """Invalid position specification raises Click error."""
    csv_path = _write_sample_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(lookup, ['invalid-spec', '--file', str(csv_path)])

    assert result.exit_code != 0
    assert "Invalid lookup format" in result.output


def test_lookup_command_missing_file():
    """Nonexistent CSV file results in Click error."""
    runner = CliRunner()

    result = runner.invoke(lookup, ['TSLA $550 C 2025-11-21', '--file', 'nonexistent.csv'])

    assert result.exit_code != 0
    assert "Path 'nonexistent.csv' does not exist" in result.output
