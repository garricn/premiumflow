"""Tests for the import command module."""

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from premiumflow.cli.import_command import import_group
from premiumflow.core.parser import ImportValidationError, ParsedImportResult
from premiumflow.persistence.storage import DuplicateImportError, StoreResult


@pytest.fixture(autouse=True)
def _stub_store_import(monkeypatch):
    def _fake_store(*args, **kwargs):
        return StoreResult(import_id=1, status="inserted")

    monkeypatch.setattr("premiumflow.cli.import_command.store_import_result", _fake_store)


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


def _write_missing_price_csv(tmp_path: Path) -> Path:
    csv_content = """Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
9/1/2025,9/1/2025,9/3/2025,TSLA,TSLA 10/17/2025 Call $515.00,STO,1,,$300.00
"""
    csv_path = tmp_path / "missing_price.csv"
    csv_path.write_text(csv_content, encoding="utf-8")
    return csv_path


def _write_assignment_without_price_csv(tmp_path: Path) -> Path:
    csv_content = """Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
9/5/2025,9/5/2025,9/8/2025,HOOD,HOOD 9/5/2025 Call $104.00,OASGN,1,,
"""
    csv_path = tmp_path / "assignment_missing_price.csv"
    csv_path.write_text(csv_content, encoding="utf-8")
    return csv_path


def _write_unsorted_csv(tmp_path: Path) -> Path:
    csv_content = """Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
9/15/2025,9/15/2025,9/17/2025,AAPL,AAPL 10/17/2025 Put $150.00,STC,1,$1.00,$100.00
9/1/2025,9/1/2025,9/3/2025,TSLA,TSLA 10/17/2025 Call $515.00,STO,1,$3.00,$300.00
"""
    csv_path = tmp_path / "unsorted.csv"
    csv_path.write_text(csv_content, encoding="utf-8")
    return csv_path


def _write_non_option_row_csv(tmp_path: Path) -> Path:
    csv_content = """Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
9/22/2025,9/22/2025,9/23/2025,AMD,AMD CUSIP: 007903107,Sell,200,$161.66,$32,331.17
"""
    csv_path = tmp_path / "non_option.csv"
    csv_path.write_text(csv_content, encoding="utf-8")
    return csv_path


def test_import_command_table_output(tmp_path):
    """Import command renders table output by default."""
    csv_path = _write_sample_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(
        import_group,
        [
            "--file",
            str(csv_path),
            "--account-name",
            "Test Account",
            "--account-number",
            "ACCT-123",
        ],
    )

    assert result.exit_code == 0
    assert "Importing" in result.output
    assert "Options Transactions" in result.output
    assert "TSLA" in result.output
    assert "Account:" in result.output
    assert "Credit" not in result.output
    assert "Net Premium" not in result.output
    assert "Totals:" not in result.output
    assert "Fees" not in result.output
    assert "Reg Fee" not in result.output


def test_import_command_json_output(tmp_path):
    """JSON mode emits serialized payload."""
    csv_path = _write_sample_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(
        import_group,
        [
            "--file",
            str(csv_path),
            "--account-name",
            "Test Account",
            "--account-number",
            "ACCT-123",
            "--json-output",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["source_file"] == str(csv_path)
    assert payload["filters"]["options_only"] is True
    assert payload["account"]["name"] == "Test Account"
    assert payload["account"]["number"] == "ACCT-123"
    assert "cash_flow" not in payload
    first_txn = payload["transactions"][0]
    assert first_txn["instrument"] == "TSLA"
    assert first_txn["price"] == "3"
    assert first_txn["amount"] == "300"


def test_import_command_filters_by_ticker(tmp_path):
    """Ticker filter reports when no transactions exist."""
    csv_path = _write_sample_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(
        import_group,
        [
            "--file",
            str(csv_path),
            "--account-name",
            "Test Account",
            "--account-number",
            "ACCT-123",
            "--ticker",
            "ZZZ",
        ],
    )

    assert result.exit_code == 0
    assert "No options transactions found for ticker ZZZ" in result.output


def test_import_command_open_only(tmp_path):
    """Open-only flag reports open positions count."""
    csv_path = _write_open_positions_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(
        import_group,
        [
            "--file",
            str(csv_path),
            "--account-name",
            "Test Account",
            "--account-number",
            "ACCT-123",
            "--open-only",
        ],
    )

    assert result.exit_code == 0
    assert "Open positions:" in result.output


def test_import_command_rejects_target_option(tmp_path):
    """Deprecated target option should be rejected."""
    csv_path = _write_sample_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(
        import_group,
        [
            "--file",
            str(csv_path),
            "--account-name",
            "Test Account",
            "--account-number",
            "ACCT-123",
            "--target",
            "invalid",
        ],
    )

    assert result.exit_code != 0
    assert "No such option" in result.output


def test_import_command_requires_account_name(tmp_path):
    """Missing account name should fail before parsing."""
    csv_path = _write_sample_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(
        import_group,
        ["--file", str(csv_path), "--account-number", "ACCT-123"],
    )

    assert result.exit_code != 0
    assert "Missing option" in result.output or "--account-name is required" in result.output


def test_import_command_requires_account_number(tmp_path):
    """Missing account number should fail before parsing."""
    csv_path = _write_sample_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(
        import_group,
        [
            "--file",
            str(csv_path),
            "--account-name",
            "Test Account",
        ],
    )

    assert result.exit_code != 0
    assert "Missing option" in result.output or "--account-number is required" in result.output


def test_import_command_surfaces_parser_errors(monkeypatch, tmp_path):
    """Import validation failures should be reported to the CLI."""
    csv_path = _write_sample_csv(tmp_path)
    runner = CliRunner()

    def _fake_loader(*args, **kwargs):
        raise ImportValidationError("Account name required")

    monkeypatch.setattr("premiumflow.cli.import_command.load_option_transactions", _fake_loader)

    result = runner.invoke(
        import_group,
        [
            "--file",
            str(csv_path),
            "--account-name",
            "Test Account",
            "--account-number",
            "ACCT-123",
        ],
    )

    assert result.exit_code != 0
    assert "Account name required" in result.output


def test_import_command_passes_account_metadata(monkeypatch, tmp_path):
    """CLI should forward account metadata to the parser."""
    csv_path = _write_sample_csv(tmp_path)
    runner = CliRunner()
    captured = {}
    persistence_calls = {}

    def _fake_loader(csv_file, *, account_name, account_number):
        captured["csv_file"] = csv_file
        captured["account_name"] = account_name
        captured["account_number"] = account_number
        return ParsedImportResult(
            account_name=account_name,
            account_number=account_number,
            transactions=[],
        )

    monkeypatch.setattr("premiumflow.cli.import_command.load_option_transactions", _fake_loader)

    def _fake_store(parsed_result, **kwargs):
        persistence_calls["parsed"] = parsed_result
        persistence_calls["kwargs"] = kwargs
        return StoreResult(import_id=42, status="inserted")

    monkeypatch.setattr("premiumflow.cli.import_command.store_import_result", _fake_store)

    result = runner.invoke(
        import_group,
        [
            "--file",
            str(csv_path),
            "--account-name",
            "Primary",
            "--account-number",
            "ACCT-123",
            "--json-output",
        ],
    )

    assert result.exit_code == 0
    assert captured["csv_file"] == str(csv_path)
    assert captured["account_name"] == "Primary"
    assert captured["account_number"] == "ACCT-123"
    assert persistence_calls["parsed"].account_name == "Primary"
    kwargs = persistence_calls["kwargs"]
    assert kwargs["duplicate_strategy"] == "error"
    assert kwargs["source_path"] == str(csv_path)
    assert kwargs["options_only"] is True
    assert kwargs["ticker"] is None
    assert kwargs["strategy"] is None
    assert kwargs["open_only"] is False


def test_import_command_infers_price_from_amount(tmp_path):
    """Missing prices are derived from Amount values."""
    csv_path = _write_missing_price_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(
        import_group,
        [
            "--file",
            str(csv_path),
            "--account-name",
            "Test Account",
            "--account-number",
            "ACCT-123",
            "--json-output",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    first_txn = payload["transactions"][0]
    assert first_txn["price"] == "3"


def test_import_command_duplicate_error(monkeypatch, tmp_path):
    csv_path = _write_sample_csv(tmp_path)
    runner = CliRunner()

    def _fake_loader(csv_file, *, account_name, account_number):
        return ParsedImportResult(
            account_name=account_name, account_number=account_number, transactions=[]
        )

    monkeypatch.setattr("premiumflow.cli.import_command.load_option_transactions", _fake_loader)

    def _fake_store(*args, **kwargs):
        raise DuplicateImportError("Primary", "ACCT-123")

    monkeypatch.setattr("premiumflow.cli.import_command.store_import_result", _fake_store)

    result = runner.invoke(
        import_group,
        ["--file", str(csv_path), "--account-name", "Primary", "--account-number", "ACCT-123"],
    )

    assert result.exit_code != 0
    assert "already recorded" in result.output


def test_import_command_skip_existing(monkeypatch, tmp_path):
    csv_path = _write_sample_csv(tmp_path)
    runner = CliRunner()
    persistence_calls = {}

    def _fake_loader(csv_file, *, account_name, account_number):
        return ParsedImportResult(
            account_name=account_name, account_number=account_number, transactions=[]
        )

    monkeypatch.setattr("premiumflow.cli.import_command.load_option_transactions", _fake_loader)

    def _fake_store(parsed_result, **kwargs):
        persistence_calls.update(kwargs)
        return StoreResult(import_id=99, status="skipped")

    monkeypatch.setattr("premiumflow.cli.import_command.store_import_result", _fake_store)

    result = runner.invoke(
        import_group,
        [
            "--file",
            str(csv_path),
            "--account-name",
            "Primary",
            "--account-number",
            "ACCT-123",
            "--skip-existing",
        ],
    )

    assert result.exit_code == 0
    assert "Import already persisted" in result.output
    assert persistence_calls["duplicate_strategy"] == "skip"


def test_import_command_replace_existing(monkeypatch, tmp_path):
    csv_path = _write_sample_csv(tmp_path)
    runner = CliRunner()
    persistence_calls = {}

    def _fake_loader(csv_file, *, account_name, account_number):
        return ParsedImportResult(
            account_name=account_name, account_number=account_number, transactions=[]
        )

    monkeypatch.setattr("premiumflow.cli.import_command.load_option_transactions", _fake_loader)

    def _fake_store(parsed_result, **kwargs):
        persistence_calls.update(kwargs)
        return StoreResult(import_id=100, status="replaced")

    monkeypatch.setattr("premiumflow.cli.import_command.store_import_result", _fake_store)

    result = runner.invoke(
        import_group,
        [
            "--file",
            str(csv_path),
            "--account-name",
            "Primary",
            "--account-number",
            "ACCT-123",
            "--replace-existing",
        ],
    )

    assert result.exit_code == 0
    assert "Existing persisted import replaced with new data." in result.output
    assert persistence_calls["duplicate_strategy"] == "replace"


def test_import_command_allows_assignment_with_blank_price(tmp_path):
    """Assignments without price/amount default to zero price."""
    csv_path = _write_assignment_without_price_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(
        import_group,
        [
            "--file",
            str(csv_path),
            "--account-name",
            "Test Account",
            "--account-number",
            "ACCT-123",
            "--json-output",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    first_txn = payload["transactions"][0]
    assert first_txn["price"] == "0"


def test_import_skips_rows_without_option_trans_code(tmp_path):
    """Non-option rows with blank or unexpected codes are ignored."""
    csv_content = """Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
9/1/2025,9/1/2025,9/3/2025,TSLA,TSLA 10/17/2025 Call $515.00,STO,1,$3.00,$300.00
9/2/2025,9/2/2025,9/4/2025,AMD,AMD Common Stock,,100,$110.00,$11,000.00
"""
    csv_path = tmp_path / "mixed_rows.csv"
    csv_path.write_text(csv_content, encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        import_group,
        [
            "--file",
            str(csv_path),
            "--account-name",
            "Test Account",
            "--account-number",
            "ACCT-123",
            "--json-output",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert len(payload["transactions"]) == 1
    assert payload["transactions"][0]["instrument"] == "TSLA"


def test_import_command_sorts_transactions_by_date(tmp_path):
    """Transactions are sorted chronologically before computing cash flows."""
    csv_path = _write_unsorted_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(
        import_group,
        [
            "--file",
            str(csv_path),
            "--account-name",
            "Test Account",
            "--account-number",
            "ACCT-123",
            "--json-output",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    dates = [txn["activity_date"] for txn in payload["transactions"]]
    assert dates == sorted(dates)
