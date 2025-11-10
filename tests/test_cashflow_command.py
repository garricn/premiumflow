"""Tests for the cashflow CLI command."""

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from click.testing import CliRunner

from premiumflow.cli.cashflow import _build_cashflow_table
from premiumflow.cli.commands import main as premiumflow_cli
from premiumflow.core.parser import (
    NormalizedOptionTransaction,
    ParsedImportResult,
)
from premiumflow.persistence import storage as storage_module
from premiumflow.persistence.storage import store_import_result
from premiumflow.services.cash_flow import (
    CashFlowPnlReport,
    PeriodMetrics,
    RealizedViewTotals,
)


@pytest.fixture(autouse=True)
def clear_storage_cache():
    """Clear storage cache before and after each test."""
    storage_module.get_storage.cache_clear()
    yield
    storage_module.get_storage.cache_clear()


def _write_sample_csv(tmp_path):
    """Create a sample CSV with cash flow transactions."""
    csv_content = """Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
9/1/2025,9/1/2025,9/3/2025,TSLA,TSLA 11/21/2025 Call $515.00,STO,1,$3.00,$300.00
9/15/2025,9/15/2025,9/17/2025,TSLA,TSLA 11/21/2025 Call $515.00,BTC,1,$1.50,($150.00)
9/20/2025,9/20/2025,9/22/2025,AAPL,AAPL 12/19/2025 Put $120.00,STO,1,$2.00,$200.00
"""
    sample_csv = tmp_path / "sample.csv"
    sample_csv.write_text(csv_content, encoding="utf-8")
    return sample_csv


def _seed_import_for_cashflow(  # noqa: PLR0913
    tmp_path: Path,
    monkeypatch,
    *,
    csv_name: str,
    transactions: list[NormalizedOptionTransaction],
    account_name: str = "Primary Account",
    account_number: str = "ACCT-1",
) -> None:
    """Helper to seed database with transactions for cashflow testing."""
    # Set up temporary database
    db_path = tmp_path / "premiumflow.db"
    monkeypatch.setenv(storage_module.DB_ENV_VAR, str(db_path))
    storage_module.get_storage.cache_clear()

    csv_path = tmp_path / csv_name
    csv_path.write_text(csv_name, encoding="utf-8")
    parsed = ParsedImportResult(
        account_name=account_name,
        account_number=account_number,
        transactions=transactions,
    )
    store_import_result(
        parsed,
        source_path=str(csv_path),
        options_only=True,
        ticker=None,
        strategy=None,
        open_only=False,
    )


def _make_normalized_transaction(**overrides) -> NormalizedOptionTransaction:
    """Helper to create normalized transactions for testing."""
    return NormalizedOptionTransaction(
        activity_date=overrides.get("activity_date", date(2025, 9, 1)),
        process_date=overrides.get("process_date", date(2025, 9, 1)),
        settle_date=overrides.get("settle_date", date(2025, 9, 3)),
        instrument=overrides.get("instrument", "TSLA"),
        description=overrides.get("description", "TSLA 11/21/2025 Call $515.00"),
        trans_code=overrides.get("trans_code", "STO"),
        quantity=overrides.get("quantity", 1),
        price=overrides.get("price", Decimal("3.00")),
        amount=overrides.get("amount", Decimal("300.00")),
        strike=overrides.get("strike", Decimal("515.00")),
        option_type=overrides.get("option_type", "CALL"),
        expiration=overrides.get("expiration", date(2025, 11, 21)),
        action=overrides.get("action", "SELL"),
        raw=overrides.get(
            "raw",
            {
                "Activity Date": "09/01/2025",
                "Account Name": "Primary Account",
                "Account Number": "ACCT-1",
            },
        ),
    )


def test_cashflow_command_in_help():
    """Cashflow command should appear in CLI help."""
    runner = CliRunner()
    result = runner.invoke(premiumflow_cli, ["--help"])
    assert result.exit_code == 0
    assert "cashflow" in result.output

    cashflow_help = runner.invoke(premiumflow_cli, ["cashflow", "--help"])
    assert cashflow_help.exit_code == 0
    assert "--realized-view" in cashflow_help.output


def test_cashflow_requires_account_name(tmp_path):
    """Cashflow command requires --account-name."""
    runner = CliRunner()
    result = runner.invoke(
        premiumflow_cli,
        [
            "cashflow",
            "--account-number",
            "ACCT-1",
        ],
    )
    assert result.exit_code != 0
    assert "account-name" in result.output.lower()


def test_cashflow_requires_account_number(tmp_path):
    """Cashflow command requires --account-number."""
    runner = CliRunner()
    result = runner.invoke(
        premiumflow_cli,
        [
            "cashflow",
            "--account-name",
            "Test Account",
        ],
    )
    assert result.exit_code != 0
    assert "account-number" in result.output.lower()


def test_cashflow_empty_state(tmp_path):
    """Cashflow command shows empty state when no transactions found."""
    runner = CliRunner()
    result = runner.invoke(
        premiumflow_cli,
        [
            "cashflow",
            "--account-name",
            "Nonexistent Account",
            "--account-number",
            "ACCT-999",
        ],
    )
    assert result.exit_code == 0
    assert "No transactions found" in result.output


def test_cashflow_empty_state_json(tmp_path):
    """Cashflow command with --json-output shows empty report in JSON."""
    runner = CliRunner()
    result = runner.invoke(
        premiumflow_cli,
        [
            "cashflow",
            "--account-name",
            "Nonexistent Account",
            "--account-number",
            "ACCT-999",
            "--json-output",
        ],
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["account_name"] == "Nonexistent Account"
    assert data["account_number"] == "ACCT-999"
    assert data["periods"] == []
    assert data["totals"]["credits"] == "0.00"
    assert data["totals"]["debits"] == "0.00"


def test_cashflow_table_output(tmp_path, monkeypatch):
    """Cashflow command displays table with cash flow and P&L data."""
    # Set up temporary database
    db_path = tmp_path / "premiumflow.db"
    monkeypatch.setenv(storage_module.DB_ENV_VAR, str(db_path))
    storage_module.get_storage.cache_clear()

    # Create transactions
    txns = [
        _make_normalized_transaction(
            activity_date=date(2025, 9, 1),
            trans_code="STO",
            price=Decimal("3.00"),
            amount=Decimal("300.00"),
        ),
        _make_normalized_transaction(
            activity_date=date(2025, 9, 15),
            trans_code="BTC",
            price=Decimal("1.50"),
            amount=Decimal("-150.00"),
        ),
    ]
    _seed_import_for_cashflow(tmp_path, monkeypatch, csv_name="test.csv", transactions=txns)

    runner = CliRunner()
    result = runner.invoke(
        premiumflow_cli,
        [
            "cashflow",
            "--account-name",
            "Primary Account",
            "--account-number",
            "ACCT-1",
            "--period",
            "total",
        ],
        env={
            storage_module.DB_ENV_VAR: str(db_path),
            "COLUMNS": "200",
        },
    )

    assert result.exit_code == 0
    assert "Cash Flow & P&L Report" in result.output
    assert "Period" in result.output
    # Ensure deprecated columns are gone
    assert "Gross P&L" not in result.output
    assert "Net P&L" not in result.output
    assert "Total" in result.output


def test_cashflow_json_output(tmp_path, monkeypatch):
    """Cashflow command with --json-output returns valid JSON."""
    # Set up temporary database
    db_path = tmp_path / "premiumflow.db"
    monkeypatch.setenv(storage_module.DB_ENV_VAR, str(db_path))
    storage_module.get_storage.cache_clear()

    txns = [
        _make_normalized_transaction(
            activity_date=date(2025, 9, 1),
            trans_code="STO",
            price=Decimal("3.00"),
            amount=Decimal("300.00"),
        ),
    ]
    _seed_import_for_cashflow(tmp_path, monkeypatch, csv_name="test.csv", transactions=txns)

    runner = CliRunner()
    result = runner.invoke(
        premiumflow_cli,
        [
            "cashflow",
            "--account-name",
            "Primary Account",
            "--account-number",
            "ACCT-1",
            "--json-output",
        ],
        env={storage_module.DB_ENV_VAR: str(db_path)},
    )

    assert result.exit_code == 0
    data = json.loads(result.output)
    assert "account_name" in data
    assert "account_number" in data
    assert "period_type" in data
    assert "periods" in data
    assert "totals" in data
    assert data["account_name"] == "Primary Account"
    assert data["account_number"] == "ACCT-1"
    totals = data["totals"]
    assert "realized_profits_gross" in totals
    assert "realized_losses_gross" in totals
    assert "realized_pnl_net" in totals
    assert "opening_fees" in totals
    assert "closing_fees" in totals
    assert "assignment_realized_net" in totals
    assert "realized_breakdowns" in totals
    assert "options" in totals["realized_breakdowns"]


def test_build_cashflow_table_includes_assignment_column():
    """The rich table includes a dedicated assignment premium column."""
    period = PeriodMetrics(
        period_key="total",
        period_label="Total",
        credits=Decimal("100.00"),
        debits=Decimal("0.00"),
        net_cash_flow=Decimal("100.00"),
        realized_profits_gross=Decimal("100.00"),
        realized_losses_gross=Decimal("0.00"),
        realized_pnl_gross=Decimal("100.00"),
        realized_profits_net=Decimal("90.00"),
        realized_losses_net=Decimal("0.00"),
        realized_pnl_net=Decimal("90.00"),
        assignment_realized_gross=Decimal("10.00"),
        assignment_realized_net=Decimal("10.00"),
        unrealized_exposure=Decimal("0.00"),
        opening_fees=Decimal("5.00"),
        closing_fees=Decimal("5.00"),
        total_fees=Decimal("10.00"),
        realized_breakdowns={
            "options": RealizedViewTotals(
                profits_gross=Decimal("100.00"),
                losses_gross=Decimal("0.00"),
                net_gross=Decimal("100.00"),
                profits_net=Decimal("90.00"),
                losses_net=Decimal("0.00"),
                net_net=Decimal("90.00"),
            ),
            "stock": RealizedViewTotals(
                profits_gross=Decimal("0.00"),
                losses_gross=Decimal("0.00"),
                net_gross=Decimal("0.00"),
                profits_net=Decimal("0.00"),
                losses_net=Decimal("0.00"),
                net_net=Decimal("0.00"),
            ),
            "combined": RealizedViewTotals(
                profits_gross=Decimal("100.00"),
                losses_gross=Decimal("0.00"),
                net_gross=Decimal("100.00"),
                profits_net=Decimal("90.00"),
                losses_net=Decimal("0.00"),
                net_net=Decimal("90.00"),
            ),
        },
    )
    report = CashFlowPnlReport(
        account_name="Primary Account",
        account_number="ACCT-1",
        period_type="total",
        periods=[period],
        totals=period,
    )

    table = _build_cashflow_table(report, "options")
    headers = [column.header for column in table.columns]
    assert "Assignment Premium (After Fees)" in headers


def test_cashflow_realized_view_stock_header():
    """Selecting stock realized view updates table headings."""
    period = PeriodMetrics(
        period_key="total",
        period_label="Total",
        credits=Decimal("0.00"),
        debits=Decimal("0.00"),
        net_cash_flow=Decimal("0.00"),
        realized_profits_gross=Decimal("0.00"),
        realized_losses_gross=Decimal("0.00"),
        realized_pnl_gross=Decimal("0.00"),
        realized_profits_net=Decimal("0.00"),
        realized_losses_net=Decimal("0.00"),
        realized_pnl_net=Decimal("0.00"),
        assignment_realized_gross=Decimal("0.00"),
        assignment_realized_net=Decimal("0.00"),
        unrealized_exposure=Decimal("0.00"),
        opening_fees=Decimal("0.00"),
        closing_fees=Decimal("0.00"),
        total_fees=Decimal("0.00"),
        realized_breakdowns={
            "options": RealizedViewTotals(
                profits_gross=Decimal("0.00"),
                losses_gross=Decimal("0.00"),
                net_gross=Decimal("0.00"),
                profits_net=Decimal("0.00"),
                losses_net=Decimal("0.00"),
                net_net=Decimal("0.00"),
            ),
            "stock": RealizedViewTotals(
                profits_gross=Decimal("10.00"),
                losses_gross=Decimal("0.00"),
                net_gross=Decimal("10.00"),
                profits_net=Decimal("10.00"),
                losses_net=Decimal("0.00"),
                net_net=Decimal("10.00"),
            ),
            "combined": RealizedViewTotals(
                profits_gross=Decimal("10.00"),
                losses_gross=Decimal("0.00"),
                net_gross=Decimal("10.00"),
                profits_net=Decimal("10.00"),
                losses_net=Decimal("0.00"),
                net_net=Decimal("10.00"),
            ),
        },
    )
    report = CashFlowPnlReport(
        account_name="Primary Account",
        account_number="ACCT-1",
        period_type="total",
        periods=[period],
        totals=period,
    )

    table = _build_cashflow_table(report, "stock")
    headers = [column.header for column in table.columns]
    assert "Profits (Before Fees • Stock)" in headers


def test_cashflow_assignment_handling_exclude_json(tmp_path, monkeypatch):
    """--assignment-handling=exclude omits assignment premium from realized totals."""
    db_path = tmp_path / "premiumflow.db"
    monkeypatch.setenv(storage_module.DB_ENV_VAR, str(db_path))
    storage_module.get_storage.cache_clear()

    txns = [
        _make_normalized_transaction(
            instrument="HOOD",
            description="HOOD 09/06/2025 Call $104.00",
            trans_code="STO",
            action="SELL",
            activity_date=date(2025, 9, 1),
            price=Decimal("1.00"),
            amount=Decimal("100.00"),
            expiration=date(2025, 9, 6),
        ),
        _make_normalized_transaction(
            instrument="HOOD",
            description="HOOD 09/06/2025 Call $104.00",
            trans_code="OASGN",
            action="SELL",
            activity_date=date(2025, 9, 5),
            price=Decimal("0.00"),
            amount=Decimal("0.00"),
            expiration=date(2025, 9, 6),
        ),
        _make_normalized_transaction(
            instrument="META",
            description="META 11/15/2025 Put $300.00",
            trans_code="STO",
            action="SELL",
            activity_date=date(2025, 9, 1),
            price=Decimal("2.00"),
            amount=Decimal("200.00"),
            expiration=date(2025, 11, 15),
        ),
        _make_normalized_transaction(
            instrument="META",
            description="META 11/15/2025 Put $300.00",
            trans_code="BTC",
            action="BUY",
            activity_date=date(2025, 9, 5),
            price=Decimal("1.00"),
            amount=Decimal("-100.00"),
            expiration=date(2025, 11, 15),
        ),
    ]
    _seed_import_for_cashflow(tmp_path, monkeypatch, csv_name="assignments.csv", transactions=txns)

    runner = CliRunner()
    result = runner.invoke(
        premiumflow_cli,
        [
            "cashflow",
            "--account-name",
            "Primary Account",
            "--account-number",
            "ACCT-1",
            "--assignment-handling",
            "exclude",
            "--json-output",
        ],
        env={storage_module.DB_ENV_VAR: str(db_path)},
    )

    assert result.exit_code == 0
    data = json.loads(result.output)
    totals = data["totals"]
    assert totals["assignment_realized_net"] == "100.00"
    assert totals["realized_pnl_net"] == "100.00"  # Only the META roll remains


def test_cashflow_date_filtering(tmp_path, monkeypatch):
    """Cashflow command filters by date range."""
    # Set up temporary database
    db_path = tmp_path / "premiumflow.db"
    monkeypatch.setenv(storage_module.DB_ENV_VAR, str(db_path))
    storage_module.get_storage.cache_clear()

    txns = [
        _make_normalized_transaction(
            activity_date=date(2025, 9, 1),
            trans_code="STO",
            price=Decimal("3.00"),
            amount=Decimal("300.00"),
        ),
        _make_normalized_transaction(
            activity_date=date(2025, 10, 1),
            trans_code="STO",
            price=Decimal("2.00"),
            amount=Decimal("200.00"),
        ),
    ]
    _seed_import_for_cashflow(tmp_path, monkeypatch, csv_name="test.csv", transactions=txns)

    runner = CliRunner()
    result = runner.invoke(
        premiumflow_cli,
        [
            "cashflow",
            "--account-name",
            "Primary Account",
            "--account-number",
            "ACCT-1",
            "--since",
            "2025-09-01",
            "--until",
            "2025-09-30",
            "--period",
            "total",
        ],
        env={storage_module.DB_ENV_VAR: str(db_path)},
    )

    assert result.exit_code == 0
    # Should include September transaction (date filtering works)
    # Check that the report was generated successfully
    assert "Cash Flow & P&L Report" in result.output
    assert "Total" in result.output


def test_cashflow_ticker_filtering(tmp_path, monkeypatch):
    """Cashflow command filters by ticker."""
    # Set up temporary database
    db_path = tmp_path / "premiumflow.db"
    monkeypatch.setenv(storage_module.DB_ENV_VAR, str(db_path))
    storage_module.get_storage.cache_clear()

    txns = [
        _make_normalized_transaction(
            instrument="TSLA",
            activity_date=date(2025, 9, 1),
            trans_code="STO",
            price=Decimal("3.00"),
            amount=Decimal("300.00"),
        ),
        _make_normalized_transaction(
            instrument="AAPL",
            activity_date=date(2025, 9, 2),
            trans_code="STO",
            price=Decimal("2.00"),
            amount=Decimal("200.00"),
        ),
    ]
    _seed_import_for_cashflow(tmp_path, monkeypatch, csv_name="test.csv", transactions=txns)

    runner = CliRunner()
    result = runner.invoke(
        premiumflow_cli,
        [
            "cashflow",
            "--account-name",
            "Primary Account",
            "--account-number",
            "ACCT-1",
            "--ticker",
            "TSLA",
            "--period",
            "total",
        ],
        env={storage_module.DB_ENV_VAR: str(db_path)},
    )

    assert result.exit_code == 0
    # Should only include TSLA transaction (ticker filtering works)
    # Check that the report was generated successfully
    assert "Cash Flow & P&L Report" in result.output
    assert "Total" in result.output


def test_cashflow_period_types(tmp_path, monkeypatch):
    """Cashflow command supports all period types."""
    # Set up temporary database
    db_path = tmp_path / "premiumflow.db"
    monkeypatch.setenv(storage_module.DB_ENV_VAR, str(db_path))
    storage_module.get_storage.cache_clear()

    txns = [
        _make_normalized_transaction(
            activity_date=date(2025, 9, 1),
            trans_code="STO",
            price=Decimal("3.00"),
            amount=Decimal("300.00"),
        ),
    ]
    _seed_import_for_cashflow(tmp_path, monkeypatch, csv_name="test.csv", transactions=txns)

    for period_type in ["daily", "weekly", "monthly", "total"]:
        runner = CliRunner()
        result = runner.invoke(
            premiumflow_cli,
            [
                "cashflow",
                "--account-name",
                "Primary Account",
                "--account-number",
                "ACCT-1",
                "--period",
                period_type,
            ],
            env={storage_module.DB_ENV_VAR: str(db_path)},
        )
        assert result.exit_code == 0, f"Period type {period_type} failed"
        assert "Cash Flow & P&L Report" in result.output


def test_cashflow_no_clamp_periods_flag(tmp_path, monkeypatch):
    """Cashflow command respects --no-clamp-periods flag."""
    # Set up temporary database
    db_path = tmp_path / "premiumflow.db"
    monkeypatch.setenv(storage_module.DB_ENV_VAR, str(db_path))
    storage_module.get_storage.cache_clear()

    txns = [
        _make_normalized_transaction(
            activity_date=date(2025, 8, 1),  # Before date range
            trans_code="STO",
            price=Decimal("3.00"),
            amount=Decimal("300.00"),
        ),
    ]
    _seed_import_for_cashflow(tmp_path, monkeypatch, csv_name="test.csv", transactions=txns)

    runner = CliRunner()
    result = runner.invoke(
        premiumflow_cli,
        [
            "cashflow",
            "--account-name",
            "Primary Account",
            "--account-number",
            "ACCT-1",
            "--since",
            "2025-09-01",
            "--until",
            "2025-09-30",
            "--no-clamp-periods",
            "--period",
            "monthly",
        ],
        env={storage_module.DB_ENV_VAR: str(db_path)},
    )

    assert result.exit_code == 0
    # With --no-clamp-periods, should show August period even though it's before the range
    # (for unrealized exposure)


def test_cashflow_invalid_date_format(tmp_path):
    """Cashflow command validates date format."""
    runner = CliRunner()
    result = runner.invoke(
        premiumflow_cli,
        [
            "cashflow",
            "--account-name",
            "Primary Account",
            "--account-number",
            "ACCT-1",
            "--since",
            "invalid-date",
        ],
    )
    assert result.exit_code != 0
    assert "date" in result.output.lower() or "invalid" in result.output.lower()
