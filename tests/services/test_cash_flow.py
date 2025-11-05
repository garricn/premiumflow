"""Tests for account-level cash flow and P&L reporting service."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from premiumflow.core.parser import NormalizedOptionTransaction, ParsedImportResult
from premiumflow.persistence import repository as repository_module
from premiumflow.persistence import storage as storage_module
from premiumflow.services.cash_flow import (
    generate_cash_flow_pnl_report,
)


def _make_transaction(**overrides) -> NormalizedOptionTransaction:
    """Convenience factory for normalized option transactions."""
    return NormalizedOptionTransaction(
        activity_date=overrides.get("activity_date", date(2025, 10, 7)),
        process_date=overrides.get("process_date", date(2025, 10, 7)),
        settle_date=overrides.get("settle_date", date(2025, 10, 8)),
        instrument=overrides.get("instrument", "TSLA"),
        description=overrides.get("description", "TSLA 10/25/2025 Call $200.00"),
        trans_code=overrides.get("trans_code", "STO"),
        quantity=overrides.get("quantity", 1),
        price=overrides.get("price", Decimal("3.00")),
        amount=overrides.get("amount", Decimal("300.00")),
        strike=overrides.get("strike", Decimal("200.00")),
        option_type=overrides.get("option_type", "CALL"),
        expiration=overrides.get("expiration", date(2025, 10, 25)),
        action=overrides.get("action", "SELL"),
        raw=overrides.get("raw", {"Activity Date": "10/07/2025"}),
    )


def _make_parsed(
    transactions: list[NormalizedOptionTransaction],
    *,
    account_name: str = "Primary Account",
    account_number: str | None = "ACCT-1",
) -> ParsedImportResult:
    return ParsedImportResult(
        account_name=account_name,
        account_number=account_number,
        transactions=transactions,
    )


def _seed_import(
    tmp_dir,
    *,
    account_name: str = "Primary Account",
    account_number: str | None = "ACCT-1",
    csv_name: str,
    transactions: list[NormalizedOptionTransaction],
    options_only: bool = True,
    ticker: str | None = "TSLA",
    strategy: str | None = "calls",
    open_only: bool = False,
) -> None:
    """Helper to seed an import into the database."""
    from premiumflow.persistence.storage import store_import_result

    csv_path = tmp_dir / csv_name
    csv_path.write_text(csv_name, encoding="utf-8")
    parsed = _make_parsed(transactions, account_name=account_name, account_number=account_number)
    store_import_result(
        parsed,
        source_path=str(csv_path),
        options_only=options_only,
        ticker=ticker,
        strategy=strategy,
        open_only=open_only,
    )


@pytest.fixture(autouse=True)
def clear_storage_cache():
    """Clear storage cache before and after each test."""
    storage_module.get_storage.cache_clear()
    yield
    storage_module.get_storage.cache_clear()


@pytest.fixture
def repository(tmp_path, monkeypatch):
    """Provide a SQLiteRepository backed by a temporary database."""
    db_path = tmp_path / "premiumflow.db"
    monkeypatch.setenv(storage_module.DB_ENV_VAR, str(db_path))
    storage_module.get_storage.cache_clear()
    return repository_module.SQLiteRepository()


def test_generate_report_empty_account(repository):
    """Test report generation for an account with no transactions."""
    report = generate_cash_flow_pnl_report(
        repository,
        account_name="Empty Account",
        account_number="ACCT-EMPTY",
    )

    assert report.account_name == "Empty Account"
    assert report.account_number == "ACCT-EMPTY"
    assert report.period_type == "total"
    assert len(report.periods) == 0
    assert report.totals.credits == Decimal("0")
    assert report.totals.debits == Decimal("0")
    assert report.totals.realized_pnl == Decimal("0")
    assert report.totals.unrealized_exposure == Decimal("0")


def test_generate_report_total_period_simple_flow(tmp_path, repository):
    """Test report generation with total period for a simple buy/sell flow."""
    _seed_import(
        tmp_path,
        csv_name="simple.csv",
        transactions=[
            _make_transaction(
                trans_code="STO",
                action="SELL",
                quantity=2,
                price=Decimal("3.00"),
                amount=Decimal("600.00"),
                activity_date=date(2025, 10, 7),
            ),
            _make_transaction(
                trans_code="BTC",
                action="BUY",
                quantity=1,
                price=Decimal("1.50"),
                amount=Decimal("-150.00"),
                activity_date=date(2025, 10, 15),
            ),
        ],
    )

    report = generate_cash_flow_pnl_report(
        repository,
        account_name="Primary Account",
        account_number="ACCT-1",
        period_type="total",
    )

    assert report.account_name == "Primary Account"
    assert report.account_number == "ACCT-1"
    assert report.period_type == "total"
    assert len(report.periods) == 1
    assert report.periods[0].period_key == "total"
    assert report.periods[0].credits == Decimal("600.00")
    assert report.periods[0].debits == Decimal("150.00")
    assert report.periods[0].net_cash_flow == Decimal("450.00")
    # Realized P&L should be calculated from matched legs
    # One lot closed: opened with 600, closed with 150, realized_pnl should be positive
    assert report.periods[0].realized_pnl > Decimal("0")
    # One lot still open (1 contract remaining)
    assert report.periods[0].unrealized_exposure > Decimal("0")


def test_generate_report_daily_period(tmp_path, repository):
    """Test report generation with daily period grouping."""
    _seed_import(
        tmp_path,
        csv_name="daily.csv",
        transactions=[
            _make_transaction(
                trans_code="STO",
                action="SELL",
                quantity=1,
                price=Decimal("3.00"),
                amount=Decimal("300.00"),
                activity_date=date(2025, 10, 7),
            ),
            _make_transaction(
                trans_code="STO",
                action="SELL",
                quantity=1,
                price=Decimal("2.50"),
                amount=Decimal("250.00"),
                activity_date=date(2025, 10, 8),
            ),
        ],
    )

    report = generate_cash_flow_pnl_report(
        repository,
        account_name="Primary Account",
        account_number="ACCT-1",
        period_type="daily",
    )

    assert report.period_type == "daily"
    assert len(report.periods) == 2

    # Check that periods are sorted and have correct dates
    period_dates = [p.period_key for p in report.periods]
    assert period_dates == ["2025-10-07", "2025-10-08"]

    # Check totals
    assert report.totals.credits == Decimal("550.00")
    assert report.totals.debits == Decimal("0")


def test_generate_report_weekly_period(tmp_path, repository):
    """Test report generation with weekly period grouping."""
    _seed_import(
        tmp_path,
        csv_name="weekly.csv",
        transactions=[
            _make_transaction(
                trans_code="STO",
                action="SELL",
                quantity=1,
                price=Decimal("3.00"),
                amount=Decimal("300.00"),
                activity_date=date(2025, 10, 7),  # Monday
            ),
            _make_transaction(
                trans_code="STO",
                action="SELL",
                quantity=1,
                price=Decimal("2.50"),
                amount=Decimal("250.00"),
                activity_date=date(2025, 10, 10),  # Thursday, same week
            ),
            _make_transaction(
                trans_code="STO",
                action="SELL",
                quantity=1,
                price=Decimal("2.00"),
                amount=Decimal("200.00"),
                activity_date=date(2025, 10, 14),  # Monday, different week
            ),
        ],
    )

    report = generate_cash_flow_pnl_report(
        repository,
        account_name="Primary Account",
        account_number="ACCT-1",
        period_type="weekly",
    )

    assert report.period_type == "weekly"
    # Should have 2 weeks
    assert len(report.periods) == 2
    assert report.totals.credits == Decimal("750.00")


def test_generate_report_monthly_period(tmp_path, repository):
    """Test report generation with monthly period grouping."""
    _seed_import(
        tmp_path,
        csv_name="monthly.csv",
        transactions=[
            _make_transaction(
                trans_code="STO",
                action="SELL",
                quantity=1,
                price=Decimal("3.00"),
                amount=Decimal("300.00"),
                activity_date=date(2025, 10, 7),
            ),
            _make_transaction(
                trans_code="STO",
                action="SELL",
                quantity=1,
                price=Decimal("2.50"),
                amount=Decimal("250.00"),
                activity_date=date(2025, 10, 15),
            ),
            _make_transaction(
                trans_code="STO",
                action="SELL",
                quantity=1,
                price=Decimal("2.00"),
                amount=Decimal("200.00"),
                activity_date=date(2025, 11, 5),
            ),
        ],
    )

    report = generate_cash_flow_pnl_report(
        repository,
        account_name="Primary Account",
        account_number="ACCT-1",
        period_type="monthly",
    )

    assert report.period_type == "monthly"
    # Should have 2 months
    assert len(report.periods) == 2
    assert report.totals.credits == Decimal("750.00")


def test_generate_report_filters_by_ticker(tmp_path, repository):
    """Test that report filters correctly by ticker."""
    _seed_import(
        tmp_path,
        csv_name="multi.csv",
        transactions=[
            _make_transaction(
                trans_code="STO",
                action="SELL",
                quantity=1,
                price=Decimal("3.00"),
                amount=Decimal("300.00"),
                instrument="TSLA",
                activity_date=date(2025, 10, 7),
            ),
            _make_transaction(
                trans_code="STO",
                action="SELL",
                quantity=1,
                price=Decimal("2.50"),
                amount=Decimal("250.00"),
                instrument="AAPL",
                activity_date=date(2025, 10, 8),
            ),
        ],
    )

    report = generate_cash_flow_pnl_report(
        repository,
        account_name="Primary Account",
        account_number="ACCT-1",
        ticker="TSLA",
    )

    assert report.totals.credits == Decimal("300.00")
    assert report.totals.debits == Decimal("0")


def test_generate_report_filters_by_dates(tmp_path, repository):
    """Test that report filters correctly by date range."""
    _seed_import(
        tmp_path,
        csv_name="dates.csv",
        transactions=[
            _make_transaction(
                trans_code="STO",
                action="SELL",
                quantity=1,
                price=Decimal("3.00"),
                amount=Decimal("300.00"),
                activity_date=date(2025, 10, 5),
            ),
            _make_transaction(
                trans_code="STO",
                action="SELL",
                quantity=1,
                price=Decimal("2.50"),
                amount=Decimal("250.00"),
                activity_date=date(2025, 10, 10),
            ),
            _make_transaction(
                trans_code="STO",
                action="SELL",
                quantity=1,
                price=Decimal("2.00"),
                amount=Decimal("200.00"),
                activity_date=date(2025, 10, 15),
            ),
        ],
    )

    report = generate_cash_flow_pnl_report(
        repository,
        account_name="Primary Account",
        account_number="ACCT-1",
        since=date(2025, 10, 7),
        until=date(2025, 10, 12),
    )

    # Should only include transactions on 10/10 (within range)
    assert report.totals.credits == Decimal("250.00")


def test_generate_report_realized_pnl_from_closed_lots(tmp_path, repository):
    """Test that realized P&L correctly aggregates from closed matched leg lots."""
    _seed_import(
        tmp_path,
        csv_name="realized.csv",
        transactions=[
            _make_transaction(
                trans_code="STO",
                action="SELL",
                quantity=2,
                price=Decimal("3.00"),
                amount=Decimal("600.00"),
                activity_date=date(2025, 10, 7),
            ),
            _make_transaction(
                trans_code="BTC",
                action="BUY",
                quantity=2,
                price=Decimal("1.00"),
                amount=Decimal("-200.00"),
                activity_date=date(2025, 10, 15),
            ),
        ],
    )

    report = generate_cash_flow_pnl_report(
        repository,
        account_name="Primary Account",
        account_number="ACCT-1",
        period_type="total",
    )

    # Realized P&L should be positive (sold for 600, bought back for 200)
    assert report.totals.realized_pnl > Decimal("0")
    # Unrealized should be 0 since all lots are closed
    assert report.totals.unrealized_exposure == Decimal("0")


def test_generate_report_unrealized_exposure_from_open_lots(tmp_path, repository):
    """Test that unrealized exposure correctly aggregates from open matched leg lots."""
    _seed_import(
        tmp_path,
        csv_name="unrealized.csv",
        transactions=[
            _make_transaction(
                trans_code="STO",
                action="SELL",
                quantity=2,
                price=Decimal("3.00"),
                amount=Decimal("600.00"),
                activity_date=date(2025, 10, 7),
            ),
            # No closing transaction, so lots remain open
        ],
    )

    report = generate_cash_flow_pnl_report(
        repository,
        account_name="Primary Account",
        account_number="ACCT-1",
        period_type="total",
    )

    # Unrealized exposure should be positive (credit remaining on open positions)
    assert report.totals.unrealized_exposure > Decimal("0")
    # Realized P&L should be 0 since no lots are closed
    assert report.totals.realized_pnl == Decimal("0")


def test_generate_report_multiple_accounts_isolation(tmp_path, repository):
    """Test that reports are correctly isolated by account."""
    _seed_import(
        tmp_path,
        account_name="Account A",
        account_number="ACCT-A",
        csv_name="account_a.csv",
        transactions=[
            _make_transaction(
                trans_code="STO",
                action="SELL",
                quantity=1,
                price=Decimal("3.00"),
                amount=Decimal("300.00"),
            ),
        ],
    )
    _seed_import(
        tmp_path,
        account_name="Account B",
        account_number="ACCT-B",
        csv_name="account_b.csv",
        transactions=[
            _make_transaction(
                trans_code="STO",
                action="SELL",
                quantity=1,
                price=Decimal("5.00"),
                amount=Decimal("500.00"),
            ),
        ],
    )

    report_a = generate_cash_flow_pnl_report(
        repository,
        account_name="Account A",
        account_number="ACCT-A",
    )
    report_b = generate_cash_flow_pnl_report(
        repository,
        account_name="Account B",
        account_number="ACCT-B",
    )

    assert report_a.totals.credits == Decimal("300.00")
    assert report_b.totals.credits == Decimal("500.00")


def test_generate_report_multiple_imports_aggregation(tmp_path, repository):
    """Test that reports correctly aggregate across multiple imports for the same account."""
    _seed_import(
        tmp_path,
        csv_name="import1.csv",
        transactions=[
            _make_transaction(
                trans_code="STO",
                action="SELL",
                quantity=1,
                price=Decimal("3.00"),
                amount=Decimal("300.00"),
                activity_date=date(2025, 10, 7),
            ),
        ],
    )
    _seed_import(
        tmp_path,
        csv_name="import2.csv",
        transactions=[
            _make_transaction(
                trans_code="STO",
                action="SELL",
                quantity=1,
                price=Decimal("2.50"),
                amount=Decimal("250.00"),
                activity_date=date(2025, 10, 8),
            ),
        ],
    )

    report = generate_cash_flow_pnl_report(
        repository,
        account_name="Primary Account",
        account_number="ACCT-1",
    )

    # Should aggregate across both imports
    assert report.totals.credits == Decimal("550.00")


def test_generate_report_pnl_includes_legs_opened_before_range(tmp_path, repository):
    """Test that realized P&L correctly includes positions opened before date range."""
    _seed_import(
        tmp_path,
        csv_name="cross_range.csv",
        transactions=[
            # Position opened before date range
            _make_transaction(
                trans_code="STO",
                action="SELL",
                quantity=2,
                price=Decimal("3.00"),
                amount=Decimal("600.00"),
                activity_date=date(2025, 10, 1),  # Before range
            ),
            # Position closed within date range
            _make_transaction(
                trans_code="BTC",
                action="BUY",
                quantity=2,
                price=Decimal("1.00"),
                amount=Decimal("-200.00"),
                activity_date=date(2025, 10, 15),  # Within range
            ),
        ],
    )

    report = generate_cash_flow_pnl_report(
        repository,
        account_name="Primary Account",
        account_number="ACCT-1",
        since=date(2025, 10, 7),  # Start after opening
        until=date(2025, 10, 20),
    )

    # Cash flow should only show closing transaction (debit) within range
    assert report.totals.credits == Decimal("0")
    assert report.totals.debits == Decimal("200.00")

    # Realized P&L should be calculated correctly (opened for 600, closed for 200)
    # Even though opening was before the date range
    assert report.totals.realized_pnl > Decimal("0")
    # Should be approximately 400 (600 - 200), minus fees
    assert report.totals.realized_pnl < Decimal("500")


def test_generate_report_unrealized_exposure_includes_positions_opened_before_range(
    tmp_path, repository
):
    """Test that unrealized exposure includes positions opened before date range."""
    _seed_import(
        tmp_path,
        csv_name="open_before_range.csv",
        transactions=[
            # Position opened before date range, still open
            _make_transaction(
                trans_code="STO",
                action="SELL",
                quantity=2,
                price=Decimal("3.00"),
                amount=Decimal("600.00"),
                activity_date=date(2025, 9, 25),  # Before range (September)
            ),
            # No closing transaction, so position remains open
        ],
    )

    report = generate_cash_flow_pnl_report(
        repository,
        account_name="Primary Account",
        account_number="ACCT-1",
        since=date(2025, 10, 7),  # October - start after opening
        until=date(2025, 10, 20),
    )

    # Cash flow should show no transactions (all were before the range)
    assert report.totals.credits == Decimal("0")
    assert report.totals.debits == Decimal("0")

    # Unrealized exposure should be included even though position was opened before range
    # The position is still open during the requested period, so exposure should be included
    assert report.totals.unrealized_exposure > Decimal("0")
    # Should be approximately 600 (credit remaining on open positions)
    assert report.totals.unrealized_exposure < Decimal("700")

    # Realized P&L should be 0 since no positions were closed
    assert report.totals.realized_pnl == Decimal("0")


def test_generate_report_clamps_periods_to_range(tmp_path, repository):
    """Test that periods are clamped to the date range when clamp_periods_to_range=True."""
    _seed_import(
        tmp_path,
        csv_name="clamp_test.csv",
        transactions=[
            # Position opened in September, still open
            _make_transaction(
                trans_code="STO",
                action="SELL",
                quantity=2,
                price=Decimal("3.00"),
                amount=Decimal("600.00"),
                activity_date=date(2025, 9, 15),  # September
            ),
            # Transaction in October
            _make_transaction(
                trans_code="STO",
                action="SELL",
                quantity=1,
                price=Decimal("2.00"),
                amount=Decimal("200.00"),
                activity_date=date(2025, 10, 10),  # October
            ),
        ],
    )

    # With clamping enabled (default) - should only show October periods
    report_clamped = generate_cash_flow_pnl_report(
        repository,
        account_name="Primary Account",
        account_number="ACCT-1",
        period_type="monthly",
        since=date(2025, 10, 1),  # October only
        until=date(2025, 10, 31),
        clamp_periods_to_range=True,
    )

    # Should only have October period (September exposure clamped to October)
    period_keys = [p.period_key for p in report_clamped.periods]
    assert "2025-09" not in period_keys  # September period should not appear
    assert "2025-10" in period_keys  # October period should appear

    # Unrealized exposure should be in October period (clamped)
    october_period = next(p for p in report_clamped.periods if p.period_key == "2025-10")
    assert october_period.unrealized_exposure > Decimal("0")

    # With clamping disabled - should show both September and October periods
    report_unclamped = generate_cash_flow_pnl_report(
        repository,
        account_name="Primary Account",
        account_number="ACCT-1",
        period_type="monthly",
        since=date(2025, 10, 1),
        until=date(2025, 10, 31),
        clamp_periods_to_range=False,
    )

    # Should have both September and October periods
    period_keys_unclamped = [p.period_key for p in report_unclamped.periods]
    assert "2025-09" in period_keys_unclamped  # September period should appear
    assert "2025-10" in period_keys_unclamped  # October period should appear

    # Unrealized exposure should be in September period (not clamped)
    september_period = next(p for p in report_unclamped.periods if p.period_key == "2025-09")
    assert september_period.unrealized_exposure > Decimal("0")

    # Totals should be the same in both cases
    assert report_clamped.totals.unrealized_exposure == report_unclamped.totals.unrealized_exposure
