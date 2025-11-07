"""Tests for the read/query helpers over the persistence layer."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Optional

import pytest

from premiumflow.core.parser import (
    NormalizedOptionTransaction,
    NormalizedStockTransaction,
    ParsedImportResult,
)
from premiumflow.persistence import repository as repository_module
from premiumflow.persistence import storage as storage_module
from premiumflow.persistence.storage import store_import_result


@pytest.fixture(autouse=True)
def clear_storage_cache():
    storage_module.get_storage.cache_clear()
    yield
    storage_module.get_storage.cache_clear()


def _make_transaction(**overrides) -> NormalizedOptionTransaction:
    return NormalizedOptionTransaction(
        activity_date=overrides.get("activity_date", date(2025, 9, 1)),
        process_date=overrides.get("process_date", date(2025, 9, 1)),
        settle_date=overrides.get("settle_date", date(2025, 9, 3)),
        instrument=overrides.get("instrument", "TSLA"),
        description=overrides.get("description", "TSLA 10/17/2025 Call $515.00"),
        trans_code=overrides.get("trans_code", "STO"),
        quantity=overrides.get("quantity", 1),
        price=overrides.get("price", Decimal("3.00")),
        amount=overrides.get("amount", Decimal("300.00")),
        strike=overrides.get("strike", Decimal("515.00")),
        option_type=overrides.get("option_type", "CALL"),
        expiration=overrides.get("expiration", date(2025, 10, 17)),
        action=overrides.get("action", "SELL"),
        raw=overrides.get("raw", {"Activity Date": "09/01/2025"}),
    )


def _make_stock_transaction(**overrides) -> NormalizedStockTransaction:
    return NormalizedStockTransaction(
        activity_date=overrides.get("activity_date", date(2025, 9, 1)),
        process_date=overrides.get("process_date", date(2025, 9, 1)),
        settle_date=overrides.get("settle_date", date(2025, 9, 3)),
        instrument=overrides.get("instrument", "HOOD"),
        description=overrides.get("description", "Robinhood Markets"),
        trans_code=overrides.get("trans_code", "BUY"),
        quantity=overrides.get("quantity", Decimal("100")),
        price=overrides.get("price", Decimal("100.00")),
        amount=overrides.get("amount", Decimal("-10000.00")),
        action=overrides.get("action", "BUY"),
        raw=overrides.get("raw", {"Activity Date": "09/01/2025"}),
    )


def _make_parsed(
    transactions: list[NormalizedOptionTransaction],
    *,
    stock_transactions: Optional[list[NormalizedStockTransaction]] = None,
    **overrides,
) -> ParsedImportResult:
    return ParsedImportResult(
        account_name=overrides.get("account_name", "Primary Account"),
        account_number=overrides.get("account_number", "ACCT-1"),
        transactions=transactions,
        stock_transactions=stock_transactions or [],
    )


def _seed_import(
    tmp_dir: Path,
    *,
    account_name: str = "Primary Account",
    account_number: str | None = "ACCT-1",
    csv_name: str,
    transactions: list[NormalizedOptionTransaction],
    stock_transactions: Optional[list[NormalizedStockTransaction]] = None,
    options_only: bool = True,
    ticker: str | None = "TSLA",
    strategy: str | None = "calls",
    open_only: bool = False,
) -> None:
    csv_path = tmp_dir / csv_name
    csv_path.write_text(csv_name, encoding="utf-8")
    parsed = _make_parsed(
        transactions,
        stock_transactions=stock_transactions,
        account_name=account_name,
        account_number=account_number,
    )
    store_import_result(
        parsed,
        source_path=str(csv_path),
        options_only=options_only,
        ticker=ticker,
        strategy=strategy,
        open_only=open_only,
    )


@pytest.fixture
def repository(tmp_path, monkeypatch):
    db_path = tmp_path / "premiumflow.db"
    monkeypatch.setenv(storage_module.DB_ENV_VAR, str(db_path))
    return repository_module.SQLiteRepository()


def test_list_imports_returns_joined_records(tmp_path, repository):
    _seed_import(
        tmp_path,
        csv_name="one.csv",
        transactions=[_make_transaction(instrument="TSLA")],
        ticker="TSLA",
        strategy="calls",
        open_only=False,
    )
    _seed_import(
        tmp_path,
        csv_name="two.csv",
        transactions=[_make_transaction(instrument="AAPL")],
        ticker="AAPL",
        strategy="puts",
        open_only=True,
    )

    imports = repository.list_imports()
    assert len(imports) == 2

    latest = imports[0]
    assert latest.source_path.endswith("two.csv")
    assert latest.account_name == "Primary Account"
    assert latest.account_number == "ACCT-1"
    assert latest.ticker == "AAPL"
    assert latest.open_only is True


def test_list_imports_filters_by_account(tmp_path, repository):
    _seed_import(
        tmp_path,
        account_name="Primary Account",
        account_number="ACCT-1",
        csv_name="one.csv",
        transactions=[_make_transaction(instrument="TSLA")],
        ticker="TSLA",
    )
    _seed_import(
        tmp_path,
        account_name="Second Account",
        account_number="ACCT-2",
        csv_name="two.csv",
        transactions=[_make_transaction(instrument="AAPL")],
        ticker="AAPL",
    )

    imports = repository.list_imports(account_name="Second Account")
    assert len(imports) == 1
    assert imports[0].source_path.endswith("two.csv")
    assert imports[0].ticker == "AAPL"


def test_fetch_transactions_filters_by_ticker_and_dates(tmp_path, repository):
    _seed_import(
        tmp_path,
        csv_name="one.csv",
        transactions=[
            _make_transaction(instrument="TSLA", activity_date=date(2025, 9, 1)),
            _make_transaction(instrument="TSLA", activity_date=date(2025, 9, 2)),
        ],
        ticker="TSLA",
        open_only=False,
    )
    _seed_import(
        tmp_path,
        csv_name="two.csv",
        transactions=[_make_transaction(instrument="AAPL", activity_date=date(2025, 9, 3))],
        ticker="AAPL",
        open_only=False,
    )

    transactions = repository.fetch_transactions(ticker="TSLA")
    assert [txn.instrument for txn in transactions] == ["TSLA", "TSLA"]

    ranged = repository.fetch_transactions(ticker="TSLA", since=date(2025, 9, 2))
    assert [txn.activity_date for txn in ranged] == ["2025-09-02"]


def test_fetch_transactions_respects_status_flag(tmp_path, repository):
    _seed_import(
        tmp_path,
        csv_name="closed.csv",
        transactions=[_make_transaction(instrument="TSLA")],
        ticker="TSLA",
        open_only=False,
    )
    _seed_import(
        tmp_path,
        csv_name="open.csv",
        transactions=[_make_transaction(instrument="AAPL")],
        ticker="AAPL",
        open_only=True,
    )

    open_transactions = repository.fetch_transactions(status="open")
    assert {txn.instrument for txn in open_transactions} == {"AAPL"}

    closed_transactions = repository.fetch_transactions(status="closed")
    assert {txn.instrument for txn in closed_transactions} == {"TSLA"}


def test_fetch_stock_transactions_filters(tmp_path, repository):
    _seed_import(
        tmp_path,
        csv_name="stocks.csv",
        transactions=[],
        stock_transactions=[
            _make_stock_transaction(instrument="HOOD", trans_code="Buy", action="BUY"),
            _make_stock_transaction(
                instrument="HOOD",
                trans_code="Sell",
                action="SELL",
                amount=Decimal("5000.00"),
                activity_date=date(2025, 9, 2),
            ),
            _make_stock_transaction(
                instrument="TSLA",
                trans_code="Buy",
                action="BUY",
                activity_date=date(2025, 9, 3),
                quantity=Decimal("0.25"),
            ),
        ],
    )

    hood_rows = repository.fetch_stock_transactions(ticker="HOOD")
    assert len(hood_rows) == 2
    assert {row.action for row in hood_rows} == {"BUY", "SELL"}
    assert {row.quantity for row in hood_rows} == {Decimal("100"), Decimal("100")}

    filtered = repository.fetch_stock_transactions(ticker="HOOD", since=date(2025, 9, 2))
    assert len(filtered) == 1
    assert filtered[0].action == "SELL"
    tsla_rows = repository.fetch_stock_transactions(ticker="TSLA")
    assert len(tsla_rows) == 1
    assert tsla_rows[0].quantity == Decimal("0.25")


def test_fetch_import_activity_ranges(tmp_path, repository):
    _seed_import(
        tmp_path,
        csv_name="one.csv",
        transactions=[
            _make_transaction(instrument="TSLA", activity_date=date(2025, 9, 1)),
            _make_transaction(instrument="TSLA", activity_date=date(2025, 9, 3)),
        ],
        ticker="TSLA",
    )
    _seed_import(
        tmp_path,
        csv_name="two.csv",
        transactions=[_make_transaction(instrument="AAPL", activity_date=date(2025, 9, 5))],
        ticker="AAPL",
    )

    imports = repository.list_imports(order="asc")
    first_id = imports[0].id
    second_id = imports[1].id

    ranges = repository.fetch_import_activity_ranges([first_id, second_id])

    assert ranges[first_id] == ("2025-09-01", "2025-09-03")
    assert ranges[second_id] == ("2025-09-05", "2025-09-05")
    assert repository.fetch_import_activity_ranges([]) == {}


def test_delete_import_removes_record_and_transactions(tmp_path, repository):
    _seed_import(
        tmp_path,
        csv_name="keep.csv",
        transactions=[_make_transaction(instrument="TSLA")],
        ticker="TSLA",
    )
    _seed_import(
        tmp_path,
        csv_name="remove.csv",
        transactions=[_make_transaction(instrument="AAPL")],
        ticker="AAPL",
    )

    imports = repository.list_imports(order="asc")
    remove_id = imports[0].id

    assert repository.delete_import(remove_id) is True
    remaining = repository.list_imports()
    assert all(import_record.id != remove_id for import_record in remaining)

    # second delete should report missing record
    assert repository.delete_import(remove_id) is False
