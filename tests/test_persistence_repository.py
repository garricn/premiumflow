"""Tests for the read/query helpers over the persistence layer."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from premiumflow.core.parser import NormalizedOptionTransaction, ParsedImportResult
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
        fees=overrides.get("fees", Decimal("0")),
        raw=overrides.get("raw", {"Activity Date": "09/01/2025"}),
    )


def _make_parsed(
    transactions: list[NormalizedOptionTransaction], **overrides
) -> ParsedImportResult:
    return ParsedImportResult(
        account_name=overrides.get("account_name", "Primary Account"),
        account_number=overrides.get("account_number", "ACCT-1"),
        transactions=transactions,
    )


def _seed_import(
    tmp_dir: Path,
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
