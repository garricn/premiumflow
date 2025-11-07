"""Tests for the SQLite persistence layer."""

from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal
from typing import Optional

import pytest

from premiumflow.core.parser import (
    CSV_ROW_NUMBER_KEY,
    NormalizedOptionTransaction,
    NormalizedStockTransaction,
    ParsedImportResult,
)
from premiumflow.persistence import storage as storage_module


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
) -> ParsedImportResult:
    return ParsedImportResult(
        account_name="Primary Account",
        account_number="ACCT-1",
        transactions=transactions,
        stock_transactions=stock_transactions or [],
    )


def test_store_import_creates_records(tmp_path, monkeypatch):
    db_path = tmp_path / "premiumflow.db"
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("sample", encoding="utf-8")
    monkeypatch.setenv(storage_module.DB_ENV_VAR, str(db_path))

    parsed = _make_parsed(
        [
            _make_transaction(trans_code="STO", quantity=2, amount=Decimal("600")),
            _make_transaction(
                trans_code="BTC",
                action="BUY",
                quantity=1,
                price=Decimal("1.50"),
                amount=Decimal("-150"),
                raw={"Activity Date": "09/15/2025"},
            ),
        ],
        stock_transactions=[_make_stock_transaction()],
    )

    result = storage_module.store_import_result(
        parsed,
        source_path=str(csv_path),
        options_only=True,
        ticker="TSLA",
        strategy="calls",
        open_only=False,
    )

    assert db_path.exists()
    assert result.status == "inserted"

    with sqlite3.connect(db_path) as conn:
        accounts = conn.execute("SELECT name, number FROM accounts").fetchall()
        assert accounts == [("Primary Account", "ACCT-1")]

        imports = conn.execute(
            "SELECT source_path, source_hash, row_count, ticker FROM imports"
        ).fetchall()
        assert len(imports) == 1
        assert imports[0][0] == str(csv_path)
        assert imports[0][2] == 2
        assert imports[0][3] == "TSLA"

        transactions = conn.execute(
            "SELECT import_id, row_index, trans_code, instrument FROM option_transactions ORDER BY row_index"
        ).fetchall()
        assert len(transactions) == 2
        assert [row[2] for row in transactions] == ["STO", "BTC"]

        stock_rows = conn.execute(
            "SELECT import_id, trans_code, action, instrument FROM stock_transactions ORDER BY row_index"
        ).fetchall()
        assert len(stock_rows) == 1
        assert stock_rows[0][1] == "BUY"
        assert stock_rows[0][2] == "BUY"


def test_store_import_preserves_csv_row_numbers(tmp_path, monkeypatch):
    db_path = tmp_path / "premiumflow.db"
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("sample", encoding="utf-8")
    monkeypatch.setenv(storage_module.DB_ENV_VAR, str(db_path))

    option_raw = {"Activity Date": "09/01/2025", CSV_ROW_NUMBER_KEY: "7"}
    stock_raw = {"Activity Date": "09/02/2025", CSV_ROW_NUMBER_KEY: "12"}

    parsed = _make_parsed(
        [_make_transaction(raw=option_raw)],
        stock_transactions=[_make_stock_transaction(raw=stock_raw)],
    )

    storage_module.store_import_result(
        parsed,
        source_path=str(csv_path),
        options_only=True,
        ticker=None,
        strategy=None,
        open_only=False,
    )

    with sqlite3.connect(db_path) as conn:
        option_row_index = conn.execute(
            "SELECT row_index FROM option_transactions LIMIT 1"
        ).fetchone()[0]
        stock_row_index = conn.execute(
            "SELECT row_index FROM stock_transactions LIMIT 1"
        ).fetchone()[0]

    assert option_row_index == 7
    assert stock_row_index == 12


def test_store_import_handles_fractional_shares(tmp_path, monkeypatch):
    db_path = tmp_path / "premiumflow.db"
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("sample", encoding="utf-8")
    monkeypatch.setenv(storage_module.DB_ENV_VAR, str(db_path))

    parsed = _make_parsed(
        [],
        stock_transactions=[_make_stock_transaction(quantity=Decimal("0.5"))],
    )

    storage_module.store_import_result(
        parsed,
        source_path=str(csv_path),
        options_only=False,
        ticker=None,
        strategy=None,
        open_only=False,
    )

    with sqlite3.connect(db_path) as conn:
        stored_quantity = conn.execute(
            "SELECT quantity FROM stock_transactions LIMIT 1"
        ).fetchone()[0]

    assert stored_quantity == "0.5"


def test_store_import_reuses_account(tmp_path, monkeypatch):
    db_path = tmp_path / "premiumflow.db"
    csv_one = tmp_path / "one.csv"
    csv_two = tmp_path / "two.csv"
    csv_one.write_text("one", encoding="utf-8")
    csv_two.write_text("two", encoding="utf-8")
    monkeypatch.setenv(storage_module.DB_ENV_VAR, str(db_path))

    parsed = _make_parsed([_make_transaction()])

    first = storage_module.store_import_result(
        parsed,
        source_path=str(csv_one),
        options_only=True,
        ticker=None,
        strategy=None,
        open_only=False,
    )
    second = storage_module.store_import_result(
        parsed,
        source_path=str(csv_two),
        options_only=False,
        ticker="TSLA",
        strategy="calls",
        open_only=True,
    )

    with sqlite3.connect(db_path) as conn:
        account_rows = conn.execute("SELECT id FROM accounts").fetchall()
        assert len(account_rows) == 1

        import_rows = conn.execute(
            "SELECT options_only, open_only FROM imports ORDER BY id"
        ).fetchall()
        assert [tuple(row) for row in import_rows] == [(1, 0), (0, 1)]
    assert first.status == "inserted"
    assert second.status == "inserted"


def test_store_import_skip_existing(tmp_path, monkeypatch):
    db_path = tmp_path / "premiumflow.db"
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("sample", encoding="utf-8")
    monkeypatch.setenv(storage_module.DB_ENV_VAR, str(db_path))

    parsed = _make_parsed([_make_transaction()])

    initial = storage_module.store_import_result(
        parsed,
        source_path=str(csv_path),
        options_only=True,
        ticker=None,
        strategy=None,
        open_only=False,
    )

    skipped = storage_module.store_import_result(
        parsed,
        source_path=str(csv_path),
        options_only=True,
        ticker=None,
        strategy=None,
        open_only=False,
        duplicate_strategy="skip",
    )

    with sqlite3.connect(db_path) as conn:
        imports = conn.execute("SELECT COUNT(*) FROM imports").fetchone()[0]
        assert imports == 1
        txn_count = conn.execute("SELECT COUNT(*) FROM option_transactions").fetchone()[0]
        assert txn_count == len(parsed.transactions)
    assert initial.status == "inserted"
    assert skipped.status == "skipped"


def test_store_import_replace_existing(tmp_path, monkeypatch):
    db_path = tmp_path / "premiumflow.db"
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("one", encoding="utf-8")
    monkeypatch.setenv(storage_module.DB_ENV_VAR, str(db_path))

    parsed = _make_parsed([_make_transaction()])

    initial = storage_module.store_import_result(
        parsed,
        source_path=str(csv_path),
        options_only=True,
        ticker=None,
        strategy=None,
        open_only=False,
    )

    csv_path.write_text("two", encoding="utf-8")

    replaced = storage_module.store_import_result(
        parsed,
        source_path=str(csv_path),
        options_only=False,
        ticker="TSLA",
        strategy="calls",
        open_only=True,
        duplicate_strategy="replace",
    )

    with sqlite3.connect(db_path) as conn:
        imports = conn.execute("SELECT id, options_only, open_only FROM imports").fetchall()
        assert [(row[1], row[2]) for row in imports] == [(0, 1)]
        import_id = imports[0][0]
        txn_count = conn.execute(
            "SELECT COUNT(*) FROM option_transactions WHERE import_id = ?",
            (import_id,),
        ).fetchone()[0]
        assert txn_count == len(parsed.transactions)

    assert initial.status == "inserted"
    assert replaced.status == "replaced"


def test_initialization_does_not_drop_existing_stock_lots(tmp_path, monkeypatch):
    db_path = tmp_path / "premiumflow.db"
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("sample", encoding="utf-8")
    monkeypatch.setenv(storage_module.DB_ENV_VAR, str(db_path))

    parsed = _make_parsed([_make_transaction()])
    storage_module.store_import_result(
        parsed,
        source_path=str(csv_path),
        options_only=True,
        ticker=None,
        strategy=None,
        open_only=False,
    )

    storage = storage_module.SQLiteStorage(db_path)
    storage._ensure_initialized()
    with storage._connect() as conn:  # type: ignore[attr-defined]
        account_id = conn.execute("SELECT id FROM accounts").fetchone()[0]
        option_txn_id = conn.execute("SELECT id FROM option_transactions").fetchone()[0]
        conn.execute(
            """
            INSERT INTO stock_lots (
                account_id,
                source_transaction_id,
                symbol,
                opened_at,
                closed_at,
                quantity,
                direction,
                option_type,
                strike_price,
                expiration,
                share_price_total,
                share_price_per_share,
                open_premium_total,
                open_premium_per_share,
                open_fee_total,
                net_credit_total,
                net_credit_per_share,
                assignment_kind,
                status,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                account_id,
                option_txn_id,
                "TSLA",
                "2025-09-01",
                None,
                100,
                "long",
                "PUT",
                "100.00",
                "2025-10-01",
                "10000.00",
                "100.00",
                "500.00",
                "5.00",
                "0.00",
                "500.00",
                "5.00",
                "assignment",
                "open",
                "2025-09-01T00:00:00Z",
                "2025-09-01T00:00:00Z",
            ),
        )

    # Recreate storage to simulate a later startup.
    storage = storage_module.SQLiteStorage(db_path)
    storage._ensure_initialized()
    with storage._connect() as conn:  # type: ignore[attr-defined]
        rows = conn.execute("SELECT symbol, quantity FROM stock_lots").fetchall()

    assert [tuple(row) for row in rows] == [("TSLA", 100)]
