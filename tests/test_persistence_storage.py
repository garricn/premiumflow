"""Tests for the SQLite persistence layer."""

from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal
from typing import Optional

import pytest

from premiumflow.core.parser import (
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
        trans_code=overrides.get("trans_code", "Buy"),
        quantity=overrides.get("quantity", 100),
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
            "SELECT trans_code, action, instrument FROM stock_transactions"
        ).fetchall()
        assert len(stock_rows) == 1
        assert stock_rows[0][0] == "Buy"


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


def test_initialization_preserves_existing_stock_lots(tmp_path):
    db_path = tmp_path / "premiumflow.db"
    storage = storage_module.SQLiteStorage(db_path)
    storage._ensure_initialized()

    with storage._connect() as conn:  # type: ignore[attr-defined]
        conn.execute("INSERT INTO accounts (name, number) VALUES (?, ?)", ("Acct", "123"))
        account_id = conn.execute("SELECT id FROM accounts").fetchone()[0]
        conn.execute(
            """
            INSERT INTO stock_lots (
                account_id,
                symbol,
                opened_at,
                closed_at,
                quantity,
                direction,
                cost_basis_total,
                cost_basis_per_share,
                open_fee_total,
                assignment_premium_total,
                proceeds_total,
                proceeds_per_share,
                close_fee_total,
                realized_pnl_total,
                realized_pnl_per_share,
                open_source,
                open_source_id,
                close_source,
                close_source_id,
                status,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                account_id,
                "HOOD",
                "2025-09-01",
                None,
                100,
                "long",
                "1000.00",
                "10.00",
                "0.00",
                "0.00",
                None,
                None,
                "0.00",
                None,
                None,
                "manual",
                None,
                None,
                None,
                "open",
                "2025-09-02T00:00:00Z",
                "2025-09-02T00:00:00Z",
            ),
        )

    # Simulate a new process startup pointing at the same DB.
    storage = storage_module.SQLiteStorage(db_path)
    storage._ensure_initialized()

    with storage._connect() as conn:  # type: ignore[attr-defined]
        rows = conn.execute("SELECT symbol, quantity FROM stock_lots").fetchall()

    assert [tuple(row) for row in rows] == [("HOOD", 100)]


def test_initialization_migrates_legacy_stock_lots(tmp_path):
    db_path = tmp_path / "premiumflow.db"
    storage = storage_module.SQLiteStorage(db_path)
    storage._ensure_initialized()

    with storage._connect() as conn:  # type: ignore[attr-defined]
        conn.execute("INSERT INTO accounts (name, number) VALUES (?, ?)", ("Acct", "123"))
        account_id = conn.execute("SELECT id FROM accounts LIMIT 1").fetchone()[0]
        conn.execute(
            """
            INSERT INTO imports (
                account_id, source_path, source_hash, imported_at,
                options_only, ticker, strategy, open_only, row_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                account_id,
                "legacy.csv",
                "hash",
                "2025-09-02T00:00:00Z",
                1,
                None,
                None,
                0,
                1,
            ),
        )
        import_id = conn.execute("SELECT id FROM imports LIMIT 1").fetchone()[0]
        conn.execute(
            """
            INSERT INTO option_transactions (
                import_id, row_index, activity_date, process_date, settle_date,
                instrument, description, trans_code, quantity, price, amount,
                strike, option_type, expiration, action, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                import_id,
                1,
                "2025-09-01",
                "2025-09-01",
                "2025-09-02",
                "HOOD",
                "HOOD 10/01/2025 Put $100",
                "STO",
                1,
                "1.00",
                "100.00",
                "100.00",
                "PUT",
                "2025-10-01",
                "SELL",
                "{}",
            ),
        )
        option_txn_id = conn.execute("SELECT id FROM option_transactions LIMIT 1").fetchone()[0]
        conn.execute("DROP TABLE stock_lots")
        conn.execute(
            """
            CREATE TABLE stock_lots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
                source_transaction_id INTEGER NOT NULL REFERENCES option_transactions(id) ON DELETE CASCADE,
                symbol TEXT NOT NULL,
                opened_at TEXT NOT NULL,
                closed_at TEXT,
                quantity INTEGER NOT NULL,
                direction TEXT NOT NULL,
                option_type TEXT NOT NULL,
                strike_price TEXT NOT NULL,
                expiration TEXT NOT NULL,
                share_price_total TEXT NOT NULL,
                share_price_per_share TEXT NOT NULL,
                open_premium_total TEXT NOT NULL,
                open_premium_per_share TEXT NOT NULL,
                open_fee_total TEXT NOT NULL,
                net_credit_total TEXT NOT NULL,
                net_credit_per_share TEXT NOT NULL,
                assignment_kind TEXT,
                status TEXT NOT NULL DEFAULT 'open',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
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
                "HOOD",
                "2025-09-01",
                None,
                100,
                "long",
                "PUT",
                "100.00",
                "2025-10-01",
                "1000.00",
                "10.00",
                "175.00",
                "1.75",
                "0.00",
                "0.00",
                "0.00",
                "assignment",
                "open",
                "2025-09-02T00:00:00Z",
                "2025-09-02T00:00:00Z",
            ),
        )

    storage = storage_module.SQLiteStorage(db_path)
    storage._ensure_initialized()

    with storage._connect() as conn:  # type: ignore[attr-defined]
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(stock_lots)")}
        assert "cost_basis_total" in columns
        migrated = conn.execute(
            """
            SELECT
                symbol,
                cost_basis_total,
                assignment_premium_total,
                proceeds_total,
                open_source,
                open_source_id
            FROM stock_lots
            """
        ).fetchone()

    assert migrated["symbol"] == "HOOD"
    assert migrated["cost_basis_total"] == "1000.00"
    assert migrated["assignment_premium_total"] == "175.00"
    assert migrated["proceeds_total"] == "0.00"
    assert migrated["open_source"] == "legacy_migration"
    assert migrated["open_source_id"] == option_txn_id
