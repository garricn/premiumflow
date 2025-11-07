"""Tests for stock lot rebuild service."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from premiumflow.core.parser import (
    NormalizedOptionTransaction,
    NormalizedStockTransaction,
    ParsedImportResult,
)
from premiumflow.persistence import repository as repository_module
from premiumflow.persistence import storage as storage_module
from premiumflow.persistence.storage import store_import_result
from premiumflow.services.stock_lot_service import rebuild_stock_lots


def _make_option_transaction(**overrides) -> NormalizedOptionTransaction:
    return NormalizedOptionTransaction(
        activity_date=overrides.get("activity_date", date(2025, 8, 20)),
        process_date=overrides.get("process_date", date(2025, 8, 20)),
        settle_date=overrides.get("settle_date", date(2025, 8, 21)),
        instrument=overrides.get("instrument", "ETHU"),
        description=overrides.get("description", "ETHU 11/01/2025 Put $110.00"),
        trans_code=overrides.get("trans_code", "STO"),
        quantity=overrides.get("quantity", 1),
        price=overrides.get("price", Decimal("1.50")),
        amount=overrides.get("amount", Decimal("150.00")),
        strike=overrides.get("strike", Decimal("110.00")),
        option_type=overrides.get("option_type", "PUT"),
        expiration=overrides.get("expiration", date(2025, 11, 1)),
        action=overrides.get("action", "SELL"),
        raw=overrides.get("raw", {"Activity Date": "08/20/2025"}),
    )


def _make_stock_transaction(**overrides) -> NormalizedStockTransaction:
    return NormalizedStockTransaction(
        activity_date=overrides.get("activity_date", date(2025, 8, 22)),
        process_date=overrides.get("process_date", date(2025, 8, 22)),
        settle_date=overrides.get("settle_date", date(2025, 8, 23)),
        instrument=overrides.get("instrument", "HOOD"),
        description=overrides.get("description", "Robinhood Markets"),
        trans_code=overrides.get("trans_code", "Buy"),
        quantity=overrides.get("quantity", 100),
        price=overrides.get("price", Decimal("100.00")),
        amount=overrides.get("amount", Decimal("-10000.00")),
        action=overrides.get("action", "BUY"),
        raw=overrides.get("raw", {"Activity Date": "08/22/2025"}),
    )


def _persist(
    tmp_path,
    *,
    account_name: str,
    account_number: str,
    transactions: list[NormalizedOptionTransaction],
    stock_transactions: list[NormalizedStockTransaction],
) -> None:
    csv_path = tmp_path / "test.csv"
    csv_path.write_text("stub", encoding="utf-8")
    parsed = ParsedImportResult(
        account_name=account_name,
        account_number=account_number,
        transactions=transactions,
        stock_transactions=stock_transactions,
    )
    store_import_result(
        parsed,
        source_path=str(csv_path),
        options_only=False,
        ticker=None,
        strategy=None,
        open_only=False,
    )


@pytest.fixture(autouse=True)
def clear_storage_cache():
    storage_module.get_storage.cache_clear()
    yield
    storage_module.get_storage.cache_clear()


@pytest.fixture
def repository(tmp_path, monkeypatch):
    db_path = tmp_path / "premiumflow.db"
    monkeypatch.setenv(storage_module.DB_ENV_VAR, str(db_path))
    storage_module.get_storage.cache_clear()
    return repository_module.SQLiteRepository()


def test_rebuild_stock_lots_handles_direct_trades(repository, tmp_path):
    """Direct stock buys and sells are matched FIFO."""
    _persist(
        tmp_path,
        account_name="Primary",
        account_number="ACCT-1",
        transactions=[],
        stock_transactions=[
            _make_stock_transaction(
                instrument="HOOD",
                trans_code="Buy",
                action="BUY",
                price=Decimal("100.00"),
                amount=Decimal("-10000.00"),
                quantity=100,
                activity_date=date(2025, 9, 1),
            ),
            _make_stock_transaction(
                instrument="HOOD",
                trans_code="Sell",
                action="SELL",
                price=Decimal("120.00"),
                amount=Decimal("4800.00"),
                quantity=40,
                activity_date=date(2025, 9, 5),
            ),
            _make_stock_transaction(
                instrument="HOOD",
                trans_code="Sell",
                action="SELL",
                price=Decimal("130.00"),
                amount=Decimal("7800.00"),
                quantity=60,
                activity_date=date(2025, 9, 10),
            ),
        ],
    )

    rebuild_stock_lots(repository, account_name="Primary", account_number="ACCT-1")

    with repository._storage._connect() as conn:  # type: ignore[attr-defined]
        rows = conn.execute("SELECT * FROM stock_lots ORDER BY opened_at").fetchall()

    assert len(rows) == 2
    first = rows[0]
    assert first["symbol"] == "HOOD"
    assert first["status"] == "closed"
    assert first["quantity"] == 40
    assert Decimal(first["cost_basis_per_share"]) == Decimal("100.0000")
    assert Decimal(first["proceeds_per_share"]) == Decimal("120.0000")
    second = rows[1]
    assert Decimal(second["proceeds_per_share"]) == Decimal("130.0000")
    assert second["status"] == "closed"
    assert second["quantity"] == 60


def test_rebuild_stock_lots_includes_put_assignment(repository, tmp_path):
    """Put assignments open share lots that later close via sells."""
    _persist(
        tmp_path,
        account_name="Primary",
        account_number="ACCT-1",
        transactions=[
            _make_option_transaction(
                trans_code="STO",
                quantity=1,
                price=Decimal("1.75"),
                amount=Decimal("175.00"),
                activity_date=date(2025, 8, 20),
            ),
            _make_option_transaction(
                trans_code="OASGN",
                quantity=1,
                price=Decimal("0.00"),
                amount=None,
                activity_date=date(2025, 8, 25),
            ),
        ],
        stock_transactions=[
            _make_stock_transaction(
                instrument="ETHU",
                trans_code="Sell",
                action="SELL",
                price=Decimal("120.00"),
                amount=Decimal("12000.00"),
                quantity=100,
                activity_date=date(2025, 9, 1),
            )
        ],
    )

    rebuild_stock_lots(repository, account_name="Primary", account_number="ACCT-1")

    with repository._storage._connect() as conn:  # type: ignore[attr-defined]
        rows = conn.execute("SELECT * FROM stock_lots").fetchall()

    assert len(rows) == 1
    row = rows[0]
    assert row["symbol"] == "ETHU"
    assert row["status"] == "closed"
    assert Decimal(row["cost_basis_per_share"]) == Decimal("108.25")
    assert Decimal(row["proceeds_per_share"]) == Decimal("120.0000")
    assert Decimal(row["assignment_premium_total"]) == Decimal("175.00")
    assert Decimal(row["realized_pnl_total"]).quantize(Decimal("0.01")) == Decimal("1175.00")


def test_rebuild_stock_lots_buy_covers_short_and_opens_long(repository, tmp_path):
    """Buys that cover shorts and leave leftover shares keep correct basis."""
    _persist(
        tmp_path,
        account_name="Primary",
        account_number="ACCT-1",
        transactions=[],
        stock_transactions=[
            _make_stock_transaction(
                instrument="HOOD",
                trans_code="Sell",
                action="SELL",
                price=Decimal("9.00"),
                amount=Decimal("900.00"),
                quantity=100,
                activity_date=date(2025, 8, 28),
            ),
            _make_stock_transaction(
                instrument="HOOD",
                trans_code="Buy",
                action="BUY",
                price=Decimal("10.00"),
                amount=Decimal("-1500.00"),
                quantity=150,
                activity_date=date(2025, 9, 2),
            ),
        ],
    )

    rebuild_stock_lots(repository, account_name="Primary", account_number="ACCT-1")

    with repository._storage._connect() as conn:  # type: ignore[attr-defined]
        rows = conn.execute("SELECT * FROM stock_lots ORDER BY opened_at").fetchall()

    assert len(rows) == 2
    closed = next(row for row in rows if row["status"] == "closed")
    assert closed["direction"] == "short"
    assert closed["quantity"] == -100
    assert Decimal(closed["cost_basis_per_share"]) == Decimal("10.0000")
    open_row = next(row for row in rows if row["status"] == "open")
    assert open_row["direction"] == "long"
    assert open_row["quantity"] == 50
    assert Decimal(open_row["cost_basis_per_share"]) == Decimal("10.0000")
    assert Decimal(open_row["assignment_premium_total"]) == Decimal("0")
