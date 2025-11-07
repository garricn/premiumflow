"""Tests for stock lot rebuild service."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from premiumflow.core.parser import (
    CSV_ROW_NUMBER_KEY,
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


def test_rebuild_stock_lots_isolates_symbols(repository, tmp_path):
    """Lots are matched FIFO per symbol so symbols do not cross-consume."""
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
                price=Decimal("10.00"),
                amount=Decimal("-1000.00"),
                quantity=100,
                activity_date=date(2025, 8, 20),
            ),
            _make_stock_transaction(
                instrument="TSLA",
                trans_code="Buy",
                action="BUY",
                price=Decimal("200.00"),
                amount=Decimal("-20000.00"),
                quantity=100,
                activity_date=date(2025, 8, 21),
            ),
            _make_stock_transaction(
                instrument="TSLA",
                trans_code="Sell",
                action="SELL",
                price=Decimal("210.00"),
                amount=Decimal("21000.00"),
                quantity=100,
                activity_date=date(2025, 8, 22),
            ),
            _make_stock_transaction(
                instrument="HOOD",
                trans_code="Sell",
                action="SELL",
                price=Decimal("11.00"),
                amount=Decimal("1100.00"),
                quantity=100,
                activity_date=date(2025, 8, 23),
            ),
        ],
    )

    rebuild_stock_lots(repository, account_name="Primary", account_number="ACCT-1")

    with repository._storage._connect() as conn:  # type: ignore[attr-defined]
        rows = conn.execute(
            "SELECT * FROM stock_lots WHERE status='closed' ORDER BY symbol"
        ).fetchall()

    assert len(rows) == 2
    hood = next(row for row in rows if row["symbol"] == "HOOD")
    assert Decimal(hood["cost_basis_per_share"]) == Decimal("10.0000")
    assert Decimal(hood["proceeds_per_share"]) == Decimal("11.0000")
    assert Decimal(hood["realized_pnl_total"]).quantize(Decimal("0.01")) == Decimal("100.00")
    tsla = next(row for row in rows if row["symbol"] == "TSLA")
    assert Decimal(tsla["cost_basis_per_share"]) == Decimal("200.0000")
    assert Decimal(tsla["proceeds_per_share"]) == Decimal("210.0000")
    assert Decimal(tsla["realized_pnl_total"]).quantize(Decimal("0.01")) == Decimal("1000.00")


def test_rebuild_stock_lots_prorates_assignment_premium(repository, tmp_path):
    """Assignment premiums are prorated when closing a lot via multiple sells."""
    _persist(
        tmp_path,
        account_name="Primary",
        account_number="ACCT-1",
        transactions=[
            _make_option_transaction(
                instrument="ETHU",
                trans_code="STO",
                quantity=1,
                price=Decimal("1.75"),
                amount=Decimal("175.00"),
                activity_date=date(2025, 8, 20),
            ),
            _make_option_transaction(
                instrument="ETHU",
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
                price=Decimal("118.00"),
                amount=Decimal("5900.00"),
                quantity=50,
                activity_date=date(2025, 9, 1),
            ),
            _make_stock_transaction(
                instrument="ETHU",
                trans_code="Sell",
                action="SELL",
                price=Decimal("122.00"),
                amount=Decimal("6100.00"),
                quantity=50,
                activity_date=date(2025, 9, 3),
            ),
        ],
    )

    rebuild_stock_lots(repository, account_name="Primary", account_number="ACCT-1")

    with repository._storage._connect() as conn:  # type: ignore[attr-defined]
        rows = conn.execute(
            "SELECT * FROM stock_lots WHERE symbol='ETHU' ORDER BY closed_at"
        ).fetchall()

    assert len(rows) == 2
    assert all(row["status"] == "closed" for row in rows)
    first, second = rows
    assert Decimal(first["assignment_premium_total"]).quantize(Decimal("0.01")) == Decimal("87.50")
    assert Decimal(second["assignment_premium_total"]).quantize(Decimal("0.01")) == Decimal("87.50")


def test_rebuild_stock_lots_respects_same_day_order(repository, tmp_path):
    """Same-day stock buys should occur before call assignments when CSV order says so."""
    option_raw_open = {"Activity Date": "09/01/2025", CSV_ROW_NUMBER_KEY: "20"}
    option_raw_assign = {"Activity Date": "09/01/2025", CSV_ROW_NUMBER_KEY: "25"}
    stock_raw = {"Activity Date": "09/01/2025", CSV_ROW_NUMBER_KEY: "21"}
    _persist(
        tmp_path,
        account_name="Primary",
        account_number="ACCT-1",
        transactions=[
            _make_option_transaction(
                instrument="HOOD",
                option_type="CALL",
                trans_code="STO",
                quantity=1,
                price=Decimal("1.00"),
                amount=Decimal("100.00"),
                activity_date=date(2025, 9, 1),
                raw=option_raw_open,
                description="HOOD 10/01/2025 Call $100.00",
                strike=Decimal("100.00"),
            ),
            _make_option_transaction(
                instrument="HOOD",
                option_type="CALL",
                trans_code="OASGN",
                quantity=1,
                price=Decimal("0.00"),
                amount=None,
                activity_date=date(2025, 9, 1),
                raw=option_raw_assign,
                description="Assignment HOOD 10/01/2025 Call $100.00",
                strike=Decimal("100.00"),
            ),
        ],
        stock_transactions=[
            _make_stock_transaction(
                instrument="HOOD",
                trans_code="Buy",
                action="BUY",
                price=Decimal("95.00"),
                amount=Decimal("-9500.00"),
                quantity=100,
                activity_date=date(2025, 9, 1),
                raw=stock_raw,
            )
        ],
    )

    rebuild_stock_lots(repository, account_name="Primary", account_number="ACCT-1")

    with repository._storage._connect() as conn:  # type: ignore[attr-defined]
        rows = conn.execute("SELECT * FROM stock_lots").fetchall()

    assert len(rows) == 1
    lot = rows[0]
    assert lot["direction"] == "long"
    assert lot["status"] == "closed"
    assert lot["open_source"] == "stock_buy"
    assert lot["close_source"] == "assignment_call"


def test_rebuild_stock_lots_preserves_assignment_premium_on_short_close(repository, tmp_path):
    """Covering a short lot with a put assignment retains the assignment premium."""
    _persist(
        tmp_path,
        account_name="Primary",
        account_number="ACCT-1",
        transactions=[
            _make_option_transaction(
                instrument="HOOD",
                option_type="PUT",
                trans_code="STO",
                quantity=1,
                price=Decimal("1.75"),
                amount=Decimal("175.00"),
                activity_date=date(2025, 9, 1),
                description="HOOD 10/01/2025 Put $100.00",
                strike=Decimal("100.00"),
            ),
            _make_option_transaction(
                instrument="HOOD",
                option_type="PUT",
                trans_code="OASGN",
                quantity=1,
                price=Decimal("0.00"),
                amount=None,
                activity_date=date(2025, 9, 2),
                description="Assignment HOOD 10/01/2025 Put $100.00",
                strike=Decimal("100.00"),
            ),
        ],
        stock_transactions=[
            _make_stock_transaction(
                instrument="HOOD",
                trans_code="Sell",
                action="SELL",
                price=Decimal("110.00"),
                amount=Decimal("11000.00"),
                quantity=100,
                activity_date=date(2025, 9, 1),
            ),
        ],
    )

    rebuild_stock_lots(repository, account_name="Primary", account_number="ACCT-1")

    with repository._storage._connect() as conn:  # type: ignore[attr-defined]
        row = conn.execute("SELECT * FROM stock_lots WHERE status='closed'").fetchone()

    assert row is not None
    assert row["direction"] == "short"
    assert Decimal(row["assignment_premium_total"]).quantize(Decimal("0.01")) == Decimal("175.00")
    assert Decimal(row["realized_pnl_total"]).quantize(Decimal("0.01")) == Decimal("1000.00")
