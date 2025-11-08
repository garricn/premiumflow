"""Tests for the consolidated stock lot builder."""

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
from premiumflow.services.stock_lot_builder import rebuild_stock_lots


def _make_transaction(**overrides) -> NormalizedOptionTransaction:
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
        strike=overrides.get("strike", Decimal("200.00")),
        option_type=overrides.get("option_type", "CALL"),
        expiration=overrides.get("expiration", date(2025, 10, 25)),
        action=overrides.get("action", "SELL"),
        raw=overrides.get("raw", {"Activity Date": "09/01/2025"}),
    )


def _make_stock_transaction(**overrides) -> NormalizedStockTransaction:
    return NormalizedStockTransaction(
        activity_date=overrides.get("activity_date", date(2025, 9, 1)),
        process_date=overrides.get("process_date", date(2025, 9, 1)),
        settle_date=overrides.get("settle_date", date(2025, 9, 3)),
        instrument=overrides.get("instrument", "TSLA"),
        description=overrides.get("description", "Tesla Inc"),
        trans_code=overrides.get("trans_code", overrides.get("action", "BUY")),
        quantity=overrides.get("quantity", Decimal("100")),
        price=overrides.get("price", Decimal("100.00")),
        amount=overrides.get("amount", Decimal("-10000.00")),
        action=overrides.get("action", "BUY"),
        raw=overrides.get("raw", {"Activity Date": "09/01/2025"}),
    )


def _seed_import(
    tmp_dir,
    *,
    account_name: str,
    account_number: str,
    csv_name: str,
    transactions: list[NormalizedOptionTransaction],
    stock_transactions: list[NormalizedStockTransaction] | None = None,
) -> None:
    csv_path = tmp_dir / csv_name
    csv_path.write_text(csv_name, encoding="utf-8")
    parsed = ParsedImportResult(
        account_name=account_name,
        account_number=account_number,
        transactions=transactions,
        stock_transactions=stock_transactions or [],
    )
    store_import_result(
        parsed,
        source_path=str(csv_path),
        options_only=True,
        ticker=None,
        strategy=None,
        open_only=False,
    )


@pytest.fixture(autouse=True)
def clear_storage_cache():
    """Clear storage cache before and after each test."""
    storage_module.get_storage.cache_clear()
    yield
    storage_module.get_storage.cache_clear()


@pytest.fixture
def repository(tmp_path, monkeypatch):
    db_path = tmp_path / "premiumflow.db"
    monkeypatch.setenv(storage_module.DB_ENV_VAR, str(db_path))
    storage_module.get_storage.cache_clear()
    return repository_module.SQLiteRepository()


def test_rebuild_stock_lots_assignments_only(repository, tmp_path):
    """Assignments alone produce distinct open lots with aggregated premiums."""
    _seed_import(
        tmp_path,
        account_name="Primary Account",
        account_number="ACCT-1",
        csv_name="assignments.csv",
        transactions=[
            # HOOD call sold (2 contracts) then partially assigned twice
            _make_transaction(
                instrument="HOOD",
                description="HOOD 09/06/2025 Call $104.00",
                trans_code="STO",
                option_type="CALL",
                strike=Decimal("104.00"),
                expiration=date(2025, 9, 6),
                price=Decimal("1.08"),
                amount=Decimal("216.00"),
                quantity=2,
                activity_date=date(2025, 8, 28),
            ),
            _make_transaction(
                instrument="HOOD",
                description="HOOD 09/06/2025 Call $104.00",
                trans_code="OASGN",
                option_type="CALL",
                strike=Decimal("104.00"),
                expiration=date(2025, 9, 6),
                price=Decimal("0.00"),
                amount=None,
                activity_date=date(2025, 9, 5),
                quantity=1,
            ),
            _make_transaction(
                instrument="HOOD",
                description="HOOD 09/06/2025 Call $104.00",
                trans_code="OASGN",
                option_type="CALL",
                strike=Decimal("104.00"),
                expiration=date(2025, 9, 6),
                price=Decimal("0.00"),
                amount=None,
                activity_date=date(2025, 9, 6),
                quantity=1,
            ),
            # ETHU put sold then assigned
            _make_transaction(
                instrument="ETHU",
                description="ETHU 11/01/2025 Put $110.00",
                trans_code="STO",
                option_type="PUT",
                strike=Decimal("110.00"),
                expiration=date(2025, 11, 1),
                price=Decimal("1.75"),
                amount=Decimal("175.00"),
                activity_date=date(2025, 10, 24),
            ),
            _make_transaction(
                instrument="ETHU",
                description="ETHU 11/01/2025 Put $110.00",
                trans_code="OASGN",
                option_type="PUT",
                strike=Decimal("110.00"),
                expiration=date(2025, 11, 1),
                price=Decimal("0.00"),
                amount=None,
                activity_date=date(2025, 10, 31),
            ),
        ],
    )

    rebuild_stock_lots(
        repository,
        account_name="Primary Account",
        account_number="ACCT-1",
    )

    with repository._storage._connect() as conn:  # type: ignore[attr-defined]
        rows = conn.execute("SELECT * FROM stock_lots").fetchall()

    assert len(rows) == 3
    rows_by_symbol: dict[str, list] = {}
    for row in rows:
        rows_by_symbol.setdefault(row["symbol"], []).append(row)

    hood_rows = rows_by_symbol["HOOD"]
    assert len(hood_rows) == 2
    assert {row["status"] for row in hood_rows} == {"open"}
    assert {row["assignment_kind"] for row in hood_rows} == {"call_assignment"}
    assert {row["direction"] for row in hood_rows} == {"short"}
    assert {row["quantity"] for row in hood_rows} == {-100}
    assert all(Decimal(row["share_price_total"]) == Decimal("0.00") for row in hood_rows)
    assert all(Decimal(row["net_credit_total"]) == Decimal("10508.00") for row in hood_rows)
    assert all(Decimal(row["net_credit_per_share"]) == Decimal("105.0800") for row in hood_rows)

    ethu_row = rows_by_symbol["ETHU"][0]
    assert ethu_row["assignment_kind"] == "put_assignment"
    assert ethu_row["direction"] == "long"
    assert ethu_row["status"] == "open"
    assert ethu_row["quantity"] == 100
    assert Decimal(ethu_row["share_price_total"]) == Decimal("11000.00")
    assert Decimal(ethu_row["net_credit_total"]) == Decimal("175.00")
    assert Decimal(ethu_row["net_credit_per_share"]) == Decimal("1.7500")

    # Ensure stock lots reference the assignment transactions
    with repository._storage._connect() as conn:  # type: ignore[attr-defined]
        assignment_ids = conn.execute(
            """
            SELECT instrument, id FROM option_transactions
            WHERE trans_code = 'OASGN'
            ORDER BY instrument ASC, activity_date ASC
            """
        ).fetchall()
    hood_assignment_ids_db = [row["id"] for row in assignment_ids if row["instrument"] == "HOOD"]
    hood_record_ids = {row["source_transaction_id"] for row in hood_rows}
    assert sorted(hood_record_ids) == sorted(hood_assignment_ids_db)
    assert ethu_row["source_transaction_id"] == next(
        row["id"] for row in assignment_ids if row["instrument"] == "ETHU"
    )


def test_rebuild_stock_lots_with_trades_closes_fifo(repository, tmp_path):
    """Assignments combined with stock trades yield closed lots and FIFO treatment."""
    _seed_import(
        tmp_path,
        account_name="Primary Account",
        account_number="ACCT-1",
        csv_name="with-trades.csv",
        transactions=[
            _make_transaction(
                instrument="HOOD",
                trans_code="STO",
                option_type="CALL",
                strike=Decimal("104.00"),
                expiration=date(2025, 9, 6),
                price=Decimal("1.08"),
                amount=Decimal("216.00"),
                quantity=2,
                activity_date=date(2025, 8, 28),
            ),
            _make_transaction(
                instrument="HOOD",
                trans_code="OASGN",
                option_type="CALL",
                strike=Decimal("104.00"),
                expiration=date(2025, 9, 6),
                quantity=1,
                activity_date=date(2025, 9, 5),
            ),
            _make_transaction(
                instrument="HOOD",
                trans_code="OASGN",
                option_type="CALL",
                strike=Decimal("104.00"),
                expiration=date(2025, 9, 6),
                quantity=1,
                activity_date=date(2025, 9, 6),
            ),
            _make_transaction(
                instrument="ETHU",
                trans_code="STO",
                option_type="PUT",
                strike=Decimal("110.00"),
                expiration=date(2025, 11, 1),
                price=Decimal("1.75"),
                amount=Decimal("175.00"),
                activity_date=date(2025, 10, 24),
            ),
            _make_transaction(
                instrument="ETHU",
                trans_code="OASGN",
                option_type="PUT",
                strike=Decimal("110.00"),
                expiration=date(2025, 11, 1),
                quantity=1,
                activity_date=date(2025, 10, 31),
            ),
        ],
        stock_transactions=[
            _make_stock_transaction(
                instrument="HOOD",
                action="BUY",
                quantity=Decimal("100"),
                price=Decimal("103.00"),
                amount=Decimal("-10300.00"),
                activity_date=date(2025, 9, 9),
            ),
            _make_stock_transaction(
                instrument="HOOD",
                action="BUY",
                quantity=Decimal("100"),
                price=Decimal("101.00"),
                amount=Decimal("-10100.00"),
                activity_date=date(2025, 9, 10),
            ),
            _make_stock_transaction(
                instrument="ETHU",
                action="SELL",
                quantity=Decimal("100"),
                price=Decimal("115.00"),
                amount=Decimal("11500.00"),
                activity_date=date(2025, 11, 5),
            ),
            _make_stock_transaction(
                instrument="TSLA",
                action="BUY",
                quantity=Decimal("80"),
                price=Decimal("200.00"),
                amount=Decimal("-16000.00"),
                activity_date=date(2025, 9, 15),
            ),
            _make_stock_transaction(
                instrument="TSLA",
                action="SELL",
                quantity=Decimal("50"),
                price=Decimal("215.00"),
                amount=Decimal("10750.00"),
                activity_date=date(2025, 9, 18),
            ),
        ],
    )

    rebuild_stock_lots(
        repository,
        account_name="Primary Account",
        account_number="ACCT-1",
    )

    with repository._storage._connect() as conn:  # type: ignore[attr-defined]
        rows = conn.execute(
            "SELECT * FROM stock_lots ORDER BY symbol ASC, opened_at ASC, status DESC"
        ).fetchall()

    assert len(rows) == 5

    hood_rows = [row for row in rows if row["symbol"] == "HOOD"]
    assert len(hood_rows) == 2
    assert {row["status"] for row in hood_rows} == {"closed"}
    assert {row["quantity"] for row in hood_rows} == {-100}
    assert [row["closed_at"] for row in hood_rows] == ["2025-09-09", "2025-09-10"]
    assert {Decimal(row["share_price_total"]) for row in hood_rows} == {
        Decimal("10300.00"),
        Decimal("10100.00"),
    }
    assert {Decimal(row["net_credit_total"]) for row in hood_rows} == {Decimal("10508.00")}

    ethu_row = next(row for row in rows if row["symbol"] == "ETHU")
    assert ethu_row["status"] == "closed"
    assert ethu_row["quantity"] == 100
    assert ethu_row["closed_at"] == "2025-11-05"
    assert Decimal(ethu_row["share_price_total"]) == Decimal("11000.00")
    assert Decimal(ethu_row["net_credit_total"]) == Decimal("11675.00")
    assert Decimal(ethu_row["net_credit_per_share"]) == Decimal("116.75")

    tsla_rows = [row for row in rows if row["symbol"] == "TSLA"]
    closed_tsla = next(row for row in tsla_rows if row["status"] == "closed")
    open_tsla = next(row for row in tsla_rows if row["status"] == "open")

    assert closed_tsla["quantity"] == 50
    assert closed_tsla["closed_at"] == "2025-09-18"
    assert Decimal(closed_tsla["share_price_total"]) == Decimal("10000.00")
    assert Decimal(closed_tsla["net_credit_total"]) == Decimal("10750.00")

    assert open_tsla["quantity"] == 30
    assert open_tsla["closed_at"] is None
    assert Decimal(open_tsla["share_price_total"]) == Decimal("6000.00")
    assert Decimal(open_tsla["net_credit_total"]) == Decimal("0.00")
    assert open_tsla["direction"] == "long"
