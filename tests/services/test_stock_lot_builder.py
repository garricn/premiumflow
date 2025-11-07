"""Tests for assignment-driven stock lot builder."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from premiumflow.core.parser import NormalizedOptionTransaction, ParsedImportResult
from premiumflow.persistence import repository as repository_module
from premiumflow.persistence import storage as storage_module
from premiumflow.persistence.storage import store_import_result
from premiumflow.services.stock_lot_builder import rebuild_assignment_stock_lots


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


def _seed_import(
    tmp_dir,
    *,
    account_name: str,
    account_number: str,
    csv_name: str,
    transactions: list[NormalizedOptionTransaction],
) -> None:
    csv_path = tmp_dir / csv_name
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


def test_rebuild_assignment_stock_lots_records_put_and_call(repository, tmp_path):
    """Rebuilding stock lots captures both put and call assignments."""
    _seed_import(
        tmp_path,
        account_name="Primary Account",
        account_number="ACCT-1",
        csv_name="assignments.csv",
        transactions=[
            # HOOD call sold then assigned
            _make_transaction(
                instrument="HOOD",
                description="HOOD 09/06/2025 Call $104.00",
                trans_code="STO",
                option_type="CALL",
                strike=Decimal("104.00"),
                expiration=date(2025, 9, 6),
                price=Decimal("1.08"),
                amount=Decimal("108.00"),
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

    rebuild_assignment_stock_lots(
        repository,
        account_name="Primary Account",
        account_number="ACCT-1",
    )

    with repository._storage._connect() as conn:  # type: ignore[attr-defined]
        rows = conn.execute("SELECT * FROM stock_lots").fetchall()

    assert len(rows) == 2
    rows_by_symbol = {row["symbol"]: row for row in rows}

    hood_row = rows_by_symbol["HOOD"]
    assert hood_row["symbol"] == "HOOD"
    assert hood_row["assignment_kind"] == "call_assignment"
    assert hood_row["direction"] == "short"
    assert hood_row["quantity"] == -100
    assert Decimal(hood_row["share_price_total"]) == Decimal("10400")
    assert Decimal(hood_row["open_premium_total"]) == Decimal("108")
    assert Decimal(hood_row["net_credit_total"]) == Decimal("108")

    ethu_row = rows_by_symbol["ETHU"]
    assert ethu_row["symbol"] == "ETHU"
    assert ethu_row["assignment_kind"] == "put_assignment"
    assert ethu_row["direction"] == "long"
    assert ethu_row["quantity"] == 100
    assert Decimal(ethu_row["share_price_total"]) == Decimal("11000")
    assert Decimal(ethu_row["open_premium_total"]) == Decimal("175")
    assert Decimal(ethu_row["net_credit_total"]) == Decimal("175")

    # Ensure stock lots reference the assignment transactions
    with repository._storage._connect() as conn:  # type: ignore[attr-defined]
        assignment_ids = conn.execute(
            """
            SELECT id FROM option_transactions
            WHERE trans_code = 'OASGN'
            ORDER BY instrument ASC
            """
        ).fetchall()
    assert sorted(row["id"] for row in assignment_ids) == sorted(
        [
            hood_row["source_transaction_id"],
            ethu_row["source_transaction_id"],
        ]
    )
