"""Tests for the combined positions aggregation service."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from uuid import uuid4

from premiumflow.core.parser import NormalizedOptionTransaction, ParsedImportResult
from premiumflow.persistence import AssignmentStockLotRecord
from premiumflow.persistence import repository as repository_module
from premiumflow.persistence import storage as storage_module
from premiumflow.persistence.storage import store_import_result
from premiumflow.services.positions import (
    fetch_equity_positions,
    fetch_option_positions,
    fetch_positions,
)


def _make_option_transaction(**overrides) -> NormalizedOptionTransaction:
    return NormalizedOptionTransaction(
        activity_date=overrides.get("activity_date", date(2025, 9, 1)),
        process_date=overrides.get("process_date", date(2025, 9, 1)),
        settle_date=overrides.get("settle_date", date(2025, 9, 3)),
        instrument=overrides.get("instrument", "AAPL"),
        description=overrides.get("description", "AAPL 09/20/2025 Call $200.00"),
        trans_code=overrides.get("trans_code", "BTO"),
        quantity=overrides.get("quantity", 2),
        price=overrides.get("price", Decimal("5.00")),
        amount=overrides.get("amount", Decimal("-1000.00")),
        strike=overrides.get("strike", Decimal("200.00")),
        option_type=overrides.get("option_type", "CALL"),
        expiration=overrides.get("expiration", date(2025, 9, 20)),
        action=overrides.get("action", "BUY"),
        raw=overrides.get("raw", {"Activity Date": "09/01/2025"}),
    )


def _seed_import(
    tmp_path,
    *,
    account_name: str,
    account_number: str,
    options: list[NormalizedOptionTransaction] | None = None,
    csv_name: str | None = None,
) -> None:
    file_name = csv_name or f"import-{uuid4().hex}.csv"
    csv_path = tmp_path / file_name
    csv_path.write_text("import", encoding="utf-8")
    parsed = ParsedImportResult(
        account_name=account_name,
        account_number=account_number,
        transactions=options or [],
        stock_transactions=[],
    )
    store_import_result(
        parsed,
        source_path=str(csv_path),
        options_only=False,
        ticker=None,
        strategy=None,
        open_only=False,
    )


def test_fetch_equity_positions_groups_open_lots(tmp_path, monkeypatch):
    monkeypatch.setenv(storage_module.DB_ENV_VAR, str(tmp_path / "positions.db"))
    storage_module.get_storage.cache_clear()
    repository = repository_module.SQLiteRepository()

    _seed_import(
        tmp_path,
        account_name="Primary",
        account_number="ACCT-1",
        options=[],
    )

    repository.replace_assignment_stock_lots(
        account_name="Primary",
        account_number="ACCT-1",
        records=[
            AssignmentStockLotRecord(
                symbol="AAPL",
                opened_at=date(2025, 9, 1),
                share_quantity=150,
                direction="long",
                option_type="PUT",
                strike_price=Decimal("51.6667"),
                expiration=date(2025, 9, 20),
                share_price_total=Decimal("7750.00"),
                share_price_per_share=Decimal("51.6667"),
                open_premium_total=Decimal("0.00"),
                open_premium_per_share=Decimal("0.0000"),
                open_fee_total=Decimal("0.00"),
                net_credit_total=Decimal("0.00"),
                net_credit_per_share=Decimal("0.0000"),
                assignment_kind="put_assignment",
                source_transaction_id=None,
            )
        ],
    )

    positions = fetch_equity_positions(repository)
    assert len(positions) == 1
    position = positions[0]
    assert position.account_name == "Primary"
    assert position.symbol == "AAPL"
    assert position.direction == "long"
    assert position.shares == 150
    assert position.basis_total == Decimal("7750.00")
    assert position.basis_per_share == Decimal("51.6667")


def test_fetch_option_positions_returns_open_contracts(tmp_path, monkeypatch):
    monkeypatch.setenv(storage_module.DB_ENV_VAR, str(tmp_path / "positions.db"))
    storage_module.get_storage.cache_clear()
    repository = repository_module.SQLiteRepository()

    _seed_import(
        tmp_path,
        account_name="Primary",
        account_number="ACCT-1",
        options=[
            _make_option_transaction(
                quantity=2, trans_code="BTO", action="BUY", amount=Decimal("-200.00")
            ),
            _make_option_transaction(
                instrument="MSFT",
                description="MSFT 10/18/2025 Call $350.00",
                strike=Decimal("350.00"),
                option_type="CALL",
                expiration=date(2025, 10, 18),
                quantity=1,
                price=Decimal("7.00"),
                amount=Decimal("-700.00"),
            ),
        ],
    )

    option_positions = fetch_option_positions(repository)
    assert len(option_positions) == 2
    aapl_position = next(pos for pos in option_positions if pos.symbol == "AAPL")
    assert aapl_position.direction == "long"
    assert aapl_position.contracts == 2
    assert aapl_position.open_credit < 0
    assert aapl_position.credit_remaining == aapl_position.open_credit


def test_fetch_positions_returns_equities_and_options(tmp_path, monkeypatch):
    monkeypatch.setenv(storage_module.DB_ENV_VAR, str(tmp_path / "positions.db"))
    storage_module.get_storage.cache_clear()
    repository = repository_module.SQLiteRepository()

    _seed_import(
        tmp_path,
        account_name="Primary",
        account_number="ACCT-1",
        options=[],
        csv_name="equity-seed.csv",
    )

    repository.replace_assignment_stock_lots(
        account_name="Primary",
        account_number="ACCT-1",
        records=[
            AssignmentStockLotRecord(
                symbol="AAPL",
                opened_at=date(2025, 9, 1),
                share_quantity=10,
                direction="long",
                option_type="PUT",
                strike_price=Decimal("40.00"),
                expiration=date(2025, 9, 20),
                share_price_total=Decimal("400.00"),
                share_price_per_share=Decimal("40.00"),
                open_premium_total=Decimal("0.00"),
                open_premium_per_share=Decimal("0.0000"),
                open_fee_total=Decimal("0.00"),
                net_credit_total=Decimal("0.00"),
                net_credit_per_share=Decimal("0.0000"),
                assignment_kind="put_assignment",
                source_transaction_id=None,
            )
        ],
    )

    _seed_import(
        tmp_path,
        account_name="Primary",
        account_number="ACCT-1",
        options=[
            _make_option_transaction(quantity=1, price=Decimal("3.00"), amount=Decimal("-300.00"))
        ],
        csv_name="options-seed.csv",
    )

    equities, options = fetch_positions(repository)
    assert len(equities) == 1
    assert equities[0].shares == 10
    assert len(options) == 1
    assert options[0].contracts == 1
