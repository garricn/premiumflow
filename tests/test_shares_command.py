"""Tests for the premiumflow shares CLI command."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Iterable

import pytest
from click.testing import CliRunner

from premiumflow.cli.commands import main
from premiumflow.core.parser import NormalizedOptionTransaction, ParsedImportResult
from premiumflow.persistence import repository as repository_module
from premiumflow.persistence import storage as storage_module
from premiumflow.persistence.storage import store_import_result
from premiumflow.services.stock_lot_builder import rebuild_assignment_stock_lots


def _make_transaction(**overrides) -> NormalizedOptionTransaction:
    return NormalizedOptionTransaction(
        activity_date=overrides.get("activity_date", date(2025, 8, 28)),
        process_date=overrides.get("process_date", date(2025, 8, 29)),
        settle_date=overrides.get("settle_date", date(2025, 8, 30)),
        instrument=overrides.get("instrument", "HOOD"),
        description=overrides.get("description", "HOOD 09/06/2025 Call $104.00"),
        trans_code=overrides.get("trans_code", "STO"),
        quantity=overrides.get("quantity", 1),
        price=overrides.get("price", Decimal("1.00")),
        amount=overrides.get("amount", Decimal("100.00")),
        strike=overrides.get("strike", Decimal("100.00")),
        option_type=overrides.get("option_type", "CALL"),
        expiration=overrides.get("expiration", date(2025, 9, 6)),
        action=overrides.get("action", "SELL"),
        raw=overrides.get("raw", {"Activity Date": "08/28/2025"}),
    )


def _seed_assignment_lots(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> repository_module.SQLiteRepository:
    db_path = tmp_path / "stock.db"
    monkeypatch.setenv(storage_module.DB_ENV_VAR, str(db_path))
    storage_module.get_storage.cache_clear()

    repository = repository_module.SQLiteRepository()

    csv_path = tmp_path / "assignments.csv"
    csv_path.write_text("assignments", encoding="utf-8")

    transactions: Iterable[NormalizedOptionTransaction] = [
        _make_transaction(
            instrument="HOOD",
            description="HOOD 09/06/2025 Call $104.00",
            trans_code="STO",
            option_type="CALL",
            strike=Decimal("104.00"),
            quantity=2,
            price=Decimal("1.08"),
            amount=Decimal("216.00"),
            activity_date=date(2025, 8, 28),
        ),
        _make_transaction(
            instrument="HOOD",
            description="HOOD 09/06/2025 Call $104.00",
            trans_code="OASGN",
            option_type="CALL",
            strike=Decimal("104.00"),
            activity_date=date(2025, 9, 5),
            quantity=1,
            price=Decimal("0"),
            amount=None,
        ),
        _make_transaction(
            instrument="HOOD",
            description="HOOD 09/06/2025 Call $104.00",
            trans_code="OASGN",
            option_type="CALL",
            strike=Decimal("104.00"),
            activity_date=date(2025, 9, 6),
            quantity=1,
            price=Decimal("0"),
            amount=None,
        ),
        _make_transaction(
            instrument="ETHU",
            description="ETHU 11/01/2025 Put $110.00",
            trans_code="STO",
            option_type="PUT",
            strike=Decimal("110.00"),
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
            activity_date=date(2025, 10, 31),
            quantity=1,
            price=Decimal("0"),
            amount=None,
        ),
    ]

    parsed = ParsedImportResult(
        account_name="Primary Account",
        account_number="ACCT-1",
        transactions=list(transactions),
    )

    store_import_result(
        parsed,
        source_path=str(csv_path),
        options_only=True,
        ticker=None,
        strategy=None,
        open_only=False,
    )

    rebuild_assignment_stock_lots(
        repository,
        account_name="Primary Account",
        account_number="ACCT-1",
    )

    return repository


def test_shares_command_renders_table(tmp_path, monkeypatch):
    _seed_assignment_lots(tmp_path, monkeypatch)

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "shares",
            "--account-name",
            "Primary Account",
            "--account-number",
            "ACCT-1",
        ],
    )

    storage_module.get_storage.cache_clear()

    assert result.exit_code == 0
    assert "Stock Lots" in result.output
    assert "HOOD" in result.output
    assert "ETHU" in result.output
    assert "$10,508.00" in result.output
    assert "$10,825.00" in result.output


def test_shares_command_json_output(tmp_path, monkeypatch):
    _seed_assignment_lots(tmp_path, monkeypatch)

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "shares",
            "--account-name",
            "Primary Account",
            "--account-number",
            "ACCT-1",
            "--format",
            "json",
        ],
    )

    storage_module.get_storage.cache_clear()

    assert result.exit_code == 0
    payload = json.loads(result.output)
    lots = payload["lots"]
    assert len(lots) == 3
    symbols = {lot["symbol"] for lot in lots}
    assert symbols == {"HOOD", "ETHU"}

    ethus = [lot for lot in lots if lot["symbol"] == "ETHU"]
    assert ethus and ethus[0]["basis_total"] == "10825"
    hoods = [lot for lot in lots if lot["symbol"] == "HOOD"]
    assert len(hoods) == 2
    assert all(lot["basis_total"] == "10508" for lot in hoods)
