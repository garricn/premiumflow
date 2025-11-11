"""Tests for handling transfer and ACH stock rows during import."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from premiumflow.core.parser import load_option_transactions
from premiumflow.persistence import repository as repository_module
from premiumflow.persistence import storage as storage_module
from premiumflow.persistence.storage import store_import_result


@pytest.fixture(autouse=True)
def clear_storage_cache():
    storage_module.get_storage.cache_clear()
    yield
    storage_module.get_storage.cache_clear()


def _write_csv(tmp_dir: Path) -> Path:
    content = """Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
09/05/2025,09/05/2025,09/05/2025,TSLA,ACATS Transfer In,ACATI,10,,
09/06/2025,09/06/2025,09/06/2025,,ACH Deposit,ACHDEP,,,$1500.00
"""
    csv_path = tmp_dir / "stock_transfers.csv"
    csv_path.write_text(content, encoding="utf-8")
    return csv_path


def test_transfer_and_ach_rows_are_normalized_and_persisted(tmp_path, monkeypatch):
    csv_path = _write_csv(tmp_path)
    parsed = load_option_transactions(
        str(csv_path),
        account_name="Transfer Account",
        account_number="XFER-123",
    )

    assert len(parsed.stock_transactions) == 2
    transfer_txn, ach_txn = parsed.stock_transactions

    assert transfer_txn.trans_code == "ACATI"
    assert transfer_txn.instrument == "TSLA"
    assert transfer_txn.action == "BUY"
    assert transfer_txn.quantity == Decimal("10")
    assert transfer_txn.price == Decimal("0")
    assert transfer_txn.amount == Decimal("0")

    assert ach_txn.trans_code == "ACHDEP"
    assert ach_txn.instrument == "ACHDEP"
    assert ach_txn.action == "BUY"
    assert ach_txn.quantity == Decimal("0")
    assert ach_txn.price == Decimal("0")
    assert ach_txn.amount == Decimal("1500.00")

    db_path = tmp_path / "premiumflow.db"
    monkeypatch.setenv(storage_module.DB_ENV_VAR, str(db_path))

    store_import_result(
        parsed,
        source_path=str(csv_path),
        options_only=False,
        ticker=None,
        strategy=None,
        open_only=False,
    )

    repository = repository_module.SQLiteRepository()
    stored = repository.fetch_stock_transactions()
    codes = {row.trans_code for row in stored}
    assert {"ACATI", "ACHDEP"} <= codes
