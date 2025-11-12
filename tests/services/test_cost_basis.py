"""Tests for cost basis override services."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from premiumflow.core.parser import load_option_transactions
from premiumflow.persistence import repository as repository_module
from premiumflow.persistence import storage as storage_module
from premiumflow.persistence.storage import store_import_result
from premiumflow.services.cost_basis import (
    CostBasisNotFoundError,
    get_due_transfer_basis_items,
    resolve_transfer_basis_override,
    snooze_transfer_basis_item,
)


@pytest.fixture(autouse=True)
def clear_storage_cache():
    """Ensure storage cache is reset between tests."""

    storage_module.get_storage.cache_clear()
    yield
    storage_module.get_storage.cache_clear()


@pytest.fixture
def repository(tmp_path, monkeypatch):
    db_path = tmp_path / "premiumflow.db"
    monkeypatch.setenv(storage_module.DB_ENV_VAR, str(db_path))
    storage_module.get_storage.cache_clear()
    return repository_module.SQLiteRepository()


def _write_transfer_csv(tmp_dir: Path) -> Path:
    content = """Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
09/05/2025,09/05/2025,09/05/2025,VTI,ACATS Transfer In,ACATI,577,,
"""
    csv_path = tmp_dir / "acats.csv"
    csv_path.write_text(content, encoding="utf-8")
    return csv_path


def _seed_transfer_import(tmp_dir: Path, repository):
    csv_path = _write_transfer_csv(tmp_dir)
    parsed = load_option_transactions(
        str(csv_path),
        account_name="Transfer Account",
        account_number="XFER-123",
    )
    store_import_result(
        parsed,
        source_path=str(csv_path),
        options_only=False,
        ticker=None,
        strategy=None,
        open_only=False,
    )
    return parsed


def test_get_due_transfer_basis_items_detects_pending(repository, tmp_path):
    _seed_transfer_import(tmp_path, repository)

    due_items = get_due_transfer_basis_items(
        repository,
        account_name="Transfer Account",
        account_number="XFER-123",
    )
    assert len(due_items) == 1
    item = due_items[0]
    assert item.instrument == "VTI"
    assert item.status == "pending"
    assert item.shares == Decimal("577")


def test_resolve_transfer_basis_override_updates_entry(repository, tmp_path):
    _seed_transfer_import(tmp_path, repository)

    resolved = resolve_transfer_basis_override(
        repository,
        account_name="Transfer Account",
        account_number="XFER-123",
        instrument="VTI",
        activity_date=datetime(2025, 9, 5).date(),
        shares=Decimal("577"),
        basis_total=Decimal("96321.00"),
    )

    assert resolved.status == "resolved"
    assert resolved.basis_total == Decimal("96321.00")
    assert resolved.basis_per_share == Decimal("166.9341")

    due_items = get_due_transfer_basis_items(
        repository,
        account_name="Transfer Account",
        account_number="XFER-123",
    )
    assert due_items == []


def test_resolve_transfer_basis_override_requires_match(repository, tmp_path):
    _seed_transfer_import(tmp_path, repository)

    with pytest.raises(CostBasisNotFoundError):
        resolve_transfer_basis_override(
            repository,
            account_name="Transfer Account",
            account_number="XFER-123",
            instrument="SPY",
            activity_date=datetime(2025, 9, 5).date(),
            shares=Decimal("10"),
            basis_total=Decimal("1000.00"),
        )


def test_snooze_transfer_basis_item_defers_warning(repository, tmp_path):
    _seed_transfer_import(tmp_path, repository)

    due_items = get_due_transfer_basis_items(
        repository,
        account_name="Transfer Account",
        account_number="XFER-123",
    )
    item = due_items[0]

    remind_after = datetime.utcnow() + timedelta(days=2)
    snoozed = snooze_transfer_basis_item(
        repository,
        item_id=item.id,
        remind_after=remind_after,
    )

    assert snoozed.status == "snoozed"
    assert snoozed.remind_after is not None

    still_due = get_due_transfer_basis_items(
        repository,
        account_name="Transfer Account",
        account_number="XFER-123",
    )
    assert still_due == []
