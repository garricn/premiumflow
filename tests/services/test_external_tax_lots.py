'"""Tests for external tax lot importer."""'

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest

from premiumflow.persistence import repository as repository_module
from premiumflow.persistence import storage as storage_module
from premiumflow.services.external_tax_lots import import_external_tax_lot_snapshot


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


def test_import_external_tax_lot_snapshot_resolves_pending_entries(
    repository, monkeypatch, tmp_path
):
    sample_text = """\
Open Date    Hold Date    Security ID                             Security                   Units           Price      Book Cost       Adj     Tax Cost    GL Term   Proceeds       Adj
09/05/2025    09/05/2025 922908769   VANGUARD TOTAL STOCK MARKET (VTI)     577.    166.9341    (96,321.00)             (96,321.00) st
"""
    monkeypatch.setattr(
        "premiumflow.services.external_tax_lots._extract_pdf_text",
        lambda path: sample_text,
    )

    pdf_path = tmp_path / "unrealized.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n% Dummy content\n")

    storage = repository._storage  # type: ignore[attr-defined]
    storage._ensure_initialized()  # type: ignore[attr-defined]
    timestamp = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    with storage._connect() as conn:  # type: ignore[attr-defined]
        account_id = storage._get_or_create_account(conn, "Transfer Account", "XFER-123")  # type: ignore[attr-defined]
        cursor = conn.execute(
            """
            INSERT INTO transfer_basis_items (
                account_id,
                instrument,
                activity_date,
                shares,
                trans_code,
                description,
                status,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)
            """,
            (
                account_id,
                "VTI",
                "2025-09-05",
                "577",
                "ACATI",
                "ACATS transfer in",
                timestamp,
                timestamp,
            ),
        )
        item_id = int(cursor.lastrowid)

    result = import_external_tax_lot_snapshot(
        repository,
        pdf_path=pdf_path,
        account_name="Transfer Account",
        account_number="XFER-123",
        snapshot_label="test-snapshot",
    )

    assert result.total_snapshot_lots == 1
    assert result.stored_snapshot_lots == 1
    assert result.resolved_transfer_items == 1
    assert result.unresolved_transfer_items == []
    assert result.ambiguous_transfer_items == []
    assert result.resolution_errors == []

    updated = repository.get_transfer_basis_item_by_id(item_id)
    assert updated is not None
    assert updated.status == "resolved"
    assert updated.basis_total == Decimal("96321.00")
    assert updated.basis_per_share == Decimal("166.9341")


def test_import_external_tax_lot_snapshot_allocates_fifo(repository, monkeypatch, tmp_path):
    sample_text = """\
Open Date    Hold Date    Security ID                             Security                   Units           Price      Book Cost       Adj     Tax Cost    GL Term   Proceeds       Adj
07/08/2025    07/08/2025 111111111   SAMPLE ETF (SAMP)                     400.          5.00    (2,000.00)             (2,000.00) st
07/09/2025    07/09/2025 111111111   SAMPLE ETF (SAMP)                     200.          6.50    (1,300.00)             (1,300.00) st
"""
    monkeypatch.setattr(
        "premiumflow.services.external_tax_lots._extract_pdf_text",
        lambda path: sample_text,
    )

    pdf_path = tmp_path / "lots.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n")

    storage = repository._storage  # type: ignore[attr-defined]
    storage._ensure_initialized()  # type: ignore[attr-defined]
    timestamp = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    with storage._connect() as conn:  # type: ignore[attr-defined]
        account_id = storage._get_or_create_account(conn, "FIFO Account", None)  # type: ignore[attr-defined]
        conn.executemany(
            """
            INSERT INTO transfer_basis_items (
                account_id,
                instrument,
                activity_date,
                shares,
                trans_code,
                description,
                status,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)
            """,
            [
                (
                    account_id,
                    "SAMP",
                    "2025-08-01",
                    "400",
                    "ACATI",
                    "Transfer row A",
                    timestamp,
                    timestamp,
                ),
                (
                    account_id,
                    "SAMP",
                    "2025-08-02",
                    "200",
                    "ACATI",
                    "Transfer row B",
                    timestamp,
                    timestamp,
                ),
            ],
        )

    result = import_external_tax_lot_snapshot(
        repository,
        pdf_path=pdf_path,
        account_name="FIFO Account",
        account_number=None,
        snapshot_label="fifo-test",
    )

    assert result.resolved_transfer_items == 2
    resolved = repository.list_transfer_basis_items(
        account_name="FIFO Account",
        account_number=None,
        statuses=("resolved",),
    )

    basis_by_shares = {item.shares: item for item in resolved if item.instrument == "SAMP"}
    assert Decimal("400") in basis_by_shares
    assert Decimal("200") in basis_by_shares

    assert basis_by_shares[Decimal("200")].basis_total == Decimal("1000.00")
    assert basis_by_shares[Decimal("200")].basis_per_share == Decimal("5.0000")
    assert basis_by_shares[Decimal("400")].basis_total == Decimal("2300.00")
    assert basis_by_shares[Decimal("400")].basis_per_share == Decimal("5.7500")
