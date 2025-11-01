"""Tests for the PremiumFlow FastAPI application."""

from __future__ import annotations

import io
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from premiumflow.core.parser import NormalizedOptionTransaction, ParsedImportResult
from premiumflow.persistence import repository as repository_module
from premiumflow.persistence import storage as storage_module
from premiumflow.persistence.storage import store_import_result
from premiumflow.web import create_app, dependencies
from premiumflow.web.app import MIN_PAGE_SIZE
from premiumflow.web.dependencies import get_repository

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


class StubRepository:
    """Minimal stub to satisfy dependency overrides during smoke tests."""

    pass


def _make_client() -> TestClient:
    app = create_app()
    app.dependency_overrides[get_repository] = lambda: StubRepository()
    return TestClient(app)


def _make_transaction(**overrides) -> NormalizedOptionTransaction:
    """Convenience factory for normalized option transactions."""

    return NormalizedOptionTransaction(
        activity_date=overrides.get("activity_date", date(2024, 9, 1)),
        process_date=overrides.get("process_date", date(2024, 9, 2)),
        settle_date=overrides.get("settle_date", date(2024, 9, 3)),
        instrument=overrides.get("instrument", "TSLA"),
        description=overrides.get("description", "TSLA 09/20/2024 Call $240.00"),
        trans_code=overrides.get("trans_code", "STO"),
        quantity=overrides.get("quantity", 1),
        price=overrides.get("price", Decimal("2.50")),
        amount=overrides.get("amount", Decimal("250.00")),
        strike=overrides.get("strike", Decimal("240.00")),
        option_type=overrides.get("option_type", "CALL"),
        expiration=overrides.get("expiration", date(2024, 9, 20)),
        action=overrides.get("action", "SELL"),
        raw=overrides.get("raw", {"Activity Date": "09/01/2024"}),
    )


def _persist_import(
    directory: Path,
    *,
    account_name: str,
    account_number: str | None,
    csv_name: str,
    transactions: list[NormalizedOptionTransaction],
    options_only: bool = True,
    ticker: str | None = None,
    strategy: str | None = None,
    open_only: bool = False,
) -> int:
    """Seed the persistence layer and return the stored import id."""

    csv_path = directory / csv_name
    csv_path.write_text(csv_name, encoding="utf-8")
    parsed = ParsedImportResult(
        account_name=account_name,
        account_number=account_number,
        transactions=transactions,
    )
    result = store_import_result(
        parsed,
        source_path=str(csv_path),
        options_only=options_only,
        ticker=ticker,
        strategy=strategy,
        open_only=open_only,
    )
    return result.import_id


@pytest.fixture
def client_with_storage(tmp_path, monkeypatch):
    """Provide a TestClient backed by a temporary SQLite database."""

    monkeypatch.setenv(storage_module.DB_ENV_VAR, str(tmp_path / "web-ui.db"))
    storage_module.get_storage.cache_clear()
    dependencies._get_cached_repository.cache_clear()

    app = create_app()
    app.dependency_overrides[get_repository] = lambda: repository_module.SQLiteRepository()
    client = TestClient(app)

    yield client

    storage_module.get_storage.cache_clear()
    dependencies._get_cached_repository.cache_clear()


def test_health_endpoint_returns_ok():
    client = _make_client()
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_index_renders_placeholder_template():
    client = _make_client()
    response = client.get("/")
    assert response.status_code == 200
    assert "PremiumFlow web interface" in response.text


def test_upload_persists_transactions(client_with_storage):
    csv_path = FIXTURES_DIR / "options_sample.csv"
    with csv_path.open("rb") as handle:
        response = client_with_storage.post(
            "/upload",
            data={
                "account_name": "Web Account",
                "account_number": "WEB-1",
                "duplicate_strategy": "error",
                "options_only": "true",
                "open_only": "false",
            },
            files={"csv_file": ("options_sample.csv", handle, "text/csv")},
        )

    assert response.status_code == 200
    assert "Imported 3 transactions" in response.text

    repo = repository_module.SQLiteRepository()
    imports = repo.list_imports(account_name="Web Account")
    assert len(imports) == 1
    transactions = repo.fetch_transactions(import_ids=[imports[0].id])
    assert len(transactions) == 3


def test_upload_skips_existing_when_requested(client_with_storage):
    csv_path = FIXTURES_DIR / "options_sample.csv"

    with csv_path.open("rb") as handle:
        client_with_storage.post(
            "/upload",
            data={
                "account_name": "Skip Account",
                "account_number": "",
                "duplicate_strategy": "error",
                "options_only": "true",
                "open_only": "false",
            },
            files={"csv_file": ("options_sample.csv", handle, "text/csv")},
        )

    with csv_path.open("rb") as handle:
        response = client_with_storage.post(
            "/upload",
            data={
                "account_name": "Skip Account",
                "account_number": "",
                "duplicate_strategy": "skip",
                "options_only": "true",
                "open_only": "false",
            },
            files={"csv_file": ("options_sample.csv", handle, "text/csv")},
        )

    assert response.status_code == 200
    assert "Import skipped" in response.text

    repo = repository_module.SQLiteRepository()
    imports = repo.list_imports(account_name="Skip Account")
    assert len(imports) == 1


def test_upload_reports_validation_errors(client_with_storage):
    bad_csv = io.BytesIO(b"")
    response = client_with_storage.post(
        "/upload",
        data={
            "account_name": "Broken Account",
            "account_number": "",
            "duplicate_strategy": "error",
            "options_only": "true",
            "open_only": "false",
        },
        files={"csv_file": ("bad.csv", bad_csv, "text/csv")},
    )

    assert response.status_code == 200
    assert "Import validation failed" in response.text


def test_import_history_lists_recent_imports(client_with_storage, tmp_path):
    _persist_import(
        tmp_path,
        account_name="History Account A",
        account_number="ACC-1",
        csv_name="history-a.csv",
        transactions=[_make_transaction(instrument="TSLA")],
        ticker="TSLA",
        strategy="wheel",
        open_only=False,
    )
    _persist_import(
        tmp_path,
        account_name="History Account B",
        account_number=None,
        csv_name="history-b.csv",
        transactions=[_make_transaction(instrument="AAPL")],
        ticker="AAPL",
        strategy="spread",
        open_only=True,
    )

    response = client_with_storage.get("/imports")
    assert response.status_code == 200
    assert "Import history" in response.text
    assert "History Account A" in response.text
    assert "History Account B" in response.text
    # Ensure the open-only flag is displayed
    assert "Yes" in response.text


def test_import_history_filters_by_account(client_with_storage, tmp_path):
    _persist_import(
        tmp_path,
        account_name="Filter Account",
        account_number="FILTER-1",
        csv_name="filter.csv",
        transactions=[_make_transaction(instrument="TSLA")],
        ticker="TSLA",
        strategy=None,
    )
    _persist_import(
        tmp_path,
        account_name="Other Account",
        account_number="OTHER-1",
        csv_name="other.csv",
        transactions=[_make_transaction(instrument="AAPL")],
        ticker="AAPL",
        strategy=None,
    )

    response = client_with_storage.get("/imports", params={"account_name": "Filter Account"})
    assert response.status_code == 200
    assert "Filter Account" in response.text
    assert "Other Account" not in response.text


def test_import_history_paginates_results(client_with_storage, tmp_path):
    total_imports = MIN_PAGE_SIZE + 2
    for index in range(total_imports):
        _persist_import(
            tmp_path,
            account_name=f"Paginated Account {index}",
            account_number=f"PAG-{index}",
            csv_name=f"paged-{index}.csv",
            transactions=[_make_transaction(instrument=f"TICK{index}")],
            ticker=f"TICK{index}",
            strategy=None,
        )

    first_page = client_with_storage.get("/imports", params={"page_size": MIN_PAGE_SIZE})
    assert first_page.status_code == 200
    # Latest MIN_PAGE_SIZE imports should appear on the first page.
    for index in range(total_imports - 1, total_imports - MIN_PAGE_SIZE - 1, -1):
        assert f"Paginated Account {index}" in first_page.text
    assert "Paginated Account 0" not in first_page.text
    assert "Older imports →" in first_page.text
    assert "Newer imports" not in first_page.text

    second_page = client_with_storage.get(
        "/imports", params={"page": 2, "page_size": MIN_PAGE_SIZE}
    )
    assert second_page.status_code == 200
    assert "Paginated Account 0" in second_page.text
    assert "Paginated Account 1" in second_page.text
    # Second page should show link back to newer records.
    assert "← Newer imports" in second_page.text


def test_import_detail_shows_transactions(client_with_storage, tmp_path):
    import_id = _persist_import(
        tmp_path,
        account_name="Detail Account",
        account_number="DET-1",
        csv_name="detail.csv",
        transactions=[
            _make_transaction(instrument="TSLA", description="TSLA Call", trans_code="STO"),
            _make_transaction(
                instrument="AAPL",
                description="AAPL Put",
                trans_code="BTC",
                action="BUY",
                quantity=2,
                price=Decimal("1.50"),
                amount=Decimal("300.00"),
            ),
        ],
        ticker="TSLA",
        strategy="wheel",
        open_only=False,
    )

    response = client_with_storage.get(f"/imports/{import_id}")
    assert response.status_code == 200
    assert f"Import {import_id}" in response.text
    assert "Detail Account" in response.text
    assert "TSLA" in response.text
    assert "AAPL" in response.text
    assert "AAPL Put" in response.text


def test_import_detail_returns_404_for_missing_import(client_with_storage):
    response = client_with_storage.get("/imports/999999")
    assert response.status_code == 404
