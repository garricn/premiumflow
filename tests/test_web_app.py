"""Tests for the PremiumFlow FastAPI application."""

from __future__ import annotations

import io
from datetime import date
from decimal import Decimal
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest
from fastapi.testclient import TestClient

from premiumflow.core.parser import NormalizedOptionTransaction, ParsedImportResult
from premiumflow.persistence import repository as repository_module
from premiumflow.persistence import storage as storage_module
from premiumflow.persistence.storage import store_import_result
from premiumflow.services.stock_lot_builder import rebuild_assignment_stock_lots
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


def _persist_import(  # noqa: PLR0913
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


def _seed_assignment_stock_lots(tmp_path: Path) -> None:
    """Populate the persistence layer with assignment-driven stock lots."""

    csv_path = tmp_path / "assignment-stock.csv"
    csv_path.write_text("assignment lots", encoding="utf-8")

    transactions = [
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
        account_name="Lot Account",
        account_number="LOTS-1",
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

    repository = repository_module.SQLiteRepository()
    rebuild_assignment_stock_lots(
        repository,
        account_name="Lot Account",
        account_number="LOTS-1",
    )


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


def test_smoke_web_app_serves_primary_ui_routes(client_with_storage, tmp_path):
    """Smoke test that boots the real app and hits `/` and `/cashflow`."""

    account_name = "Smoke Account"
    account_number = "SMOKE-1"
    _persist_import(
        tmp_path,
        account_name=account_name,
        account_number=account_number,
        csv_name="smoke.csv",
        transactions=[_make_transaction()],
    )

    index_response = client_with_storage.get("/")
    assert index_response.status_code == 200
    assert "PremiumFlow web interface" in index_response.text

    cashflow_response = client_with_storage.get(
        "/cashflow",
        params={"account": f"{account_name}|{account_number}"},
    )
    assert cashflow_response.status_code == 200
    assert "Cash Flow" in cashflow_response.text
    assert "Total Cash Flow" in cashflow_response.text


def test_imports_and_legs_routes_render_with_real_data(client_with_storage, tmp_path):
    """Smoke test for `/imports`, `/imports/{id}` and `/legs` using the real database."""

    account_name = "Primed Account"
    account_number = "PRIMED-1"
    import_id = _persist_import(
        tmp_path,
        account_name=account_name,
        account_number=account_number,
        csv_name="imports-smoke.csv",
        transactions=[_make_transaction()],
        ticker="TSLA",
        strategy="covered-call",
    )

    imports_response = client_with_storage.get("/imports")
    assert imports_response.status_code == 200
    assert "Import history" in imports_response.text
    assert account_name in imports_response.text

    detail_response = client_with_storage.get(f"/imports/{import_id}")
    assert detail_response.status_code == 200
    assert f"Import {import_id}" in detail_response.text

    legs_response = client_with_storage.get("/legs")
    assert legs_response.status_code == 200
    # Allow either populated legs or the empty-state copy to prove the view still renders.
    assert "Matched Legs" in legs_response.text or "No matched legs found" in legs_response.text


def test_stock_lots_route_accepts_seeded_assignment(tmp_path, client_with_storage):
    """Smoke test that renders `/stock-lots` after seeding assignment-driven lots."""

    _seed_assignment_stock_lots(tmp_path)
    stock_lots_response = client_with_storage.get("/stock-lots")
    assert stock_lots_response.status_code == 200
    assert "Stock Lots" in stock_lots_response.text


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
                "account_number": "SKIP-123",
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
                "account_number": "SKIP-123",
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
            "account_number": "BROKEN-123",
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
    assert "Activity start" in response.text
    assert "Activity end" in response.text
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


def test_import_history_shows_activity_range(client_with_storage, tmp_path):
    _persist_import(
        tmp_path,
        account_name="Range Account",
        account_number="RANGE-1",
        csv_name="range.csv",
        transactions=[
            _make_transaction(activity_date=date(2024, 9, 2)),
            _make_transaction(activity_date=date(2024, 9, 5)),
        ],
    )

    response = client_with_storage.get("/imports")
    assert response.status_code == 200
    assert "2024-09-02" in response.text
    assert "2024-09-05" in response.text
    assert "Delete" in response.text


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
    assert "Activity start" in response.text
    assert "Activity end" in response.text
    assert "Delete import" in response.text


def test_import_detail_returns_404_for_missing_import(client_with_storage):
    response = client_with_storage.get("/imports/999999")
    assert response.status_code == 404


def test_delete_import_removes_record_and_shows_message(client_with_storage, tmp_path):
    import_id = _persist_import(
        tmp_path,
        account_name="Delete Account",
        account_number="DEL-1",
        csv_name="delete.csv",
        transactions=[_make_transaction(activity_date=date(2024, 9, 10))],
    )

    response = client_with_storage.post(
        f"/imports/{import_id}/delete",
        data={"page": "1", "page_size": str(MIN_PAGE_SIZE)},
        follow_redirects=False,
    )
    assert response.status_code == 303
    location = response.headers["location"]
    parsed = urlparse(location)
    assert parsed.path == "/imports"
    params = parse_qs(parsed.query)
    assert params["message"] == ["deleted"]
    assert params["deleted_id"] == [str(import_id)]
    assert params["account_label"] == ["Delete Account (DEL-1)"]
    assert params["source_filename"] == ["delete.csv"]

    follow = client_with_storage.get(location)
    assert follow.status_code == 200
    assert "Import deleted" in follow.text
    assert "Re-upload the original CSV" in follow.text

    repo = repository_module.SQLiteRepository()
    assert repo.get_import(import_id) is None


def test_delete_import_returns_404_for_missing_import(client_with_storage):
    response = client_with_storage.post("/imports/999999/delete", follow_redirects=False)
    assert response.status_code == 404


def test_legs_view_renders_template(client_with_storage, tmp_path):
    """Legs view renders HTML template with matched legs."""
    _persist_import(
        tmp_path,
        account_name="Legs Account",
        account_number="LEG-1",
        csv_name="legs.csv",
        transactions=[
            _make_transaction(
                instrument="TSLA",
                description="TSLA 10/17/2025 Call $500.00",
                trans_code="STO",
                quantity=1,
                price=Decimal("2.50"),
                amount=Decimal("250.00"),
                strike=Decimal("500.00"),
                expiration=date(2025, 10, 17),
            ),
            _make_transaction(
                instrument="TSLA",
                description="TSLA 10/17/2025 Call $500.00",
                trans_code="OEXP",
                quantity=1,
                price=Decimal("0.00"),
                amount=Decimal("0.00"),
                strike=Decimal("500.00"),
                expiration=date(2025, 10, 17),
                activity_date=date(2025, 10, 17),
            ),
        ],
    )

    response = client_with_storage.get("/legs")
    assert response.status_code == 200
    assert "Matched Legs" in response.text
    assert "TSLA" in response.text
    assert "Legs Account" in response.text


def test_legs_view_filters_by_account(client_with_storage, tmp_path):
    """Legs view filters by account name and number."""
    _persist_import(
        tmp_path,
        account_name="Filter Account",
        account_number="FILTER-1",
        csv_name="filter.csv",
        transactions=[_make_transaction(instrument="TSLA")],
    )
    _persist_import(
        tmp_path,
        account_name="Other Account",
        account_number="OTHER-1",
        csv_name="other.csv",
        transactions=[_make_transaction(instrument="AAPL")],
    )

    response = client_with_storage.get("/legs", params={"account_name": "Filter Account"})
    assert response.status_code == 200
    assert "Filter Account" in response.text
    assert "Other Account" not in response.text


def test_legs_view_filters_by_ticker(client_with_storage, tmp_path):
    """Legs view filters by ticker symbol."""
    _persist_import(
        tmp_path,
        account_name="Ticker Account",
        account_number="TICK-1",
        csv_name="ticker.csv",
        transactions=[
            _make_transaction(instrument="TSLA"),
            _make_transaction(instrument="AAPL"),
        ],
    )

    response = client_with_storage.get("/legs", params={"ticker": "TSLA"})
    assert response.status_code == 200
    assert "TSLA" in response.text
    assert "AAPL" not in response.text


def test_legs_view_filters_by_status(client_with_storage, tmp_path):
    """Legs view filters by open/closed status."""
    _persist_import(
        tmp_path,
        account_name="Status Account",
        account_number="STAT-1",
        csv_name="status.csv",
        transactions=[
            _make_transaction(
                instrument="TSLA",
                description="TSLA 10/17/2025 Call $500.00",
                trans_code="STO",
                quantity=1,
                expiration=date(2025, 10, 17),
            ),
            _make_transaction(
                instrument="AAPL",
                description="AAPL 10/17/2025 Call $200.00",
                trans_code="STO",
                quantity=1,
                expiration=date(2025, 10, 17),
            ),
            _make_transaction(
                instrument="AAPL",
                description="AAPL 10/17/2025 Call $200.00",
                trans_code="OEXP",
                quantity=1,
                expiration=date(2025, 10, 17),
                activity_date=date(2025, 10, 17),
            ),
        ],
    )

    open_response = client_with_storage.get("/legs", params={"status": "open"})
    assert open_response.status_code == 200
    assert "TSLA" in open_response.text
    assert "AAPL" not in open_response.text

    closed_response = client_with_storage.get("/legs", params={"status": "closed"})
    assert closed_response.status_code == 200
    assert "AAPL" in closed_response.text
    assert "TSLA" not in closed_response.text


def test_legs_api_returns_json(client_with_storage, tmp_path):
    """Legs API endpoint returns JSON data."""
    _persist_import(
        tmp_path,
        account_name="API Account",
        account_number="API-1",
        csv_name="api.csv",
        transactions=[
            _make_transaction(
                instrument="TSLA",
                description="TSLA 10/17/2025 Call $500.00",
                trans_code="STO",
                quantity=1,
                price=Decimal("2.50"),
                amount=Decimal("250.00"),
                strike=Decimal("500.00"),
                expiration=date(2025, 10, 17),
            ),
        ],
    )

    response = client_with_storage.get("/api/legs")
    assert response.status_code == 200
    data = response.json()
    assert "legs" in data
    assert "warnings" in data
    assert isinstance(data["legs"], list)
    assert isinstance(data["warnings"], list)
    if data["legs"]:
        leg = data["legs"][0]
        assert "contract" in leg
        assert "account_name" in leg
        assert "lots" in leg


def test_legs_api_filters_work(client_with_storage, tmp_path):
    """Legs API respects filter parameters."""
    _persist_import(
        tmp_path,
        account_name="API Filter Account",
        account_number="API-FILTER-1",
        csv_name="api-filter.csv",
        transactions=[
            _make_transaction(instrument="TSLA"),
            _make_transaction(instrument="AAPL"),
        ],
    )

    response = client_with_storage.get("/api/legs", params={"ticker": "TSLA"})
    assert response.status_code == 200
    data = response.json()
    assert all(leg["contract"]["symbol"] == "TSLA" for leg in data["legs"])


def test_stock_lots_api_returns_json(client_with_storage, tmp_path):
    """Stock lots API endpoint returns serialized stock lots."""
    _seed_assignment_stock_lots(tmp_path)

    response = client_with_storage.get(
        "/api/stock-lots",
        params={"account_name": "Lot Account", "account_number": "LOTS-1"},
    )
    assert response.status_code == 200
    payload = response.json()
    lots = payload.get("lots", [])
    assert len(lots) == 3

    symbols = {lot["symbol"] for lot in lots}
    assert symbols == {"HOOD", "ETHU"}

    hood_lots = [lot for lot in lots if lot["symbol"] == "HOOD"]
    assert len(hood_lots) == 2
    assert all(lot["status"] == "open" for lot in hood_lots)
    assert all(lot["basis_total"] == "10508" for lot in hood_lots)

    ethus = [lot for lot in lots if lot["symbol"] == "ETHU"]
    assert ethus and ethus[0]["basis_total"] == "10825"


def test_stock_lots_api_validates_status(client_with_storage):
    """Stock lots API rejects unsupported status values."""
    response = client_with_storage.get("/api/stock-lots", params={"status": "archived"})
    assert response.status_code == 400
    assert response.json()["detail"] == "Unsupported status filter"


def test_stock_lots_view_lists_lots(client_with_storage, tmp_path):
    """Stock lots page renders persisted lots with summary metrics."""
    _seed_assignment_stock_lots(tmp_path)

    response = client_with_storage.get(
        "/stock-lots",
        params={"account_name": "Lot Account", "account_number": "LOTS-1"},
    )

    assert response.status_code == 200
    body = response.text
    assert "Stock Lots" in body
    assert "HOOD" in body and "ETHU" in body
    assert "$10,508.00" in body
    assert "Total lots" in body


def test_stock_lots_view_filters_by_ticker(client_with_storage, tmp_path):
    """Ticker filter narrows stock lots table."""
    _seed_assignment_stock_lots(tmp_path)

    response = client_with_storage.get("/stock-lots", params={"ticker": "ETHU"})

    assert response.status_code == 200
    body = response.text
    assert "ETHU" in body
    assert "HOOD" not in body


def test_stock_lots_view_filters_by_opened_date(client_with_storage, tmp_path):
    """Opened date range filter trims lots outside range."""
    _seed_assignment_stock_lots(tmp_path)

    response = client_with_storage.get("/stock-lots", params={"opened_from": "2025-10-01"})

    assert response.status_code == 200
    body = response.text
    assert "ETHU" in body
    assert "HOOD" not in body


def test_stock_lots_view_empty_state(client_with_storage):
    """Stock lots page shows empty-state message when nothing matches."""
    response = client_with_storage.get("/stock-lots", params={"ticker": "XYZ"})

    assert response.status_code == 200
    assert "No stock lots found for the selected filters." in response.text


def test_legs_view_empty_state(client_with_storage):
    """Legs view shows empty state when no legs exist."""
    response = client_with_storage.get("/legs")
    assert response.status_code == 200
    assert "No matched legs found" in response.text


def test_cashflow_view_renders_template(client_with_storage, tmp_path):
    """Cashflow view renders HTML template with report data."""
    _persist_import(
        tmp_path,
        account_name="Cashflow Account",
        account_number="CF-1",
        csv_name="cashflow.csv",
        transactions=[
            _make_transaction(
                instrument="TSLA",
                description="TSLA 10/17/2025 Call $500.00",
                trans_code="STO",
                quantity=1,
                price=Decimal("2.50"),
                amount=Decimal("250.00"),
                strike=Decimal("500.00"),
                expiration=date(2025, 10, 17),
            ),
        ],
    )

    response = client_with_storage.get(
        "/cashflow",
        params={"account": "Cashflow Account|CF-1"},
    )
    assert response.status_code == 200
    assert "Cash Flow" in response.text and "P&L" in response.text
    assert "Total Cash Flow" in response.text
    assert "Period" in response.text
    assert "Profits (Before Fees • Options)" in response.text
    assert "Realized P&L (After Fees • Options)" in response.text
    assert "Opening Fees" in response.text
    assert "Gross P&L" not in response.text
    assert "Assignment Premium (After Fees)" in response.text
    assert "Assignment premium" in response.text
    assert "Realized totals" in response.text


def test_cashflow_view_filters_by_account(client_with_storage, tmp_path):
    """Cashflow view filters by account name."""
    _persist_import(
        tmp_path,
        account_name="Filter Account",
        account_number="FILTER-1",
        csv_name="filter.csv",
        transactions=[_make_transaction(instrument="TSLA", amount=Decimal("100.00"))],
    )
    _persist_import(
        tmp_path,
        account_name="Other Account",
        account_number="OTHER-1",
        csv_name="other.csv",
        transactions=[_make_transaction(instrument="AAPL", amount=Decimal("200.00"))],
    )

    response = client_with_storage.get(
        "/cashflow",
        params={"account": "Filter Account|FILTER-1"},
    )
    assert response.status_code == 200
    assert "Filter Account" in response.text


def test_cashflow_view_filters_by_period(client_with_storage, tmp_path):
    """Cashflow view filters by time period."""
    _persist_import(
        tmp_path,
        account_name="Period Account",
        account_number="PERIOD-1",
        csv_name="period.csv",
        transactions=[
            _make_transaction(
                instrument="TSLA",
                amount=Decimal("100.00"),
                activity_date=date(2024, 9, 1),
            ),
            _make_transaction(
                instrument="TSLA",
                amount=Decimal("200.00"),
                activity_date=date(2024, 9, 15),
            ),
        ],
    )

    response = client_with_storage.get(
        "/cashflow",
        params={
            "account": "Period Account|PERIOD-1",
            "period": "monthly",
        },
    )
    assert response.status_code == 200
    assert "Cash Flow" in response.text and "P&L" in response.text


def test_cashflow_view_filters_by_date_range(client_with_storage, tmp_path):
    """Cashflow view filters by date range."""
    _persist_import(
        tmp_path,
        account_name="Date Account",
        account_number="DATE-1",
        csv_name="date.csv",
        transactions=[
            _make_transaction(
                instrument="TSLA",
                amount=Decimal("100.00"),
                activity_date=date(2024, 9, 1),
            ),
            _make_transaction(
                instrument="TSLA",
                amount=Decimal("200.00"),
                activity_date=date(2024, 9, 15),
            ),
        ],
    )

    response = client_with_storage.get(
        "/cashflow",
        params={
            "account": "Date Account|DATE-1",
            "since": "2024-09-01",
            "until": "2024-09-10",
        },
    )
    assert response.status_code == 200
    assert "Cash Flow" in response.text and "P&L" in response.text


def test_cashflow_view_empty_state_no_account(client_with_storage):
    """Cashflow view shows empty state when no accounts exist."""
    response = client_with_storage.get("/cashflow")
    assert response.status_code == 200
    assert "No accounts found" in response.text


def test_cashflow_view_empty_state_no_transactions(client_with_storage):
    """Cashflow view shows empty state when no transactions exist for account."""
    response = client_with_storage.get(
        "/cashflow",
        params={"account": "Empty Account|EMPTY-1"},
    )
    assert response.status_code == 200
    assert "No transactions found" in response.text


def test_cashflow_api_returns_json(client_with_storage, tmp_path):
    """Cashflow API endpoint returns JSON data."""
    _persist_import(
        tmp_path,
        account_name="API Account",
        account_number="API-1",
        csv_name="api.csv",
        transactions=[
            _make_transaction(
                instrument="TSLA",
                description="TSLA 10/17/2025 Call $500.00",
                trans_code="STO",
                quantity=1,
                price=Decimal("2.50"),
                amount=Decimal("250.00"),
                strike=Decimal("500.00"),
                expiration=date(2025, 10, 17),
            ),
        ],
    )

    response = client_with_storage.get(
        "/api/cashflow",
        params={"account": "API Account|API-1"},
    )
    assert response.status_code == 200
    data = response.json()
    assert "account_name" in data
    assert "account_number" in data
    assert "period_type" in data
    assert "periods" in data
    assert "totals" in data
    assert isinstance(data["periods"], list)
    assert "credits" in data["totals"]
    assert "debits" in data["totals"]
    assert "net_cash_flow" in data["totals"]
    assert "realized_profits_gross" in data["totals"]
    assert "realized_losses_gross" in data["totals"]
    assert "realized_pnl_net" in data["totals"]
    assert "opening_fees" in data["totals"]
    assert "closing_fees" in data["totals"]
    assert "assignment_realized_net" in data["totals"]
    assert "realized_breakdowns" in data["totals"]
    assert "options" in data["totals"]["realized_breakdowns"]


def test_cashflow_api_assignment_handling_toggle(client_with_storage, tmp_path):
    """API should drop assignment premium from totals when asked."""
    _persist_import(
        tmp_path,
        account_name="Toggle Account",
        account_number="TOGGLE-1",
        csv_name="toggle.csv",
        transactions=[
            _make_transaction(
                instrument="HOOD",
                description="HOOD 09/06/2025 Call $104.00",
                trans_code="STO",
                activity_date=date(2025, 9, 1),
                price=Decimal("1.00"),
                amount=Decimal("100.00"),
                expiration=date(2025, 9, 6),
            ),
            _make_transaction(
                instrument="HOOD",
                description="HOOD 09/06/2025 Call $104.00",
                trans_code="OASGN",
                activity_date=date(2025, 9, 5),
                price=Decimal("0.00"),
                amount=Decimal("0.00"),
                expiration=date(2025, 9, 6),
            ),
            _make_transaction(
                instrument="META",
                description="META 11/15/2025 Put $300.00",
                trans_code="STO",
                activity_date=date(2025, 9, 1),
                price=Decimal("2.00"),
                amount=Decimal("200.00"),
                expiration=date(2025, 11, 15),
            ),
            _make_transaction(
                instrument="META",
                description="META 11/15/2025 Put $300.00",
                trans_code="BTC",
                activity_date=date(2025, 9, 5),
                price=Decimal("1.00"),
                amount=Decimal("-100.00"),
                expiration=date(2025, 11, 15),
            ),
        ],
    )

    include_resp = client_with_storage.get(
        "/api/cashflow",
        params={"account": "Toggle Account|TOGGLE-1"},
    )
    exclude_resp = client_with_storage.get(
        "/api/cashflow",
        params={"account": "Toggle Account|TOGGLE-1", "assignment_handling": "exclude"},
    )

    assert include_resp.status_code == 200
    assert exclude_resp.status_code == 200

    include_totals = include_resp.json()["totals"]
    exclude_totals = exclude_resp.json()["totals"]
    assert include_totals["assignment_realized_net"] == "100.00"
    assert include_totals["realized_pnl_net"] != exclude_totals["realized_pnl_net"]
    assert exclude_totals["realized_pnl_net"] == "100.00"


def test_cashflow_api_filters_work(client_with_storage, tmp_path):
    """Cashflow API respects filter parameters."""
    _persist_import(
        tmp_path,
        account_name="API Filter Account",
        account_number="API-FILTER-1",
        csv_name="api-filter.csv",
        transactions=[
            _make_transaction(instrument="TSLA", amount=Decimal("100.00")),
            _make_transaction(instrument="AAPL", amount=Decimal("200.00")),
        ],
    )

    response = client_with_storage.get(
        "/api/cashflow",
        params={
            "account": "API Filter Account|API-FILTER-1",
            "ticker": "TSLA",
            "period": "total",
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["period_type"] == "total"
    assert "periods" in data
    assert "totals" in data


def test_cashflow_view_defaults_to_first_account(client_with_storage, tmp_path):
    """Cashflow view defaults to first account when no account is selected."""
    _persist_import(
        tmp_path,
        account_name="First Account",
        account_number="FIRST-1",
        csv_name="first.csv",
        transactions=[
            _make_transaction(
                instrument="TSLA",
                amount=Decimal("100.00"),
            ),
        ],
    )
    _persist_import(
        tmp_path,
        account_name="Second Account",
        account_number="SECOND-1",
        csv_name="second.csv",
        transactions=[
            _make_transaction(
                instrument="AAPL",
                amount=Decimal("200.00"),
            ),
        ],
    )

    # Load without account parameter - should default to first account
    response = client_with_storage.get("/cashflow")
    assert response.status_code == 200
    assert "First Account" in response.text
    assert "Cash Flow" in response.text and "P&L" in response.text


def test_cashflow_api_requires_account(client_with_storage):
    """Cashflow API requires account parameter."""
    response = client_with_storage.get("/api/cashflow")
    assert response.status_code == 400
    assert "account is required" in response.text


def test_cashflow_view_validates_date_range(client_with_storage, tmp_path):
    """Cashflow view shows error when start date is after end date."""
    _persist_import(
        tmp_path,
        account_name="Date Account",
        account_number="DATE-1",
        csv_name="date.csv",
        transactions=[
            _make_transaction(
                instrument="TSLA",
                amount=Decimal("100.00"),
                activity_date=date(2024, 9, 1),
            ),
        ],
    )

    response = client_with_storage.get(
        "/cashflow",
        params={
            "account": "Date Account|DATE-1",
            "since": "2024-09-10",
            "until": "2024-09-01",
        },
    )
    assert response.status_code == 200
    assert "Start date must be before or equal to end date" in response.text


def test_cashflow_api_validates_date_range(client_with_storage, tmp_path):
    """Cashflow API returns error when start date is after end date."""
    _persist_import(
        tmp_path,
        account_name="Date Account",
        account_number="DATE-1",
        csv_name="date.csv",
        transactions=[
            _make_transaction(
                instrument="TSLA",
                amount=Decimal("100.00"),
                activity_date=date(2024, 9, 1),
            ),
        ],
    )

    response = client_with_storage.get(
        "/api/cashflow",
        params={
            "account": "Date Account|DATE-1",
            "since": "2024-09-10",
            "until": "2024-09-01",
        },
    )
    assert response.status_code == 400
    assert "Start date must be before or equal to end date" in response.json()["detail"]


def test_cashflow_api_supports_account_without_number(client_with_storage, tmp_path):
    """Cashflow API supports accounts without account numbers."""
    _persist_import(
        tmp_path,
        account_name="Test Account",
        account_number=None,  # Account without number
        csv_name="test.csv",
        transactions=[_make_transaction(instrument="TSLA", amount=Decimal("100.00"))],
    )

    # Test with account name only (no account number) - should work
    response = client_with_storage.get("/api/cashflow", params={"account": "Test Account"})
    assert response.status_code == 200
    data = response.json()
    assert data["account_name"] == "Test Account"
    assert data["account_number"] is None
