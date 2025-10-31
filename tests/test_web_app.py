"""Tests for the PremiumFlow FastAPI application."""

from __future__ import annotations

import io
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from premiumflow.persistence import repository as repository_module
from premiumflow.persistence import storage as storage_module
from premiumflow.web import create_app, dependencies
from premiumflow.web.dependencies import get_repository

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


class StubRepository:
    """Minimal stub to satisfy dependency overrides during smoke tests."""

    pass


def _make_client() -> TestClient:
    app = create_app()
    app.dependency_overrides[get_repository] = lambda: StubRepository()
    return TestClient(app)


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
