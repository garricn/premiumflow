"""Smoke tests for the PremiumFlow FastAPI application."""

from __future__ import annotations

from fastapi.testclient import TestClient

from premiumflow.web import create_app
from premiumflow.web.dependencies import get_repository


class StubRepository:
    """Minimal stub to satisfy dependency overrides during tests."""

    pass


def _make_client() -> TestClient:
    app = create_app()
    app.dependency_overrides[get_repository] = lambda: StubRepository()
    return TestClient(app)


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
