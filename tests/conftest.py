"""Pytest configuration and fixtures."""

from pathlib import Path

import pytest

from premiumflow.persistence import storage as storage_module


@pytest.fixture(scope="session", autouse=True)
def isolated_persistence(tmp_path_factory):
    """Ensure tests use an isolated SQLite database and reset caches between runs."""

    db_dir = tmp_path_factory.mktemp("persistence-db")
    db_path = db_dir / "premiumflow.db"
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setenv(storage_module.DB_ENV_VAR, str(db_path))
    storage_module.get_storage.cache_clear()
    try:
        yield
    finally:
        storage_module.get_storage.cache_clear()
        monkeypatch.undo()


@pytest.fixture
def sample_csv_closed():
    """Path to sample closed roll chain CSV."""
    return Path(__file__).parent / "fixtures" / "tsla_rc-001-closed.csv"


@pytest.fixture
def sample_csv_open():
    """Path to sample open roll chain CSV."""
    return Path(__file__).parent / "fixtures" / "tsla_rc-001-open.csv"
