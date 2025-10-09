"""Pytest configuration and fixtures."""

import pytest
from pathlib import Path


@pytest.fixture
def sample_csv_closed():
    """Path to sample closed roll chain CSV."""
    return Path(__file__).parent / "fixtures" / "tsla_rc-001-closed.csv"


@pytest.fixture
def sample_csv_open():
    """Path to sample open roll chain CSV."""
    return Path(__file__).parent / "fixtures" / "tsla_rc-001-open.csv"
