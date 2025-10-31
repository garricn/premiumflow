"""Common dependency providers for the web application."""

from __future__ import annotations

from functools import lru_cache

from ..persistence import SQLiteRepository


@lru_cache(maxsize=1)
def _get_cached_repository() -> SQLiteRepository:
    """Return a cached repository instance for reuse within the process."""
    return SQLiteRepository()


def get_repository() -> SQLiteRepository:
    """
    FastAPI dependency that yields a repository.

    Tests can override this dependency to supply fakes or fixtures.
    """
    return _get_cached_repository()
