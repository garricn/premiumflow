"""Minimal console implementation used for tests.

This module mimics the subset of :mod:`rich.console` functionality required by the
unit tests.  It provides a drop-in replacement when the real Rich dependency is not
available.
"""

from __future__ import annotations

import json
from typing import Any


class Console:
    """Simplified Console that proxies to :func:`print`."""

    def print(self, *objects: Any, sep: str = " ", end: str = "\n") -> None:
        text = sep.join(str(obj) for obj in objects)
        print(text, end=end)

    def print_json(self, *, data: Any | None = None, **kwargs: Any) -> None:
        """Render JSON output similar to :meth:`rich.console.Console.print_json`."""

        payload = data if data is not None else kwargs
        print(json.dumps(payload, indent=2, sort_keys=True))
