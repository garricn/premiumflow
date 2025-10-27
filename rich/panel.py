"""Minimal :mod:`rich.panel` replacement."""

from dataclasses import dataclass
from typing import Any


@dataclass
class Panel:
    """Container that records the renderable and metadata for display."""

    renderable: Any
    title: str | None = None
    border_style: str | None = None

    def __str__(self) -> str:  # pragma: no cover - trivial formatting helper
        header = f"[{self.title}]" if self.title else ""
        return f"{header}\n{self.renderable}"
