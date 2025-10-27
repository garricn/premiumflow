"""Minimal :mod:`rich.table` implementation used for tests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List


@dataclass
class Column:
    """Simple representation of a table column."""

    header: str
    style: str | None = None
    justify: str | None = None
    width: int | None = None


class Table:
    """Lightweight table structure that stores column metadata and rows."""

    def __init__(
        self,
        *,
        title: str | None = None,
        show_header: bool = True,
        header_style: str | None = None,
        expand: bool | None = None,
    ) -> None:
        self.title = title
        self.show_header = show_header
        self.header_style = header_style
        self.expand = expand
        self.columns: List[Column] = []
        self._rows: List[List[str]] = []

    def add_column(self, header: str, **kwargs: Any) -> None:
        column = Column(
            header=header,
            style=kwargs.get("style"),
            justify=kwargs.get("justify"),
            width=kwargs.get("width"),
        )
        self.columns.append(column)

    def add_row(self, *values: Any) -> None:
        self._rows.append([str(value) for value in values])

    @property
    def row_count(self) -> int:
        return len(self._rows)

    @property
    def rows(self) -> List[List[str]]:
        return list(self._rows)

    def __str__(self) -> str:  # pragma: no cover - trivial formatting helper
        lines: List[str] = []
        if self.title:
            lines.append(self.title)
        if self.show_header and self.columns:
            lines.append(" | ".join(column.header for column in self.columns))
        for row in self._rows:
            lines.append(" | ".join(row))
        return "\n".join(lines)
