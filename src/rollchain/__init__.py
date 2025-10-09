"""RollChain package shim.

This package provides a forward-looking namespace while continuing to expose
existing roll module functionality for downstream callers.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

_roll = import_module("roll")

__all__ = [
    "__version__",
    "detect_roll_chains",
    "find_chain_by_position",
    "format_position_spec",
    "is_call_option",
    "is_options_transaction",
    "is_put_option",
    "parse_lookup_input",
]

__version__ = "0.1.0"


def __getattr__(name: str) -> Any:
    """Forward attribute lookups to the legacy :mod:`roll` module."""
    try:
        return getattr(_roll, name)
    except AttributeError as exc:  # pragma: no cover - mirrors AttributeError
        raise AttributeError(name) from exc


def __dir__() -> list[str]:
    """Combine rollchain exports with the legacy module attributes."""
    return sorted(set(__all__) | set(dir(_roll)))


detect_roll_chains = _roll.detect_roll_chains
find_chain_by_position = _roll.find_chain_by_position
format_position_spec = _roll.format_position_spec
is_call_option = _roll.is_call_option
is_options_transaction = _roll.is_options_transaction
is_put_option = _roll.is_put_option
parse_lookup_input = _roll.parse_lookup_input
