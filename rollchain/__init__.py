"""Compatibility shim for the legacy rollchain package name.

This module re-exports the new ``options`` package to preserve backward
compatibility for existing scripts. The shim will be removed in a future
major release.
"""
from __future__ import annotations

import warnings

import options as _options

for _name in dir(_options):
    if _name.startswith("_"):
        continue
    globals()[_name] = getattr(_options, _name)

__version__ = getattr(_options, "__version__", None)

warnings.warn(
    "rollchain has been renamed to options. Please update imports to "
    "`import options`.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = [name for name in globals() if not name.startswith("_")]
del _options
