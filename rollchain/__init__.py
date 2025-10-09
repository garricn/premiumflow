"""Compatibility shim so the package is importable from a source checkout."""
from __future__ import annotations

import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType

_pkg_dir = Path(__file__).resolve().parent
_src_root = _pkg_dir.parent / "src"
_src_pkg_dir = _src_root / "rollchain"

if _src_root.exists():
    src_root_str = str(_src_root)
    if src_root_str not in sys.path:
        sys.path.insert(0, src_root_str)

__path__ = []
if _src_pkg_dir.exists():
    __path__.append(str(_src_pkg_dir))
__path__.append(str(_pkg_dir))


def _load_impl() -> ModuleType:
    spec = spec_from_file_location(
        "_rollchain_impl",
        _src_pkg_dir / "__init__.py",
        submodule_search_locations=[str(_src_pkg_dir)],
    )
    if spec is None or spec.loader is None:  # pragma: no cover - defensive guard
        raise ModuleNotFoundError("rollchain implementation module could not be loaded")
    module = module_from_spec(spec)
    loader = spec.loader
    assert loader is not None
    loader.exec_module(module)
    return module


_impl = _load_impl()

for _name, _value in _impl.__dict__.items():
    if _name in {"__name__", "__loader__", "__package__", "__spec__", "__path__", "__file__"}:
        continue
    globals()[_name] = _value

__all__ = getattr(_impl, "__all__", [])
if hasattr(_impl, "__getattr__"):
    __getattr__ = _impl.__getattr__
if hasattr(_impl, "__dir__"):
    __dir__ = _impl.__dir__

# Tidy module namespace.
del ModuleType, module_from_spec, spec_from_file_location, Path, sys, _impl, _name, _value
