"""Compatibility package that exposes the implementation living under ``src/``."""

from __future__ import annotations

import sys
from importlib import util
from pathlib import Path

_src_root = Path(__file__).resolve().parent.parent / "src"
if str(_src_root) not in sys.path:
    sys.path.insert(0, str(_src_root))

_src_package = _src_root / "premiumflow"
__path__ = [str(_src_package)]

_spec = util.spec_from_file_location(
    "_premiumflow_impl", _src_package / "__init__.py", submodule_search_locations=[str(_src_package)]
)
if _spec and _spec.loader:
    _impl = util.module_from_spec(_spec)
    sys.modules["_premiumflow_impl"] = _impl
    _spec.loader.exec_module(_impl)
    for name, value in vars(_impl).items():
        if name.startswith("__") and name not in {"__doc__", "__all__", "__version__", "__author__", "__email__"}:
            continue
        globals()[name] = value
    __all__ = getattr(_impl, "__all__", [])
else:  # pragma: no cover - defensive fallback
    __all__ = []
