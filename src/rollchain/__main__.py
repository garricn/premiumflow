"""Enable ``python -m rollchain`` to invoke the legacy CLI."""

from roll import main

if __name__ == "__main__":  # pragma: no cover - thin wrapper
    main()
