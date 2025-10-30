"""SQLite-backed persistence layer for premiumflow imports."""

from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from functools import lru_cache
from hashlib import sha256
from pathlib import Path
from typing import Optional, Union

from ..core.parser import ParsedImportResult

DEFAULT_DB_PATH = Path.home() / ".premiumflow" / "premiumflow.db"
DB_ENV_VAR = "PREMIUMFLOW_DB_PATH"


def _determine_db_path() -> Path:
    override = os.environ.get(DB_ENV_VAR)
    if override:
        return Path(override).expanduser().resolve()
    return DEFAULT_DB_PATH


@dataclass
class ImportContext:
    """Metadata captured alongside a stored import."""

    source_path: str
    options_only: bool
    ticker: Optional[str]
    strategy: Optional[str]
    open_only: bool


class SQLiteStorage:
    """Thin wrapper around a SQLite database used to persist imports."""

    def __init__(self, db_path: Path | None = None) -> None:
        self._db_path = Path(db_path) if db_path is not None else _determine_db_path()
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialized = False

    @property
    def db_path(self) -> Path:
        return self._db_path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        with self._connect() as conn:
            conn.executescript(
                """
                PRAGMA foreign_keys = ON;

                CREATE TABLE IF NOT EXISTS accounts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    number TEXT,
                    UNIQUE(name, number)
                );

                CREATE TABLE IF NOT EXISTS imports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
                    source_path TEXT NOT NULL,
                    source_hash TEXT NOT NULL,
                    imported_at TEXT NOT NULL,
                    options_only INTEGER NOT NULL,
                    ticker TEXT,
                    strategy TEXT,
                    open_only INTEGER NOT NULL,
                    row_count INTEGER NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_imports_account_id ON imports(account_id);

                CREATE TABLE IF NOT EXISTS option_transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    import_id INTEGER NOT NULL REFERENCES imports(id) ON DELETE CASCADE,
                    row_index INTEGER NOT NULL,
                    activity_date TEXT NOT NULL,
                    process_date TEXT,
                    settle_date TEXT,
                    instrument TEXT NOT NULL,
                    description TEXT NOT NULL,
                    trans_code TEXT NOT NULL,
                    quantity INTEGER NOT NULL,
                    price TEXT NOT NULL,
                    amount TEXT,
                    strike TEXT NOT NULL,
                    option_type TEXT NOT NULL,
                    expiration TEXT NOT NULL,
                    action TEXT NOT NULL,
                    fees TEXT NOT NULL,
                    raw_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_transactions_import
                    ON option_transactions(import_id);
                CREATE INDEX IF NOT EXISTS idx_transactions_symbol
                    ON option_transactions(instrument);
                CREATE INDEX IF NOT EXISTS idx_transactions_expiration
                    ON option_transactions(expiration);
                """
            )
        self._initialized = True

    def store_import(self, parsed: ParsedImportResult, context: ImportContext) -> int:
        """Persist an import and return the generated import id."""
        self._ensure_initialized()
        source_hash = _hash_file(context.source_path)
        imported_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        with self._connect() as conn:
            account_id = self._get_or_create_account(
                conn, parsed.account_name, parsed.account_number
            )
            cur = conn.execute(
                """
                INSERT INTO imports (
                    account_id, source_path, source_hash, imported_at,
                    options_only, ticker, strategy, open_only, row_count
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    account_id,
                    context.source_path,
                    source_hash,
                    imported_at,
                    int(context.options_only),
                    context.ticker,
                    context.strategy,
                    int(context.open_only),
                    len(parsed.transactions),
                ),
            )
            import_id = cur.lastrowid
            if import_id is None:  # pragma: no cover - sqlite should always return a value
                raise RuntimeError("Failed to record import metadata")
            rows_to_insert = [
                (
                    int(import_id),
                    index,
                    txn.activity_date.isoformat(),
                    txn.process_date.isoformat() if txn.process_date else None,
                    txn.settle_date.isoformat() if txn.settle_date else None,
                    txn.instrument,
                    txn.description,
                    txn.trans_code,
                    txn.quantity,
                    _decimal_to_text(txn.price),
                    _decimal_to_text(txn.amount),
                    _decimal_to_text(txn.strike),
                    txn.option_type,
                    txn.expiration.isoformat(),
                    txn.action,
                    _decimal_to_text(txn.fees),
                    json.dumps(txn.raw, sort_keys=True),
                )
                for index, txn in enumerate(parsed.transactions, start=1)
            ]
            if rows_to_insert:
                conn.executemany(
                    """
                    INSERT INTO option_transactions (
                        import_id, row_index, activity_date, process_date, settle_date,
                        instrument, description, trans_code, quantity, price, amount,
                        strike, option_type, expiration, action, fees, raw_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows_to_insert,
                )
        return int(import_id)

    def _get_or_create_account(
        self, conn: sqlite3.Connection, name: str, number: Optional[str]
    ) -> int:
        cur = conn.execute(
            "SELECT id FROM accounts WHERE name = ? AND IFNULL(number, '') = IFNULL(?, '')",
            (name, number),
        )
        row = cur.fetchone()
        if row:
            return row["id"]
        cur = conn.execute(
            "INSERT INTO accounts (name, number) VALUES (?, ?)",
            (name, number),
        )
        account_id = cur.lastrowid
        if account_id is None:  # pragma: no cover - sqlite should always return a value
            raise RuntimeError("Failed to create account record")
        return int(account_id)


def _hash_file(path: str) -> str:
    file_path = Path(path)
    try:
        data = file_path.read_bytes()
    except FileNotFoundError:
        data = path.encode("utf-8")
    return sha256(data).hexdigest()


NumberLike = Union[Decimal, float, int]


def _decimal_to_text(value: Optional[NumberLike]) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


@lru_cache(maxsize=1)
def get_storage() -> SQLiteStorage:
    """Return a cached storage instance."""
    return SQLiteStorage()


def store_import_result(
    parsed: ParsedImportResult,
    *,
    source_path: str,
    options_only: bool,
    ticker: Optional[str],
    strategy: Optional[str],
    open_only: bool,
) -> int:
    """Persist the supplied import result using the default storage."""
    storage = get_storage()
    context = ImportContext(
        source_path=source_path,
        options_only=options_only,
        ticker=ticker,
        strategy=strategy,
        open_only=open_only,
    )
    return storage.store_import(parsed, context)
