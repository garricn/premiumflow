"""Query helpers for persisted PremiumFlow imports and transactions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Dict, List, Literal, Optional, Sequence, Tuple

from .storage import SQLiteStorage, get_storage

StoredStatusFilter = Literal["all", "open", "closed"]


@dataclass(frozen=True)
class StoredImport:
    """Representation of a persisted import record joined with account data."""

    id: int
    account_name: str
    account_number: Optional[str]
    source_path: str
    source_hash: str
    imported_at: str
    options_only: bool
    ticker: Optional[str]
    strategy: Optional[str]
    open_only: bool
    row_count: int


@dataclass(frozen=True)
class StoredTransaction:
    """Representation of a persisted option transaction with account metadata."""

    id: int
    import_id: int
    account_name: str
    account_number: Optional[str]
    row_index: int
    activity_date: str
    process_date: Optional[str]
    settle_date: Optional[str]
    instrument: str
    description: str
    trans_code: str
    quantity: int
    price: str
    amount: Optional[str]
    strike: str
    option_type: str
    expiration: str
    action: str
    raw_json: str


@dataclass(frozen=True)
class StoredStockTransaction:
    """Representation of a persisted stock (equity) transaction."""

    id: int
    import_id: int
    account_name: str
    account_number: Optional[str]
    row_index: int
    activity_date: str
    process_date: Optional[str]
    settle_date: Optional[str]
    instrument: str
    description: str
    trans_code: str
    action: str
    quantity: int
    price: str
    amount: str
    raw_json: str


@dataclass(frozen=True)
class AssignmentStockLotRecord:
    """Stock lot opened by an assignment event."""

    symbol: str
    opened_at: date
    share_quantity: int
    direction: str
    option_type: str
    strike_price: Decimal
    expiration: date
    share_price_total: Decimal
    share_price_per_share: Decimal
    open_premium_total: Decimal
    open_premium_per_share: Decimal
    open_fee_total: Decimal
    net_credit_total: Decimal
    net_credit_per_share: Decimal
    assignment_kind: str
    source_transaction_id: int


class SQLiteRepository:
    """High-level read accessors for the SQLite persistence layer."""

    def __init__(self, storage: Optional[SQLiteStorage] = None) -> None:
        self._storage = storage or get_storage()

    def list_imports(
        self,
        *,
        account_name: Optional[str] = None,
        account_number: Optional[str] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        order: Literal["asc", "desc"] = "desc",
    ) -> List[StoredImport]:
        """Return persisted imports, optionally filtered by account metadata."""
        self._storage._ensure_initialized()  # type: ignore[attr-defined]
        query = [
            "SELECT",
            "  i.id,",
            "  a.name AS account_name,",
            "  a.number AS account_number,",
            "  i.source_path,",
            "  i.source_hash,",
            "  i.imported_at,",
            "  i.options_only,",
            "  i.ticker,",
            "  i.strategy,",
            "  i.open_only,",
            "  i.row_count",
            "FROM imports AS i",
            "JOIN accounts AS a ON i.account_id = a.id",
        ]
        clauses: list[str] = []
        params: list[object] = []

        if account_name is not None:
            clauses.append("a.name = ?")
            params.append(account_name)
        if account_number is not None:
            clauses.append("IFNULL(a.number, '') = IFNULL(?, '')")
            params.append(account_number)

        if clauses:
            query.append("WHERE " + " AND ".join(clauses))

        order_dir = "DESC" if order.lower() == "desc" else "ASC"
        query.append("ORDER BY i.imported_at " + order_dir + ", i.id " + order_dir)
        if limit is not None:
            query.append("LIMIT ?")
            params.append(limit)
            if offset:
                query.append("OFFSET ?")
                params.append(offset)
        elif offset:
            query.append("LIMIT -1 OFFSET ?")
            params.append(offset)

        sql = "\n".join(query)
        with self._storage._connect() as conn:  # type: ignore[attr-defined]
            rows = conn.execute(sql, params).fetchall()
        return [_row_to_stored_import(row) for row in rows]

    def get_import(self, import_id: int) -> Optional[StoredImport]:
        """Return a single stored import by identifier, if present."""
        self._storage._ensure_initialized()  # type: ignore[attr-defined]
        sql = """
            SELECT
              i.id,
              a.name AS account_name,
              a.number AS account_number,
              i.source_path,
              i.source_hash,
              i.imported_at,
              i.options_only,
              i.ticker,
              i.strategy,
              i.open_only,
              i.row_count
            FROM imports AS i
            JOIN accounts AS a ON i.account_id = a.id
            WHERE i.id = ?
        """
        with self._storage._connect() as conn:  # type: ignore[attr-defined]
            row = conn.execute(sql, (import_id,)).fetchone()
        if row is None:
            return None
        return _row_to_stored_import(row)

    def fetch_import_activity_ranges(
        self, import_ids: Sequence[int]
    ) -> Dict[int, Tuple[Optional[str], Optional[str]]]:
        """Return activity date ranges for each requested import id."""

        if not import_ids:
            return {}

        self._storage._ensure_initialized()  # type: ignore[attr-defined]
        placeholders = ", ".join("?" for _ in import_ids)
        sql = f"""
            SELECT import_id, MIN(activity_date) AS first_activity_date, MAX(activity_date) AS last_activity_date
            FROM option_transactions
            WHERE import_id IN ({placeholders})
            GROUP BY import_id
        """
        with self._storage._connect() as conn:  # type: ignore[attr-defined]
            rows = conn.execute(sql, tuple(int(import_id) for import_id in import_ids)).fetchall()
        ranges: Dict[int, Tuple[Optional[str], Optional[str]]] = {}
        for row in rows:
            ranges[int(row["import_id"])] = (row["first_activity_date"], row["last_activity_date"])
        return ranges

    def fetch_transactions(
        self,
        *,
        account_name: Optional[str] = None,
        account_number: Optional[str] = None,
        import_ids: Optional[Sequence[int]] = None,
        ticker: Optional[str] = None,
        since: Optional[date] = None,
        until: Optional[date] = None,
        status: StoredStatusFilter = "all",
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> List[StoredTransaction]:
        """Return persisted transactions applying the requested filters."""
        self._storage._ensure_initialized()  # type: ignore[attr-defined]
        query = [
            "SELECT",
            "  t.id,",
            "  t.import_id,",
            "  a.name AS account_name,",
            "  a.number AS account_number,",
            "  t.row_index,",
            "  t.activity_date,",
            "  t.process_date,",
            "  t.settle_date,",
            "  t.instrument,",
            "  t.description,",
            "  t.trans_code,",
            "  t.quantity,",
            "  t.price,",
            "  t.amount,",
            "  t.strike,",
            "  t.option_type,",
            "  t.expiration,",
            "  t.action,",
            "  t.raw_json",
            "FROM option_transactions AS t",
            "JOIN imports AS i ON t.import_id = i.id",
            "JOIN accounts AS a ON i.account_id = a.id",
        ]
        clauses: list[str] = []
        params: list[object] = []

        if account_name is not None:
            clauses.append("a.name = ?")
            params.append(account_name)
        if account_number is not None:
            clauses.append("IFNULL(a.number, '') = IFNULL(?, '')")
            params.append(account_number)
        if import_ids:
            placeholders = ", ".join("?" for _ in import_ids)
            clauses.append(f"t.import_id IN ({placeholders})")
            params.extend(int(import_id) for import_id in import_ids)
        if ticker is not None:
            clauses.append("UPPER(t.instrument) = ?")
            params.append(ticker.strip().upper())
        if since is not None:
            clauses.append("t.activity_date >= ?")
            params.append(since.isoformat())
        if until is not None:
            clauses.append("t.activity_date <= ?")
            params.append(until.isoformat())
        if status == "open":
            clauses.append("i.open_only = 1")
        elif status == "closed":
            clauses.append("i.open_only = 0")

        if clauses:
            query.append("WHERE " + " AND ".join(clauses))

        query.append("ORDER BY t.activity_date ASC, t.row_index ASC, t.id ASC")

        if limit is not None:
            query.append("LIMIT ?")
            params.append(limit)
            if offset:
                query.append("OFFSET ?")
                params.append(offset)
        elif offset:
            query.append("LIMIT -1 OFFSET ?")
            params.append(offset)

        sql = "\n".join(query)
        with self._storage._connect() as conn:  # type: ignore[attr-defined]
            rows = conn.execute(sql, params).fetchall()
        return [_row_to_stored_transaction(row) for row in rows]

    def fetch_stock_transactions(
        self,
        *,
        account_name: Optional[str] = None,
        account_number: Optional[str] = None,
        ticker: Optional[str] = None,
        since: Optional[date] = None,
        until: Optional[date] = None,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> List[StoredStockTransaction]:
        """Return persisted stock transactions for the requested filters."""

        self._storage._ensure_initialized()  # type: ignore[attr-defined]
        query = [
            "SELECT",
            "  t.id,",
            "  t.import_id,",
            "  a.name AS account_name,",
            "  a.number AS account_number,",
            "  t.row_index,",
            "  t.activity_date,",
            "  t.process_date,",
            "  t.settle_date,",
            "  t.instrument,",
            "  t.description,",
            "  t.trans_code,",
            "  t.action,",
            "  t.quantity,",
            "  t.price,",
            "  t.amount,",
            "  t.raw_json",
            "FROM stock_transactions AS t",
            "JOIN imports AS i ON t.import_id = i.id",
            "JOIN accounts AS a ON i.account_id = a.id",
        ]
        clauses: list[str] = []
        params: list[object] = []

        if account_name is not None:
            clauses.append("a.name = ?")
            params.append(account_name)
        if account_number is not None:
            clauses.append("IFNULL(a.number, '') = IFNULL(?, '')")
            params.append(account_number)
        if ticker is not None:
            clauses.append("UPPER(t.instrument) = ?")
            params.append(ticker.strip().upper())
        if since is not None:
            clauses.append("t.activity_date >= ?")
            params.append(since.isoformat())
        if until is not None:
            clauses.append("t.activity_date <= ?")
            params.append(until.isoformat())

        if clauses:
            query.append("WHERE " + " AND ".join(clauses))

        query.append("ORDER BY t.activity_date ASC, t.row_index ASC, t.id ASC")

        if limit is not None:
            query.append("LIMIT ?")
            params.append(limit)
            if offset:
                query.append("OFFSET ?")
                params.append(offset)
        elif offset:
            query.append("LIMIT -1 OFFSET ?")
            params.append(offset)

        sql = "\n".join(query)
        with self._storage._connect() as conn:  # type: ignore[attr-defined]
            rows = conn.execute(sql, params).fetchall()
        return [_row_to_stored_stock_transaction(row) for row in rows]

    def replace_assignment_stock_lots(
        self,
        *,
        account_name: str,
        account_number: Optional[str],
        records: Sequence[AssignmentStockLotRecord],
    ) -> None:
        """Replace assignment-sourced stock lots for the specified account."""

        self._storage._ensure_initialized()  # type: ignore[attr-defined]
        with self._storage._connect() as conn:  # type: ignore[attr-defined]
            account_id = self._get_account_id(conn, account_name, account_number)
            conn.execute(
                "DELETE FROM stock_lots WHERE account_id = ? AND assignment_kind IS NOT NULL",
                (account_id,),
            )
            if not records:
                return

            timestamp = datetime.utcnow().isoformat(timespec="seconds") + "Z"
            rows = [
                (
                    account_id,
                    record.source_transaction_id,
                    record.symbol,
                    record.opened_at.isoformat(),
                    None,  # closed_at
                    record.share_quantity,
                    record.direction,
                    record.option_type,
                    self._decimal_to_text(record.strike_price),
                    record.expiration.isoformat(),
                    self._decimal_to_text(record.share_price_total),
                    self._decimal_to_text(record.share_price_per_share),
                    self._decimal_to_text(record.open_premium_total),
                    self._decimal_to_text(record.open_premium_per_share),
                    self._decimal_to_text(record.open_fee_total),
                    self._decimal_to_text(record.net_credit_total),
                    self._decimal_to_text(record.net_credit_per_share),
                    record.assignment_kind,
                    "open",
                    timestamp,
                    timestamp,
                )
                for record in records
            ]
            conn.executemany(
                """
                INSERT INTO stock_lots (
                    account_id,
                    source_transaction_id,
                    symbol,
                    opened_at,
                    closed_at,
                    quantity,
                    direction,
                    option_type,
                    strike_price,
                    expiration,
                    share_price_total,
                    share_price_per_share,
                    open_premium_total,
                    open_premium_per_share,
                    open_fee_total,
                    net_credit_total,
                    net_credit_per_share,
                    assignment_kind,
                    status,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    def _get_account_id(
        self,
        conn,  # type: ignore[no-untyped-def]
        account_name: str,
        account_number: Optional[str],
    ) -> int:
        row = conn.execute(
            "SELECT id FROM accounts WHERE name = ? AND IFNULL(number, '') = IFNULL(?, '')",
            (account_name, account_number),
        ).fetchone()
        if row is None:
            raise ValueError(
                f"Account {account_name}"
                + (f" ({account_number})" if account_number else "")
                + " not found in persistence layer."
            )
        return int(row["id"])

    @staticmethod
    def _decimal_to_text(value: Decimal) -> str:
        return format(value, "f")

    def delete_import(self, import_id: int) -> bool:
        """Delete an import and associated transactions. Returns True when a row was removed."""

        self._storage._ensure_initialized()  # type: ignore[attr-defined]
        with self._storage._connect() as conn:  # type: ignore[attr-defined]
            cursor = conn.execute("DELETE FROM imports WHERE id = ?", (int(import_id),))
            deleted = cursor.rowcount or 0
        return deleted > 0


def _row_to_stored_import(row) -> StoredImport:
    return StoredImport(
        id=int(row["id"]),
        account_name=row["account_name"],
        account_number=row["account_number"],
        source_path=row["source_path"],
        source_hash=row["source_hash"],
        imported_at=row["imported_at"],
        options_only=bool(row["options_only"]),
        ticker=row["ticker"],
        strategy=row["strategy"],
        open_only=bool(row["open_only"]),
        row_count=int(row["row_count"]),
    )


def _row_to_stored_transaction(row) -> StoredTransaction:
    return StoredTransaction(
        id=int(row["id"]),
        import_id=int(row["import_id"]),
        account_name=row["account_name"],
        account_number=row["account_number"],
        row_index=int(row["row_index"]),
        activity_date=row["activity_date"],
        process_date=row["process_date"],
        settle_date=row["settle_date"],
        instrument=row["instrument"],
        description=row["description"],
        trans_code=row["trans_code"],
        quantity=int(row["quantity"]),
        price=row["price"],
        amount=row["amount"],
        strike=row["strike"],
        option_type=row["option_type"],
        expiration=row["expiration"],
        action=row["action"],
        raw_json=row["raw_json"],
    )


def _row_to_stored_stock_transaction(row) -> StoredStockTransaction:
    return StoredStockTransaction(
        id=int(row["id"]),
        import_id=int(row["import_id"]),
        account_name=row["account_name"],
        account_number=row["account_number"],
        row_index=int(row["row_index"]),
        activity_date=row["activity_date"],
        process_date=row["process_date"],
        settle_date=row["settle_date"],
        instrument=row["instrument"],
        description=row["description"],
        trans_code=row["trans_code"],
        action=row["action"],
        quantity=int(row["quantity"]),
        price=row["price"],
        amount=row["amount"],
        raw_json=row["raw_json"],
    )
