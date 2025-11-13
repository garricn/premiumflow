# file-length-ignore
"""FastAPI application factory for the PremiumFlow web UI."""

from __future__ import annotations

import re
import sqlite3
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Literal
from urllib.parse import urlencode

from fastapi import Depends, FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from ..core.parser import ImportValidationError, load_option_transactions
from ..persistence import (
    DuplicateImportError,
    SQLiteRepository,
    get_storage,
    store_import_result,
)
from ..services.cash_flow_report import generate_cash_flow_pnl_report
from ..services.cli_helpers import format_account_label
from ..services.display import format_currency
from ..services.json_serializer import serialize_cash_flow_pnl_report, serialize_leg
from ..services.leg_matching import (
    _stored_to_normalized,
    group_fills_by_account,
    match_legs_with_errors,
)
from ..services.stock_lot_builder import rebuild_assignment_stock_lots
from ..services.stock_lots import (
    StockLotSummary,
    fetch_stock_lot_summaries,
    serialize_stock_lot_summary,
)
from .dependencies import get_repository

TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
STATIC_DIR = Path(__file__).resolve().parent / "static"

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

DuplicateStrategy = Literal["error", "skip", "replace"]

DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 100
MIN_PAGE_SIZE = 5

REALIZED_VIEW_CHOICES: dict[str, dict[str, str]] = {
    "options": {"label": "Options", "select": "Options Only"},
    "stock": {"label": "Stock", "select": "Stock Only"},
    "combined": {"label": "Combined", "select": "Options + Stock"},
}


def _default_form() -> dict[str, object]:
    return {
        "account_name": "",
        "account_number": "",
        "duplicate_strategy": "error",
        "options_only": True,
        "open_only": False,
    }


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower())
    slug = slug.strip("-")
    return slug or "account"


def _account_folder(name: str, number: str | None) -> str:
    parts = [_slugify(name)]
    if number:
        parts.append(_slugify(number))
    return "-".join(parts)


def _format_timestamp(value: str) -> str:
    """Render persisted timestamps as a human-readable UTC string."""
    if not value:
        return value
    normalized = value
    if value.endswith("Z"):
        normalized = value[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return value
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _build_query(filters: dict[str, str], *, page: int, page_size: int) -> str:
    """Construct a query string preserving filters and pagination state."""
    params: list[tuple[str, str]] = []
    if filters.get("account_name"):
        params.append(("account_name", filters["account_name"]))
    if filters.get("account_number"):
        params.append(("account_number", filters["account_number"]))
    params.append(("page", str(page)))
    params.append(("page_size", str(page_size)))
    return urlencode(params)


def _fetch_matched_legs(
    repository: SQLiteRepository,
    *,
    account_name: str | None = None,
    account_number: str | None = None,
    ticker: str | None = None,
    status: str = "all",
) -> tuple[list[dict[str, object]], list[str]]:
    """Fetch and match legs with filters. Returns (legs_data, warnings)."""
    account_name_filter = (account_name or "").strip()
    account_number_filter = (account_number or "").strip()
    ticker_filter = (ticker or "").strip()
    status_filter = status.strip().lower() if status else "all"

    stored_txns = repository.fetch_transactions(
        account_name=account_name_filter or None,
        account_number=account_number_filter or None,
        ticker=ticker_filter or None,
        since=None,
        until=None,
        status="all",
    )

    legs_data: list[dict[str, object]] = []
    warnings: list[str] = []

    if stored_txns:
        normalized_txns = [_stored_to_normalized(stored) for stored in stored_txns]
        all_fills = group_fills_by_account(normalized_txns)
        matched_map, errors = match_legs_with_errors(all_fills)
        legs_list = sorted(
            matched_map.values(),
            key=lambda leg: (
                leg.account_name,
                leg.account_number or "",
                leg.contract.symbol,
                leg.contract.expiration,
                leg.contract.option_type,
                leg.contract.strike,
                leg.contract.leg_id,
            ),
        )

        if status_filter != "all":
            want_open = status_filter == "open"
            legs_list = [leg for leg in legs_list if leg.is_open == want_open]

        for (acct_name, acct_number, leg_id), exc, bucket in errors:
            account_label = format_account_label(acct_name, acct_number)
            descriptor = bucket[0].transaction.description if bucket else "Unknown"
            warnings.append(f"{account_label} • {leg_id} • {descriptor}: {exc}")

        legs_data = [serialize_leg(leg) for leg in legs_list]

    return legs_data, warnings


def _get_unique_accounts(repository: SQLiteRepository) -> list[dict[str, str | None]]:
    """Get unique account name/number pairs from existing imports."""
    imports = repository.list_imports()
    accounts_map: dict[tuple[str | None, str | None], None] = {}
    for imp in imports:
        accounts_map[(imp.account_name, imp.account_number)] = None
    # Sort with a key that normalizes None to empty string to avoid TypeError
    sorted_accounts = sorted(
        accounts_map.keys(),
        key=lambda pair: (pair[0] or "", pair[1] or ""),
    )
    return [{"account_name": name, "account_number": number} for (name, number) in sorted_accounts]


def _parse_account_selection(account_value: str | None) -> tuple[str | None, str | None]:
    """Parse account selection from dropdown value (format: 'name|number' or just 'name')."""
    if not account_value:
        return (None, None)
    # Format is "account_name|account_number" or just "account_name" if no number
    parts = account_value.split("|", 1)
    account_name = parts[0] if parts[0] else None
    account_number = parts[1] if len(parts) > 1 and parts[1] else None
    return (account_name, account_number)


def _parse_date_param(value: str | None) -> date | None:
    """Parse date query parameter from YYYY-MM-DD string."""
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def _parse_lot_date(value: str | None) -> date | None:
    """Parse ISO datetime string from lot metadata into a date."""
    if not value:
        return None
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return parsed.date()


def create_app() -> FastAPI:  # noqa: C901
    """Construct and return the FastAPI application."""
    app = FastAPI(title="PremiumFlow Web UI")

    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.get("/health", tags=["health"])
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/", response_class=HTMLResponse, tags=["ui"])
    async def index(
        request: Request, repository: SQLiteRepository = Depends(get_repository)
    ) -> HTMLResponse:
        return templates.TemplateResponse(
            request=request,
            name="index.html",
            context={
                "title": "PremiumFlow Web UI",
                "message": None,
                "form": _default_form(),
            },
        )

    @app.post("/upload", response_class=HTMLResponse, tags=["ui"])
    async def upload(  # noqa: C901, PLR0913
        request: Request,
        csv_file: UploadFile = File(...),
        account_name: str = Form(...),
        account_number: str = Form(...),
        duplicate_strategy: DuplicateStrategy = Form("error"),
        options_only: bool = Form(True),
        open_only: bool = Form(False),
        repository: SQLiteRepository = Depends(get_repository),
    ) -> HTMLResponse:
        form_values: dict[str, str | bool] = {
            "account_name": account_name,
            "account_number": account_number,
            "duplicate_strategy": duplicate_strategy,
            "options_only": options_only,
            "open_only": open_only,
        }

        message: dict[str, object] | None = None

        if not csv_file.filename:
            message = {
                "type": "error",
                "title": "No file selected",
                "body": "Choose a Robinhood CSV export before uploading.",
            }
        else:
            normalized_account_name = account_name.strip()
            normalized_account_number = account_number.strip()

            if not normalized_account_name:
                message = {
                    "type": "error",
                    "title": "Account name required",
                    "body": "Account name must contain non-whitespace characters.",
                }
                return templates.TemplateResponse(
                    request=request,
                    name="index.html",
                    context={
                        "title": "PremiumFlow Web UI",
                        "message": message,
                        "form": form_values,
                    },
                )

            if not normalized_account_number:
                message = {
                    "type": "error",
                    "title": "Account number required",
                    "body": "Account number must contain non-whitespace characters.",
                }
                return templates.TemplateResponse(
                    request=request,
                    name="index.html",
                    context={
                        "title": "PremiumFlow Web UI",
                        "message": message,
                        "form": form_values,
                    },
                )

            storage = get_storage()
            uploads_dir = storage.db_path.parent / "uploads"
            account_dir = uploads_dir / _account_folder(
                normalized_account_name, normalized_account_number
            )
            account_dir.mkdir(parents=True, exist_ok=True)

            safe_name = Path(csv_file.filename).name or "uploaded.csv"
            final_path = account_dir / safe_name

            existing_imports = repository.list_imports(
                account_name=normalized_account_name,
                account_number=normalized_account_number,
            )
            has_existing_import = any(
                imp.source_path == str(final_path) for imp in existing_imports
            )

            if has_existing_import and duplicate_strategy == "error":
                message = {
                    "type": "error",
                    "title": "Import already recorded",
                    "body": (
                        "An import for this account and file already exists. "
                        "Choose skip or replace to continue."
                    ),
                }
                return templates.TemplateResponse(
                    request=request,
                    name="index.html",
                    context={
                        "title": "PremiumFlow Web UI",
                        "message": message,
                        "form": form_values,
                    },
                )

            if has_existing_import and duplicate_strategy == "skip":
                message = {
                    "type": "warning",
                    "title": "Import skipped",
                    "body": "An identical import already exists; no changes were made.",
                }
                return templates.TemplateResponse(
                    request=request,
                    name="index.html",
                    context={
                        "title": "PremiumFlow Web UI",
                        "message": message,
                        "form": form_values,
                    },
                )

            try:
                content = await csv_file.read()
                if not content:
                    raise ImportValidationError("The uploaded file is empty.")

                tmp_path: Path | None = None
                backup_path = None
                try:
                    suffix = Path(csv_file.filename or "uploaded.csv").suffix or ".csv"
                    with NamedTemporaryFile(
                        delete=False, prefix="upload-", suffix=suffix, dir=uploads_dir
                    ) as tmp:
                        tmp.write(content)
                        tmp_path = Path(tmp.name)

                    parsed = load_option_transactions(
                        str(tmp_path),
                        account_name=normalized_account_name,
                        account_number=normalized_account_number,
                    )

                    if final_path.exists():
                        backup_path = final_path.with_suffix(final_path.suffix + ".bak")
                        if backup_path.exists():
                            backup_path.unlink()
                        final_path.replace(backup_path)

                    tmp_path.replace(final_path)
                    try:
                        store_result = store_import_result(
                            parsed,
                            source_path=str(final_path),
                            options_only=bool(options_only),
                            ticker=None,
                            strategy=None,
                            open_only=bool(open_only),
                            duplicate_strategy=duplicate_strategy,
                        )
                        if store_result.status != "skipped":
                            rebuild_assignment_stock_lots(
                                repository,
                                account_name=parsed.account_name,
                                account_number=parsed.account_number,
                            )
                    except Exception:
                        if backup_path and backup_path.exists():
                            backup_path.replace(final_path)
                        raise
                    else:
                        if backup_path and backup_path.exists():
                            backup_path.unlink(missing_ok=True)
                finally:
                    if tmp_path and tmp_path.exists():
                        tmp_path.unlink(missing_ok=True)
            except ImportValidationError as exc:
                message = {
                    "type": "error",
                    "title": "Import validation failed",
                    "body": str(exc),
                }
            except DuplicateImportError as exc:
                message = {
                    "type": "warning",
                    "title": "Import skipped",
                    "body": str(exc),
                }
            except (sqlite3.Error, OSError) as exc:
                message = {
                    "type": "error",
                    "title": "Persistence error",
                    "body": f"Failed to store import: {exc}",
                }
            except Exception as exc:  # pragma: no cover - unexpected error
                message = {
                    "type": "error",
                    "title": "Unexpected error",
                    "body": str(exc),
                }
            else:
                row_count = len(parsed.transactions) + len(
                    getattr(parsed, "stock_transactions", [])
                )
                if store_result.status == "skipped":
                    message = {
                        "type": "warning",
                        "title": "Import already recorded",
                        "body": "An identical import already exists; no changes were made.",
                    }
                elif store_result.status == "replaced":
                    message = {
                        "type": "success",
                        "title": "Import replaced",
                        "body": f"Replaced the existing import with {row_count} transactions.",
                    }
                else:
                    message = {
                        "type": "success",
                        "title": "Import stored",
                        "body": f"Imported {row_count} transactions for {parsed.account_name}.",
                    }

        return templates.TemplateResponse(
            request=request,
            name="index.html",
            context={
                "title": "PremiumFlow Web UI",
                "message": message,
                "form": form_values,
            },
        )

    @app.get("/imports", response_class=HTMLResponse, tags=["ui"])
    async def imports_history(  # noqa: PLR0913
        request: Request,
        account_name: str | None = Query(default=None),
        account_number: str | None = Query(default=None),
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
        repository: SQLiteRepository = Depends(get_repository),
    ) -> HTMLResponse:
        message: dict[str, object] | None = None
        if request.query_params.get("message") == "deleted":
            deleted_id = request.query_params.get("deleted_id", "")
            account_label = request.query_params.get("account_label") or "the selected account"
            source_filename = request.query_params.get("source_filename")
            file_hint = f" (source: {source_filename})" if source_filename else ""
            message = {
                "type": "success",
                "title": "Import deleted",
                "body": (
                    f"Removed import {deleted_id} for {account_label}{file_hint}. "
                    "Re-upload the original CSV from the Upload page if you need to ingest it again."
                ),
            }

        normalized_page_size = max(MIN_PAGE_SIZE, min(page_size, MAX_PAGE_SIZE))
        account_name_filter = (account_name or "").strip()
        account_number_filter = (account_number or "").strip()

        filters = {
            "account_name": account_name_filter,
            "account_number": account_number_filter,
        }

        offset = (page - 1) * normalized_page_size
        records = repository.list_imports(
            account_name=account_name_filter or None,
            account_number=account_number_filter or None,
            limit=normalized_page_size + 1,
            offset=offset,
        )

        has_next = len(records) > normalized_page_size
        displayed_records = records[:normalized_page_size]
        activity_ranges = repository.fetch_import_activity_ranges(
            [record.id for record in displayed_records]
        )
        history = [
            {
                "id": record.id,
                "account_name": record.account_name,
                "account_number": record.account_number,
                "account_label": (
                    record.account_name
                    if record.account_number is None
                    else f"{record.account_name} ({record.account_number})"
                ),
                "imported_at": _format_timestamp(record.imported_at),
                "row_count": record.row_count,
                "options_only": record.options_only,
                "open_only": record.open_only,
                "ticker": record.ticker,
                "strategy": record.strategy,
                "source_path": record.source_path,
                "source_filename": Path(record.source_path).name,
                "activity_start": activity_ranges.get(record.id, (None, None))[0],
                "activity_end": activity_ranges.get(record.id, (None, None))[1],
            }
            for record in displayed_records
        ]

        pagination = {
            "page": page,
            "page_size": normalized_page_size,
            "has_previous": page > 1,
            "has_next": has_next,
            "previous_query": (
                _build_query(filters, page=page - 1, page_size=normalized_page_size)
                if page > 1
                else ""
            ),
            "next_query": (
                _build_query(filters, page=page + 1, page_size=normalized_page_size)
                if has_next
                else ""
            ),
        }

        return templates.TemplateResponse(
            request=request,
            name="imports.html",
            context={
                "title": "Import history • PremiumFlow Web UI",
                "filters": filters,
                "history": history,
                "pagination": pagination,
                "message": message,
            },
        )

    @app.post("/imports/{import_id}/delete", response_class=HTMLResponse, tags=["ui"])
    async def delete_import(  # noqa: PLR0913
        request: Request,
        import_id: int,
        account_name: str | None = Form(default=None),
        account_number: str | None = Form(default=None),
        page: int | None = Form(default=None),
        page_size: int | None = Form(default=None),
        repository: SQLiteRepository = Depends(get_repository),
    ) -> RedirectResponse:
        record = repository.get_import(import_id)
        if record is None:
            raise HTTPException(status_code=404, detail="Import not found")

        deleted = repository.delete_import(import_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Import not found")

        account_label = (
            record.account_name
            if record.account_number is None
            else f"{record.account_name} ({record.account_number})"
        )

        redirect_params = {
            key: value
            for key, value in (
                ("account_name", (account_name or "").strip()),
                ("account_number", (account_number or "").strip()),
            )
            if value
        }

        if page is not None:
            redirect_params["page"] = str(page)
        if page_size is not None:
            redirect_params["page_size"] = str(page_size)

        redirect_params.update(
            {
                "message": "deleted",
                "deleted_id": str(import_id),
                "account_label": account_label,
                "source_filename": Path(record.source_path).name,
            }
        )

        redirect_url = str(request.url_for("imports_history"))
        if redirect_params:
            redirect_url = f"{redirect_url}?{urlencode(redirect_params)}"

        return RedirectResponse(redirect_url, status_code=303)

    @app.get("/imports/{import_id}", response_class=HTMLResponse, tags=["ui"])
    async def view_import(
        request: Request,
        import_id: int,
        repository: SQLiteRepository = Depends(get_repository),
    ) -> HTMLResponse:
        record = repository.get_import(import_id)
        if record is None:
            raise HTTPException(status_code=404, detail="Import not found")

        transactions = repository.fetch_transactions(import_ids=[import_id])
        stock_transactions = repository.fetch_stock_transactions(import_ids=[import_id])
        activity_start, activity_end = repository.fetch_import_activity_ranges([import_id]).get(
            import_id, (None, None)
        )
        account_label = (
            record.account_name
            if record.account_number is None
            else f"{record.account_name} ({record.account_number})"
        )

        return templates.TemplateResponse(
            request=request,
            name="import_detail.html",
            context={
                "title": f"Import {record.id} • PremiumFlow Web UI",
                "import_record": record,
                "imported_at": _format_timestamp(record.imported_at),
                "account_label": account_label,
                "transactions": transactions,
                "stock_transactions": stock_transactions,
                "has_stock_transactions": bool(stock_transactions),
                "activity_start": activity_start,
                "activity_end": activity_end,
                "default_page_size": DEFAULT_PAGE_SIZE,
            },
        )

    @app.get("/legs", response_class=HTMLResponse, tags=["ui"])
    async def legs_view(  # noqa: PLR0913
        request: Request,
        account_name: str | None = Query(default=None),
        account_number: str | None = Query(default=None),
        ticker: str | None = Query(default=None),
        status: str = Query(default="all"),
        repository: SQLiteRepository = Depends(get_repository),
    ) -> HTMLResponse:
        """Display matched option legs with filters."""
        legs_data, warnings = _fetch_matched_legs(
            repository,
            account_name=account_name,
            account_number=account_number,
            ticker=ticker,
            status=status,
        )

        account_name_filter = (account_name or "").strip()
        account_number_filter = (account_number or "").strip()
        ticker_filter = (ticker or "").strip()
        status_filter = status.strip().lower() if status else "all"

        filters = {
            "account_name": account_name_filter,
            "account_number": account_number_filter,
            "ticker": ticker_filter,
            "status": status_filter,
        }

        return templates.TemplateResponse(
            request=request,
            name="legs.html",
            context={
                "title": "Matched Legs",
                "legs": legs_data,
                "warnings": warnings,
                "filters": filters,
            },
        )

    @app.get("/api/legs", tags=["api"])
    async def legs_api(
        account_name: str | None = Query(default=None),
        account_number: str | None = Query(default=None),
        ticker: str | None = Query(default=None),
        status: str = Query(default="all"),
        repository: SQLiteRepository = Depends(get_repository),
    ) -> dict[str, object]:
        """API endpoint returning matched legs as JSON."""
        legs_data, warnings = _fetch_matched_legs(
            repository,
            account_name=account_name,
            account_number=account_number,
            ticker=ticker,
            status=status,
        )
        return {"legs": legs_data, "warnings": warnings}

    @app.get("/api/stock-lots", tags=["api"])
    async def stock_lots_api(
        account_name: str | None = Query(default=None),
        account_number: str | None = Query(default=None),
        ticker: str | None = Query(default=None),
        status: str = Query(default="all"),
        repository: SQLiteRepository = Depends(get_repository),
    ) -> dict[str, object]:
        """API endpoint returning persisted stock lots as JSON."""

        status_filter = (status or "all").strip().lower()
        if status_filter not in {"all", "open", "closed"}:
            raise HTTPException(status_code=400, detail="Unsupported status filter")

        summaries = fetch_stock_lot_summaries(
            repository,
            account_name=(account_name or "").strip() or None,
            account_number=(account_number or "").strip() or None,
            ticker=(ticker or "").strip() or None,
            status=status_filter,  # type: ignore[arg-type]
        )
        lots = [serialize_stock_lot_summary(summary) for summary in summaries]
        return {"lots": lots}

    @app.get("/stock-lots", response_class=HTMLResponse, tags=["ui"])
    async def stock_lots_view(  # noqa: C901, PLR0913
        request: Request,
        account_name: str | None = Query(default=None),
        account_number: str | None = Query(default=None),
        ticker: str | None = Query(default=None),
        status: str = Query(default="all"),
        opened_from: str | None = Query(default=None),
        opened_until: str | None = Query(default=None),
        closed_from: str | None = Query(default=None),
        closed_until: str | None = Query(default=None),
        repository: SQLiteRepository = Depends(get_repository),
    ) -> HTMLResponse:
        """Render stock lot summaries in the web UI."""

        status_filter = (status or "all").strip().lower()
        if status_filter not in {"all", "open", "closed"}:
            status_filter = "all"

        opened_from_date = _parse_date_param(opened_from)
        opened_until_date = _parse_date_param(opened_until)
        closed_from_date = _parse_date_param(closed_from)
        closed_until_date = _parse_date_param(closed_until)

        error_message = None
        if opened_from and not opened_from_date:
            error_message = "Opened (from) must be YYYY-MM-DD."
        elif opened_until and not opened_until_date:
            error_message = "Opened (to) must be YYYY-MM-DD."
        elif opened_from_date and opened_until_date and opened_from_date > opened_until_date:
            error_message = "Opened start date must be before or equal to opened end date."
        elif closed_from and not closed_from_date:
            error_message = "Closed (from) must be YYYY-MM-DD."
        elif closed_until and not closed_until_date:
            error_message = "Closed (to) must be YYYY-MM-DD."
        elif closed_from_date and closed_until_date and closed_from_date > closed_until_date:
            error_message = "Closed start date must be before or equal to closed end date."

        summaries = fetch_stock_lot_summaries(
            repository,
            account_name=(account_name or "").strip() or None,
            account_number=(account_number or "").strip() or None,
            ticker=(ticker or "").strip() or None,
            status=status_filter,  # type: ignore[arg-type]
        )

        def _matches_date_filters(lot: StockLotSummary) -> bool:
            opened_dt = _parse_lot_date(lot.opened_at)
            closed_dt = _parse_lot_date(lot.closed_at)

            if opened_from_date and (opened_dt is None or opened_dt < opened_from_date):
                return False
            if opened_until_date and (opened_dt is None or opened_dt > opened_until_date):
                return False
            if closed_from_date:
                if not closed_dt or closed_dt < closed_from_date:
                    return False
            if closed_until_date:
                if not closed_dt or closed_dt > closed_until_date:
                    return False
            return True

        filtered_summaries = [summary for summary in summaries if _matches_date_filters(summary)]

        filtered_summaries.sort(
            key=lambda lot: (
                lot.account_name,
                lot.account_number or "",
                lot.symbol,
                lot.opened_at,
            )
        )

        total_basis = Decimal("0")
        total_realized = Decimal("0")
        total_shares = 0
        open_count = 0
        lots_payload: list[dict[str, object]] = []

        for summary in filtered_summaries:
            total_basis += summary.basis_total
            total_realized += summary.realized_pnl_total
            total_shares += abs(summary.quantity)
            if summary.status == "open":
                open_count += 1

            lots_payload.append(
                {
                    "symbol": summary.symbol,
                    "account_name": summary.account_name,
                    "account_number": summary.account_number,
                    "direction": summary.direction.upper(),
                    "status": summary.status.upper(),
                    "shares": abs(summary.quantity),
                    "quantity": summary.quantity,
                    "opened_raw": summary.opened_at,
                    "opened_at": _format_timestamp(summary.opened_at),
                    "closed_at": (
                        _format_timestamp(summary.closed_at) if summary.closed_at else None
                    ),
                    "basis_total": summary.basis_total,
                    "basis_per_share": summary.basis_per_share,
                    "realized_total": summary.realized_pnl_total,
                    "realized_per_share": summary.realized_pnl_per_share,
                    "share_price_total": summary.share_price_total,
                    "share_price_per_share": summary.share_price_per_share,
                    "assignment_kind": summary.assignment_kind,
                    "strike_price": summary.strike_price,
                    "option_type": summary.option_type,
                    "expiration": summary.expiration,
                }
            )

        summary_metrics = {
            "total_lots": len(filtered_summaries),
            "open_lots": open_count,
            "closed_lots": len(filtered_summaries) - open_count,
            "total_shares": total_shares,
            "aggregate_basis": total_basis,
            "aggregate_realized": total_realized,
        }

        filters = {
            "account_name": (account_name or "").strip(),
            "account_number": (account_number or "").strip(),
            "ticker": (ticker or "").strip(),
            "status": status_filter,
            "opened_from": opened_from or "",
            "opened_until": opened_until or "",
            "closed_from": closed_from or "",
            "closed_until": closed_until or "",
        }

        return templates.TemplateResponse(
            request=request,
            name="stock_lots.html",
            context={
                "title": "Stock Lots",
                "lots": lots_payload,
                "filters": filters,
                "summary": summary_metrics,
                "format_currency": format_currency,
                "error_message": error_message,
            },
        )

    @app.get("/cashflow", response_class=HTMLResponse, tags=["ui"])
    async def cashflow_view(  # noqa: PLR0913
        request: Request,
        account: str | None = Query(default=None),
        period: str = Query(default="total"),
        ticker: str | None = Query(default=None),
        since: str | None = Query(default=None),
        until: str | None = Query(default=None),
        assignment_handling: str = Query(default="include"),
        realized_view: str = Query(default="options"),
        repository: SQLiteRepository = Depends(get_repository),
    ) -> HTMLResponse:
        """Display cash flow and P&L dashboard view."""
        # Parse account selection
        account_name_filter, account_number_filter = _parse_account_selection(account)
        ticker_filter = (ticker or "").strip() or None
        period_type = period.strip().lower() if period else "total"
        if period_type not in ("daily", "weekly", "monthly", "total"):
            period_type = "total"
        assignment_mode = assignment_handling.strip().lower() if assignment_handling else "include"
        if assignment_mode not in ("include", "exclude"):
            assignment_mode = "include"
        realized_mode = realized_view.strip().lower() if realized_view else "options"
        if realized_mode not in REALIZED_VIEW_CHOICES:
            realized_mode = "options"
        realized_mode_label = REALIZED_VIEW_CHOICES[realized_mode]["label"]

        # Get unique accounts for dropdown
        accounts = _get_unique_accounts(repository)

        # Default to first account if no account is selected and accounts exist
        if not account_name_filter and accounts:
            first_account = accounts[0]
            account_name_filter = first_account["account_name"]
            account_number_filter = first_account["account_number"]

        # Build account value for form (format: "name|number" or just "name")
        selected_account = None
        if account_name_filter:
            if account_number_filter:
                selected_account = f"{account_name_filter}|{account_number_filter}"
            else:
                selected_account = account_name_filter

        filters = {
            "account": selected_account or "",
            "period": period_type,
            "ticker": ticker_filter or "",
            "since": since or "",
            "until": until or "",
            "assignment_handling": assignment_mode,
            "realized_view": realized_mode,
        }

        # Generate report if account is available
        report = None
        error_message = None
        if account_name_filter:
            # Parse dates
            since_date = _parse_date_param(since)
            until_date = _parse_date_param(until)

            # Validate date range
            if since_date and until_date and since_date > until_date:
                error_message = "Start date must be before or equal to end date"
            else:
                # Generate report
                report = generate_cash_flow_pnl_report(
                    repository,
                    account_name=account_name_filter,
                    account_number=account_number_filter or None,
                    period_type=period_type,  # type: ignore[arg-type]
                    ticker=ticker_filter,
                    since=since_date,
                    until=until_date,
                    assignment_handling=assignment_mode,  # type: ignore[arg-type]
                )

        return templates.TemplateResponse(
            request=request,
            name="cashflow.html",
            context={
                "title": "Cash Flow & P&L",
                "report": report,
                "accounts": accounts,
                "filters": filters,
                "format_currency": format_currency,
                "error_message": error_message,
                "realized_view_choices": REALIZED_VIEW_CHOICES,
                "realized_view_label": realized_mode_label,
            },
        )

    @app.get("/api/cashflow", tags=["api"])
    async def cashflow_api(  # noqa: PLR0913
        account: str | None = Query(default=None),
        period: str = Query(default="total"),
        ticker: str | None = Query(default=None),
        since: str | None = Query(default=None),
        until: str | None = Query(default=None),
        assignment_handling: str = Query(default="include"),
        repository: SQLiteRepository = Depends(get_repository),
    ) -> dict[str, object]:
        """API endpoint returning cash flow and P&L report as JSON."""
        # Parse account selection
        account_name_filter, account_number_filter = _parse_account_selection(account)
        ticker_filter = (ticker or "").strip() or None
        period_type = period.strip().lower() if period else "total"
        if period_type not in ("daily", "weekly", "monthly", "total"):
            period_type = "total"
        assignment_mode = assignment_handling.strip().lower() if assignment_handling else "include"
        if assignment_mode not in ("include", "exclude"):
            assignment_mode = "include"

        # Validate required fields
        if not account_name_filter:
            raise HTTPException(status_code=400, detail="account is required")

        # Parse dates
        since_date = _parse_date_param(since)
        until_date = _parse_date_param(until)

        # Validate date range
        if since_date and until_date and since_date > until_date:
            raise HTTPException(
                status_code=400, detail="Start date must be before or equal to end date"
            )

        # Generate report
        report = generate_cash_flow_pnl_report(
            repository,
            account_name=account_name_filter,
            account_number=account_number_filter or None,
            period_type=period_type,  # type: ignore[arg-type]
            ticker=ticker_filter,
            since=since_date,
            until=until_date,
            assignment_handling=assignment_mode,  # type: ignore[arg-type]
        )

        return serialize_cash_flow_pnl_report(report)

    return app
