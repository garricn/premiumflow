"""FastAPI application factory for the PremiumFlow web UI."""

from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timezone
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
from .dependencies import get_repository

TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
STATIC_DIR = Path(__file__).resolve().parent / "static"

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

DuplicateStrategy = Literal["error", "skip", "replace"]

DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 100
MIN_PAGE_SIZE = 5


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


def create_app() -> FastAPI:
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
    async def upload(
        request: Request,
        csv_file: UploadFile = File(...),
        account_name: str = Form(...),
        account_number: str = Form(""),
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
            normalized_account_number = account_number.strip() or None

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
                row_count = len(parsed.transactions)
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
    async def imports_history(
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
    async def delete_import(
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
                "activity_start": activity_start,
                "activity_end": activity_end,
                "default_page_size": DEFAULT_PAGE_SIZE,
            },
        )

    return app
