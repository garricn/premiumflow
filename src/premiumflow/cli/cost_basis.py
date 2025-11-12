"""CLI for managing transfer cost basis overrides."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import click

from ..persistence import SQLiteRepository
from ..services.cost_basis import CostBasisError, resolve_transfer_basis_override
from ..services.external_tax_lots import (
    ExternalTaxLotImportError,
    import_external_tax_lot_snapshot,
)


def _parse_decimal(ctx: click.Context, value: str, label: str) -> Decimal:
    try:
        decimal_value = Decimal(value)
    except (InvalidOperation, TypeError):
        ctx.fail(f"{label} must be a valid decimal number.")
    return decimal_value


@click.group(name="cost-basis")
def cost_basis() -> None:
    """Manage cost basis overrides for transferred shares."""


@cost_basis.command("set")
@click.option("--account-name", required=True, help="Account name attached to the import.")
@click.option(
    "--account-number",
    help="Account identifier attached to the import (required when set during import).",
)
@click.option("--ticker", "instrument", required=True, help="Ticker symbol for the transfer.")
@click.option(
    "--activity-date",
    required=True,
    help="Activity date for the transfer row (YYYY-MM-DD).",
)
@click.option(
    "--shares",
    required=True,
    help="Share quantity for the transfer row.",
)
@click.option(
    "--basis-total",
    "basis_total_value",
    help="Total cost basis value to apply to the transfer.",
)
@click.option(
    "--basis-per-share",
    "basis_per_share_value",
    help="Per-share basis value to apply to the transfer.",
)
@click.option(
    "--trans-code",
    help="Optional transfer code to disambiguate when multiple entries match.",
)
@click.pass_context
def set_cost_basis(  # noqa: PLR0913
    ctx: click.Context,
    *,
    account_name: str,
    account_number: str | None,
    instrument: str,
    activity_date: str,
    shares: str,
    basis_total_value: str | None,
    basis_per_share_value: str | None,
    trans_code: str | None,
) -> None:
    """Set a manual cost basis override for a transferred position."""

    account_name = account_name.strip()
    account_number = account_number.strip() if account_number else None

    try:
        activity_dt = datetime.strptime(activity_date.strip(), "%Y-%m-%d").date()
    except ValueError as exc:
        ctx.fail(f"Invalid activity date: {exc}")
        return

    share_count = _parse_decimal(ctx, shares.strip(), "shares")
    basis_total = (
        _parse_decimal(ctx, basis_total_value.strip(), "basis_total")
        if basis_total_value is not None
        else None
    )
    basis_per_share = (
        _parse_decimal(ctx, basis_per_share_value.strip(), "basis_per_share")
        if basis_per_share_value is not None
        else None
    )

    if basis_total is None and basis_per_share is None:
        ctx.fail("Provide either --basis-total or --basis-per-share.")
        return

    repository = SQLiteRepository()

    try:
        resolved = resolve_transfer_basis_override(
            repository,
            account_name=account_name,
            account_number=account_number,
            instrument=instrument,
            activity_date=activity_dt,
            shares=share_count,
            basis_total=basis_total,
            basis_per_share=basis_per_share,
            trans_code=trans_code,
        )
    except CostBasisError as exc:
        ctx.fail(str(exc))
        return

    click.echo(
        f"Resolved cost basis for {resolved.instrument} on {resolved.activity_date}: "
        f"total {resolved.basis_total} / per-share {resolved.basis_per_share}"
    )


@cost_basis.command("import-lots")
@click.argument("pdf_path", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option("--account-name", required=True, help="Account name attached to the import.")
@click.option(
    "--account-number",
    help="Account identifier attached to the import (required when set during import).",
)
@click.option(
    "--snapshot-label",
    help="Optional label to identify this snapshot (defaults to the PDF file name).",
)
@click.pass_context
def import_lots(  # noqa: PLR0913
    ctx: click.Context,
    pdf_path: Path,
    *,
    account_name: str,
    account_number: str | None,
    snapshot_label: str | None,
) -> None:
    """Import an external tax-lot PDF snapshot and apply basis overrides."""

    repository = SQLiteRepository()
    account_name = account_name.strip()
    account_number = account_number.strip() if account_number else None

    try:
        result = import_external_tax_lot_snapshot(
            repository,
            pdf_path=pdf_path,
            account_name=account_name,
            account_number=account_number,
            snapshot_label=snapshot_label,
        )
    except ExternalTaxLotImportError as exc:
        ctx.fail(str(exc))
        return

    click.echo(
        f"Imported {result.stored_snapshot_lots} tax lots from '{pdf_path.name}' "
        f"(snapshot '{snapshot_label or pdf_path.stem}')."
    )
    click.echo(f"Resolved {result.resolved_transfer_items} transfer basis entries automatically.")

    if result.unresolved_transfer_items:
        click.echo("\nUnresolved transfer entries:")
        for item in result.unresolved_transfer_items:
            click.echo(
                f"  - {item.instrument} - {item.shares} shares on {item.activity_date} "
                f"(status {item.status})"
            )

    if result.ambiguous_transfer_items:
        click.echo("\nAmbiguous matches (manual intervention required):")
        for item, matches in result.ambiguous_transfer_items:
            click.echo(
                f"  - {item.instrument} - {item.shares} shares on {item.activity_date} "
                f"- found {len(matches)} external lots"
            )

    if result.resolution_errors:
        click.echo("\nErrors while applying basis overrides:")
        for item, message in result.resolution_errors:
            click.echo(
                f"  - {item.instrument} - {item.shares} shares on {item.activity_date}: {message}"
            )
