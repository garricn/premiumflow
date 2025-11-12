"""Tests for cost basis CLI commands."""

from __future__ import annotations

from decimal import Decimal

from click.testing import CliRunner

from premiumflow.cli.cost_basis import cost_basis
from premiumflow.services.cost_basis import CostBasisError


def test_cost_basis_set_invokes_service(monkeypatch):
    runner = CliRunner()
    captured: dict[str, object] = {}

    class DummyRepository:
        pass

    monkeypatch.setattr(
        "premiumflow.cli.cost_basis.SQLiteRepository",
        lambda: DummyRepository(),
    )

    def _fake_resolve(repo, **kwargs):
        captured["repo"] = repo
        captured["kwargs"] = kwargs

        class Result:
            instrument = kwargs["instrument"].upper()
            activity_date = kwargs["activity_date"].isoformat()
            basis_total = Decimal("96321.00")
            basis_per_share = Decimal("166.9203")

        return Result()

    monkeypatch.setattr(
        "premiumflow.cli.cost_basis.resolve_transfer_basis_override",
        _fake_resolve,
    )

    result = runner.invoke(
        cost_basis,
        [
            "set",
            "--account-name",
            "Transfer Account",
            "--account-number",
            "XFER-123",
            "--ticker",
            "VTI",
            "--activity-date",
            "2025-09-05",
            "--shares",
            "577",
            "--basis-total",
            "96321.00",
        ],
    )

    assert result.exit_code == 0
    assert "Resolved cost basis" in result.output
    assert isinstance(captured["repo"], DummyRepository)
    assert captured["kwargs"]["shares"] == Decimal("577")


def test_cost_basis_set_requires_basis_value():
    runner = CliRunner()
    result = runner.invoke(
        cost_basis,
        [
            "set",
            "--account-name",
            "Transfer Account",
            "--account-number",
            "XFER-123",
            "--ticker",
            "VTI",
            "--activity-date",
            "2025-09-05",
            "--shares",
            "10",
        ],
    )

    assert result.exit_code != 0
    assert "Provide either --basis-total or --basis-per-share." in result.output


def test_cost_basis_set_reports_service_error(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(
        "premiumflow.cli.cost_basis.SQLiteRepository",
        lambda: object(),
    )

    def _raise(*args, **kwargs):
        raise CostBasisError("No matching entry.")

    monkeypatch.setattr(
        "premiumflow.cli.cost_basis.resolve_transfer_basis_override",
        _raise,
    )

    result = runner.invoke(
        cost_basis,
        [
            "set",
            "--account-name",
            "Transfer Account",
            "--account-number",
            "XFER-123",
            "--ticker",
            "VTI",
            "--activity-date",
            "2025-09-05",
            "--shares",
            "577",
            "--basis-total",
            "100.00",
        ],
    )

    assert result.exit_code != 0
    assert "No matching entry." in result.output
